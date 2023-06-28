"""
vortex_training_data_graphs.py
"""
import os
import logging
import traceback
import json
import gzip
from ast import literal_eval

from functions.plots.plot_timeseries import plot_timeseries
from algorithm_scores_plot import get_algorithm_scores_plot


# @added 20221202 - Feature #4732: flux vortex
#                   Feature #4734: mirage_vortex
def get_vortex_training_data_graphs(current_skyline_app, metric_data_archive, undownsampled_archive):
    """
    Plots vortex graphs

    :param current_skyline_app: the app calling the function
    :param metric_data_archive: the metric_data_archive, path and file
    :param undownsampled_archive: the undownsampled_archive, path and file
    :type current_skyline_app: str
    :type metric_data_archive: str
    :type undownsampled_archive: str
    :return: do_not_alert_on_stale_metrics_list
    :rtype: list

    """
    vortex_graphs = []

    # Open the vortex metric_data_archive
    function_str = 'functions.plot.vortex_training_data_graphs'
    current_skyline_app_logger = current_skyline_app + 'Log'
    current_logger = logging.getLogger(current_skyline_app_logger)

    if not os.path.isfile(metric_data_archive):
        current_logger.error('error :: %s :: not found metric_data_archive: %s' % (
            function_str, str(metric_data_archive)))
        return vortex_graphs

    current_logger.info('%s :: loading vortex metric_data archive - %s' % (
        function_str, metric_data_archive))

    metric_data_dir = os.path.dirname(metric_data_archive)
    file_content = None
    try:
        if os.path.isfile(metric_data_archive):
            with gzip.open(metric_data_archive, 'rb') as f:
                file_content = f.read()
    except Exception as err:
        current_logger.error(traceback.format_exc())
        current_logger.error('error :: %s :: failed to gzip decompress %s - %s' % (
            function_str, metric_data_archive, err))
    vortex_metric_data = {}
    if file_content:
        try:
            vortex_metric_data = json.loads(file_content)
        except Exception as err:
            current_logger.error(traceback.format_exc())
            current_logger.error('error :: %s :: failed to json loads vortex metric_data - %s' % (
                function_str, err))
    if not vortex_metric_data:
        current_logger.error('error :: %s :: no vortex_metric_data found from metric_data_archive: %s' % (
            function_str, str(metric_data_archive)))
        return vortex_graphs

    try:
        use_base_name = vortex_metric_data['labelled_metric_name']
    except Exception as err:
        current_logger.error('error :: %s :: labelled_metric_name not found in vortex_metric_data - %s' % (
            function_str, err))
        return vortex_graphs

    try:
        resolution = vortex_metric_data['resolution']
    except:
        resolution = 'unknown'

    undownsampled_graph_image_file = '%s/vortex.undownsampled.%s.png' % (metric_data_dir, use_base_name)
    if not os.path.isfile(undownsampled_graph_image_file):
        undownsampled_timeseries = None
        if undownsampled_archive:
            file_content = None
            try:
                if os.path.isfile(undownsampled_archive):
                    with gzip.open(undownsampled_archive, 'rb') as f:
                        file_content = f.read()
            except Exception as err:
                current_logger.error(traceback.format_exc())
                current_logger.error('error :: %s :: failed to gzip decompress %s - %s' % (
                    function_str, undownsampled_archive, err))
            if file_content:
                try:
                    undownsampled_timeseries_json = json.loads(file_content)
                except Exception as err:
                    current_logger.error('error :: %s :: failed to json loads undownsampled_archive content - %s' % (
                        function_str, err))
                try:
                    undownsampled_timeseries = undownsampled_timeseries_json['timeseries']
                    current_logger.info('%s :: loaded undownsampled timeseries from %s' % (function_str, undownsampled_archive))
                except Exception as err:
                    current_logger.error('error :: %s :: failed to load undownsampled timeseries from %s' % (
                        function_str, undownsampled_archive))
        if undownsampled_timeseries:
            try:
                downsampled_resolution = vortex_metric_data['downsampled_resolution']
            except:
                downsampled_resolution = 'unknown'
            if not os.path.isfile(undownsampled_graph_image_file):
                graph_image_file = undownsampled_graph_image_file
                title = '%s\nOriginal undownsampled data (data points: %s, resolution: %s seconds)' % (
                    use_base_name, str(len(undownsampled_timeseries)), str(downsampled_resolution))
                plot_parameters = {
                    'title': title, 'line_color': 'blue', 'bg_color': 'black',
                    'figsize': (8, 4)
                }
                try:
                    success, graph_image = plot_timeseries(
                        current_skyline_app, use_base_name, undownsampled_timeseries, graph_image_file,
                        plot_parameters)
                    if success:
                        current_logger.info('%s :: plotted %s' % (function_str, str(graph_image)))
                        vortex_graphs.append(graph_image_file)
                except Exception as err:
                    current_logger.error('error :: %s :: plot_timeseries failed - %s' % (
                        function_str, err))
            else:
                vortex_graphs.append(graph_image_file)
    else:
        vortex_graphs.append(undownsampled_graph_image_file)

    timeseries = []
    anomaly_json = '%s/%s.json' % (metric_data_dir, use_base_name)
    if os.path.isfile(anomaly_json):
        try:
            with open((anomaly_json), 'r') as f:
                raw_timeseries = f.read()
            timeseries_array_str = str(raw_timeseries).replace('(', '[').replace(')', ']')
            del raw_timeseries
            timeseries = literal_eval(timeseries_array_str)
        except Exception as err:
            current_logger.error('error :: %s :: no timeseries in %s - %s' % (
                function_str, anomaly_json, err))

    if not timeseries:
        current_logger.error('error :: %s :: no timeseries data to plot' % (
            function_str))
        return vortex_graphs

    # Coerce timestamps to ints
    timeseries = [[int(t), v] for t, v in timeseries]

    try:
        full_duration_in_hours = int(vortex_metric_data['nearest_full_duration'] / 3600)
    except:
        full_duration_in_hours = 'unknown'

    graph_image_file = '%s/vortex.analysed.%s.%sh.png' % (
        metric_data_dir, use_base_name, str(full_duration_in_hours))
    if not os.path.isfile(graph_image_file):
        if vortex_metric_data['downsampled']:
            title = '%s\nanalysed downsampled data (data points: %s, resolution: %s seconds)' % (
                use_base_name, str(len(timeseries)), str(resolution))
        else:
            title = '%s\nanalysed data' % (use_base_name)

        plot_parameters = {
            'title': title, 'line_color': 'blue', 'bg_color': 'black',
            'figsize': (8, 4), 'line_width': 0.5,
        }
        try:
            success, graph_image = plot_timeseries(
                current_skyline_app, use_base_name, timeseries, graph_image_file,
                plot_parameters)
            if success:
                current_logger.info('%s :: plotted %s' % (function_str, str(graph_image)))
                vortex_graphs.append(graph_image_file)
        except Exception as err:
            current_logger.error('error :: %s :: plot_timeseries failed - %s' % (
                function_str, err))
    else:
        vortex_graphs.append(graph_image_file)

    # @added 20230616 - Feature #4952: vortex - consensus_count
    consensus_results = {}
    try:
        consensus_results = vortex_metric_data['results']['consensus_results']
    except:
        consensus_results = {}
    if consensus_results:
        try:
            algorithm = 'consensus'
            anomalous = vortex_metric_data['results']['consensus_results']['anomalous']
            anomaly_window = vortex_metric_data['results']['consensus_results']['anomaly_window']
            anomalies_in_window = vortex_metric_data['results']['consensus_results']['anomalies_in_window']
            unreliable = vortex_metric_data['results']['consensus_results']['unreliable']
            anomalies = vortex_metric_data['results']['consensus_results']['anomalies']
            scores = vortex_metric_data['results']['consensus_results']['scores']
            try:
                graph_image_file = '%s/vortex.algorithm.%s.%s.%sh.png' % (metric_data_dir, algorithm, use_base_name, str(full_duration_in_hours))
            except Exception as err:
                current_logger.error('error :: %s :: could not interpolate graph_image_file - %s' % (
                    function_str, err))
                graph_image_file = None
            if graph_image_file:
                if not os.path.isfile(graph_image_file):
                    image_created = None
                    current_logger.info('%s :: plotting %s' % (function_str, graph_image_file))
                    try:
                        image_created = get_algorithm_scores_plot(
                            current_skyline_app, graph_image_file, timeseries, algorithm,
                            anomalous, anomalies, scores, anomaly_window, anomalies_in_window,
                            unreliable)
                    except Exception as err:
                        current_logger.error('error :: %s :: get_algorithm_scores_plot failed for %s - %s' % (
                            function_str, algorithm, err))
                    if image_created:
                        vortex_graphs.append(graph_image_file)
                else:
                    vortex_graphs.append(graph_image_file)
        except Exception as err:
            current_logger.error('error :: %s :: plotting consensus_results failed - %s' % (
                function_str, err))

    # @added 20230616 - Feature #4952: vortex - consensus_count
    consensus_count_results = {}
    try:
        consensus_count_results = vortex_metric_data['results']['consensus_count_results']
    except:
        consensus_count_results = {}
    if consensus_count_results:
        try:
            algorithm = 'consensus_count'
            anomalous = vortex_metric_data['results']['consensus_count_results']['anomalous']
            anomaly_window = vortex_metric_data['results']['consensus_count_results']['anomaly_window']
            anomalies_in_window = vortex_metric_data['results']['consensus_count_results']['anomalies_in_window']
            unreliable = vortex_metric_data['results']['consensus_count_results']['unreliable']
            anomalies = vortex_metric_data['results']['consensus_count_results']['anomalies']
            scores = vortex_metric_data['results']['consensus_count_results']['scores']
            try:
                graph_image_file = '%s/vortex.algorithm.%s.%s.%sh.png' % (metric_data_dir, algorithm, use_base_name, str(full_duration_in_hours))
            except Exception as err:
                current_logger.error('error :: %s :: could not interpolate graph_image_file - %s' % (
                    function_str, err))
                graph_image_file = None
            if graph_image_file:
                if not os.path.isfile(graph_image_file):
                    image_created = None
                    current_logger.info('%s :: plotting %s' % (function_str, graph_image_file))
                    try:
                        image_created = get_algorithm_scores_plot(
                            current_skyline_app, graph_image_file, timeseries, algorithm,
                            anomalous, anomalies, scores, anomaly_window, anomalies_in_window,
                            unreliable)
                    except Exception as err:
                        current_logger.error('error :: %s :: get_algorithm_scores_plot failed for %s - %s' % (
                            function_str, algorithm, err))
                    if image_created:
                        vortex_graphs.append(graph_image_file)
                else:
                    vortex_graphs.append(graph_image_file)
        except Exception as err:
            current_logger.error('error :: %s :: plotting consensus_count failed - %s' % (
                function_str, err))

    algorithms_run = []
    try:
        algorithms_run = list(vortex_metric_data['results']['algorithms_run'])
    except:
        algorithms_run = []

    for algorithm in algorithms_run:
        try:
            anomalous = vortex_metric_data['results']['triggered_algorithms'][algorithm]
        except Exception as err:
            current_logger.error('error :: %s :: %s not found in vortex_metric_data[\'results\'][\'triggered_algorithms\'] - %s' % (
                function_str, algorithm, err))
            anomalous = False
        try:
            anomalies = vortex_metric_data['results']['algorithms'][algorithm]['anomalies']
        except KeyError:
            anomalies = {}
        except Exception as err:
            current_logger.error('error :: %s :: anomalies not found in vortex_metric_data[\'results\'][\'algorithms\'][\'%s\'][\'anomalies\'] - %s' % (
                function_str, algorithm, err))
            anomalies = {}
        current_logger.info('%s :: %s - len(anomalies): %s' % (function_str, algorithm, str(len(anomalies))))
        try:
            scores = vortex_metric_data['results']['algorithms'][algorithm]['scores']
        except KeyError:
            scores = []
        except Exception as err:
            current_logger.error('error :: %s :: scores not found in vortex_metric_data[\'results\'][\'algorithms\'][\'%s\'][\'scores\'] - %s' % (
                function_str, algorithm, err))
            scores = []

        try:
            anomaly_window = vortex_metric_data['algorithms'][algorithm]['algorithm_parameters']['anomaly_window']
        except KeyError:
            anomaly_window = 1
        except Exception as err:
            current_logger.error('error :: %s :: scores not found in vortex_metric_data[\'algorithms\'][\'%s\'][\'algorithm_parameters\'][\'anomaly_window\'] - %s' % (
                function_str, algorithm, err))
            anomaly_window = 1

        try:
            anomalies_in_window = vortex_metric_data['results']['algorithms'][algorithm]['anomalies_in_window']
        except KeyError:
            anomalies_in_window = None
        except Exception as err:
            current_logger.error('error :: %s :: scores not found in vortex_metric_data[\'results\'][\'algorithms\'][\'%s\'][\'anomalies_in_window\'] - %s' % (
                function_str, algorithm, err))
            anomalies_in_window = None

        try:
            unreliable = vortex_metric_data['results']['algorithms'][algorithm]['unreliable']
        except KeyError:
            unreliable = False
        except Exception as err:
            current_logger.error('error :: %s :: unreliable not found in vortex_metric_data[\'results\'][\'algorithms\'][\'%s\'][\'unreliable\'] - %s' % (
                function_str, algorithm, err))
            unreliable = False

        if algorithm == 'sigma' and anomalies:
            if not scores:
                current_logger.info('%s :: creating %s scores' % (function_str, algorithm))
                scores = []
                for item in timeseries:
                    score = 0
                    ts_str = str(int(item[0]))
                    try:
                        score = anomalies[ts_str]['anomalyScore']
                    except:
                        score = 0
                    scores.append(score)
        current_logger.info('%s :: %s - len(scores): %s' % (function_str, algorithm, str(len(scores))))

        try:
            graph_image_file = '%s/vortex.algorithm.%s.%s.%sh.png' % (metric_data_dir, algorithm, use_base_name, str(full_duration_in_hours))
        except Exception as err:
            current_logger.error('error :: %s :: could not interpolate graph_image_file - %s' % (
                function_str, err))
            graph_image_file = None

        # @added 20230615 - Feature #4950: custom_algorithm - spectral_entropy
        low_entropy_value = None
        if algorithm == 'spectral_entropy':
            try:
                low_entropy_value = vortex_metric_data['results']['algorithms'][algorithm]['low_entropy_value']
            except Exception as err:
                current_logger.error('error :: %s :: could not determine low_entropy_value for spectral_entropy - %s' % (
                    function_str, err))

        if graph_image_file:
            if not os.path.isfile(graph_image_file):
                image_created = None
                current_logger.info('%s :: plotting %s' % (function_str, graph_image_file))
                try:
                    image_created = get_algorithm_scores_plot(
                        current_skyline_app, graph_image_file, timeseries, algorithm,
                        anomalous, anomalies, scores, anomaly_window, anomalies_in_window,
                        unreliable,
                        # @added 20230615 - Feature #4950: custom_algorithm - spectral_entropy
                        low_entropy_value)
                except Exception as err:
                    current_logger.error('error :: %s :: get_algorithm_scores_plot failed for %s - %s' % (
                        function_str, algorithm, err))
                if image_created:
                    vortex_graphs.append(graph_image_file)
            else:
                vortex_graphs.append(graph_image_file)

    return vortex_graphs
