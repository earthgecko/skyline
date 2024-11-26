"""
snab_training_data_graphs.py
"""
import os
import logging
import traceback
import json
import gzip
from ast import literal_eval

from settings import IONOSPHERE_DATA_FOLDER, IONOSPHERE_PROFILES_FOLDER
from functions.metrics.get_metric_id_from_base_name import get_metric_id_from_base_name
from functions.plots.plot_timeseries import plot_timeseries
from functions.timeseries.determine_data_frequency import determine_data_frequency
from algorithm_scores_plot import get_algorithm_scores_plot


# @added 20230707 - Feature #4988: Allow snab to return and save results
# @modified 20231224 - Feature #5190: Add custom_algorithm results to Mirage and plots
#                      Feature #3566: custom_algorithms
# Added context to allow this same function to plot custom_algorithm graphs
def get_snab_training_data_graphs(current_skyline_app, results, context='snab'):
    """
    Plots SNAB (or custom_algorithms) graphs

    :param current_skyline_app: the app calling the function
    :param results: the results dict
    :param context: the context
    :type current_skyline_app: str
    :type results: dict
    :type context: str
    :return: snab_graphs
    :rtype: list

    """
    snab_graphs = []

    # Open the snab
    function_str = 'functions.plot.snab_training_data_graphs'
    current_skyline_app_logger = current_skyline_app + 'Log'
    current_logger = logging.getLogger(current_skyline_app_logger)

    metric = None
    try:
        metric = results['metric']
    except Exception as err:
        current_logger.error('error :: %s :: failed to determine metric from results - %s' % (
            function_str, err))
        return snab_graphs

    # @added 20231224 - Feature #5190: Add custom_algorithm results to Mirage and plots
    #                      Feature #3566: custom_algorithms
    results_path = None
    if context == 'custom_algorithms':
        try:
            results_path = results['results_path']
        except Exception as err:
            current_logger.error('error :: %s :: failed to determine results_path from results - %s' % (
                function_str, err))
            return snab_graphs

    # @modified 20231224 - Feature #5190: Add custom_algorithm results to Mirage and plots
    #                      Feature #3566: custom_algorithms
    if context == 'snab':
        try:
            results_path = results['snab_results_path']
        except Exception as err:
            current_logger.error('error :: %s :: failed to determine results_path from results - %s' % (
                function_str, err))
            return snab_graphs

    # @added 20240128 - Task #5178: Build and test skyline v4.1.0
    # Handle when a features profile is requested with a match and there are no
    # snab results graphs AND the results_path points to the original training
    # data dir.  This is an issue when the training has been saved as a features
    # profile and the data has been moved from the training data path to the
    # features_profile data path.
    current_logger.info('%s :: results_path set to %s' % (
        function_str, str(results_path)))
    if not os.path.isdir(results_path):

        # @added 20240203 - Task #5178: Build and test skyline v4.1.0
        # Handled labelled_metric
        use_metric = str(metric)
        labelled_metric = None
        try:
            labelled_metric = results['labelled_metric']
        except Exception as err:
            current_logger.error('error :: %s :: failed to determine labelled_metric from results - %s' % (
                function_str, err))
        if labelled_metric:
            use_metric = str(labelled_metric)

        replace_str = '%s/' % IONOSPHERE_DATA_FOLDER
        results_path_timestamp_1 = results_path.replace(replace_str, '')
        results_path_timestamp_2 = results_path_timestamp_1.split('/')
        results_path_timestamp = results_path_timestamp_2[0]
        # @modified 20240203 - Task #5178: Build and test skyline v4.1.0
        # metric_timeseries_dir = metric.replace('.', '/')
        metric_timeseries_dir = use_metric.replace('.', '/')

        features_profile_dir = '%s/%s/%s' % (
            IONOSPHERE_PROFILES_FOLDER, metric_timeseries_dir,
            str(results_path_timestamp))
        current_logger.info('%s :: features_profile_dir: %s' % (
            function_str, str(features_profile_dir)))

        if os.path.isdir(features_profile_dir):
            results_path = features_profile_dir
            # @added 20240203 - Task #5178: Build and test skyline v4.1.0
            current_logger.info('%s :: results_path now set to %s' % (
                function_str, str(results_path)))

    timeseries = []
    try:
        timeseries = results['timeseries']
    except:
        timeseries = []

    snab_timeseries_json = None
    if not timeseries:
        try:
            snab_timeseries_json = results['timeseries_json']
        except Exception as err:
            current_logger.error('error :: %s :: failed to determine timeseries_json from results - %s' % (
                function_str, err))
            return snab_graphs
    if snab_timeseries_json:
        try:
            with open((snab_timeseries_json), 'r') as f:
                raw_timeseries = f.read()
            timeseries_array_str = str(raw_timeseries).replace('(', '[').replace(')', ']')
            del raw_timeseries
            timeseries = literal_eval(timeseries_array_str)
        except Exception as err:
            current_logger.error(traceback.format_exc())
            current_logger.error('error :: %s :: failed to load timeseries data from %s - %s' % (
                function_str, snab_timeseries_json, err))

    # @added 20231224 - Feature #5190: Add custom_algorithm results to Mirage and plots
    #                      Feature #3566: custom_algorithms
    if context == 'custom_algorithms':
        if 'downsampled_timeseries' in results:
            current_logger.info('%s :: using downsampled_timeseries')
            try:
                timeseries = results['downsampled_timeseries']
            except:
                pass

    if not timeseries:
        current_logger.error('error :: %s :: failed to determine timeseries or timeseries_json from results' % (
            function_str))
        return snab_graphs

    try:
        resolution = determine_data_frequency(current_skyline_app, timeseries, log=False)
    except Exception as err:
        current_logger.error('error :: %s :: determine_data_frequency failed - %s' % (
            function_str, err))
        return snab_graphs

    use_metric = str(metric)
    if '_tenant_id="' in str(metric):
        metric_id = 0
        try:
            metric_id = get_metric_id_from_base_name(current_skyline_app, metric)
        except Exception as err:
            current_logger.error('error :: get_metric_id_from_base_name failed with base_name: %s - %s' % (str(metric), err))
        if metric_id:
            use_metric = 'labelled_metrics.%s' % str(metric_id)

    aligned_timeseries = [[int((int(t) // resolution * resolution)), v] for t, v in timeseries]

    algorithms = []
    if 'algorithms' in list(results.keys()):
        algorithms = list(results['algorithms'].keys())

    for algorithm in algorithms:
        # @modified 20231224 - Feature #5190: Add custom_algorithm results to Mirage and plots
        #                      Feature #3566: custom_algorithms
        # graph_image_file = '%s/snab.%s.%s.png' % (results_path, algorithm, use_metric)
        graph_image_file = '%s/%s.%s.%s.png' % (results_path, context, algorithm, use_metric)

        if os.path.isfile(graph_image_file):
            snab_graphs.append(graph_image_file)
            continue

        current_logger.info('%s :: processing %s' % (function_str, algorithm))
        if algorithm == 'irregular_unstable':
            try:
                if 'anomalous' in results['algorithms'][algorithm]:
                    if results['algorithms'][algorithm]['anomalous']:
                        if 'reason' in results['algorithms'][algorithm]:
                            if results['algorithms'][algorithm]['reason'] == 'not low variance data':
                                current_logger.info('%s :: skipping %s as not low variance data' % (function_str, algorithm))
                                continue
            except Exception as err:
                current_logger.error(traceback.format_exc())
                current_logger.error('error :: %s :: failed to determine something from %s results - %s' % (
                    function_str, algorithm, err))
                
        try:
            anomalous = results['algorithms'][algorithm]['anomalous']
            anomalies = results['algorithms'][algorithm]['anomalies']

            if len(anomalies) == 0:
                current_logger.info('%s :: skipping %s as no anomalies' % (function_str, algorithm))
                continue

            aligned_anomalies = {}
            for ts in list(anomalies.keys()):
                aligned_ts = int(int(float(ts)) // resolution * resolution)
                aligned_anomalies[str(aligned_ts)] = anomalies[ts]
            anomalies = aligned_anomalies

            # Handle sigma algorithm not returning scores
            try:
                scores = results['algorithms'][algorithm]['anomalyScore_list']
            except:
                scores = results['algorithms'][algorithm]['scores']
        except Exception as err:
            current_logger.error(traceback.format_exc())
            current_logger.error('error :: %s :: failed to determine anomaly related data from results - %s' % (
                function_str, err))
            continue

        anomaly_window = 1
        try:
            anomaly_window = results['algorithms'][algorithm]['algorithm_parameters_used']['anomaly_window']
        except KeyError:
            try:
                anomaly_window = results['algorithms'][algorithm]['algorithm_parameters']['anomaly_window']
            except:
                try:
                    anomaly_window = results['algorithms'][algorithm]['anomaly_window']
                except:
                    anomaly_window = 1
        except:
            anomaly_window = 1

        anomalies_in_window = 0
        try:
            anomalies_in_window = results['algorithms'][algorithm]['anomalies_in_window']
        except:
            try:
                anomalies_in_window = sum(scores[-anomaly_window:])
                if algorithm == 'spectral_residual' and anomaly_window == 1:
                    anomalies_in_window = sum(scores[-3:])
            except:
                if anomalous:
                    anomalies_in_window = 1

        unreliable = False
        try:
            unreliable = results['algorithms'][algorithm]['unreliable']
        except:
            unreliable = False        

        image_created = None
        current_logger.info('%s :: plotting %s' % (function_str, graph_image_file))
        try:
            image_created = get_algorithm_scores_plot(
                current_skyline_app, graph_image_file, aligned_timeseries, algorithm,
                anomalous, anomalies, scores, anomaly_window, anomalies_in_window,
                unreliable)
        except Exception as err:
            current_logger.error('error :: %s :: get_algorithm_scores_plot failed for %s - %s' % (
                function_str, algorithm, err))
        if image_created:
            snab_graphs.append(graph_image_file)

    consensus_results = {}
    try:
        consensus_results = results['consensus_results']
    except:
        consensus_results = {}
    if consensus_results:
        try:
            algorithm = 'consensus'
            anomalous = results['consensus_results']['anomalous']
            anomaly_window = results['consensus_results']['anomaly_window']
            anomalies_in_window = results['consensus_results']['anomalies_in_window']
            unreliable = False
            anomalies = results['consensus_results']['anomalies']
            aligned_anomalies = {}
            for ts in list(anomalies.keys()):
                aligned_ts = int(int(float(ts)) // resolution * resolution)
                aligned_anomalies[str(aligned_ts)] = anomalies[ts]
            anomalies = aligned_anomalies
            scores = results['consensus_results']['scores']
            # @modified 20231224 - Feature #5190: Add custom_algorithm results to Mirage and plots
            #                      Feature #3566: custom_algorithms
            # graph_image_file = '%s/snab.%s.%s.png' % (results_path, algorithm, use_metric)
            graph_image_file = '%s/%s.%s.%s.png' % (results_path, context, algorithm, use_metric)
            if os.path.isfile(graph_image_file):
                snab_graphs.append(graph_image_file)
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
                        snab_graphs.append(graph_image_file)
        except Exception as err:
            current_logger.error('error :: %s :: plotting consensus_results failed - %s' % (
                function_str, err))

    return snab_graphs
