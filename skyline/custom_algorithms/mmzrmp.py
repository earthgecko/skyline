"""
mmzrmp.py
"""
# REQUIRED Skyline imports.  All custom algorithms MUST have the following two
# imports.  These are required for exception handling and to record algorithm
# errors regardless of debug_logging setting for the custom_algorithm
import logging
import traceback
from custom_algorithms import record_algorithm_error

# Import ALL modules that the custom algorithm requires.  Remember that if a
# requirement is not one that is provided by the Skyline requirements.txt you
# must ensure it is installed in the Skyline virtualenv
import copy
from os import getpid
from timeit import default_timer as timer
from time import time

import bottleneck as bn
import numpy as np
import ruptures as rpt

from custom_algorithms.m66 import m66
from custom_algorithms.macd import macd
from custom_algorithms.moving_sum_and_value_decrease import moving_sum_and_value_decrease
from functions.timeseries.downsample import downsample_timeseries
from functions.timeseries_predictions.fft_extrapolation import fft_extrapolation


# The name of the function MUST be the same as the name declared in
# settings.CUSTOM_ALGORITHMS.
# It MUST have 3 parameters:
# current_skyline_app, timeseries, algorithm_parameters
# See https://earthgecko-skyline.readthedocs.io/en/latest/algorithms/custom-algorithms.html
# for a full explanation about each.
# ALWAYS WRAP YOUR ALGORITHM IN try and except


# @added 20230920 - Feature #5064: custom_algorithm - inflection
#                   Feature #5551: custom_algorithm - mmzrmp
def mmzrmp(current_skyline_app, parent_pid, timeseries, algorithm_parameters):

    """
    EXPERIMENTAL

    mmzrmp is an ensemble based changepoint detection algorithm that identifies
    significant **sustained** changes in time series data.

    The TCPDBench type of naming convention is used here - mmzrmp, which denotes
    to each algorithm in the ensemble, namely:

    - m66
    - macd
    - zscore
    - ruptures_pelt
    - matrixprofile
    - moving_sum_and_value_decrease

    Requires the pip install of ``ruptures==1.1.9``
    skyline/requirements-torch.cpu.txt in your Skyline virtualenv.

    :param current_skyline_app: the Skyline app executing the algorithm.  This
        will be passed to the algorithm by Skyline.  This is **required** for
        error handling and logging.  You do not have to worry about handling the
        argument in the scope of the custom algorithm itself,  but the algorithm
        must accept it as the first agrument.
    :param parent_pid: the parent pid which is executing the algorithm, this is
        **required** for error handling and logging.  You do not have to worry
        about handling this argument in the scope of algorithm, but the
        algorithm must accept it as the second argument.
    :param timeseries: the time series as a list
         e.g. ``[[1694632860, 8.5], [1694633460, 5.8], ...,
          [1695237660, 0.4], [1695238260, 3.3]]``
    :param algorithm_parameters: a dictionary of any required parameters for the
        custom_algorithm and algorithm itself.  For the mmzrmp custom algorithm
        requires an algorithms_parameters dict to be pass that specifies the
        anomaly_window.  mmzrmp being an opinionated method has required
        parameters hard coded, however other parameters that can be passed are:

        - ``'anomaly_window'`` (int): The anomaly_window value. [REQUIRED]
            This specifies how many of the last data points should be considered
            when determining if the metric is anomalous. Only the last
            ``anomaly_window`` data points in the time series will be used to
            determine if the metric is anomalous.  Default is ``1``.
         - ``'return_results'`` (bool): Optional.
            If ``True``, returns the results dict in addition to anomalous and
            anomalyScore.  Default is ``False``.
        - ``'debug_logging'`` (bool): Optional.
            If ``True``, enables debug logging.
        - ``'debug_print'`` (bool): Optional.
            If ``True``, enables debug printing  (for Jupyter testing). Default
            is ``False``.

        Example usage:
        
            algorithm_parameters={
                'anomaly_window': 1,
                'debug_logging': True,
                'return_results': True,
            }

    :type current_skyline_app: str
    :type parent_pid: int
    :type timeseries: list
    :type algorithm_parameters: dict
    :return: anomalous, anomalyScore, results
    :rtype: tuple(boolean, float, dict)

    """

    # You MUST define the algorithm_name
    algorithm_name = 'mmzrmp'

    # Define the default state of None and None, anomalous does not default to
    # False as that is not correct, False is only correct if the algorithm
    # determines the data point is not anomalous.  The same is true for the
    # anomalyScore.
    anomalous = None
    anomalyScore = None
    anomalies = {}
    anomalyScore_list = []
    scores = []
    results = {'anomalous': anomalous, 'anomalies': anomalies, 'anomalyScore_list': anomalyScore_list, 'scores': scores}

    timings = {'algorithms': {}}

    current_logger = None

    # If you wanted to log, you can but this should only be done during
    # testing and development
    def get_log(current_skyline_app):
        current_skyline_app_logger = current_skyline_app + 'Log'
        current_logger = logging.getLogger(current_skyline_app_logger)
        return current_logger

    start = time()

    return_results = False
    try:
        return_results = algorithm_parameters['return_results']
    except:
        return_results = False

    if not return_results:
        try:
            return_results = algorithm_parameters['return_anomalies']
        except:
            return_results = False

    # Use the algorithm_parameters to determine the sample_period
    debug_logging = None
    try:
        debug_logging = algorithm_parameters['debug_logging']
    except:
        debug_logging = False
    if debug_logging:
        try:
            current_logger = get_log(current_skyline_app)
            current_logger.debug('debug :: %s :: debug_logging enabled with algorithm_parameters - %s' % (
                algorithm_name, str(algorithm_parameters)))
        except:
            # This except pattern MUST be used in ALL custom algortihms to
            # facilitate the traceback from any errors.  The algorithm we want to
            # run super fast and without spamming the log with lots of errors.
            # But we do not want the function returning and not reporting
            # anything to the log, so the pythonic except is used to "sample" any
            # algorithm errors to a tmp file and report once per run rather than
            # spewing tons of errors into the log e.g. analyzer.log
            record_algorithm_error(current_skyline_app, parent_pid, algorithm_name, traceback.format_exc())
            # Return None and None as the algorithm could not determine True or False
            if return_results:
                return (None, None, None)
            return (None, None)

    # Use the algorithm_parameters to determine variables
    debug_print = None
    try:
        debug_print = algorithm_parameters['debug_print']
    except:
        debug_print = False

    base_name = None
    try:
        if 'metric' in algorithm_parameters:
            metric = algorithm_parameters['metric']
            base_name = algorithm_parameters['metric']
        if 'base_name' in algorithm_parameters:
            base_name = algorithm_parameters['base_name']
    except:
        base_name = None

    anomaly_window = 15
    try:
        anomaly_window = int(algorithm_parameters['anomaly_window'])
    except:
        anomaly_window = 15
    results['anomaly_window'] = anomaly_window

    if debug_print:
        print('running mmzrmp with anomaly_window: %s' % (
            str(anomaly_window)))
    if debug_logging:
        current_logger.debug('debug :: running mmzrmp with anomaly_window: %s' % (
            str(anomaly_window)))

    # This is a retrospective addition based on an in-situ deployed version
    # developed and tested through 2023-11 to 2024-06, under the moniker of
    # custom_algorithm - inflection.  The original implementation of mmzrmp
    # started out using matrixprofile in the ensemble but due to run time and
    # the fact that it proved to not add value in this use case, it was replaced
    # with the simpler moving_sum_and_value_decrease which proved  to be fast
    # and produces useful results in terms of identifying significant changes in
    # terms of the ensemble.

    def moving_timeseries(timeseries, resolution, method, seconds):
        """
        Calculate the moving mean or median timeseries.
        """
        if len(timeseries) < 30:
            return timeseries
        rolling_timeseries = list(timeseries)
        try:
            if seconds < resolution:
                return timeseries
            window = int(seconds / resolution)
            x_np = np.asarray([x[1] for x in timeseries])
            if method == 'mean':
                rolling_values = bn.move_mean(x_np, window=window)
            if method == 'median':
                rolling_values = bn.move_median(x_np, window=window)
            #rolling_list = rolling_values.tolist()
            #rolling_timeseries = []
            #for index, item in enumerate(timeseries):
            #    try:
            #        rolling_timeseries.append([item[0], rolling_list[index]])
            #    except:
            #        rolling_timeseries.append([item[0], None])
            # Replace NaN values with None and coerce to float
            rolling_timeseries = [
                [timeseries[i][0], float(rolling_values[i]) if not np.isnan(rolling_values[i]) else None]
                for i in range(len(timeseries))
            ]
            # Pad the being of the time series with the first value, the first
            # (window-1) items have a None value as the moving value cannot be
            # calculated for the first (window-1) items
            pad_value = rolling_timeseries[window-1][1]
            for index in list(range(window-1)):
                t = rolling_timeseries[index][0]
                rolling_timeseries[index] = [t, pad_value]

            if debug_print:
                print('mmzrmp preprocessed timeseries using window of %s and %s' % (
                str(window), method))
            if debug_logging:
                current_logger.debug('debug :: mmzrmp preprocessed timeseries using window of %s and %s' % (
                str(window), method))
        except:
            return timeseries
        return rolling_timeseries
    
    def zscore(timeseries, zscores):
        anomalous = None
        anomalyScore = 0.0
        anomalies = {}
        results = {'anomalies': {}, 'anomalyScore_list': [], 'scores': []}  
        anomalyScore_list = []
        #az_scores = []
        for index, z_score in enumerate(zscores):
            anomalyScore_list_score = 0
            # OPINIONATED
            if z_score > 3 or z_score < -3:
                #az_scores.append([index, float(z_score)])
                anomalyScore_list_score = 1
                ts = int(timeseries[index][0])
                value = timeseries[index][1]
                anomalies[ts] = {'value': value, 'index': index, 'score': z_score}
                anomalous = True
                anomalyScore = 1.0
            anomalyScore_list.append(anomalyScore_list_score)
        #results = {'anomalous': anomalous, 'anomalies': anomalies, 'anomalyScore_list': anomalyScore_list, 'scores': zscores, 'timeseries': timeseries}
        results = {'anomalous': anomalous, 'anomalies': anomalies, 'anomalyScore_list': anomalyScore_list, 'scores': zscores}
        return anomalous, anomalyScore, results


    def ruptures_pelt(timeseries, values, min_size=6):
        anomalous = None
        anomalyScore = 0.0
        anomalies = {}
        results = {'anomalies': {}, 'anomalyScore_list': [], 'scores': []}  
        anomalyScore_list = []
        timings = {}

        #values = np.array([v for t, v in timeseries])
        start_algo_c = timer()
        algo_c = rpt.KernelCPD(kernel="rbf", min_size=min_size).fit(values)  # written in C
        end_algo_c = timer()
        timings['algo_c'] = (end_algo_c - start_algo_c)
        start_predict = timer()
        # result is a list of np.int dtypes
        result = algo_c.predict(pen=30)
        end_predict = timer()
        timings['predict'] = (end_predict - start_predict)
        if result:
            # result is not 0 indexed
            result = [int(index - 1) for index in result]
            result_set = set(result)
            #anomalyScore_list = [1.0 if index in result_set else 0.0 for index in range(len(timeseries))]
            anomalyScore_list = []
            for index, item in enumerate(timeseries):
                score = 0
                if index in result_set:
                    #print(index)
                    anomalous = True
                    score = 1
                    anomalies[timeseries[index][0]] = {'value': float(timeseries[index][1]), 'index': index, 'score': score}
                anomalyScore_list.append(score)
            scores = list(anomalyScore_list)
        if not anomalies:
            anomalous = False
        results = {
            'anomalous': anomalous, 'anomalies': anomalies,
            'anomalyScore_list': anomalyScore_list, 'scores': scores, 
            'predictions': list(result_set),
            'timings': timings
        }
        return anomalous, anomalyScore, results, timings

    def ruptures_binseg(timeseries, values, min_size=6):
        anomalous = None
        anomalyScore = 0.0
        anomalies = {}
        results = {'anomalies': {}, 'anomalyScore_list': [], 'scores': []}  
        anomalyScore_list = []
        timings = {}

        #values = np.array([v for t, v in timeseries])
        start_algo_c = timer()
        #algo_c = rpt.Binseg(model="l2", min_size=min_size).fit(values)
        algo_c = rpt.Binseg(model="rbf", min_size=min_size).fit(values)
        end_algo_c = timer()
        timings['algo_c'] = (end_algo_c - start_algo_c)
        start_predict = timer()
        # result is a list of np.int dtypes
        result = algo_c.predict(pen=30)
        end_predict = timer()
        timings['predict'] = (end_predict - start_predict)
        if result:
            # result is not 0 indexed
            result = [int(index - 1) for index in result]
            result_set = set(result)
            anomalyScore_list = []
            for index, item in enumerate(timeseries):
                score = 0
                if index in result_set:
                    #print(index)
                    anomalous = True
                    score = 1
                    anomalies[timeseries[index][0]] = {'value': float(timeseries[index][1]), 'index': index, 'score': score}
                anomalyScore_list.append(score)
            scores = list(anomalyScore_list)
        if not anomalies:
            anomalous = False
        results = {
            'anomalous': anomalous, 'anomalies': anomalies,
            'anomalyScore_list': anomalyScore_list, 'scores': scores, 
            'predictions': list(result_set),
            'timings': timings
        }
        return anomalous, anomalyScore, results, timings

    def align_anomalies(aligned_timeseries, original_timeseries, anomalies):
        aligned_anomalies = {}
        aligned_anomaly_timestamps = list(anomalies.keys())
        for index, item in enumerate(aligned_timeseries):
            ts = int(item[0])
            if ts in aligned_anomaly_timestamps:
                aligned_anomalies[int(original_timeseries[index][0])] = anomalies[ts]
        return aligned_anomalies

    def align_anomalyScore_list(timeseries, aligned_timeseries, anomalyScore_list):
        aligned_anomalyScore_list = []
        timeseries_timestamp_scores = {}
        for index, item in enumerate(timeseries):
            ts = int(item[0])
            # handle IndexError: list index out of range
            # This can occur when the time series is at a different resolution
            # to the downsample e.g. 300 seconds
            try:
                timeseries_timestamp_scores[ts] = anomalyScore_list[index]
            except:
                timeseries_timestamp_scores[ts] = 0
        for index, item in enumerate(aligned_timeseries):
            ts = int(item[0])
            score = 0
            try:
                score = timeseries_timestamp_scores[ts]
            except:
                score = 0
            aligned_anomalyScore_list.append(score)
        return aligned_anomalyScore_list

    try:
        np_timestamps_array = np.array([t for t, v in timeseries])
        ts_diffs = np.diff(np_timestamps_array)
        resolution_counts = np.unique(ts_diffs, return_counts=True)
        resolution = int(resolution_counts[0][np.argmax(resolution_counts[1])])

        original_timeseries = list(timeseries)
        original_aligned_timeseries = [[int(int(t) // resolution * resolution), v] for t, v in timeseries]
        aligned_timeseries = list(original_aligned_timeseries)
        anomaly_window_timestamps = [t for t, _ in aligned_timeseries[-anomaly_window:]]

        tseries = list(aligned_timeseries)

        # OPINIONATED
        downsampled_timeseries = None
        if resolution < 180 and len(timeseries) > 1800:
            if debug_print:
                print('mmzrmp downsampling time series - len(timeseries): %s, resolution: %s' % (
                str(len(timeseries)), resolution))
            if debug_logging:
                current_logger.debug('debug :: mmzrmp downsampling time series - len(timeseries): %s, resolution: %s' % (
                str(len(timeseries)), resolution))
            start_downsample = timer()
            downsampled_timeseries = downsample_timeseries(current_skyline_app, aligned_timeseries, resolution, 600, 'mean', 'end', bfill=True)
            if downsampled_timeseries:
                timeseries = list(downsampled_timeseries)
                resolution = 600
                tseries = list(timeseries)
                aligned_timeseries = list(downsampled_timeseries)

            end_downsample = timer() - start_downsample
            if debug_print:
                print('mmzrmp downsampled time series - len(timeseries): %s, resolution: %s, in %s seconds' % (
                str(len(timeseries)), str(resolution), str(end_downsample)))
            if debug_logging:
                current_logger.debug('debug :: mmzrmp downsampled time series - len(timeseries): %s, resolution: %s, in %s seconds' % (
                str(len(timeseries)), str(resolution), str(end_downsample)))
            timings['downsample'] = end_downsample

        values = np.array([float(item[1]) for item in aligned_timeseries])
        # Check if there are any NaN values before attempting to fill
        imputed_nans = False
        if np.isnan(values).any():
            # Backward fill NaNs by reversing, forward filling, then reversing back.
            # This is required as m66 does not tolerate NaNs.  This may not
            # be the most appropriate manner in which to achieve this with all data,
            # but given that a lot of metric data has sudden jumps or discrete
            # shifts.  It assumes small gaps.  A min percentage data points strategy
            # would probably be sutiable here, to ensure that only mostly fully
            # populated data sets have the method applied.
            reversed_values = values[::-1]
            mask = np.isnan(reversed_values)
            reversed_values[mask] = np.where(mask, np.maximum.accumulate(np.where(mask, 0, reversed_values)), reversed_values)
            values = reversed_values[::-1]
            imputed_nans = True

        zscores = (values - values.mean()) / values.std()
        # Coerce np.float64 to float
        zscores = [float(v) for v in zscores]

        if imputed_nans:
            # m66 is not tolerant of nans
            timeseries = [[item[0], float(values[index])] for index, item in enumerate(aligned_timeseries)]
            tseries = list(timeseries)
            aligned_timeseries = list(timeseries)
            if debug_print:
                print('mmzrmp recreated timeseries with imputed_nans')
            if debug_logging:
                current_logger.debug('debug :: mzrmp recreated timeseries with imputed_nans')

        # Although these parameters are all tunable, mmzrmp is opinionated,
        # apart from the anomaly_window which were originally set to:
        anomaly_windows = {
            'default': 15,
            'm66': 30,
        }
        algorithms = {
            'm66': {'algorithm_parameters': {'base_name': base_name, 'anomaly_window': int(anomaly_window * 2), 'nth_median': 6, 'sigma': 6, 'window': 5, 'minimum_sparsity': 70, 'return_results': True, 'debug_logging': debug_logging, 'print_debug': debug_print}},
            'macd': {'algorithm_parameters': {'anomaly_window': anomaly_window, 'fast_window': 12, 'slow_window': 26, 'signal_window': 9, 'feature': 'macd', 'return_results': True, 'max_execution_time': 3.0, 'debug_logging': debug_logging, 'print_debug': debug_print}},
            'zscore': {},
            'ruptures_pelt': {'algorithm_parameters': {'base_name': base_name, 'anomaly_window': anomaly_window, 'return_results': True, 'max_execution_time': 3.0, 'debug_logging': debug_logging, 'print_debug': debug_print}},
            'ruptures_binseg': {'algorithm_parameters': {'base_name': base_name, 'anomaly_window': anomaly_window, 'return_results': True, 'max_execution_time': 3.0, 'debug_logging': debug_logging, 'print_debug': debug_print}},
            # @added 20231114 - testing the addition of matrixprofile
            # @added 20240628 - testing the addition of moving_sum_and_value_decrease
            #                   disabled matrixprofile long runtime and results
            #                   not useful, has never fired in the ensemble.
            # 'matrixprofile': {'algorithm_parameters': {'base_name': base_name, 'anomaly_window': anomaly_window, 'return_results': True, 'max_execution_time': 3.0, 'debug_logging': debug_logging, 'print_debug': debug_print}},
            # @added 20240628 - testing the addition of moving_sum_and_value_decrease
            'moving_sum_and_value_decrease':  {'algorithm_parameters': {'base_name': base_name, 'anomaly_window': anomaly_window, 'realtime_analysis': False, 'return_results': True, 'max_execution_time': 3.0, 'debug_logging': debug_logging, 'print_debug': debug_print}},
        }

        # NOW OPINIONATED
        #if use_algorithms:
        #    algorithms = dict(use_algorithms)

        window = 6

        # NOW OPINIONATED
        moving_median = True
        if moving_median:
            start_moving_timeseries = timer()
            tseries = moving_timeseries(aligned_timeseries, resolution, 'median', (resolution * 4))
            #print('moving median at', (resolution * 4), 'seconds', 'len(tseries):', len(tseries))
            took = timer() - start_moving_timeseries
            if debug_print:
                print('mmzrmp - moving_timeseries took %s seconds' % (
                str(took)))
                print('mmzrmp - len(tseries): %s' % (str(len(tseries))))

            if debug_logging:
                current_logger.debug('debug :: mmzrmp moving_timeseries took %s seconds' % (
                str(took)))
                current_logger.debug('debug :: mmzrmp - len(tseries): %s' % (
                    str(len(tseries))))

        start_algos = timer()
        # This algorithm does not conform to the normal custom_algorithm results
        # during analysis as it was developed and tested over a period of time
        # independent of custom_algorithms.  Here the final results are coerced
        # to standard custom_algorithms structure.
        all_results = {}
        algorithms_results = {}
        for algorithm in list(algorithms.keys()):
            results = {'anomalies': {}, 'anomalyScore_list': [], 'scores': []}
            start_algo = timer()
            if algorithm == 'm66':
                # @modified 20240201
                # n_predict = 60
                n_predict = 10
                m66_timeseries = list(tseries)
                try:
                    m66_timeseries = fft_extrapolation(current_skyline_app, m66_timeseries, n_predict=n_predict, log=debug_logging)
                except Exception as err:
                    m66_timeseries = list(tseries)
                    if debug_print:
                        print('error :: %s fft_extrapolation error - %s' % (algorithm, err))
                    if debug_logging:
                        current_logger.error('error :: debug :: mmzrmp :: m66 fft_extrapolation failed, err: %s' % (
                        err))
                if debug_logging:
                    current_logger.debug('debug :: mmzrmp :: m66 - len(m66_timeseries): %s' % (
                        str(len(m66_timeseries))))
                anomalous, anomalyScore, results = m66(current_skyline_app, getpid(), m66_timeseries, algorithms[algorithm]['algorithm_parameters'])
                results['fft_extrapolation'] = {}
                results['fft_extrapolation']['n_predict'] = n_predict
                use_anomalies = {}
                last_timeseries_timestamp = tseries[-1][0]
                for anomaly_ts in list(results['anomalies'].keys()):
                    if anomaly_ts <= last_timeseries_timestamp:
                        use_anomalies[anomaly_ts] = results['anomalies'][anomaly_ts].copy()
                results['anomalies_fft_extrapolation'] = copy.deepcopy(results['anomalies'])
                results['anomalies'] = copy.deepcopy(use_anomalies)
                last_timeseries_index = len(tseries) - 1
                use_anomalyScore_list = [score for index, score in enumerate(results['anomalyScore_list']) if index <= last_timeseries_index]
                results['anomalyScore_list'] = use_anomalyScore_list
                use_scores = [score for index, score in enumerate(results['scores']) if index <= last_timeseries_index]
                results['scores'] = use_scores
                use_anomaly_window = algorithms[algorithm]['algorithm_parameters']['anomaly_window']
                anomalies_in_window = len([x for x in use_anomalyScore_list[-use_anomaly_window:] if x == 1])
                if anomalies_in_window:
                    anomalous = True
                    anomalyScore = 1.0
                    results['anomalous'] = anomalous
                results['anomalous'] = anomalous
                results['anomalies_in_window'] = anomalies_in_window
            elif algorithm == 'zscore':
                anomalous, anomalyScore, results = zscore(tseries, zscores)
            elif algorithm == 'ruptures_pelt':
                # @modified 20231114
                # The key to getting ruptures pelt to detect the type of
                # significant changes this ensemble is identifying is to NOT use
                # moving_median.  This is key, if the moving_median is used pelt
                # does not suit this method, it never fires.  Using the real
                # data it detects similar to m66 and macd!  Making for a viable
                # ensemble.
                #anomalous, anomalyScore, results = ruptures_pelt(tseries)
                anomalous, anomalyScore, results, pelt_timings = ruptures_pelt(aligned_timeseries, values)
                if 'anomalies' not in results:
                    results['anomalies']
                if 'anomalyScore_list' not in results:
                    results['anomalyScore_list']
            elif algorithm == 'ruptures_binseg':
                anomalous, anomalyScore, results, pelt_timings = ruptures_binseg(aligned_timeseries, values)
                if 'anomalies' not in results:
                    results['anomalies']
                if 'anomalyScore_list' not in results:
                    results['anomalyScore_list']
            # @modified 20240628 - disabled matrixprofile long runtime and poor results
            #elif algorithm == 'matrixprofile':
            #    anomalous, anomalyScore, results = matrixprofile(timeseries)
            # @added 20240628 - testing the addition of moving_sum_and_value_decrease
            # Although does not align with the ensemble in terms of what it
            # detects, m66, macd and pelt detect more towards the onset of the
            # changepoint. This detects towards the "tail" of the onset which
            # m66, macd and pelt DO NOT all detect on the tail side, some do,
            # some times.
            # That makes this method useful in terms of the ensemble, given that
            # the nature of detection here is over a much longer period e.g.
            # window >= 15, this method therefore suppliments the onset tail
            # detections, which are valid.  It is a sustained change, we want to
            # detect it,
            elif algorithm == 'moving_sum_and_value_decrease':
                if debug_print:
                    print(algorithm, 'algorithm_parameters', algorithms[algorithm]['algorithm_parameters'])
                anomalous, anomalyScore, results = moving_sum_and_value_decrease(current_skyline_app, getpid(), aligned_timeseries, algorithms[algorithm]['algorithm_parameters'])
            else:
                anomalous, anomalyScore, results = macd(current_skyline_app, getpid(), tseries, algorithms[algorithm]['algorithm_parameters'])

            if downsampled_timeseries:
                # Align to original timeseries
                if results['anomalies']:
                    if debug_logging:
                        current_logger.info('%s :: align_anomalies, len(aligned_timeseries): %s, len(original_timeseries): %s' % (
                            algorithm, str(len(aligned_timeseries)),
                            str(len(original_timeseries))))
                    aligned_anomalies = align_anomalies(aligned_timeseries, original_timeseries, results['anomalies'])
                    results['anomalies'] = aligned_anomalies
                if results['anomalyScore_list']:
                    if debug_logging:
                        current_logger.info('%s :: align_anomalyScore_list, len(aligned_timeseries): %s, len(timeseries): %s' % (
                            algorithm, str(len(aligned_timeseries)),
                            str(len(timeseries))))
                    aligned_anomalyScore_list = align_anomalyScore_list(timeseries, aligned_timeseries, anomalyScore_list)
                    results['anomalyScore_list'] = aligned_anomalyScore_list
                    anomalyScore_list = list(aligned_anomalyScore_list)
                if debug_logging:
                    current_logger.info('%s :: aligned resources' % algorithm)

            anomalies_in_window = sum(results['anomalyScore_list'][-anomaly_window:])
            results['anomaly_window'] = anomaly_window
            results['anomalies_in_window'] = int(anomalies_in_window)

            took = timer() - start_algo
            timings['algorithms'][algorithm] = took
            if debug_print:
                print('mmzrmp - %s anomalies_in_window: %s, took %s seconds' % (
                    algorithm, str(anomalies_in_window), str(took)))
            if debug_logging:
                current_logger.debug('debug :: mmzrmp - %s anomalies_in_window: %s, took %s seconds' % (
                    algorithm, str(anomalies_in_window), str(took)))
            algorithms_results[algorithm] = results
            all_results[algorithm] = results

        # This is were is goes astray from the normal custom_alogrithms results
        # generation due to development and testing producing graphs.
        took = timer() - start_algos
        if debug_print:
            print('mmzrmp - all algorithms took %s seconds' % str(took))
        if debug_logging:
            current_logger.debug('debug :: mmzrmp - all algorithms took %s seconds' % str(took))

        start_results = timer()
        all_algorithm_results = {}
        for algorithm in list(all_results.keys()):
            if algorithm == 'algorithms':
                continue
            if debug_print:
                print('mmzrmp - collating %s results' % str(algorithm))
            results = copy.deepcopy(all_results[algorithm])
            scores = list(results['anomalyScore_list'])
            anomalies = results['anomalies']
            for ts in list(anomalies.keys()):
                t_result = []
                try:
                    t_result = all_algorithm_results[ts]
                except:
                    t_result = []
                t_result.append(algorithm)
                all_algorithm_results[ts] = list(t_result)

        tx_ids = {}
        for index, item in enumerate(aligned_timeseries):
            tx_ids[int(item[0])] = index

        ruptures_appended = []
        if all_algorithm_results:
            all_algorithm_timestamps = list(all_algorithm_results.keys())
            for ts in all_algorithm_timestamps:
                t_result = all_algorithm_results[ts]
                try:
                    t_idx = tx_ids[ts]
                except KeyError:
                    if debug_logging:
                        current_logger.debug('warning :. debug :: mmzrmp - %s present in all_algorithm_results but not in in tx_ids' % (
                            str(ts)))
                    continue
                t_idxs = [t_idx]
                #tss = [ts]
                if 'ruptures_pelt' in t_result:
                    t_idxs = []
                    for tt_idx in list(range(t_idx-window, t_idx+window)):
                        t_idxs.append(tt_idx)
                    rp_tss = [item[0] for index, item in enumerate(aligned_timeseries) if index in t_idxs]
                    for rp_ts in rp_tss:
                        try:
                            all_algorithm_results[rp_ts].append('ruptures_pelt')
                        except:
                            all_algorithm_results[rp_ts] = ['ruptures_pelt']
                        # print('appended ruptures_pelt to', ts)
                        ruptures_appended.append(rp_ts)
                        algorithms_results['ruptures_pelt']['anomalyScore_list'][t_idx] = 1
                        algorithms_results['ruptures_pelt']['anomalies'][ts] = {
                            'value': float(aligned_timeseries[index][1]),
                            'index': t_idx, 'score': 1
                        }

            if ruptures_appended:
                if debug_print:
                    print('mmzrmp - appended ruptures_pelt to %s timestamps' % (
                        str(len(ruptures_appended))))
                if debug_logging:
                    current_logger.debug('debug :: mmzrmp - appended ruptures_pelt to %s timestamps' % (
                        str(len(ruptures_appended))))
                all_results[algorithm] = algorithms_results['ruptures_pelt']

            all_algorithm_results_sorted = {}
            all_algorithm_results_ts = sorted(list(all_algorithm_results.keys()))
            for ts in all_algorithm_results_ts:
                t_result = list(set(all_algorithm_results[ts]))
                all_algorithm_results_sorted[ts] = t_result
            all_algorithm_results = copy.deepcopy(all_algorithm_results_sorted)

            # Allow overlaps 3 data points either side
            n = 3
            if anomaly_window < 10:
                n = 1
            for ts in list(all_algorithm_results.keys()):
                consensus = list(all_algorithm_results[ts])
                consensus_count = len(all_algorithm_results[ts])
                if consensus_count == 1:
                    triggered_recently = []
                    ts_before = [ts - resolution * i for i in range(1, n + 1)]
                    ts_after = [ts + resolution * i for i in range(1, n + 1)]
                    consensus_window_ts = ts_before + [ts] + ts_after
                    for cts in consensus_window_ts:
                        t_result = []
                        try:
                            t_result = all_algorithm_results[cts]
                        except:
                            continue
                        for algo in t_result:
                            triggered_recently.append(algo)
                    triggered_recently = list(set(triggered_recently))
                    if len(triggered_recently) > consensus_count:
                        all_algorithm_results[ts] = list(triggered_recently)
                        if debug_logging:
                            current_logger.debug('debug :: mmzrmp - other recent triggers modified %s result from %s to %s' % (
                                str(ts), str(consensus), str(triggered_recently)))

            all_scores = []
            anomalyScore_list = []
            graph_anomalies = {}
            all_anomalies = {}

            anomaly_window_timestamps = [t for t, _ in aligned_timeseries[-anomaly_window:]]
            for ts, v in aligned_timeseries:
    #        for ts, t_result in all_algorithm_results.items():
                all_score = 0
                anomalyScore = 0
                if ts in anomaly_window_timestamps:
                    t_result = []
                    try:
                        t_result = all_algorithm_results[ts]
                    except:
                        t_result = []
                    # Do not allow a consensus of only ruptures algorithms or
                    # with zscore these identify very similar things.  The other
                    # algorithms in the ensemble detect different more
                    # significant changes.  If left just to the ruptures and
                    # zscore results alone the "sustained" part can be lost.
                    exclude = ['ruptures_binseg', 'ruptures_pelt', 'zscore']
                    modified_t_result = []
                    if len(list(set(t_result))) == 2:
                        if 'zscore' in t_result:
                            if 'ruptures_pelt' in t_result:
                                modified_t_result = ['zscore_ruptures_pelt']
                            if 'ruptures_binseg' in t_result:
                                modified_t_result = ['zscore_ruptures_binseg']
                            if 'ruptures_binseg' in t_result and 'ruptures_pelt' in t_result:
                                modified_t_result = ['zscore_ruptures_pelt_binseg']
                    if sorted(t_result) == exclude:
                        modified_t_result = ['zscore_ruptures_pelt_binseg']
                    if sorted(modified_t_result) == exclude:
                        modified_t_result = ['zscore_ruptures_pelt_binseg']
                    if sorted(t_result) == ['ruptures_binseg', 'ruptures_pelt']:
                        modified_t_result = ['ruptures_binseg_pelt']
                    if sorted(modified_t_result) == ['ruptures_binseg', 'ruptures_pelt']:
                        modified_t_result = ['ruptures_binseg_pelt']
                    if modified_t_result:
                        if debug_logging:
                            current_logger.debug('debug :: mmzrmp - modified %s result from %s to %s' % (
                                str(ts), str(t_result), str(modified_t_result)))
                        all_algorithm_results[ts] = list(modified_t_result)
                        t_result = list(modified_t_result)

                    if len(list(set(t_result))) > 1:
                        all_score = len(list(set(t_result)))
                        a_idx = tx_ids[ts]
                        graph_anomalies[str(int(ts))] = {'value': aligned_timeseries[a_idx][1], 'index': a_idx, 'score': all_score}
                        all_anomalies[ts] = {'value': aligned_timeseries[a_idx][1], 'index': a_idx, 'score': all_score, 'consensus': list(set(t_result))}
                        anomalyScore = 1
                all_scores.append(all_score)
                anomalyScore_list.append(anomalyScore)
            scores = list(all_scores)
            #all_anomalous = False

            if len(algorithms_results['ruptures_pelt']['anomalyScore_list']) > 0:
                pelt_anomalies_in_window = sum(algorithms_results['ruptures_pelt']['anomalyScore_list'][-anomaly_window:])
                algorithms_results['ruptures_pelt']['anomalies_in_window'] = pelt_anomalies_in_window

            if anomalyScore_list:
                anomalies_in_window = sum(anomalyScore_list[-anomaly_window:])
            else:
                anomalies_in_window = 0
            if anomalies_in_window > 0:
                anomalous = True
            else:
                anomalous = False

            #all_results['all'] = {'anomalous': all_anomalous, 'anomalies': all_anomalies, 'anomalyScore_list': all_scores, 'scores': all_scores, 'all_algorithm_results': all_algorithm_results}
            results = {
                'anomalous': anomalous, 'anomaly_window': anomaly_window,
                'anomalies_in_window': anomalies_in_window,
                'anomalies': all_anomalies, 
                'anomalyScore_list': anomalyScore_list, 'scores': scores, 
                'algorithms': algorithms_results,
                'all_algorithm_results': all_algorithm_results,
            }
        if 'anomalies_in_window' not in results:
            results['anomalous'] = False
            results['anomalies_in_window'] = 0

        took = timer() - start_results
        timings['results'] = took
        took = time() - start
        timings['total'] = took
        results['timings'] = timings

        if debug_print:
            print('ran mmzrmp OK in %.6f seconds' % took)
        if debug_logging:
            current_logger.debug('debug :: ran mmzrmp OK in %.6f seconds' % took)
        if results:
            if results['anomalous']:
                anomalous = True
                anomalyScore = 1.0
            else:
                anomalous = False
                anomalyScore = 0.0
            if debug_print:
                print('anomalous: %s' % str(anomalous))
            if debug_logging:
                current_logger.debug('debug :: anomalous: %s' % str(anomalous))
        else:
            if debug_print:
                print('error - no results')
            if debug_logging:
                current_logger.debug('debug :: error - no results')

    except StopIteration:
        if debug_print:
            print('warning - StopIteration called on mmzrmp')
        if debug_logging:
            current_logger.debug('debug :: warning - StopIteration called on mmzrmp')

        # This except pattern MUST be used in ALL custom algortihms to
        # facilitate the traceback from any errors.  The algorithm we want to
        # run super fast and without spamming the log with lots of errors.
        # But we do not want the function returning and not reporting
        # anything to the log, so the pythonic except is used to "sample" any
        # algorithm errors to a tmp file and report once per run rather than
        # spewing tons of errors into the log e.g. analyzer.log
        if return_results:
            return (None, None, None)
        return (None, None)
    except Exception as err:
        record_algorithm_error(current_skyline_app, parent_pid, algorithm_name, traceback.format_exc())
        if debug_print:
            print('error:', traceback.format_exc())
        if debug_logging:
            current_logger.debug('debug :: error - on mmzrmp - %s' % err)
            current_logger.debug(traceback.format_exc())

        # Return None and None as the algorithm could not determine True or False
        if return_results:
            return (None, None, None)
        return (None, None)

    if return_results:
        return (anomalous, anomalyScore, results)

    return (anomalous, anomalyScore)
