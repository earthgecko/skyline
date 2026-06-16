"""
low_variance_anomalous_peak_trough.py
"""
# REQUIRED Skyline imports.  All custom algorithms MUST have the following two
# imports.  These are required for exception handling and to record algorithm
# errors regardless of debug_logging setting for the custom_algorithm
import logging
import traceback
from time import time
from custom_algorithms import record_algorithm_error

import copy

# Import ALL modules that the custom algorithm requires.  Remember that if a
# requirement is not one that is provided by the Skyline requirements.txt you
# must ensure it is installed in the Skyline virtualenv
import numpy as np
from scipy.signal import find_peaks

from functions.timeseries_predictions.fft_extrapolation import fft_extrapolation

# The name of the function MUST be the same as the name declared in
# settings.CUSTOM_ALGORITHMS.
# It MUST have 3 parameters:
# current_skyline_app, timeseries, algorithm_parameters
# See https://earthgecko-skyline.readthedocs.io/en/latest/algorithms/custom-algorithms.html
# for a full explanation about each.
# ALWAYS WRAP YOUR ALGORITHM IN try and except


# @added 20230717 - Feature #4994: custom_algorithm - mirages
def low_variance_anomalous_peak_trough(current_skyline_app, parent_pid, timeseries, algorithm_parameters):

    """
    A time series with low variance or few peaks/troughs is anomalous if the
    data point is > 3sigma of the peaks/troughs:
    peak_values_mean + (3 * peak_values_stdDev).
    trough_values_mean - (3 * trough_values_stdDev).
    This algorithm is ONLY suited to assessing the last datapoints and is NOT
    suited to an anomaly_window > 10
    
    :param current_skyline_app: the Skyline app executing the algorithm.  This
        will be passed to the algorithm by Skyline.  This is **required** for
        error handling and logging.  You do not have to worry about handling the
        argument in the scope of the custom algorithm itself,  but the algorithm
        must accept it as the first agrument.
    :param parent_pid: the parent pid which is executing the algorithm, this is
        **required** for error handling and logging.  You do not have to worry
        about handling this argument in the scope of algorithm, but the
        algorithm must accept it as the second argument.
    :param timeseries: the time series as a list e.g. ``[[1690920000, 0.0], ...,
        [1691524200.0, 0.0], [1691524800.0, 0.5]]``
    :param algorithm_parameters: {
            'currently_anomalous': False,  # whether instance state is anomalous or not, default False
            'anomaly_window': 4,           # should be > 1 <= 10, default 4
            'return_results': False,       # whether to return the result dict, default False
            'debug_logging':  False,       # whether to log, default False
        }
    :param algorithm_parameters: a dictionary of any required parameters for the
        custom_algorithm and algorithm itself.  For the
        low_variance_anomalous_peak_trough custom algorithm no specific
        algorithm_parameters are required apart from an empty dict but
        the algorithm_parameters that can be passed are:

        - ``'anomaly_window'`` (int): The anomaly_window value.
            This specifies how many of the last data points should be considered
            when determining if the metric is anomalous. Only the last
            ``anomaly_window`` data points in the time series will be used to
            determine if the metric is anomalous.  Default is ``1``.
        - ``'currently_anomalous'`` (bool): Optional.
            Whether the instance state is anomalous or not. Default is ``False``.
         - ``'return_results'`` (bool): Optional.
            If ``True``, returns the results dict in addition to anomalous and
            anomalyScore.  Default is ``False``.
        - ``'debug_logging'`` (bool): Optional.
            If ``True``, enables debug logging.
        - ``'debug_print'`` (bool): Optional.
            If ``True``, enables debug printing  (for Jupyter testing). Default
            is ``False``.

        Example usage::

            algorithm_parameters={
                'anomaly_window': 4,
                'currently_anomalous': True,
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
    algorithm_name = 'low_variance_anomalous_peak_trough'

    # If you wanted to log, you can but this should only be done during
    # testing and development
    def get_log(current_skyline_app):
        current_skyline_app_logger = current_skyline_app + 'Log'
        current_logger = logging.getLogger(current_skyline_app_logger)
        return current_logger

    # Define the default state of None and None, anomalous does not default to
    # False as that is not correct, False is only correct if the algorithm
    # determines the data point is not anomalous.  The same is true for the
    # anomalyScore.
    anomalous = False
    anomalyScore = 0.0
    anomalies = {}
    anomalyScore_list = []
    results = {
        'algorithm': algorithm_name,
        'anomalous': anomalous,
        'anomalies': anomalies,
        'anomalyScore_list': anomalyScore_list,
        'analysed': False,
    }

    currently_anomalous = False
    try:
        currently_anomalous = algorithm_parameters['currently_anomalous']
    except:
        currently_anomalous = False
    anomalous = currently_anomalous
    results['anomalous'] = anomalous
    if anomalous:
        results['anomalyScore'] = 1.0

    anomaly_window = 4
    try:
        anomaly_window = int(algorithm_parameters['anomaly_window'])
    except:
        anomaly_window = 4
    results['anomaly_window'] = anomaly_window

    # @added 20230925 - variable sigma added
    # This allows for the sigma value used in low_variance_anomalous_peak_trough
    # be passed and reduced if certain criteria are met
    sigma = 3
    try:
        sigma = algorithm_parameters['sigma']
    except:
        sigma = 3
    results['sigma'] = sigma

    return_results = False
    try:
        return_results = algorithm_parameters['return_results']
    except:
        return_results = False

    # Use the algorithm_parameters to determine whether to log
    debug_logging = False
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
                return anomalous, anomalyScore, results
            return anomalous, anomalyScore

    print_debug = False
    try:
        print_debug = algorithm_parameters['print_debug']
    except:
        print_debug = False

    try:
        start = time()
        analyse_peaks_only = False
        number_of_peaks = 0
        number_of_troughs = 0
        anomalous = True
        last_index = len(timeseries) - 1

        results['timestamp'] = timeseries[-1][0]
        results['datapoint'] = timeseries[-1][1]
        results['index'] = last_index

        # @added 20230925 - Feature #4994: custom_algorithm - mirages
        # Handle mostly positive strictly increasing monotonity on metrics.
        # There are metrics that can exhibit > 98% positive strictly increasing
        # monotonocity over the period.  Metrics such as things like memory_usage
        # and memory_rss can slowly increment positively and over a 7 day period
        # and only experience the smallest of reductions in the values such as
        # {85: {'diff': 0.00012254714900006292, 'value': 3.267296552658},
        # 86: {'diff': -0.00011038780199967846, 'value': 3.267419099807},
        # 87: {'diff': 0.0, 'value': 3.267308712005},
        # 88: {'diff': -0.00012254715000015182, 'value': 3.267308712005},
        # 89: {'diff': 0.0, 'value': 3.267186164855},
        # 90: {'diff': 0.0006616115570001746, 'value': 3.267186164855}}
        # resulting in data such as this:
        # {'analysis_type': 'peak', 'number_of_peaks': 7, 'peak_index': 1007,
        # 'peak_value': 3.363199234008,
        # 'peaks': [
        #   [1007, 3.363199234008], [912, 3.302538871765], [854, 3.302367448806],
        #   [834, 3.302097797393], [767, 3.301387310028], [226, 3.2808201313],
        #   [86, 3.267419099807]],
        # 'sorted_peak_values_list': [
        #   [1007, 3.363199234008], [912, 3.302538871765], [854, 3.302367448806],
        #   [834, 3.302097797393], [767, 3.301387310028], [226, 3.2808201313],
        #   [86, 3.267419099807]],
        # 'anomalous_peak': False, 'anomalyScore': 0.0,
        # 'peak_values_mean': 3.3028328418724278, 'peak_values_median': 3.302097797393, 
        # 'peak_values_stdDev': 0.027737871777249984, 'mean_threshold': 3.3860464572041775,
        # 'median_threshold': 3.3853114127247497, 'all_peak_values_mean': 3.3028328418724278,
        # 'all_peak_values_median': 3.302097797393, 'all_peak_values_stdDev': 0.027737871777249984, 
        # 'all_peak_median_threshold': 3.3853114127247497, 'peak_outliers': [],
        # 'outlier_in_peak_values': True, 'threshold_on': 'all_peak_median',
        # 'threshold (all_peak_values_median + (3 * all_peak_values_stdDev))': 3.3853114127247497,
        # 'threshold': 3.3853114127247497,
        # 'peak_values': [
        #   3.267419099807, 3.2808201313, 3.301387310028, 3.302097797393,
        #   3.302367448806, 3.302538871765, 3.363199234008]}
        #
        # As per https://github.com/earthgecko/skyline/blob/master/docs/images/mostly.monotonic.lvapt.png
        # Running low_variance_anomalous_peak_trough on this type of data can
        # and will result in fNs.  Although a preprocessing stage could be added
        # to identify these types of data and convert to the derivative format,
        # excluding very small dips, for analysis if the mass majority of the
        # diffs are 0 or positive/negative, it is too contrived.  There is no
        # guarentee that preprocessing the data will have the desired results in
        # all circumstances, even if the decreases are less than the changes
        # mean and are less the 2% of the changes.
        # Therefore if data like this is identified just skip analysis on this
        # data and return what was passed as currently_anomalous
        # 
        results['timings'] = {}
        near_monotonic = False
        start_near_mono_check = time()
        np_values = np.asarray([v for t, v in timeseries])
        increases_and_zeros = [[index, diff] for index, diff in enumerate(list(np.diff(np_values))) if diff >= 0]
        decreases_and_zeros = [[index, diff] for index, diff in enumerate(list(np.diff(np_values))) if diff <= 0]
        check_monotonicity = False
        if ((len(increases_and_zeros) / len(np_values)) * 100) >= 95.0:
            check_monotonicity = True
        if ((len(decreases_and_zeros) / len(np_values)) * 100) >= 95.0:
            check_monotonicity = True
        if check_monotonicity:
            decreases = [[index, diff] for index, diff in enumerate(list(np.diff(np_values))) if diff < 0]
            increases = [[index, diff] for index, diff in enumerate(list(np.diff(np_values))) if diff > 0]
            decrease_sum = sum([v for index, v in decreases])
            increase_sum = sum([v for index, v in increases])
            total_change = increase_sum + (decrease_sum * -1)
            perc_increasing = (increase_sum / total_change) * 100
            perc_decreasing = ((decrease_sum * -1) / total_change) * 100
            if perc_increasing >= 98.0 and perc_decreasing < 2.0:
                near_monotonic = True
                monotonicity = 'increasing'
            if not near_monotonic:
                if perc_decreasing >= 98.0 and perc_increasing < 2.0:
                    near_monotonic = True
                    monotonicity = 'decreasing'
        results['timings']['near_monotonic_checks'] = time() - start_near_mono_check
        # Skip running the algorithm and return what was passed as currently_anomalous
        if near_monotonic:
            results['analysed'] = False
            reason = 'near strictly monotonically %s' % monotonicity
            results['skipped'] = {'reason': reason}
            if return_results:
                return anomalous, anomalyScore, results
            return anomalous, anomalyScore

        # Determine whether this is peak or trough analysis
        if anomaly_window < 10:
            tail_avg = sum([v for t, v in timeseries[-anomaly_window:]]) / anomaly_window
        else:
            tail_avg = sum([v for t, v in timeseries[-4:]]) / 4
        # mean = np.mean([v for t, v in timeseries])
        mean = np.mean(np_values)
        
        analysis_type = 'peak'
        if tail_avg < mean:
            analysis_type = 'trough'
        results['analysis_type'] = analysis_type
        peaks = []
        number_of_peaks = 0
        troughs = []
        number_of_troughs = 0

        # If the last data point is a peak find_peaks will not report it so add
        # predictions of 10 data points
        start_pred = time()
        pred_timeseries = fft_extrapolation(current_skyline_app, timeseries, n_predict=10, log=debug_logging)
        y_pred = np.array([v for t, v in pred_timeseries])
        results['timings']['fft_extrapolation'] = time() - start_pred

        start_find_peaks = time()
        if analysis_type == 'peak':
            peaks, _ = find_peaks(y_pred, distance=1)
            # Remove predictions
            peaks = [int(index) for index in list(peaks) if index <= last_index]
            number_of_peaks = len(peaks)
            results['number_of_peaks'] = number_of_peaks
            all_peaks = list(peaks)
        if analysis_type == 'trough':
            troughs, _ = find_peaks(-y_pred, distance=1)
            # Remove predictions
            troughs = [int(index) for index in list(troughs) if index <= last_index]
            number_of_troughs = len(troughs)
            results['number_of_troughs'] = number_of_troughs
            all_troughs = list(troughs)
        results['timings']['find_peaks'] = time() - start_find_peaks

        peak_analysis = False
        if number_of_peaks > 1:
            peak_index = int(last_index)
            not_in_peaks = True
            # peaks and troughs are identified one the leading side
            for index in list(range(last_index - 4, last_index + 1)):
                if index in peaks:
                    not_in_peaks = False
                    peak_index = int(index)
                    peak_value = timeseries[peak_index][1]
                    results['peak_index'] = peak_index
                    results['peak_value'] = peak_value
            if not not_in_peaks:
                analyse_peaks_only = True
                peak_analysis = True

        trough_analysis = False
        if number_of_troughs > 1:
            trough_index = int(last_index)
            not_in_troughs = True
            # peaks and troughs are identified one the leading side
            for index in list(range(last_index - 4, last_index + 1)):
                if index in troughs:
                    not_in_troughs = False
                    trough_index = int(index)
                    trough_value = timeseries[trough_index][1]
                    results['trough_index'] = trough_index
                    results['trough_value'] = trough_value
            if not not_in_troughs:
                analyse_peaks_only = True
                trough_analysis = True

        if not analyse_peaks_only:
            if debug_logging:
                current_logger.info('%s :: criteria not met, not running, results: %s' % (algorithm_name, str(results)))
            results['analysis_runtime'] = time() - start
            if return_results:
                return anomalous, anomalyScore, results
            return anomalous, anomalyScore

        if debug_logging:
            current_logger.info('%s :: running analysis_type: %s, for %s' % (
                algorithm_name, str(results['analysis_type']), str(results)))

        all_peak_values = []
        # Only sample to 10 largest peaks if there are many peaks
        if peak_analysis:
            peak_values_list = []
            for index in peaks:
                if index > last_index:
                    break
                value = timeseries[index][1]
                all_peak_values.append(value)
                peak_values_list.append([index, value])
            sorted_peak_values_list = sorted(peak_values_list, key=lambda x: x[1], reverse=True)
            if number_of_peaks > 20:
                peaks = [index for index, value in sorted_peak_values_list][0:10]
                sorted_peak_values_list = sorted_peak_values_list[0:10]
            peaks.sort()
            results['peaks'] = sorted_peak_values_list
            results['sorted_peak_values_list'] = sorted_peak_values_list
        if peak_analysis:
            peak_index = int(last_index)
            check_value = timeseries[peak_index][1]
            not_in_peaks = True
            # peaks and troughs are identified one the leading side
            for index in list(range(last_index - 4, last_index + 1)):
                if index in peaks:
                    not_in_peaks = False
                    peak_index = int(index)
                    check_value = timeseries[index][1]
            if not_in_peaks:
                results['last_datapoint_not_in_peaks'] = True
                results['analysis_runtime'] = time() - start
                if debug_logging:
                    current_logger.info('%s :: criteria not met, results: %s' % (
                        algorithm_name, str(results)))
                if return_results:
                    return anomalous, anomalyScore, results
                return anomalous, anomalyScore

        if trough_analysis:
            trough_values_list = []
            for index in troughs:
                if index > last_index:
                    break
                value = timeseries[index][1]
                trough_values_list.append([index, value])
            sorted_troughs_values_list = sorted(trough_values_list, key=lambda x: x[1], reverse=True)
            # Only sample to 10 largest troughs if there are many troughs
            if number_of_troughs > 20:
                troughs = [index for index, value in sorted_troughs_values_list][-10:]
                sorted_troughs_values_list = sorted_troughs_values_list[-10:]
            troughs.sort()
            results['troughs'] = sorted_troughs_values_list
            results['sorted_troughs_values_list'] = sorted_troughs_values_list
        if trough_analysis:
            trough_index = int(last_index)
            check_value = timeseries[trough_index][1]
            not_in_troughs = True
            # peaks and troughs are identified one the leading side
            for index in list(range(last_index - 4, last_index + 1)):
                if index in troughs:
                    not_in_troughs = False
                    trough_index = int(index)
                    check_value = timeseries[trough_index][1]
            if not_in_troughs:
                results['last_datapoint_not_in_troughs'] = True
                results['analysis_runtime'] = time() - start
                if debug_logging:
                    current_logger.info('%s :: criteria not met, results: %s' % (
                        algorithm_name, str(results)))
                if return_results:
                    return anomalous, anomalyScore, results
                return anomalous, anomalyScore

        if analyse_peaks_only and peak_analysis:
            peak_values = []
            peak_values_mean = False
            peak_values_median = False
            peak_values_stdDev = False
            threshold = False
            threshold_on = 'mean'
            # @modified 20230925 - variable sigma added
            # threshold_key = 'threshold (peak_values_mean + (3 * peak_values_stdDev))'
            threshold_key = 'threshold (peak_values_mean + (%s * peak_values_stdDev))' % str(sigma)
            for index in list(peaks):
                if index > last_index:
                    break
                peak_values.append(timeseries[index][1])
            if len(peak_values) == 1:
                anomalous = True
            if len(peak_values) > 1:
                # Coerce into floats
                peak_values_mean = float(np.mean(peak_values))
                peak_values_median = float(np.median(peak_values))
                peak_values_stdDev = float(np.std(peak_values))
                # @modified 20230925 - variable sigma added
                # mean_threshold = peak_values_mean + (3 * peak_values_stdDev)
                mean_threshold = peak_values_mean + (sigma * peak_values_stdDev)
                threshold = mean_threshold
                # @modified 20230925 - variable sigma added
                # median_threshold = peak_values_median + (3 * peak_values_stdDev)
                median_threshold = peak_values_median + (sigma * peak_values_stdDev)

                use_median = False
                # Are there outlier peaks?
                all_peak_values = [item[1] for index, item in enumerate(timeseries) if index in all_peaks]

                # @added 20230812
                # Only access if there are more than 2 peak values
                if len(all_peak_values) <= 2:
                    if debug_logging:
                        current_logger.info('%s :: only 2 peaks not evaluating, results: %s' % (
                            algorithm_name, str(results)))
                    if return_results:
                        return anomalous, anomalyScore, results
                    return anomalous, anomalyScore

                # Coerce into floats
                all_peak_values_mean = float(np.mean(all_peak_values))
                all_peak_values_median = float(np.median(all_peak_values))
                all_peak_values_stdDev = float(np.std(all_peak_values))
                # @modified 20230925 - variable sigma added
                # all_peak_median_threshold = all_peak_values_median + (3 * all_peak_values_stdDev)
                all_peak_median_threshold = all_peak_values_median + (sigma * all_peak_values_stdDev)
                peak_outliers = [v for v in all_peak_values if v > all_peak_median_threshold]

                # If there are outlier peaks then use the median
                if peak_outliers:
                    use_median = True

                # If the peak itself is an outlier in the peak_values being
                # considered, then use the median
                # Coerce into floats

                # other_peak_values_mean = float(np.mean(peak_values[0:-2]))
                # other_peak_values_stdDev = float(np.std(peak_values[0:-2]))
                other_peak_values = [v for v in peak_values if v != check_value]
                if other_peak_values:
                    other_peak_values_mean = float(np.mean(other_peak_values))
                    other_peak_values_stdDev = float(np.std(other_peak_values))
                    # @modified 20230925 - variable sigma added
                    # other_mean_threshold = other_peak_values_mean + (3 * other_peak_values_stdDev)
                    other_mean_threshold = other_peak_values_mean + (sigma * other_peak_values_stdDev)
                    outlier_in_peak_values = False
                    if check_value > other_mean_threshold:
                        use_median = True
                        outlier_in_peak_values = True
                else:
                    outlier_in_peak_values = True
                    use_median = True

                if use_median:
                    threshold = median_threshold
                    threshold_on = 'median'
                    # @modified 20230925 - variable sigma added
                    # threshold_key = 'threshold (peak_values_median + (3 * peak_values_stdDev))'
                    threshold_key = 'threshold (peak_values_median + (%s * peak_values_stdDev))' % str(sigma)

                # @added 20230812 - increase sensitivity to outliers to negate fNs
                if outlier_in_peak_values:
                    threshold = all_peak_median_threshold
                    threshold_on = 'all_peak_median'
                    # @modified 20230925 - variable sigma added
                    # threshold_key = 'threshold (all_peak_values_median + (3 * all_peak_values_stdDev))'
                    threshold_key = 'threshold (all_peak_values_median + (%s * all_peak_values_stdDev))' % str(sigma)

                # if timeseries[-1][1] > threshold:
                if check_value > threshold:
                    anomalous = True
                    anomalyScore = 1.0
                else:
                    anomalous = False
                if return_results:
                    for index, item in enumerate(timeseries):
                        score = 0
                        if index in peaks:
                            if item[1] > threshold:
                                score = 1
                                anomalies[int(item[0])] = {'value': item[1], 'index': index, 'score': 1}
                        anomalyScore_list.append(score)
            results['analysed'] = True
            results['anomalous'] = anomalous
            results['anomalous_peak'] = anomalous
            results['anomalyScore'] = anomalyScore
            results['number_of_peaks'] = int(number_of_peaks)
            results['peak_values_mean'] = float(peak_values_mean)
            results['peak_values_median'] = float(peak_values_median)
            results['peak_values_stdDev'] = float(peak_values_stdDev)
            results['mean_threshold'] = float(mean_threshold)
            results['median_threshold'] = float(median_threshold)
            results['all_peak_values_mean'] = float(all_peak_values_mean)
            results['all_peak_values_median'] = float(all_peak_values_median)
            results['all_peak_values_stdDev'] = float(all_peak_values_stdDev)
            results['all_peak_median_threshold'] = float(all_peak_median_threshold)
            results['peak_outliers'] = peak_outliers
            results['outlier_in_peak_values'] = outlier_in_peak_values
            results['threshold_on'] = threshold_on
            results[threshold_key] = threshold
            results['threshold'] = threshold
            results['peak_values'] = peak_values
            results['analysis_runtime'] = time() - start
            # Place this at the end
            del results['anomalyScore_list']
            if debug_logging:
                current_logger.info('%s :: results: %s' % (algorithm_name, str(results)))
            results['anomalies'] = anomalies
            results['anomalyScore_list'] = anomalyScore_list

        if analyse_peaks_only and trough_analysis:
            trough_values = []
            trough_values_mean = False
            trough_values_median = False
            trough_values_stdDev = False
            threshold = False
            threshold_on = 'mean'
            # threshold_key = 'threshold ((3 * trough_values_stdDev) - trough_values_mean)'
            # @modified 20230925 - variable sigma added
            # threshold_key = 'threshold (trough_values_mean - (3 * trough_values_stdDev))'
            threshold_key = 'threshold (trough_values_mean - (%s * trough_values_stdDev))' % str(sigma)
            for index in list(troughs):
                if index > last_index:
                    break
                trough_values.append(timeseries[index][1])
            if len(trough_values) == 1:
                anomalous = True
            if len(trough_values) > 1:
                # Coerce into floats
                trough_values_mean = float(np.mean(trough_values))
                trough_values_median = float(np.median(trough_values))
                trough_values_stdDev = float(np.std(trough_values))
                # mean_threshold = trough_values_mean - (3 * trough_values_stdDev)
                # mean_threshold = (3 * trough_values_stdDev) - trough_values_mean
                # @modified 20230925 - variable sigma added
                # mean_threshold = trough_values_mean - (3 * trough_values_stdDev)
                mean_threshold = trough_values_mean - (sigma * trough_values_stdDev)
                threshold = mean_threshold
                # median_threshold = trough_values_median - (3 * trough_values_stdDev)
                # median_threshold = (3 * trough_values_stdDev) - trough_values_median
                # @modified 20230925 - variable sigma added
                # median_threshold = trough_values_median - (3 * trough_values_stdDev)
                median_threshold = trough_values_median - (sigma * trough_values_stdDev)
                use_median = False
                # Are there outlier troughs
                all_trough_values = [item[1] for index, item in enumerate(timeseries) if index in all_troughs]

                # @added 20230812
                # Only access if there are more than 2 trough values
                if len(all_trough_values) <= 2:
                    if debug_logging:
                        current_logger.info('%s :: only 2 trough not evaluating, results: %s' % (algorithm_name, str(results)))
                    if return_results:
                        return anomalous, anomalyScore, results
                    return anomalous, anomalyScore

                # Coerce into floats
                all_trough_values_mean = float(np.mean(all_trough_values))
                all_trough_values_median = float(np.median(all_trough_values))
                all_trough_values_stdDev = float(np.std(all_trough_values))
                # all_trough_median_threshold = all_trough_values_median - (3 * all_trough_values_stdDev)
                # all_trough_median_threshold = (3 * all_trough_values_stdDev) - all_trough_values_median
                # @modified 20230925 - variable sigma added
                # all_trough_median_threshold = all_trough_values_median - (3 * all_trough_values_stdDev)
                all_trough_median_threshold = all_trough_values_median - (sigma * all_trough_values_stdDev)

                trough_outliers = [v for v in all_trough_values if v < all_trough_median_threshold]
                # If there are outlier troughs then use the median
                if trough_outliers:
                    use_median = True
                # If it is an outlier trough then use the median
                # Coerce into floats
                other_trough_values_mean = float(np.mean(trough_values[0:-2]))
                other_trough_values_stdDev = float(np.std(trough_values[0:-2]))
                # @modified 20230925 - variable sigma added
                # other_median_threshold = other_trough_values_mean - (3 * other_trough_values_stdDev)
                other_median_threshold = other_trough_values_mean - (sigma * other_trough_values_stdDev)
                outlier_in_trough_values = False
                if check_value < other_median_threshold:
                    use_median = True
                    outlier_in_trough_values = True

                if use_median:
                    # threshold = trough_values_median - (3 * trough_values_stdDev)
                    # threshold = (3 * trough_values_stdDev) - trough_values_median
                    # @modified 20230925 - variable sigma added
                    # threshold = trough_values_median - (3 * trough_values_stdDev)
                    threshold = trough_values_median - (sigma * trough_values_stdDev)
                    threshold_on = 'median'
                    # threshold_key = 'threshold ((3 * trough_values_stdDev) - trough_values_median)'
                    # @modified 20230925 - variable sigma added
                    # threshold_key = 'threshold (trough_values_median - (3 * trough_values_stdDev))'
                    threshold_key = 'threshold (trough_values_median - (%s * trough_values_stdDev))' % str(sigma)

                # @added 20230812 - increase sensitivity to outliers to negate fNs
                if outlier_in_trough_values:
                    threshold = all_trough_median_threshold
                    threshold_on = 'all_trough_median'
                    # threshold_key = 'threshold (other_tough_values_mean + (3 * other_trough_values_stdDev))'
                    # @modified 20230925 - variable sigma added
                    # threshold_key = 'threshold (all_trough_values_median + (3 * all_trough_values_stdDev))'
                    threshold_key = 'threshold (all_trough_values_median + (%s * all_trough_values_stdDev))' % str(sigma)
                    
                # if timeseries[-1][1] < threshold:
                if check_value < threshold:
                    anomalous = True
                    anomalyScore = 1.0
                else:
                    anomalous = False
                if return_results:
                    for index, item in enumerate(timeseries):
                        score = 0
                        if index in troughs:
                            if item[1] < threshold:
                                score = 1
                                anomalies[int(item[0])] = {'value': item[1], 'index': index, 'score': 1}
                        anomalyScore_list.append(score)
            results['analysed'] = True
            results['anomalous'] = anomalous
            results['anomalous_trough'] = anomalous
            results['anomalyScore'] = anomalyScore
            results['number_of_troughs'] = number_of_peaks
            results['trough_values_mean'] = float(trough_values_mean)
            results['trough_values_stdDev'] = float(trough_values_stdDev)
            results['mean_threshold'] = float(mean_threshold)
            results['median_threshold'] = float(median_threshold)
            results['all_trough_values_mean'] = float(all_trough_values_mean)
            results['all_trough_values_median'] = float(all_trough_values_median)
            results['all_trough_values_stdDev'] = float(all_trough_values_stdDev)
            results['all_trough_median_threshold'] = float(all_trough_median_threshold)
            results['trough_outliers'] = trough_outliers
            results['outlier_in_trough_values'] = outlier_in_trough_values
            results['threshold_on'] = threshold_on
            results[threshold_key] = threshold
            results['threshold'] = threshold
            results['trough_values'] = trough_values
            results['analysis_runtime'] = time() - start
            # Place this at the end
            del results['anomalyScore_list']
            if debug_logging:
                current_logger.info('%s :: results: %s' % (algorithm_name, str(results)))
            results['anomalies'] = anomalies
            results['anomalyScore_list'] = anomalyScore_list

    except StopIteration:
        # This except pattern MUST be used in ALL custom algortihms to
        # facilitate the traceback from any errors.  The algorithm we want to
        # run super fast and without spamming the log with lots of errors.
        # But we do not want the function returning and not reporting
        # anything to the log, so the pythonic except is used to "sample" any
        # algorithm errors to a tmp file and report once per run rather than
        # spewing tons of errors into the log e.g. analyzer.log
        if return_results:
            return (anomalous, anomalyScore, results)
        return (anomalous, anomalyScore)
    except Exception as err:
        record_algorithm_error(current_skyline_app, parent_pid, algorithm_name, traceback.format_exc())
        if print_debug:
            print('error:', traceback.format_exc())
        if debug_logging:
            current_logger.debug('debug :: error - on low_variance_anomalous_peak_trough - %s' % err)
            current_logger.debug(traceback.format_exc())
        if return_results:
            return anomalous, anomalyScore, results
        return anomalous, anomalyScore

    if return_results:
        return anomalous, anomalyScore, results
    return anomalous, anomalyScore
