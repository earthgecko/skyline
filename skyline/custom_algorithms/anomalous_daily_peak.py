"""
anomalous_daily_peak.py
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
import numpy as np
from scipy.signal import find_peaks

from functions.numpy.percent_different import get_percent_different

# The name of the function MUST be the same as the name declared in
# settings.CUSTOM_ALGORITHMS.
# It MUST have 3 parameters:
# current_skyline_app, timeseries, algorithm_parameters
# See https://earthgecko-skyline.readthedocs.io/en/latest/algorithms/custom-algorithms.html
# for a full explanation about each.
# ALWAYS WRAP YOUR ALGORITHM IN try and except


# @added 20221105 - Feature #4724: custom_algorithms - anomalous_daily_peak
def anomalous_daily_peak(current_skyline_app, parent_pid, timeseries, algorithm_parameters):

    """
    A 7 day timeseries is NOT anomalous if the last datapoints are in a peak whose
    values are within 3 standard deviations of the mean of x number of similar
    peaks (+/- 20%) that occurred in windows that are 24 hours apart.

    Only timeseries that are thought to be anomalous should be run through this
    algorithm.  It is meant to be run in Mirage after all algorithms, including
    custom algorithms have been run and found a metric to be ANOMALOUS.  This
    algorithm does a final check to see in the anomaly is repetetive daily
    pattern which occurs more than x times a week.

    Be aware that this algorithm will identify ANY datapoint that is NOT in a
    repetitive peak and within 3-sigma (+/- 20%) of the other peaks as ANOMALOUS.

    :param current_skyline_app: the Skyline app executing the algorithm.  This
        will be passed to the algorithm by Skyline.  This is **required** for
        error handling and logging.  You do not have to worry about handling the
        argument in the scope of the custom algorithm itself,  but the algorithm
        must accept it as the first agrument.
    :param parent_pid: the parent pid which is executing the algorithm, this is
        **required** for error handling and logging.  You do not have to worry
        about handling this argument in the scope of algorithm, but the
        algorithm must accept it as the second argument.
    :param timeseries: the time series as a list e.g. ``[[1667608854, 1269121024.0],
        [1667609454, 1269174272.0], [1667610054, 1269174272.0]]``
    :param algorithm_parameters: a dictionary of any required parameters for the
        custom_algorithm and algorithm itself.  For the anomalous_daily_peak
        custom algorithm no specific algorithm_parameters are required apart
        from an empty dict, example: ``algorithm_parameters={}``.  But the
        number_of_daily_peaks can be passed define how many peaks must exist in
        the window period to be classed as normal.  If this is set to 3 and say
        that we are checking a possible anomaly at 00:05, there need to be 3
        peaks that occur over the past 7 days in the dialy 23:35 to 00:05 window
        if there are not at least 3 then this is considered as anomalous.
        ``algorithm_parameters={'number_of_daily_peaks': 3}``
    :type current_skyline_app: str
    :type parent_pid: int
    :type timeseries: list
    :type algorithm_parameters: dict
    :return: anomalous, anomalyScore
    :rtype: tuple(boolean, float)

    """

    # You MUST define the algorithm_name
    algorithm_name = 'anomalous_daily_peak'

    # Define the default state of None and None, anomalous does not default to
    # False as that is not correct, False is only correct if the algorithm
    # determines the data point is not anomalous.  The same is true for the
    # anomalyScore.
    anomalous = None
    anomalyScore = None

    current_logger = None

    # If you wanted to log, you can but this should only be done during
    # testing and development
    def get_log(current_skyline_app):
        current_skyline_app_logger = current_skyline_app + 'Log'
        current_logger = logging.getLogger(current_skyline_app_logger)
        return current_logger

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
            return (None, None)

    print_debug = False
    try:
        print_debug = algorithm_parameters['print_debug']
    except:
        print_debug = False

    number_of_daily_peaks = 3
    try:
        number_of_daily_peaks = algorithm_parameters['number_of_daily_peaks']
        if print_debug:
            print("algorithm_parameters['number_of_daily_peaks']:", number_of_daily_peaks)
    except:
        number_of_daily_peaks = 3

    within_percent_of_normal_peaks = 0.0
    try:
        within_percent_of_normal_peaks = algorithm_parameters['within_percent_of_normal_peaks']
        if print_debug:
            print("algorithm_parameters['within_percent_of_normal_peaks']:", within_percent_of_normal_peaks)
    except:
        within_percent_of_normal_peaks = 0.0

    after_peak = True
    try:
        after_peak = algorithm_parameters['after_peak']
        if print_debug:
            print("algorithm_parameters['after_peak']:", after_peak)
    except:
        after_peak = True

    # @added 20230424 - Feature #4724: custom_algorithms - anomalous_daily_peak
    # Added expiry to record metrics identified as normal by anomalous_daily_peaks
    # to allow Mirage to set a key in a hash and to allow Analyzer labelled_metrics
    # to skip analysis of those metrics in their expiry period.  This is to reduce
    # metrics that are experiencing a normal anomalous daily peak to not have to
    # be analysed by Analyzer every run and pushed to Mirage to check as this
    # results in Mirage getting lots of unnecessary checks which caused feedback
    # for the period in question.
    expiry = 180
    try:
        expiry = algorithm_parameters['expiry']
        if print_debug:
            print("algorithm_parameters['expiry']:", expiry)
    except:
        expiry = 180

    try:
        timestamps = [item[0] for item in timeseries]
        values = [item[1] for item in timeseries]
        np_timestamps = np.array(timestamps)
        ts_diffs = np.diff(np_timestamps)
        resolution_counts = np.unique(ts_diffs, return_counts=True)
        resolution = resolution_counts[0][np.argmax(resolution_counts[1])]
        if resolution > 900:
            # Not suited to low resolution data
            return (True, 1.0)
        duration = timestamps[-1] - timestamps[0]
        if duration < 446400:
            # Not suitable for less than 5.25 days worth of data
            return (True, 1.0)

        anomaly_timestamp = int(timestamps[-1])
        y = np.array(values)
        mean = np.mean(y)

        # @added 20230323 - Feature #4724: custom_algorithms - anomalous_daily_peak
        # Remove non aligned peaks.
        # Before surfacing peaks, set all values that do not occur in the daily
        # windows as the mean value.  This removes peaks that are not periodic
        # in a 24 hour period and if the data that remains in the daily windows
        # are peaks, they will be identified as such.  This mitigates large
        # spikes that are not aligned in a 24 hour window from not suppressing
        # daily peaks that do occur and align.  The algorithm is only concerned
        # with windows of 4 datapoints that are separated by ~24 hours, not any
        # large peaks that occurred at any other times.
        remove_non_aligned_peaks = True
        if remove_non_aligned_peaks:
            daily_peak_timestamps = []
            for i in list(range(1, 7)):
                t = anomaly_timestamp - (86400 * i)
                daily_peak_timestamps.append(t)
                for ii in list(range(1, 4)):
                    tt = t - (resolution * ii)
                    daily_peak_timestamps.append(tt)
            daily_peak_timestamps.append(anomaly_timestamp)
            for i in list(range(1, 4)):
                tt = anomaly_timestamp - (resolution * i)
                daily_peak_timestamps.append(tt)
            daily_peak_timestamps = sorted(daily_peak_timestamps)
            non_daily_peaks_removed_timeseries = []
            for ts, v in timeseries:
                if ts < anomaly_timestamp:
                    if ts not in daily_peak_timestamps:
                        v = mean
                non_daily_peaks_removed_timeseries.append([ts, v])
            adjusted_values = [item[1] for item in non_daily_peaks_removed_timeseries]
            y = np.array(adjusted_values)
            values = [item[1] for item in non_daily_peaks_removed_timeseries]

        # pad the trailing end of the timeseries otherwise
        # if the final peak is at the very end and does
        # not drop off it will not be identified as a peak
        padded_y = list(values)
        for i in list(range(0, 7)):
            padded_y.append(mean)
        y = np.array(padded_y)

        # The distance should cover a 23 hour period, meaning the peaks should
        # be at least 23 hours apart.  Do not that find_peaks WILL return peaks
        # even if there are none and the values in the distance period are all
        # 0s, this is handled later.
        distance = int((3600 / resolution) * 23)
        # peaks, _ = find_peaks(y, distance=(6 * 23))
        peaks, _ = find_peaks(y, distance=distance)

        if print_debug:
            print('number of peaks:', len(peaks))
        if debug_logging:
            current_logger.debug('debug :: %s :: number of peaks: %s' % (
                algorithm_name, str(len(peaks))))

        anomaly_index = [index for index, ts in enumerate(timestamps) if ts == anomaly_timestamp][0]
        anomaly_peak_index = int(anomaly_index)

        # Allow the anomaly to be a few datapoints either side of the peak
        # because often an anomaly will still trigger after a peak on the
        # downside of the peak.
        allow_for = 6
        if resolution > 600:
            allow_for = 4
        anomaly_index_in_window = False
        if anomaly_index in peaks:
            anomaly_index_in_window = True
        if anomaly_index not in peaks:
            for index in peaks:
                for ii in list(range((allow_for * -1), -1)):
                    if anomaly_index == (index + ii):
                        anomaly_index_in_window = True
                        anomaly_peak_index = index
                        break
                for ii in list(range(1, allow_for)):
                    if anomaly_index == (index + ii):
                        anomaly_index_in_window = True
                        anomaly_peak_index = index
                        break
        if anomaly_index not in peaks and not anomaly_index_in_window:
            return (True, 1.0)

        timestamps_dict = {}
        for index, item in enumerate(timeseries):
            timestamps_dict[item[0]] = index
        timestamps_reversed = [item[0] for item in timeseries]
        timestamps_reversed.reverse()

        # Define the peak periods as 3 data points before the peak, the peak
        # itself and the 3 data points after the peak, from which the 4
        # consecutive values with the largest sum will be considered the peak
        peak_periods = {}
        peak_timestamps = []
        peak_period_indices = {}
        last_peak = list(peaks)[-1]
        use_old_method = False
        for index, i in enumerate(list(peaks)):
            period_indices = []
            if i == last_peak:
                period_indices = list(range((i - 3), (i + 1)))
            else:
                period_indices = list(range((i - 3), (i + 4)))
            if use_old_method:
                if after_peak:
                    if i == last_peak:
                        for behind in list(range(-3, 0)):
                            period_indices.append((i + behind))
                    period_indices.append(i)
                    if i != last_peak:
                        for ahead in list(range(1, 4)):
                            period_indices.append((i + ahead))
                else:
                    for behind in list(range(-3, 0)):
                        period_indices.append((i + behind))
                    period_indices.append(i)
            peak_period_indices[index] = period_indices
            peak_periods[index] = []
        for ts in timestamps_reversed:
            ts_index = timestamps_dict[ts]
            for index in list(peak_period_indices.keys()):
                peak_period_timestamp_indices = peak_period_indices[index]
                if ts_index in peak_period_timestamp_indices:
                    peak_periods[index].append([ts, timeseries[ts_index][1]])
                    if ts_index in peaks:
                        peak_timestamps.append(ts)
                    break

        # Ensure that each peak is within ~24 hours of each other
        diffs = list(np.diff(peak_timestamps) * -1)
        window = list(range(83000, 89000))
        daily_peaks = []
        for index, time_diff in enumerate(diffs):
            # if time_diff in window:
            if time_diff in window or peak_timestamps[index] == anomaly_timestamp:
                daily_peaks.append(peak_timestamps[index])

        if print_debug:
            print('daily_peaks:', len(daily_peaks))
        if debug_logging:
            current_logger.debug('debug :: %s :: daily_peaks: %s' % (
                algorithm_name, str(len(daily_peaks))))

        # if len(daily_peaks) < 5:
        if len(daily_peaks) < number_of_daily_peaks:
            return (True, 1.0)

        anomaly_peak_ts = daily_peaks[-1]
        last_peak_periods_index = []
        for index in list(peak_periods.keys()):
            period_timestamps = [item[0] for item in peak_periods[index]]
            if anomaly_peak_ts in period_timestamps:
                target_peak_value_sum = sum([item[1] for item in peak_periods[index]])
        largest_sum_method = False
        closest_sum_method = True

        # Calculate the sum of the values in each peak period
        peak_period_values = {}
        for peak_ts in reversed(daily_peaks):
            for index in list(peak_periods.keys()):
                period_timestamps = [item[0] for item in peak_periods[index]]
                if peak_ts not in period_timestamps:
                    continue

                values = [item[1] for item in peak_periods[index]]
                values_sum = sum([item[1] for item in peak_periods[index]])
                # Determine which combination of 4 consecutive values result out
                # of the 7 result in the largest sum, which is considered the
                # peak
                if largest_sum_method:
                    window_sum = sum(values[:4])
                    max_sum = window_sum
                    max_index = 0
                    for i in range(1, len(values)-3):
                        window_sum = window_sum - values[i-1] + values[i+3]
                        if window_sum > max_sum:
                            max_sum = window_sum
                            max_index = i
                    selected_peak_values = values[max_index:max_index+4]

                # Determine which combination of 4 consecutive values out of the
                # 7 values, result in the sum closest to the sum of the anomaly
                # peak period, which is considered the instance peak.  This is
                # due to the fact that the anomaly peak period is defined as the
                # sum of the 3 data points BEFORE the anomaly datapoint plus the
                # anomaly datapoint, with no datapoints after the anomaly being
                # consider.  However each other peak period has 3 before, the
                # peak and 3 after.  Therefore this method allows for finding
                # the peak range sum in a peak of the sum of 4 consecutive
                # datapoints which are closest to the anomaly peak sum.
                # Methods have been tested using only values before the peak,
                # only values after the peak, but this method provides the most
                # robust method of having 0 values either in the before or after
                # period, which do occur and can skew the sums undesirably.
                if closest_sum_method:
                    window_sum = sum(values[:4])
                    closest_sum = window_sum
                    closest_values = values[:4]
                    for i in range(1, len(values)-3):
                        window_sum = window_sum - values[i-1] + values[i+3]
                        if abs(window_sum - target_peak_value_sum) < abs(closest_sum - target_peak_value_sum):
                            closest_sum = window_sum
                            closest_values = values[i:i+4]
                    selected_peak_values = closest_values

                values_sum = sum(selected_peak_values)

                # Becasue scipy will periods even if there are no peaks,
                # which can result in the values_sum equalling 0, if
                # this happens do not add the peak
                if values_sum == 0:
                    continue

                if peak_ts in period_timestamps:
                    peak_period_values[index] = {}
                    peak_period_values[index]['peak_ts'] = peak_ts
                    peak_period_values[index]['peak_value'] = [item[1] for item in peak_periods[index] if item[0] == peak_ts][0]
                    # peak_period_values[index]['values'] = [item[1] for item in peak_periods[index]]
                    peak_period_values[index]['values'] = selected_peak_values
                    peak_period_values[index]['values_sum'] = values_sum
        
        if print_debug:
            print('daily_peaks (after 0 sum peaks removed):', len(peak_period_values))
        if debug_logging:
            current_logger.debug('debug :: %s :: daily_peaks (after 0 sum peaks removed): %s' % (
                algorithm_name, str(len(peak_period_values))))

        if len(peak_period_values) < number_of_daily_peaks:
            if print_debug:
                print('peak_period_values only has %s items before calculating peak_values' % str(len(peak_period_values)))
            if debug_logging:
                current_logger.debug('debug :: %s :: peak_period_values only has %s items before calculating peak_values' % (
                    algorithm_name, str(len(peak_period_values))))
            return (True, 1.0)

        peak_value_sums = {}
        peak_values = []
        anomaly_peak_values_sum = 0
        anomaly_peak_index_timestamp = timeseries[anomaly_peak_index][0]

        # Calculate the sum of the values in each peak period excl the anomalous
        # peak period, which is calculated individually
        for index in list(peak_period_values.keys()):
            peak_value_sums[index] = {
                'peak_ts': peak_period_values[index]['peak_ts'],
                'values_sum': peak_period_values[index]['values_sum'],
            }
            # if peak_period_values[index]['peak_ts'] != anomaly_timestamp:
            if peak_period_values[index]['peak_ts'] != anomaly_peak_index_timestamp:
                peak_values.append(peak_period_values[index]['values_sum'])
            else:
                anomaly_peak_values_sum = peak_period_values[index]['values_sum']

        if len(peak_period_values) < number_of_daily_peaks:
            if print_debug:
                print('peak_period_values only has %s items' % str(len(peak_period_values)))
            if debug_logging:
                current_logger.debug('debug :: %s :: peak_period_values only has %s items' % (
                    algorithm_name, str(len(peak_period_values))))
            return (True, 1.0)

        if len(peak_values) < number_of_daily_peaks:
            if print_debug:
                print('peak_values only has %s items' % str(len(peak_values)))
            if debug_logging:
                current_logger.debug('debug :: %s :: peak_values only has %s items' % (
                    algorithm_name, str(len(peak_values))))
            return (True, 1.0)

        # Determine the mean and stdDev of the sums of each peak period (excl
        # the anomalous peak period)
        peak_values_mean = np.mean(peak_values)
        peak_values_stdDev = np.std(peak_values)

        peak_difference = anomaly_peak_values_sum - peak_values_mean
        threesigma3_peak_values_stdDev = 3 * peak_values_stdDev

        if anomaly_peak_values_sum == 0:
            debug_dict = {
                'peak_difference': peak_difference,
                'peak_period_values': peak_period_values,
                'peak_values': peak_values,
                'anomalous': anomalous,
                'anomaly_peak_values_sum': anomaly_peak_values_sum,
                'peak_values_mean': peak_values_mean,
                'peak_values_stdDev': peak_values_stdDev,
                '3 * peak_values_stdDev': threesigma3_peak_values_stdDev,
            }
            if print_debug:
                print('anomaly_peak_values_sum is 0')
                print(debug_dict)
            if debug_logging:
                current_logger.debug('debug :: %s :: anomaly_peak_values_sum is 0, debug_dict: %s' % (
                    algorithm_name, str(debug_dict)))
            return (True, 1.0)

        # The algorithm
        anomalous = abs(peak_difference) > threesigma3_peak_values_stdDev

        percent_different = 0
        if within_percent_of_normal_peaks and anomalous:
            percent_different = get_percent_different(abs(peak_difference), threesigma3_peak_values_stdDev, False)
            if percent_different is None:
                percent_different = 100
            if percent_different < 0:
                new_pdiff = percent_different * -1
                percent_different = new_pdiff
            else:
                percent_different = float(percent_different)
            if percent_different <= within_percent_of_normal_peaks:
                anomalous = False

        debug_dict = {
            'peak_difference': peak_difference,
            'peak_period_values': peak_period_values,
            'peak_values': peak_values,
            'anomalous': anomalous,
            'anomaly_peak_values_sum': anomaly_peak_values_sum,
            'peak_values_mean': peak_values_mean,
            'peak_values_stdDev': peak_values_stdDev,
            '3 * peak_values_stdDev': threesigma3_peak_values_stdDev,
        }
        debug_dict['(anomaly_peak_values_sum - peak_values_mean)'] = anomaly_peak_values_sum - peak_values_mean
        if within_percent_of_normal_peaks:
            debug_dict['within_percent_of_normal_peaks'] = within_percent_of_normal_peaks
            debug_dict['percent_different'] = percent_different

        if print_debug:
            print(debug_dict)

        if debug_logging:
            current_logger.debug('debug :: %s :: debug_dict: %s' % (
                algorithm_name, str(debug_dict)))

        if anomalous:
            anomalyScore = 1.0
        else:
            anomalyScore = 0.0
    except StopIteration:
        # This except pattern MUST be used in ALL custom algortihms to
        # facilitate the traceback from any errors.  The algorithm we want to
        # run super fast and without spamming the log with lots of errors.
        # But we do not want the function returning and not reporting
        # anything to the log, so the pythonic except is used to "sample" any
        # algorithm errors to a tmp file and report once per run rather than
        # spewing tons of errors into the log e.g. analyzer.log
        return (None, None)
    except:
        record_algorithm_error(current_skyline_app, parent_pid, algorithm_name, traceback.format_exc())
        # Return None and None as the algorithm could not determine True or False
        return (None, None)

    return (anomalous, anomalyScore)
