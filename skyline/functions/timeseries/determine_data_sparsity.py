"""
determine_data_sparsity
"""
import logging
import traceback
from collections import Counter

from settings import FULL_DURATION


# @added 20210602 - Feature #3870: metrics_manager - check_data_sparsity
#                   Feature #4076: CUSTOM_STALE_PERIOD
#                   Feature #4000: EXTERNAL_SETTINGS
#                   Branch #1444: thunder
# Prevent calculating an incorrect metric_resolution on
# periodic and infrequently sparsely populated metrics
# A common pattern to determine the metric sparsity
def determine_data_sparsity(current_skyline_app, timeseries):
    """
    Determine the sparsity of a timeseries:

    :param current_skyline_app: the app calling the function so the function
        knows which log to write too.
    :param timeseries: the timeseries as a list [[1618761782, 123],...,[1618848182, 78]]
    :type current_skyline_app: str
    :type list: list
    :return: sparsity
    :rtype:  float
    """
    data_sparsity = None
    current_skyline_app_logger = current_skyline_app + 'Log'
    current_logger = logging.getLogger(current_skyline_app_logger)
    function_str = 'functions.timeseries.determine_data_sparsity'

    try:
        timeseries_full_duration = 0
        if timeseries:
            first_timeseries_timestamp = 0
            try:
                first_timeseries_timestamp = int(timeseries[0][0])
            except:
                pass
            last_timeseries_timestamp = 0
            try:
                last_timeseries_timestamp = int(timeseries[-1][0])
            except:
                pass
            timeseries_full_duration = last_timeseries_timestamp - first_timeseries_timestamp
            # @modified 20201216 - Feature #3870: metrics_manager - check_data_sparsity
            # If roomba has not pruned some data in minutes
            if first_timeseries_timestamp < (last_timeseries_timestamp - FULL_DURATION):
                timeseries_full_duration = FULL_DURATION

            # @added 20210602 - Feature #3870: metrics_manager - check_data_sparsity
            #                   Feature #4076: CUSTOM_STALE_PERIOD
            #                   Feature #4000: EXTERNAL_SETTINGS
            #                   Branch #1444: thunder
            # Prevent calculating an incorrect metric_resolution on
            # periodic and infrequently sparsely populated metrics
            if timeseries_full_duration < 3600:
                timeseries_full_duration = FULL_DURATION

        metric_resolution = 0
        if timeseries_full_duration:
            try:
                # Determine resolution from the last 30 data points
                resolution_timestamps = []

                # @modified 20201215 - Feature #3870: metrics_manager - check_data_sparsity
                # The last 30 measures for uploaded data to flux
                # seemed sufficient, however from real time metrics
                # there is more variance, so using a larger sample
                # for metric_datapoint in timeseries[-30:]:
                for metric_datapoint in timeseries[-100:]:
                    timestamp = int(metric_datapoint[0])
                    resolution_timestamps.append(timestamp)
                timestamp_resolutions = []
                if resolution_timestamps:
                    last_timestamp = None
                    for timestamp in resolution_timestamps:
                        if last_timestamp:
                            resolution = timestamp - last_timestamp
                            timestamp_resolutions.append(resolution)
                            last_timestamp = timestamp
                        else:
                            last_timestamp = timestamp
                    del resolution_timestamps
                if timestamp_resolutions:
                    try:
                        timestamp_resolutions_count = Counter(timestamp_resolutions)
                        ordered_timestamp_resolutions_count = timestamp_resolutions_count.most_common()
                        metric_resolution = int(ordered_timestamp_resolutions_count[0][0])
                    except Exception as e:
                        current_logger.error('error :: %s :: failed to determine metric_resolution from timestamp_resolutions - %s' % (
                            function_str, e))
                    del timestamp_resolutions
            except Exception as e:
                current_logger.error('error :: %s :: failed to determine metric_resolution from timeseries - %s' % (
                    function_str, e))
            expected_datapoints = 0

            # @added 20201215 - Feature #3870: metrics_manager - check_data_sparsity
            # Handle the slight variances that occur in real time
            # metric streams
            if metric_resolution in range(57, 63):
                metric_resolution = 60
            if metric_resolution:
                try:
                    expected_datapoints = timeseries_full_duration / metric_resolution
                except Exception as e:
                    current_logger.error('error :: %s :: failed to determine expected_datapoints for timeseries - %s' % (
                        function_str, e))
            if expected_datapoints:
                try:
                    datapoints_present = len(timeseries)
                    data_sparsity = round(datapoints_present / expected_datapoints * 100, 2)
                except Exception as e:
                    current_logger.error('error :: %s :: an error occurred during check_data_sparsity - %s' % (
                        function_str, e))
    except Exception as e:
        current_logger.error(traceback.format_exc())
        current_logger.error('error :: %s :: failed to determine sparsity from timeseries - %s' % (
            function_str, e))
    return data_sparsity
