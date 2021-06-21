"""
determine_data_frequency
"""
import logging
import traceback
from collections import Counter

# @added 20210619 - Feature #4148: analyzer.metrics_manager.resolutions
#                   Bug #4146: check_data_sparsity - incorrect on low fidelity and inconsistent metrics
#                   Feature #3870: metrics_manager - check_data_sparsity
from settings import FULLY_POPULATED_PERCENTAGE


# @added 20210419 - Feature #4014: Ionosphere - inference
#                   Branch #3590: inference
# A common pattern to determine metric_resolution
def determine_data_frequency(current_skyline_app, timeseries, log=False):
    """
    Determine the data frequency of a timeseries:

    :param current_skyline_app: the app calling the function so the function
        knows which log to write too.
    :param timeseries: the timeseries as a list [[1618761782, 123],...,[1618848182, 78]]
    :param log: whether to log or not, optional, defaults to False
    :type current_skyline_app: str
    :type timeseries: list
    :type log: boolean
    :return: metric_resolution
    :rtype:  int
    """
    function_str = 'functions.timeseries.determine_data_frequency'
    metric_resolution = None
    timestamp_resolutions_count = {}
    current_logger = None
    if log:
        current_skyline_app_logger = current_skyline_app + 'Log'
        current_logger = logging.getLogger(current_skyline_app_logger)

    try:
        # Deal with lower frequency data
        # Determine resolution from the last 100 data points
        resolution_timestamps = []
        # @modified 20210619 - Bug #4146: check_data_sparsity - incorrect on low fidelity and inconsistent metrics
        # for metric_datapoint in timeseries[-100:]:
        for metric_datapoint in timeseries[-101:]:
            timestamp = int(metric_datapoint[0])
            resolution_timestamps.append(timestamp)

        # @added 20210618 - Bug #4146: check_data_sparsity - incorrect on low fidelity and inconsistent metrics
        # Identify and handle low fidelity, inconsistent and
        # sparsely populated metrics
        fully_populated_sample = False

        # @modified 20210619 - Feature #4148: analyzer.metrics_manager.resolutions
        #                      Bug #4146: check_data_sparsity - incorrect on low fidelity and inconsistent metrics
        #                      Feature #3870: metrics_manager - check_data_sparsity
        # if len(resolution_timestamps) > 94:
        if len(resolution_timestamps) > FULLY_POPULATED_PERCENTAGE:
            fully_populated_sample = True
            if log:
                current_logger.info('%s :: %s :: timeseries sample is fully populated' % (
                    current_skyline_app, function_str))

        # If the sample is not fully populated use the whole
        # time series
        if not fully_populated_sample:
            if log:
                current_logger.info('%s :: %s :: timeseries sample is not fully populated, using entire timeseries' % (
                    current_skyline_app, function_str))
            resolution_timestamps = []
            for metric_datapoint in timeseries:
                timestamp = int(metric_datapoint[0])
                resolution_timestamps.append(timestamp)
        if log:
            current_logger.info('%s :: %s :: calculating resolution from %s timestamp' % (
                current_skyline_app, function_str, str(len(resolution_timestamps))))

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
            try:
                del resolution_timestamps
            except:
                pass
        if timestamp_resolutions:
            timestamp_resolutions_count = {}
            try:
                timestamp_resolutions_count = Counter(timestamp_resolutions)
                if log:
                    current_logger.info('%s :: %s :: timestamp_resolutions_count: %s' % (
                        current_skyline_app, function_str,
                        str(dict(timestamp_resolutions_count))))
                ordered_timestamp_resolutions_count = timestamp_resolutions_count.most_common()
                metric_resolution = int(ordered_timestamp_resolutions_count[0][0])
                if log:
                    current_logger.info('%s :: %s :: resolution determined as %s' % (
                        current_skyline_app, function_str, str(resolution)))
            except Exception as e:
                if log:
                    current_logger.error(traceback.format_exc())
                    current_logger.error('error :: %s :: functions.timeseries.determine_data_frequency :: failed to determine metric_resolution from timeseries - %s' % (
                        current_skyline_app, e))
            try:
                del timestamp_resolutions
            except:
                pass

            # @added 20210617 - Bug #4146: check_data_sparsity - incorrect on low fidelity and inconsistent metrics
            #                   Feature #4144: webapp - stale_metrics API endpoint
            #                   Feature #4076: CUSTOM_STALE_PERIOD
            #                   Branch #1444: thunder
            if timestamp_resolutions_count:

                # @added 20210618 - Bug #4146: check_data_sparsity - incorrect on low fidelity and inconsistent metrics
                # Identify low fidelity and inconsistent
                # frequency metrics and set the metric
                # resolution according
                if len(dict(timestamp_resolutions_count)) > 1:
                    timestamp_resolutions_count_dict = dict(timestamp_resolutions_count)
                    timestamp_resolutions = []
                    resolution_counts = []
                    for resolution in list(timestamp_resolutions_count_dict.keys()):
                        timestamp_resolutions.append([resolution, timestamp_resolutions_count_dict[resolution]])
                        resolution_counts.append(timestamp_resolutions_count_dict[resolution])

                    # If there is equal counts for each resolution
                    # use the highest resolution/frequency
                    if len(list(set(resolution_counts))) == 1:
                        sorted_timestamp_resolutions = sorted(timestamp_resolutions, key=lambda x: (x[1], x[0]))
                        metric_resolution = sorted_timestamp_resolutions[0][0]
                        if log:
                            current_logger.info('%s :: %s :: equal resolution counts - resolution adjusted to %s' % (
                                current_skyline_app, function_str, str(resolution)))

                    # If there are multiple resolutions/frequencies
                    # set the metric resolution to the
                    # highest resolution with the highest
                    # count, e.g,
                    # {2700: 4, 900: 25, 1500: 6, 1200: 4, 3300: 1, 300: 25, 600: 22, 1800: 4, 2400: 1, 3600: 1}
                    # Should be set to 300
                    if len(list(set(resolution_counts))) > 1:
                        max_count = max([count for resolution, count in ordered_timestamp_resolutions_count])
                        metric_resolution = min([resolution for resolution, count in ordered_timestamp_resolutions_count if count == max_count])
                        if log:
                            current_logger.info('%s :: %s :: multiple resolution counts - resolution adjusted to %s' % (
                                current_skyline_app, function_str, str(resolution)))

        # @added 20201215 - Feature #3870: metrics_manager - check_data_sparsity
        # Handle the slight variances that occur in real time
        # metric streams
        if metric_resolution and metric_resolution != 60:
            if metric_resolution in range(51, 69):
                metric_resolution = 60
                if log:
                    current_logger.info('%s :: %s :: resolution in range(51, 69) - resolution adjusted to %s' % (
                        current_skyline_app, function_str, str(resolution)))

    except Exception as e:
        if log:
            current_logger.error(traceback.format_exc())
            current_logger.error('error :: %s :: functions.timeseries.determine_data_frequency :: failed to determine metric_resolution from timeseries - %s' % (
                current_skyline_app, e))

    if current_skyline_app == 'analyzer':
        return metric_resolution, dict(timestamp_resolutions_count)

    return metric_resolution
