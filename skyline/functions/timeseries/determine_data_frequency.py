"""
determine_data_frequency
"""
import logging
import traceback
from collections import Counter


# @added 20210419 - Feature #4014: Ionosphere - inference
#                   Branch #3590: inference
# A common pattern to determine metric_resolution
def determine_data_frequency(current_skyline_app, timeseries):
    """
    Determine the data frequency of a timeseries:

    :param current_skyline_app: the app calling the function so the function
        knows which log to write too.
    :param timeseries: the timeseries as a list [[1618761782, 123],...,[1618848182, 78]]
    :type current_skyline_app: str
    :type list: list
    :return: metric_resolution
    :rtype:  int
    """
    metric_resolution = None
    current_skyline_app_logger = current_skyline_app + 'Log'
    current_logger = logging.getLogger(current_skyline_app_logger)

    try:
        # Deal with lower frequency data
        # Determine resolution from the last 100 data points
        resolution_timestamps = []
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
            try:
                del resolution_timestamps
            except:
                pass
        if timestamp_resolutions:
            try:
                timestamp_resolutions_count = Counter(timestamp_resolutions)
                ordered_timestamp_resolutions_count = timestamp_resolutions_count.most_common()
                metric_resolution = int(ordered_timestamp_resolutions_count[0][0])
            except:
                current_logger.error(traceback.format_exc())
                current_logger.error('error :: determine_data_frequency :: failed to determine metric_resolution from timeseries')
            try:
                del timestamp_resolutions
            except:
                pass
    except:
        current_logger.error(traceback.format_exc())
        current_logger.error('error :: determine_data_frequency :: failed to determine metric_resolution from timeseries')
    return metric_resolution
