"""
full_duration_timeseries_fill.py
"""
import logging
from time import time

from settings import FULL_DURATION
from functions.timeseries.determine_data_frequency import determine_data_frequency


# @added 20220407 - Feature #4518: settings - LAST_KNOWN_VALUE_NAMESPACES
#                   Feature #4520: settings - ZERO_FILL_NAMESPACES
def full_duration_timeseries_fill(self, current_skyline_app, base_name, timeseries, fill_method):
    """
    Fill missing timestamps in a Redis full duration timeseries with the fill
    method specified.

    :param self: the self object
    :param current_skyline_app: the app calling the function so the function
        knows which log to write too.
    :param base_name: the metric base_name
    :param timeseries: the timeseries as a list [(1649235260, 123),...,(1649321660, 78)]
    :param fill_method: the fill method, either zero or last_known_value
    :type self: object
    :type current_skyline_app: str
    :type base_name: str
    :type timeseries: list
    :type fill_method: str
    :return: timeseries
    :rtype: list
    """

    skyline_app_logger = '%sLog' % current_skyline_app
    logger = logging.getLogger(skyline_app_logger)

    function_str = 'functions.timeseries.full_duration_timeseries_fill'

    resolution = 0
    # Check if the resolution of the metric is known
    try:
        resolution_str = self.redis_conn_decoded.hget('analyzer.metrics_manager.resolutions', base_name)
        if resolution_str:
            resolution = int(float(resolution_str))
    except Exception as err:
        logger.error('error :: %s :: hget on analyzer.metrics_manager.resolutions failed for %s - %s' % (
            function_str, base_name, err))

    if not resolution:
        try:
            if current_skyline_app == 'analyzer':
                resolution, timestamp_resolutions_count = determine_data_frequency(current_skyline_app, timeseries, False)
                del timestamp_resolutions_count
            else:
                resolution = determine_data_frequency(current_skyline_app, timeseries, False)
        except Exception as err:
            logger.error('error :: %s :: determine_data_frequency failed for %s - %s' % (
                function_str, base_name, err))
        if resolution:
            # Add it to the Redis hash
            try:
                self.redis_conn.hset(
                    'analyzer.metrics_manager.resolutions', base_name,
                    int(resolution))
            except Exception as err:
                logger.error('error :: %s :: failed to hset on analyzer.metrics_manager.resolutions failed for %s with resolution %s - %s' % (
                    function_str, base_name, str(resolution), err))

    # @added 20220505 - Feature #4518: settings - LAST_KNOWN_VALUE_NAMESPACES
    # If there is only 1 data point in the timeseries determine_data_frequency
    # will not return a resolution so set a sensible default as it does not
    # matter, it will be boring.
    if not resolution:
        resolution = 60

    # @added 20221101 - Feature #4518: settings - LAST_KNOWN_VALUE_NAMESPACES
    #                   Feature #4520: settings - ZERO_FILL_NAMESPACES
    # Use sensible resolultions and fill from and until timestamps
    sensible_resolution_set = False
    if resolution < 60:
        resolution = 60
        sensible_resolution_set = True
    if resolution > 60:
        if resolution not in [300, 600, 900, 1800, 3600]:
            resolution = 60
            sensible_resolution_set = True
    if sensible_resolution_set:
        # Update the Redis hash
        try:
            self.redis_conn.hset(
                'analyzer.metrics_manager.resolutions', base_name,
                int(resolution))
        except Exception as err:
            logger.error('error :: %s :: failed to hset on analyzer.metrics_manager.resolutions failed for %s with resolution %s - %s' % (
                function_str, base_name, str(resolution), err))
    until_timestamp = int(int(time()) // resolution * resolution)
    from_timestamp = int((until_timestamp - FULL_DURATION) // resolution * resolution)

    # Align timestamps in the timeseries to the resolution
    aligned_timeseries = []
    error_logged = False
    for ts, value in timeseries:
        try:
            aligned_timeseries.append([int(int(ts) // resolution * resolution), value])
        except Exception as err:
            if not error_logged:
                logger.error('error :: %s :: failed to align timestamp on %s with timestamp %s and resolution %s - %s' % (
                    function_str, base_name, str(ts), str(resolution), err))
                error_logged = True

    # What is the expected number of timestamps in the period
    expected_number_of_timestamps = int((aligned_timeseries[-1][0] - aligned_timeseries[0][0]) / resolution) + 1

    # @added 20221101 - Feature #4518: settings - LAST_KNOWN_VALUE_NAMESPACES
    #                   Feature #4520: settings - ZERO_FILL_NAMESPACES
    # Fill from and until timestamps
    timeseries_dict = {}
    if fill_method == 'zero':
        expected_number_of_timestamps = int((aligned_timeseries[-1][0] - from_timestamp) / resolution) + 1
        current_timestamp = from_timestamp
        while current_timestamp < (aligned_timeseries[-1][0] + 1):
            if current_timestamp > aligned_timeseries[-1][0]:
                break
            timeseries_dict[current_timestamp] = 0
            current_timestamp = current_timestamp + resolution
        for item in aligned_timeseries:
            timeseries_dict[item[0]] = item[1]
        zero_filled_timeseries = []
        for ts in list(timeseries_dict.keys()):
            zero_filled_timeseries.append([ts, timeseries_dict[ts]])
        timeseries = list(zero_filled_timeseries)

    # All timestamps are present so just return the original timeseries
    if len(timeseries) != expected_number_of_timestamps:
        if current_skyline_app == 'analyzer_batch':
            logger.info('%s :: %s filling %s missing data points for %s' % (
                function_str, fill_method, str(int(expected_number_of_timestamps - len(timeseries))),
                base_name))
        expected_timestamps = []
        current_timestamp = aligned_timeseries[0][0]
        while current_timestamp < (aligned_timeseries[-1][0] + 1):
            if current_timestamp > aligned_timeseries[-1][0]:
                break
            expected_timestamps.append(current_timestamp)
            current_timestamp = current_timestamp + resolution
        timeseries = []
        last_known_value = aligned_timeseries[0][1]

        # @added 20230609 - Bug #4940: Inefficient full_duration_timeseries_fill - last_known_value
        # Make a dict rather than a comprehension per timestamp because
        # in an almost fully populated timeseries the list comprehension
        # per timestamp causes a lot of wait.
        # The comprehension per timestamp resulted in 41 timeseries
        # taking 26.38 seconds to process and the using the dict it took
        # 1.42 
        # 'last_known_value_fill': {'sum': 29.835193157196045, 'count': 30}
        # 'last_known_value_fill': {'sum': 32.67621088027954, 'count': 34}
        # 'last_known_value_fill': {'sum': 23.94329810142517, 'count': 34}
        # 'last_known_value_fill': {'sum': 26.383304834365845, 'count': 41}
        # 'last_known_value_fill': {'sum': 1.9686686992645264, 'count': 41}
        # 'last_known_value_fill': {'sum': 1.253911018371582, 'count': 34}
        # 'last_known_value_fill': {'sum': 1.2638599872589111, 'count': 34}
        # 'last_known_value_fill': {'sum': 1.4201478958129883, 'count': 41}
        # 'last_known_value_fill': {'sum': 2.061102867126465, 'count': 41}
        aligned_timeseries_dict = {}
        for item in aligned_timeseries:
            aligned_timeseries_dict[item[0]] = item[1]

        for ts in expected_timestamps:

            # @modified 20230609 - Bug #4940: Inefficient full_duration_timeseries_fill - last_known_value
            # Make use of the dict rather than a comprehension per timestamp
            # aligned_timeseries_item = [item for item in aligned_timeseries if item[0] == ts]
            try:
                value = aligned_timeseries_dict[ts]
                aligned_timeseries_item = (ts, value)
            except:
                aligned_timeseries_item = None


            if aligned_timeseries_item:
                timeseries.append(aligned_timeseries_item)
                last_known_value = aligned_timeseries_item[1]
            else:
                # Use tuple like normal Redis timeseries data
                if fill_method == 'zero':
                    timeseries.append((ts, 0))
                if fill_method == 'last_known_value':
                    timeseries.append((ts, last_known_value))
    else:
        if current_skyline_app == 'analyzer_batch':
            logger.info('%s :: NOT %s filling, all data points are present - %s' % (
                function_str, fill_method, base_name))

    # TODO:
    # Consider recreating the Redis metric with the filled data points.  This
    # would be the most efficient.  However if the client later submitted the
    # missing data, the Redis data would need to be deduplicated and the filled
    # values and true values would be in conflict.

    return timeseries
