"""
get_analysed_events.py
"""
import logging
from time import time

from skyline_functions import get_redis_conn_decoded, get_graphite_metric
from functions.redis.get_metric_timeseries import get_metric_timeseries
from functions.timeseries.determine_data_frequency import determine_data_frequency


# @added 20220503 - Feature #4530: namespace.analysed_events
def get_metric_analysed_events(
        current_skyline_app, base_name, from_timestamp, until_timestamp):
    """
    Return a dict with the number of analysed events for a metric.

    :param current_skyline_app: the app calling the function
    :param base_name: the base_name of the metric
    :param from_timestamp: get analsed events from
    :param until_timestamp: get analsed events until
    :type current_skyline_app: str
    :type base_name: str
    :type from_timestamp: int
    :type until_timestamp: int
    :return: analysed_events
    :rtype: dict

    """

    function_str = 'functions.metrics.get_metric_analysed_events'
    analysed_events = {}
    analysed_events['metric'] = base_name
    analysed_events['from'] = from_timestamp
    analysed_events['until'] = until_timestamp
    now = int(time())
    last_day_timestamp = now - 86400
    analysed_count = 0

    current_skyline_app_logger = current_skyline_app + 'Log'
    current_logger = logging.getLogger(current_skyline_app_logger)
    current_logger.info('%s :: %s :: determining analysed events for %s from: %s, until: %s' % (
        current_skyline_app, function_str, base_name, from_timestamp,
        until_timestamp))

    try:
        redis_conn_decoded = get_redis_conn_decoded(current_skyline_app)
    except Exception as err:
        current_logger.error('error :: %s :: get_redis_conn_decoded failed - %s' % (
            function_str, err))

    resolution = 60
    resolution_str = None
    try:
        resolution_str = redis_conn_decoded.hget('analyzer.metrics_manager.resolutions', base_name)
    except Exception as err:
        current_logger.error('error :: %s :: %s :: get_graphite_metric failed - %s' % (
            current_skyline_app, function_str, str(err)))
    if resolution_str:
        try:
            resolution = int(float(resolution_str))
        except Exception as err:
            current_logger.error('error :: %s :: failed to determine resolution from %s - %s' % (
                function_str, resolution_str, err))
    resolution = max(resolution, 60)

    long_duration_timeseries = []
    if from_timestamp < last_day_timestamp:
        try:
            long_duration_timeseries = get_graphite_metric(current_skyline_app, base_name, from_timestamp, until_timestamp, 'list', 'object')
        except Exception as err:
            current_logger.error('error :: %s :: %s :: get_graphite_metric failed - %s' % (
                current_skyline_app, function_str, str(err)))

    # Count data older than today
    if long_duration_timeseries:
        full_resolution = 600
        try:
            full_resolution = determine_data_frequency(current_skyline_app, long_duration_timeseries, False)
        except Exception as err:
            current_logger.error('error :: %s :: determine_data_frequency failed to determine resolution of Graphite data - %s' % (
                function_str, err))
        for item in long_duration_timeseries:
            if int(item[0]) < last_day_timestamp:
                if full_resolution > resolution:
                    analysed_count += int(full_resolution / resolution)
                else:
                    analysed_count += 1
            else:
                break

    timeseries = []
    try:
        timeseries = get_metric_timeseries(current_skyline_app, base_name)
    except Exception as err:
        current_logger.error('error :: %s :: get_metric_timeseries failed - %s' % (
            function_str, err))

    if timeseries:
        for item in timeseries:
            if int(item[0]) >= last_day_timestamp:
                analysed_count += 1

    analysed_events['analyzer'] = analysed_count
    analysed_events['total'] = analysed_count
    boundary_metric = None
    try:
        boundary_metric = redis_conn_decoded.hget('metrics_manager.boundary_metrics', base_name)
    except Exception as err:
        current_logger.error('error :: %s :: %s :: Redis hget failed - %s' % (
            current_skyline_app, function_str, str(err)))
    if boundary_metric:
        analysed_events['boundary'] = analysed_count
        analysed_events['total'] = analysed_count * 2

    if analysed_count == 0:
        analysed_events = {}

    return analysed_events
