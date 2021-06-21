"""
determine_data_sparsity
"""
import logging
import traceback

from settings import FULL_DURATION
from functions.timeseries.determine_data_frequency import determine_data_frequency


# @added 20210602 - Feature #3870: metrics_manager - check_data_sparsity
#                   Feature #4076: CUSTOM_STALE_PERIOD
#                   Feature #4000: EXTERNAL_SETTINGS
#                   Branch #1444: thunder
# Prevent calculating an incorrect metric_resolution on
# periodic and infrequently sparsely populated metrics
# A common pattern to determine the metric sparsity
def determine_data_sparsity(
        current_skyline_app, timeseries, resolution=None, log=False):
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
    function_str = 'functions.timeseries.determine_data_sparsity'
    metric_resolution = resolution
    data_sparsity = None
    if log:
        current_skyline_app_logger = current_skyline_app + 'Log'
        current_logger = logging.getLogger(current_skyline_app_logger)
    else:
        current_logger = None

    if not metric_resolution:
        try:
            metric_resolution = determine_data_frequency(current_skyline_app, timeseries, log)
        except Exception as e:
            if log:
                current_logger.error('error :: %s :: determine_data_frequency failed - %s' % (
                    function_str, e))

    try:
        expected_datapoints = 0
        if metric_resolution:
            try:
                # expected_datapoints = timeseries_full_duration / metric_resolution
                expected_datapoints = FULL_DURATION / metric_resolution
            except Exception as e:
                if log:
                    current_logger.error('error :: %s :: failed to determine expected_datapoints for timeseries - %s' % (
                        function_str, e))
        if expected_datapoints and timeseries:
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
