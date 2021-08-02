from __future__ import division
import numpy as np
# @modified 20210518 - Support #4026: Change from scipy array to numpy array
# import scipy
import traceback
import logging
from time import time
from redis import StrictRedis

import sys
import os.path
sys.path.append(os.path.join(os.path.dirname(os.path.realpath(__file__)), os.pardir))
sys.path.insert(0, os.path.dirname(__file__))

# @modified 20200122 - Branch #3262: py3
# This prevents flake8 E402 - module level import not at top of file
if True:
    from settings import (
        MAX_TOLERABLE_BOREDOM,
        MIN_TOLERABLE_LENGTH,
        STALE_PERIOD,
        REDIS_SOCKET_PATH,
        BOREDOM_SET_SIZE,
        ENABLE_BOUNDARY_DEBUG,
        REDIS_PASSWORD,
        # @added 20210518 - Feature #4076: CUSTOM_STALE_PERIOD
        CUSTOM_STALE_PERIOD,
        # @added 20210603 - Feature #4000: EXTERNAL_SETTINGS
        EXTERNAL_SETTINGS,
    )
    from algorithm_exceptions import (TooShort, Stale, Boring)
    # @added 20210518 - Feature #4076: CUSTOM_STALE_PERIOD
    from functions.settings.get_custom_stale_period import custom_stale_period

skyline_app = 'boundary'
skyline_app_logger = '%sLog' % skyline_app
logger = logging.getLogger(skyline_app_logger)
# @modified 20180519 - Feature #2378: Add redis auth to Skyline and rebrow
if REDIS_PASSWORD:
    redis_conn = StrictRedis(password=REDIS_PASSWORD, unix_socket_path=REDIS_SOCKET_PATH)
else:
    redis_conn = StrictRedis(unix_socket_path=REDIS_SOCKET_PATH)


def boundary_no_mans_land():
    """
    This is no man's land. Do anything you want in here, as long as you return a
    boolean that determines whether the input timeseries is anomalous or not.

    To add an algorithm, define it here, and add its name to
    :mod:`settings.BOUNDARY_ALGORITHMS`.
    """
    return True


def autoaggregate_ts(timeseries, autoaggregate_value):
    """
    This is a utility function used to autoaggregate a timeseries.  If a
    timeseries data set has 6 datapoints per minute but only one data value
    every minute then autoaggregate will aggregate every autoaggregate_value.
    """
    if ENABLE_BOUNDARY_DEBUG:
        logger.debug('debug :: autoaggregate_ts at %s seconds' % str(autoaggregate_value))

    aggregated_timeseries = []

    if len(timeseries) < 60:
        if ENABLE_BOUNDARY_DEBUG:
            logger.debug('debug :: autoaggregate_ts - timeseries less than 60 datapoints, TooShort')
        raise TooShort()

    int_end_timestamp = int(timeseries[-1][0])
    last_hour = int_end_timestamp - 3600
    last_timestamp = int_end_timestamp
    next_timestamp = last_timestamp - int(autoaggregate_value)
    start_timestamp = last_hour

    if ENABLE_BOUNDARY_DEBUG:
        logger.debug('debug :: autoaggregate_ts - aggregating from %s to %s' % (str(start_timestamp), str(int_end_timestamp)))

    valid_timestamps = False
    try:
        valid_timeseries = int_end_timestamp - start_timestamp
        if valid_timeseries == 3600:
            valid_timestamps = True
    except Exception as e:
        logger.error('Algorithm error: %s' % traceback.format_exc())
        logger.error('error: %s' % e)
        aggregated_timeseries = []
        return aggregated_timeseries

    if valid_timestamps:
        try:
            # Check sane variables otherwise we can just hang here in a while loop
            while int(next_timestamp) > int(start_timestamp):
                # @modified 20210420 - Support #4026: Change from scipy array to numpy array
                # Deprecation of scipy.array
                # value = np.sum(scipy.array([int(x[1]) for x in timeseries if x[0] <= last_timestamp and x[0] > next_timestamp]))
                value = np.sum(np.array([int(x[1]) for x in timeseries if x[0] <= last_timestamp and x[0] > next_timestamp]))

                aggregated_timeseries += ((last_timestamp, value),)
                last_timestamp = next_timestamp
                next_timestamp = last_timestamp - autoaggregate_value
            aggregated_timeseries.reverse()
            return aggregated_timeseries
        except Exception as e:
            logger.error('Algorithm error: %s' % traceback.format_exc())
            logger.error('error: %s' % e)
            aggregated_timeseries = []
            return aggregated_timeseries
    else:
        logger.error('could not aggregate - timestamps not valid for aggregation')
        aggregated_timeseries = []
        return aggregated_timeseries


def less_than(timeseries, metric_name, metric_expiration_time, metric_min_average, metric_min_average_seconds, metric_trigger):
    # timeseries, metric_name, metric_expiration_time, metric_min_average,
    # metric_min_average_seconds, metric_trigger, autoaggregate,
    # autoaggregate_value):
    """
    A timeseries is anomalous if the datapoint is less than metric_trigger
    """
    # @modified 20190312 - Task #2862: Allow Boundary to analyse short time series
    #                      https://github.com/earthgecko/skyline/issues/88
    # if len(timeseries) < 10:
    if len(timeseries) < 1:
        return False

    if timeseries[-1][1] < metric_trigger:
        if ENABLE_BOUNDARY_DEBUG:
            logger.debug('debug :: less_than - %s less_than %s' % (
                str(timeseries[-1][1]), str(metric_trigger)))
        return True

    return False


def greater_than(timeseries, metric_name, metric_expiration_time, metric_min_average, metric_min_average_seconds, metric_trigger):
    """
    A timeseries is anomalous if the datapoint is greater than metric_trigger
    """
    # @modified 20190312 - Task #2862: Allow Boundary to analyse short time series
    #                      https://github.com/earthgecko/skyline/issues/88
    # if len(timeseries) < 10:
    if len(timeseries) < 1:
        return False

    if timeseries[-1][1] > metric_trigger:
        if ENABLE_BOUNDARY_DEBUG:
            logger.debug('debug :: greater_than - %s greater_than %s' % (
                str(timeseries[-1][1]), str(metric_trigger)))
        return True

    return False


def detect_drop_off_cliff(timeseries, metric_name, metric_expiration_time, metric_min_average, metric_min_average_seconds, metric_trigger):
    """
    A timeseries is anomalous if the average of the last 10 datapoints is
    <trigger> times greater than the last data point AND if has not experienced
    frequent cliff drops in the last 10 datapoints.  If the timeseries has
    experienced 2 or more datapoints of equal or less values in the last 10 or
    EXPIRATION_TIME datapoints or is less than a MIN_AVERAGE if set the
    algorithm determines the datapoint as NOT anomalous but normal.
    This algorithm is most suited to timeseries with most datapoints being > 100
    (e.g high rate).  The arbitrary <trigger> values become more noisy with
    lower value datapoints, but it still matches drops off cliffs.
    """

    if len(timeseries) < 30:
        return False

    try:
        int_end_timestamp = int(timeseries[-1][0])
        # Determine resolution of the data set
        int_second_last_end_timestamp = int(timeseries[-2][0])
        resolution = int_end_timestamp - int_second_last_end_timestamp
        ten_data_point_seconds = resolution * 10
        ten_datapoints_ago = int_end_timestamp - ten_data_point_seconds

        # @modified 20210420 - Support #4026: Change from scipy array to numpy array
        # Deprecation of scipy.array
        # ten_datapoint_array = scipy.array([x[1] for x in timeseries if x[0] <= int_end_timestamp and x[0] > ten_datapoints_ago])
        ten_datapoint_array = np.array([x[1] for x in timeseries if x[0] <= int_end_timestamp and x[0] > ten_datapoints_ago])

        ten_datapoint_array_len = len(ten_datapoint_array)
    except:
        return None

    if ten_datapoint_array_len > 3:

        ten_datapoint_min_value = np.amin(ten_datapoint_array)

        # DO NOT handle if negative integers are in the range, where is the
        # bottom of the cliff if a range goes negative?  Testing with a noisy
        # sine wave timeseries that had a drop off cliff introduced to the
        # postive data side, proved that this algorithm does work on timeseries
        # with data values in the negative range
        if ten_datapoint_min_value < 0:
            return False

        # autocorrect if there are there are 0s in the data, like graphite expects
        # 1 datapoint every 10 seconds, but the timeseries only has 1 every 60 seconds

        ten_datapoint_max_value = np.amax(ten_datapoint_array)

        # The algorithm should have already fired in 10 datapoints if the
        # timeseries dropped off a cliff, these are all zero
        if ten_datapoint_max_value == 0:
            return False

        # If the lowest is equal to the highest, no drop off cliff
        if ten_datapoint_min_value == ten_datapoint_max_value:
            return False

#        if ten_datapoint_max_value < 10:
#            return False

        ten_datapoint_array_sum = np.sum(ten_datapoint_array)
        ten_datapoint_value = int(ten_datapoint_array[-1])
        ten_datapoint_average = ten_datapoint_array_sum / ten_datapoint_array_len
        ten_datapoint_value = int(ten_datapoint_array[-1])

        # if a metric goes up and down a lot and falls off a cliff frequently
        # it is normal, not anomalous
        try:
            number_of_similar_datapoints = len(np.where(ten_datapoint_array <= ten_datapoint_min_value))
        except:
            return None

        # Detect once only - to make this useful and not noisy the first one
        # would have already fired and detected the drop
        if number_of_similar_datapoints > 2:
            return False

        # evaluate against 20 datapoints as well, reduces chatter on peaky ones
        # tested with 60 as well and 20 is sufficient to filter noise
        try:
            twenty_data_point_seconds = resolution * 20
            twenty_datapoints_ago = int_end_timestamp - twenty_data_point_seconds

            # @modified 20210420 - Support #4026: Change from scipy array to numpy array
            # Deprecation of scipy.array
            # twenty_datapoint_array = scipy.array([x[1] for x in timeseries if x[0] <= int_end_timestamp and x[0] > twenty_datapoints_ago])
            twenty_datapoint_array = np.array([x[1] for x in timeseries if x[0] <= int_end_timestamp and x[0] > twenty_datapoints_ago])

            number_of_similar_datapoints_in_twenty = len(np.where(twenty_datapoint_array <= ten_datapoint_min_value))
            if number_of_similar_datapoints_in_twenty > 2:
                return False
        except:
            return None

        # Check if there is a similar data point in EXPIRATION_TIME
        # Disabled as redis alert cache will filter on this
#        if metric_expiration_time > twenty_data_point_seconds:
#            expiration_time_data_point_seconds = metric_expiration_time
#            expiration_time_datapoints_ago = int_end_timestamp - metric_expiration_time
#            expiration_time_datapoint_array = scipy.array([x[1] for x in timeseries if x[0] <= int_end_timestamp and x[0] > expiration_time_datapoints_ago])
#            number_of_similar_datapoints_in_expiration_time = len(np.where(expiration_time_datapoint_array <= ten_datapoint_min_value))
#            if number_of_similar_datapoints_in_expiration_time > 2:
#                return False

        if metric_min_average > 0 and metric_min_average_seconds > 0:
            try:
                min_average = metric_min_average
                min_average_seconds = metric_min_average_seconds
                min_average_data_point_seconds = resolution * min_average_seconds
                # min_average_datapoints_ago = int_end_timestamp - (resolution * min_average_seconds)
                min_average_datapoints_ago = int_end_timestamp - min_average_seconds

                # @modified 20210420 - Support #4026: Change from scipy array to numpy array
                # Deprecation of scipy.array
                # min_average_array = scipy.array([x[1] for x in timeseries if x[0] <= int_end_timestamp and x[0] > min_average_datapoints_ago])
                min_average_array = np.array([x[1] for x in timeseries if x[0] <= int_end_timestamp and x[0] > min_average_datapoints_ago])

                min_average_array_average = np.sum(min_average_array) / len(min_average_array)
                if min_average_array_average < min_average:
                    return False
            except:
                return None

        if ten_datapoint_max_value < 101:
            trigger = 15
        if ten_datapoint_max_value < 20:
            trigger = ten_datapoint_average / 2
        if ten_datapoint_max_value > 100:
            trigger = 100
        if ten_datapoint_value == 0:
            # Cannot divide by 0, so set to 0.1 to prevent error
            ten_datapoint_value = 0.1
        if ten_datapoint_value == 1:
            trigger = 1
        if ten_datapoint_value == 1 and ten_datapoint_max_value < 10:
            trigger = 0.1
        if ten_datapoint_value == 0.1 and ten_datapoint_average < 1 and ten_datapoint_array_sum < 7:
            trigger = 7

        ten_datapoint_result = ten_datapoint_average / ten_datapoint_value
        if int(ten_datapoint_result) > trigger:
            if ENABLE_BOUNDARY_DEBUG:
                logger.info(
                    'detect_drop_off_cliff - %s, ten_datapoint_value = %s, ten_datapoint_array_sum = %s, ten_datapoint_average = %s, trigger = %s, ten_datapoint_result = %s' % (
                        str(int_end_timestamp),
                        str(ten_datapoint_value),
                        str(ten_datapoint_array_sum),
                        str(ten_datapoint_average),
                        str(trigger), str(ten_datapoint_result)))
            return True

    return False


def run_selected_algorithm(
        timeseries, metric_name, metric_expiration_time, metric_min_average,
        metric_min_average_seconds, metric_trigger, alert_threshold,
        metric_alerters, autoaggregate, autoaggregate_value, algorithm):
    """
    Filter timeseries and run selected algorithm.
    """

    if ENABLE_BOUNDARY_DEBUG:
        logger.info(
            'debug :: assigning in algoritms.py - %s, %s' % (
                metric_name, algorithm))

    # Get rid of short series
    # @modified 20190312 - Task #2862: Allow Boundary to analyse short time series
    #                      https://github.com/earthgecko/skyline/issues/88
    # Allow class as TooShort if the algorithm is detect_drop_off_cliff
    if algorithm == 'detect_drop_off_cliff':
        if len(timeseries) < MIN_TOLERABLE_LENGTH:
            if ENABLE_BOUNDARY_DEBUG:
                logger.debug('debug :: TooShort - %s, %s' % (metric_name, algorithm))
            raise TooShort()

    # @added 20210518 - Feature #4076: CUSTOM_STALE_PERIOD
    # In Boundary the direct settings CUSTOM_STALE_PERIOD dict is checked for
    # a custom stale period.  In Analyzer the metrics_manager Redis hask key is
    # used
    stale_period = STALE_PERIOD
    if CUSTOM_STALE_PERIOD or EXTERNAL_SETTINGS:
        try:
            stale_period = custom_stale_period(skyline_app, metric_name, log=False)
            if stale_period != STALE_PERIOD:
                if ENABLE_BOUNDARY_DEBUG:
                    logger.debug('debug :: CUSTOM_STALE_PERIOD found - %s, %s' % (
                        metric_name, str(stale_period)))
        except Exception as e:
            logger.error('error :: failed running custom_stale_period - %s' % e)

    # Get rid of stale series
    # @modified 20210518 - Feature #4076: CUSTOM_STALE_PERIOD
    # if time() - timeseries[-1][0] > STALE_PERIOD:
    if time() - timeseries[-1][0] > stale_period:
        if ENABLE_BOUNDARY_DEBUG:
            logger.debug('debug :: Stale - %s, %s' % (metric_name, algorithm))
        raise Stale()

    # Get rid of boring series
    if algorithm == 'detect_drop_off_cliff' or algorithm == 'less_than':
        if len(set(item[1] for item in timeseries[-MAX_TOLERABLE_BOREDOM:])) == BOREDOM_SET_SIZE:
            if ENABLE_BOUNDARY_DEBUG:
                logger.debug('debug :: Boring - %s, %s' % (metric_name, algorithm))
            raise Boring()

    if autoaggregate:
        if ENABLE_BOUNDARY_DEBUG:
            logger.debug('debug :: auto aggregating %s for %s' % (metric_name, algorithm))
        try:
            agg_timeseries = autoaggregate_ts(timeseries, autoaggregate_value)
            if ENABLE_BOUNDARY_DEBUG:
                logger.debug(
                    'debug :: aggregated_timeseries returned %s for %s' % (
                        metric_name, algorithm))
        except Exception as e:
            agg_timeseries = []
            if ENABLE_BOUNDARY_DEBUG:
                logger.error('Algorithm error: %s' % traceback.format_exc())
                logger.error('error: %s' % e)
                logger.debug('debug error - autoaggregate excpection %s for %s' % (metric_name, algorithm))

        if len(agg_timeseries) > 10:
            timeseries = agg_timeseries
        else:
            if ENABLE_BOUNDARY_DEBUG:
                logger.debug('debug :: TooShort - %s, %s' % (metric_name, algorithm))
            raise TooShort()

    # @modified 20190312 - Task #2862: Allow Boundary to analyse short time series
    #                      https://github.com/earthgecko/skyline/issues/88
    # if len(timeseries) < 10:
    if len(timeseries) < 1:
        if ENABLE_BOUNDARY_DEBUG:
            logger.debug(
                'debug :: timeseries too short - %s - timeseries length - %s' % (
                    metric_name, str(len(timeseries))))
        raise TooShort()

    try:
        ensemble = [globals()[algorithm](timeseries, metric_name, metric_expiration_time, metric_min_average, metric_min_average_seconds, metric_trigger)]
        if ensemble.count(True) == 1:
            if ENABLE_BOUNDARY_DEBUG:
                logger.debug(
                    # @modified 20200624 - Task #3594: Add timestamp to ENABLE_BOUNDARY_DEBUG output
                    #                      Feature #3532: Sort all time series
                    # Added timestamp to debug output
                    # 'debug :: anomalous datapoint = %s - %s, %s, %s, %s, %s, %s, %s, %s' % (
                    #     str(timeseries[-1][1]),
                    'debug :: anomalous at %s with datapoint = %s - %s, %s, %s, %s, %s, %s, %s, %s' % (
                        str(timeseries[-1][0]), str(timeseries[-1][1]),
                        str(metric_name), str(metric_expiration_time),
                        str(metric_min_average),
                        str(metric_min_average_seconds),
                        str(metric_trigger), str(alert_threshold),
                        str(metric_alerters), str(algorithm))
                )
            return True, ensemble, timeseries[-1][1], metric_name, metric_expiration_time, metric_min_average, metric_min_average_seconds, metric_trigger, alert_threshold, metric_alerters, algorithm
        else:
            if ENABLE_BOUNDARY_DEBUG:
                logger.debug(
                    # @modified 20200624 - Task #3594: Add timestamp to ENABLE_BOUNDARY_DEBUG output
                    #                      Feature #3532: Sort all time series
                    # Added timestamp to debug output
                    # 'debug :: not anomalous datapoint = %s - %s, %s, %s, %s, %s, %s, %s, %s' % (
                    #     str(timeseries[-1][1]),
                    'debug :: not anomalous at %s with datapoint = %s - %s, %s, %s, %s, %s, %s, %s, %s' % (
                        str(timeseries[-1][0]), str(timeseries[-1][1]),
                        str(metric_name), str(metric_expiration_time),
                        str(metric_min_average),
                        str(metric_min_average_seconds),
                        str(metric_trigger), str(alert_threshold),
                        str(metric_alerters), str(algorithm))
                )
            return False, ensemble, timeseries[-1][1], metric_name, metric_expiration_time, metric_min_average, metric_min_average_seconds, metric_trigger, alert_threshold, metric_alerters, algorithm
    except:
        logger.error('Algorithm error: %s' % traceback.format_exc())
        return False, [], 1, metric_name, metric_expiration_time, metric_min_average, metric_min_average_seconds, metric_trigger, alert_threshold, metric_alerters, algorithm
