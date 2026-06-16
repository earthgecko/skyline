# @added 20210717 - Feature #4180: custom_algorithm - moving_sum_decrease
#                   Feature #4164: luminosity - cloudbursts
#                   Task #4096: POC cloudbursts
# REQUIRED Skyline imports.  All custom algorithms MUST have the following two
# imports.  These are required for exception handling and to record algorithm
# errors regardless of debug_logging setting for the custom_algorithm
import traceback
from timeit import default_timer as timer
import logging

# Import ALL modules that the custom algorithm requires.  Remember that if a
# requirement is not one that is provided by the Skyline requirements.txt you
# must ensure it is installed in the Skyline virtualenv
from collections import Counter
import numpy as np
import bottleneck as bn

from custom_algorithms import record_algorithm_error
from settings import FULL_DURATION
from functions.numpy.percent_different import get_percent_different

# The name of the fucntion MUST be the same as the name declared in
# settings.CUSTOM_ALGORITHMS.
# It MUST have 3 parameters:
# current_skyline_app, timeseries, algorithm_parameters
# See https://earthgecko-skyline.readthedocs.io/en/latest/algorithms/custom-algorithms.html
# for a full explanation about each.
# ALWAYS WRAP YOUR ALGORITHM IN try and except


def moving_sum_decrease(current_skyline_app, parent_pid, timeseries, algorithm_parameters):
    """
    A time series is anomalous if the moving sum datapoint of the window is 0 or
    percent_different is greater than the defined percentage_difference and the
    datapoint is less than the moving average of the window.  This algorithm is
    useful for detecting persistent decreases and drop offs in low resolution
    timeseries (one datapoint per day) which do not have constant variability.

    :param current_skyline_app: the Skyline app executing the algorithm.  This
        will be passed to the algorithm by Skyline.  This is **required** for
        error handling and logging.  You do not have to worry about handling the
        argument in the scope of the custom algorithm itself,  but the algorithm
        must accept it as the first agrument.
    :param parent_pid: the parent pid which is executing the algorithm, this is
        **required** for error handling and logging.  You do not have to worry
        about handling this argument in the scope of algorithm, but the
        algorithm must accept it as the second argument.
    :param timeseries: the time series as a list e.g. ``[[1578916800.0, 29.0],
        [1578920400.0, 55.0], ... [1580353200.0, 55.0]]``
    :param algorithm_parameters: a dictionary of any required parameters for the
        custom_algorithm and algorithm itself for example:
        ``algorithm_parameters={
            'window': 7,
            'percent_different': 70,
            'realtime_analysis': True
        }``
    :type current_skyline_app: str
    :type parent_pid: int
    :type timeseries: list
    :type algorithm_parameters: dict
    :return: True, False or Non
    :rtype: boolean

    Example CUSTOM_ALGORITHMS configuration:

    'moving_sum_decrease': {
        'namespaces': ['app1.daily.ad_requests'],
        'algorithm_source': '/opt/skyline/github/skyline/skyline/custom_algorithms/moving_sum_decrease.py',
        'algorithm_parameters': {
            'window': 7,
            'percentage_difference': 70,
            'realtime_analysis': True,
        },
        'max_execution_time': 1.0
        'consensus': 1,
        'algorithms_allowed_in_consensus': ['moving_sum_decrease'],
        'run_3sigma_algorithms': True,
        'run_before_3sigma': True,
        'run_only_if_consensus': False,
        'use_with': ['analyzer', 'analyzer_batch', 'mirage'],
        'debug_logging': False,
    },

    """

    # You MUST define the algorithm_name
    algorithm_name = 'moving_sum_decrease'

    # Define the default state of None and None, anomalous does not default to
    # False as that is not correct, False is only correct if the algorithm
    # determines the data point is not anomalous.  The same is true for the
    # anomalyScore.
    anomalous = None
    anomalyScore = None

    anomalies = []
    anomalies_dict = {}
    anomalies_dict['algorithm'] = algorithm_name

    current_logger = None
    dev_null = None

    # If you wanted to log, you can but this should only be done during
    # testing and development
    def get_log(current_skyline_app):
        current_skyline_app_logger = current_skyline_app + 'Log'
        current_logger = logging.getLogger(current_skyline_app_logger)
        return current_logger

    start = timer()

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
        except Exception as e:
            # This except pattern MUST be used in ALL custom algortihms to
            # facilitate the traceback from any errors.  The algorithm we want to
            # run super fast and without spamming the log with lots of errors.
            # But we do not want the function returning and not reporting
            # anything to the log, so the pythonic except is used to "sample" any
            # algorithm errors to a tmp file and report once per run rather than
            # spewing tons of errors into the log e.g. analyzer.log
            dev_null = e
            record_algorithm_error(current_skyline_app, parent_pid, algorithm_name, traceback.format_exc())
            # Return None and None as the algorithm could not determine True or False
            del dev_null
            return (anomalous, anomalyScore)

    # Allow parameters to be passed in the algorithm_parameters
    window = 60
    try:
        window = algorithm_parameters['window']
    except KeyError:
        window = 60
    except Exception as e:
        record_algorithm_error(current_skyline_app, parent_pid, algorithm_name, traceback.format_exc())
        dev_null = e

    percentage_difference = 70
    try:
        percentage_difference = algorithm_parameters['percentage_difference']
    except KeyError:
        percentage_difference = 70
    except Exception as e:
        record_algorithm_error(current_skyline_app, parent_pid, algorithm_name, traceback.format_exc())
        dev_null = e

    if debug_logging:
        current_logger.debug('debug :: algorithm_parameters :: %s' % (
            str(algorithm_parameters)))

    return_anomalies = False
    try:
        return_anomalies = algorithm_parameters['return_anomalies']
    except KeyError:
        return_anomalies = False
    except Exception as e:
        record_algorithm_error(current_skyline_app, parent_pid, algorithm_name, traceback.format_exc())
        dev_null = e

    realtime_analysis = True
    try:
        realtime_analysis = algorithm_parameters['realtime_analysis']
    except KeyError:
        realtime_analysis = True
    except Exception as e:
        record_algorithm_error(current_skyline_app, parent_pid, algorithm_name, traceback.format_exc())
        dev_null = e

    try:
        base_name = algorithm_parameters['base_name']
    except Exception as e:
        # This except pattern MUST be used in ALL custom algortihms to
        # facilitate the traceback from any errors.  The algorithm we want to
        # run super fast and without spamming the log with lots of errors.
        # But we do not want the function returning and not reporting
        # anything to the log, so the pythonic except is used to "sample" any
        # algorithm errors to a tmp file and report once per run rather than
        # spewing tons of errors into the log e.g. analyzer.log
        record_algorithm_error(current_skyline_app, parent_pid, algorithm_name, traceback.format_exc())
        # Return None and None as the algorithm could not determine True or False
        dev_null = e
        del dev_null
        if return_anomalies:
            return (False, None, anomalies)
        return (False, None)
    if debug_logging:
        current_logger.debug('debug :: %s :: base_name - %s' % (
            algorithm_name, str(base_name)))

    anomalies_dict['metric'] = base_name
    anomalies_dict['anomalies'] = {}

    # ALWAYS WRAP YOUR ALGORITHM IN try and the BELOW except
    try:
        start_preprocessing = timer()

        # INFO: Sorting time series of 10079 data points took 0.002215 seconds
        timeseries = sorted(timeseries, key=lambda x: x[0])
        if debug_logging:
            current_logger.debug('debug :: %s :: time series of length - %s' % (
                algorithm_name, str(len(timeseries))))

        end_preprocessing = timer()
        preprocessing_runtime = end_preprocessing - start_preprocessing
        if debug_logging:
            current_logger.debug('debug :: %s :: preprocessing took %.6f seconds' % (
                algorithm_name, preprocessing_runtime))

        if not timeseries:
            if debug_logging:
                current_logger.debug('debug :: %s :: moving_sum_decrease not run as no data' % (
                    algorithm_name))
            anomalies = []
            if return_anomalies:
                return (anomalous, anomalyScore, anomalies)
            return (anomalous, anomalyScore)
        if debug_logging:
            current_logger.debug('debug :: %s :: timeseries length: %s' % (
                algorithm_name, str(len(timeseries))))

        anomalies_dict['timestamp'] = int(timeseries[-1][0])
        anomalies_dict['from_timestamp'] = int(timeseries[0][0])

        anomaly_trigger = None
        start_analysis = timer()
        try:
            # bottleneck is used because it is much faster
            # pd dataframe method (1445 data point - 24hrs): took 0.077915 seconds
            # bottleneck method (1445 data point - 24hrs): took 0.005692 seconds

            if len(timeseries) < (window * 3):
                if return_anomalies:
                    return (anomalous, anomalyScore, anomalies)
                return (anomalous, anomalyScore)

            original_timeseries = list(timeseries)
            original_timeseries_use_last = list(timeseries)
            if realtime_analysis:
                use_last = (window * 10) * -1
                original_timeseries_use_last = original_timeseries[use_last:]
                x_np = np.asarray([x[1] for x in timeseries[use_last:]])
                moving_sum_values = list(bn.move_sum(x_np, window=window))
                y_np = np.asarray([x[0] for x in timeseries[use_last:]])
            else:
                x_np = np.asarray([x[1] for x in timeseries])
                moving_sum_values = list(bn.move_sum(x_np, window=window))
                y_np = np.asarray([x[0] for x in timeseries])

            moving_sum_ts = []
            for index, timestamp in enumerate(y_np):
                moving_sum_ts.append([timestamp, moving_sum_values[index]])

            anomalous = False

            timeseries = list(moving_sum_ts)
            last_timestamp = original_timeseries[-1][0]
            anomalies = []
            results_timeseries = []
            if debug_logging:
                current_logger.debug('debug :: %s :: last 10 datapoints - timeseries: %s' % (
                    algorithm_name, str(timeseries[-10:])))

            for index, item in enumerate(timeseries):
                anomaly_score = 0
                if index < window:
                    results_timeseries.append([index, item, 0, 0])
                    continue
                sample = timeseries[(index - window):(index - 1)]
                last_datapoints = [val for t, val in sample]
                moving_average = sum(last_datapoints) / len(last_datapoints)
                if moving_average > 0:
                    percent_different = get_percent_different(moving_average, item[1], True)
                else:
                    percent_different = 0
                if percent_different:
                    if percent_different < 0:
                        new_pdiff = percent_different * -1
                        percent_different = new_pdiff
#                    if percent_different > percentage_difference and item[1] < (moving_average * 3):
#                        if item[1] > 0 and results_timeseries[-1][1][1] > 0:
#                            anomaly_score = 0.7
#                            if debug_logging:
#                                if item[0] == last_timestamp:
#                                    anomaly_trigger = 'moving_sum less_than 3 * moving_average'
#                                    current_logger.debug('debug :: %s :: percent_different (%s) > percentage_difference and item[1] (%s) < moving_average (%s) for %s' % (
#                                        algorithm_name, str(percent_different),
#                                        str(item[1]), str(moving_average),
#                                        str(item[0])))
                else:
                    percent_different = 0
                    results_timeseries.append([index, item, moving_average, anomaly_score, 0, original_timeseries_use_last[index]])
                    continue
                last_moving_average = results_timeseries[-1][2]
                constant_decrease = False
                if moving_average < last_moving_average:
                    last_value = last_moving_average
                    window_indices = list(range(2, (window - 1)))
                    # for i in [2, 3, 4, 5, 6]:
                    for i in window_indices:
                        i_index = i * -1
                        new_last_value = results_timeseries[i_index][2]
                        lm_percent_different = get_percent_different(new_last_value, last_value, True)
                        if last_value < new_last_value and lm_percent_different > percentage_difference:
                            constant_decrease = True
                            last_value = new_last_value
                        else:
                            constant_decrease = False
                            break
                if constant_decrease:
                    anomaly_score = 0.8
                    if debug_logging:
                        if item[0] == last_timestamp:
                            anomaly_trigger = 'moving_average of moving_sum constant decrease'
                            current_logger.debug('debug :: %s :: constant_decrease for %s, anomaly_score: %s' % (
                                algorithm_name, str(item), str(anomaly_score)))

                # Big increase
                if percent_different:
                    if percent_different > percentage_difference and original_timeseries_use_last[index][1] > (moving_average * 3):
                        anomaly_score = 0.7
                        if debug_logging:
                            if item[0] == last_timestamp:
                                anomaly_trigger = 'significant increase, datapoint > 3 * moving_average of moving_sum'
                                current_logger.debug('debug :: %s :: large increase percent_different (%s) > percentage_difference and datapoint (%s) > (moving_average * 3) (%s) for %s' % (
                                    algorithm_name, str(percent_different),
                                    str(original_timeseries_use_last[index][1]), str((moving_average * 3)),
                                    str(item[0])))

                if item[1] == 0:
                    # if results_timeseries[-1][1][1] > 0 and original_timeseries_use_last[index][1] == 0:
                    zeros_in_last_moving_sums = [i_item[1][1] for i_item in results_timeseries[-window:] if i_item[1][1] == 0]
                    if results_timeseries[-1][1][1] > 0 and original_timeseries_use_last[index][1] == 0 and len(zeros_in_last_moving_sums) == 0:
                        anomaly_score = 1.0
                        if debug_logging:
                            if item[0] == last_timestamp:
                                anomaly_trigger = 'moving_sum and datapoint are 0'
                                current_logger.debug('debug :: %s :: moving_sum is 0 for %s, anomaly_score: %s' % (
                                    algorithm_name, str(item), str(anomaly_score)))
                if anomaly_score > 0:
                    anomalies.append(item)
#                    if realtime_analysis:
#                        if item[0] == last_timestamp:
#                            anomalous = True
                results_timeseries.append([index, item, moving_average, anomaly_score, percent_different, original_timeseries_use_last[index]])

            if debug_logging:
                current_logger.debug('debug :: %s :: %s after moving_sum_decrease, anomaly_score: %s, continuing with longest_zero_streak' % (
                    algorithm_name, base_name, str(anomaly_score)))

            # longest zero streak
            min_zero_streak = 8
            max_zero_streak = 10
            longest_zero_streak_anomaly = False
            if anomaly_score == 0:
                zero_streaks = []
                current_zero_streak = []
                longest_found_zero_streak = 0
                zero_streak_lengths = []
                for item in original_timeseries_use_last:
                    try:
                        if not isinstance(item[1], float):
                            continue
                    except TypeError:
                        continue
                    except:
                        continue
                    if item[1] > 0:
                        if len(current_zero_streak) > 0:
                            zero_streaks.append(current_zero_streak)
                            current_zero_streak = []
                    if item[1] == 0:
                        current_zero_streak.append([item[0], item[1]])

                if len(current_zero_streak) > 0:
                    zero_streaks.append(current_zero_streak)
                    current_zero_streak = []

                for item in zero_streaks:
                    zero_streak_length = len(item)
                    if zero_streak_length < min_zero_streak:
                        zero_streak_lengths.append(zero_streak_length)
                if zero_streak_lengths:
                    longest_found_zero_streak = sorted(zero_streak_lengths)[-1]

                if debug_logging:
                    current_logger.debug('debug :: %s :: longest_found_zero_streak: %s' % (
                        algorithm_name, str(longest_found_zero_streak)))

                current_zero_streak = []
                for item in original_timeseries_use_last[-(min_zero_streak + 4):]:
                    try:
                        if not isinstance(item[1], float):
                            continue
                    except TypeError:
                        continue
                    except:
                        continue
                    if item[1] > 0:
                        if len(current_zero_streak) > 0:
                            current_zero_streak = []
                    if item[1] == 0:
                        current_zero_streak.append([item[0], item[1]])

                if debug_logging:
                    current_logger.debug('debug :: %s :: %s in a row, current_zero_streak: %s' % (
                        algorithm_name, str(len(current_zero_streak)),
                        str(current_zero_streak)))

    #            if len(current_zero_streak) > longest_found_zero_streak:
    #                anomaly_score = 1
                if len(current_zero_streak) == min_zero_streak:
                    anomaly_score = 1
                    anomaly_trigger = 'min_zero_streak'
                if len(current_zero_streak) == (min_zero_streak + 1):
                    anomaly_score = 1
                    anomaly_trigger = 'min_zero_streak'
                long_zero_streak = [item[1] for item in original_timeseries_use_last[-(min_zero_streak + 4):] if item[1] == 0]
                if len(long_zero_streak) > (min_zero_streak + 1):
                    anomaly_score = 0
                    anomaly_trigger = None

#                if len(current_zero_streak) >= max_zero_streak:
#                    anomaly_score = 0
#                if longest_found_zero_streak >= max_zero_streak:
#                    anomaly_score = 0

#                if zero_streak_lengths:
#                    current_zero_streak_length = zero_streak_lengths[-1]
#                    if current_zero_streak_length >= max_zero_streak:
#                        anomaly_score = 0
#                        if debug_logging:
#                            current_logger.debug('debug :: %s :: the current_zero_streak_length: %s is greater than the max_zero_streak not anomalous' % (
#                                algorithm_name, str(len(current_zero_streak_length))))

#                if anomaly_score == 1:
                    # Do not alert repetively
#                    fetch_index = (min_zero_streak + 2) * -1
#                    last_zeros_in_window = [item[1] for item in original_timeseries_use_last[fetch_index:-2] if item[1] == 0]
#                    if len(last_zeros_in_window) >= min_zero_streak:
#                        anomaly_score = 0
#                        if debug_logging:
#                            current_logger.debug('debug :: %s :: previous window had min_zero_streak - not anomalous' % (
#                                algorithm_name))

                if anomaly_score == 1:
                    if debug_logging:
                        current_logger.debug('debug :: %s :: current_zero_streak: %s >= %s' % (
                            algorithm_name, str(len(current_zero_streak)),
                            str(min_zero_streak)))

            if not realtime_analysis:
                if anomalies:
                    anomalous = True
            else:
                if results_timeseries[-1][3] > 0:
                    anomalous = True
                    if debug_logging:
                        current_logger.debug('debug :: %s :: last 10 datapoints results_timeseries: %s' % (
                            algorithm_name, str(results_timeseries[-10:])))
                        current_logger.debug('debug :: %s :: anomaly_trigger: %s' % (
                            algorithm_name, str(anomaly_trigger)))

            if anomalous:
                anomalies_data = []
                anomaly_timestamps = [int(item[0]) for item in anomalies]
                for item in timeseries:
                    if int(item[0]) in anomaly_timestamps:
                        anomalies_data.append(1)
                    else:
                        anomalies_data.append(0)
                anomalies_list = []
                for ts, value in timeseries:
                    if int(ts) in anomaly_timestamps:
                        anomalies_list.append([int(ts), value])
                        anomalies_dict['anomalies'][int(ts)] = value

        except SystemExit as e:
            if debug_logging:
                current_logger.debug('debug_logging :: %s :: SystemExit called, during analysis, exiting - %s' % (
                    algorithm_name, e))
            if return_anomalies:
                return (anomalous, anomalyScore, anomalies)
            return (anomalous, anomalyScore)
        except:
            traceback_msg = traceback.format_exc()
            record_algorithm_error(current_skyline_app, parent_pid, algorithm_name, traceback_msg)
            if debug_logging:
                current_logger.error(traceback_msg)
                current_logger.error('error :: debug_logging :: %s :: failed to run on ts' % (
                    algorithm_name))
            if return_anomalies:
                return (anomalous, anomalyScore, anomalies)
            return (anomalous, anomalyScore)

        end_analysis = timer()
        analysis_runtime = end_analysis - start_analysis

        if debug_logging:
            current_logger.debug('debug :: analysis with %s took %.6f seconds' % (
                algorithm_name, analysis_runtime))

        if anomalous:
            anomalyScore = 1.0
        else:
            anomalyScore = 0.0

        if debug_logging:
            current_logger.info('%s :: anomalous - %s, anomalyScore - %s' % (
                algorithm_name, str(anomalous), str(anomalyScore)))

        if debug_logging:
            end = timer()
            processing_runtime = end - start
            current_logger.info('%s :: completed in %.6f seconds' % (
                algorithm_name, processing_runtime))
        try:
            del timeseries
            del results_timeseries
            del moving_sum_ts
        except:
            pass
        if return_anomalies:
            return (anomalous, anomalyScore, anomalies)
        return (anomalous, anomalyScore)
    except SystemExit as e:
        if debug_logging:
            current_logger.debug('debug_logging :: %s :: SystemExit called (before StopIteration), exiting - %s' % (
                algorithm_name, e))
        if return_anomalies:
            return (anomalous, anomalyScore, anomalies)
        return (anomalous, anomalyScore)
    except StopIteration:
        # This except pattern MUST be used in ALL custom algortihms to
        # facilitate the traceback from any errors.  The algorithm we want to
        # run super fast and without spamming the log with lots of errors.
        # But we do not want the function returning and not reporting
        # anything to the log, so the pythonic except is used to "sample" any
        # algorithm errors to a tmp file and report once per run rather than
        # spewing tons of errors into the log e.g. analyzer.log
        if return_anomalies:
            return (False, None, anomalies)
        return (False, None)
    except:
        record_algorithm_error(current_skyline_app, parent_pid, algorithm_name, traceback.format_exc())
        # Return None and None as the algorithm could not determine True or False
        if return_anomalies:
            return (False, None, anomalies)
        return (False, None)

    if return_anomalies:
        return (anomalous, anomalyScore, anomalies)
    return (anomalous, anomalyScore)
