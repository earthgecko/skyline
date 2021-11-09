# @added 20210717 - Feature #4180: custom_algorithm - m66
#                   Feature #4164: luminosity - cloudbursts
#                   Task #4096: POC cloudbursts
# REQUIRED Skyline imports.  All custom algorithms MUST have the following two
# imports.  These are required for exception handling and to record algorithm
# errors regardless of debug_logging setting for the custom_algorithm
import traceback
from timeit import default_timer as timer
from os.path import dirname as os_path_dirname
from os.path import exists as os_path_exists

from custom_algorithms import record_algorithm_error

import logging

# Import ALL modules that the custom algorithm requires.  Remember that if a
# requirement is not one that is provided by the Skyline requirements.txt you
# must ensure it is installed in the Skyline virtualenv
from collections import Counter
import numpy as np
import pandas as pd

from skyline_functions import mkdir_p
from settings import FULL_DURATION

# The name of the fucntion MUST be the same as the name declared in
# settings.CUSTOM_ALGORITHMS.
# It MUST have 3 parameters:
# current_skyline_app, timeseries, algorithm_parameters
# See https://earthgecko-skyline.readthedocs.io/en/latest/algorithms/custom-algorithms.html
# for a full explanation about each.
# ALWAYS WRAP YOUR ALGORITHM IN try and except


def m66(current_skyline_app, parent_pid, timeseries, algorithm_parameters):
    """
    A time series data points are anomalous if the 6th median is 6 standard
    deviations (six-sigma) from the time series 6th median standard deviation
    and persists for x_windows, where `x_windows = int(window / 2)`.
    This algorithm finds SIGNIFICANT cahngepoints in a time series, similar to
    PELT and Bayesian Online Changepoint Detection, however it is more robust to
    instaneous outliers and more conditionally selective of changepoints.

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
            'nth_median': 6,
            'sigma': 6,
            'window': 5,
            'return_anomalies' = True,
        }``
    :type current_skyline_app: str
    :type parent_pid: int
    :type timeseries: list
    :type algorithm_parameters: dict
    :return: True, False or Non
    :rtype: boolean

    Example CUSTOM_ALGORITHMS configuration:

    'm66': {
        'namespaces': [
            'skyline.analyzer.run_time', 'skyline.analyzer.total_metrics',
            'skyline.analyzer.exceptions'
        ],
        'algorithm_source': '/opt/skyline/github/skyline/skyline/custom_algorithms/m66.py',
        'algorithm_parameters': {
            'nth_median': 6, 'sigma': 6, 'window': 5, 'resolution': 60,
            'minimum_sparsity': 0, 'determine_duration': False,
            'return_anomalies': True, 'save_plots_to': False,
            'save_plots_to_absolute_dir': False, 'filename_prefix': False
        },
        'max_execution_time': 1.0
        'consensus': 1,
        'algorithms_allowed_in_consensus': ['m66'],
        'run_3sigma_algorithms': False,
        'run_before_3sigma': False,
        'run_only_if_consensus': False,
        'use_with': ['crucible', 'luminosity'],
        'debug_logging': False,
    },

    """

    # You MUST define the algorithm_name
    algorithm_name = 'm66'

    # Define the default state of None and None, anomalous does not default to
    # False as that is not correct, False is only correct if the algorithm
    # determines the data point is not anomalous.  The same is true for the
    # anomalyScore.
    anomalous = None
    anomalyScore = None

    return_anomalies = False
    anomalies = []
    anomalies_dict = {}
    anomalies_dict['algorithm'] = algorithm_name

    realtime_analysis = False

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
            if current_skyline_app == 'webapp':
                return (anomalous, anomalyScore, anomalies, anomalies_dict)
            if return_anomalies:
                return (anomalous, anomalyScore, anomalies)
            return (anomalous, anomalyScore)

    # Allow the m66 parameters to be passed in the algorithm_parameters
    window = 6
    try:
        window = algorithm_parameters['window']
    except KeyError:
        window = 6
    except Exception as e:
        record_algorithm_error(current_skyline_app, parent_pid, algorithm_name, traceback.format_exc())
        dev_null = e

    nth_median = 6
    try:
        nth_median = algorithm_parameters['nth_median']
    except KeyError:
        nth_median = 6
    except Exception as e:
        record_algorithm_error(current_skyline_app, parent_pid, algorithm_name, traceback.format_exc())
        dev_null = e

    n_sigma = 6
    try:
        n_sigma = algorithm_parameters['sigma']
    except KeyError:
        n_sigma = 6
    except Exception as e:
        record_algorithm_error(current_skyline_app, parent_pid, algorithm_name, traceback.format_exc())
        dev_null = e

    resolution = 0
    try:
        resolution = algorithm_parameters['resolution']
    except KeyError:
        resolution = 0
    except Exception as e:
        record_algorithm_error(current_skyline_app, parent_pid, algorithm_name, traceback.format_exc())
        dev_null = e

    determine_duration = False
    try:
        determine_duration = algorithm_parameters['determine_duration']
    except KeyError:
        determine_duration = False
    except Exception as e:
        record_algorithm_error(current_skyline_app, parent_pid, algorithm_name, traceback.format_exc())
        dev_null = e

    minimum_sparsity = 0
    try:
        minimum_sparsity = algorithm_parameters['minimum_sparsity']
    except KeyError:
        minimum_sparsity = 0
    except Exception as e:
        record_algorithm_error(current_skyline_app, parent_pid, algorithm_name, traceback.format_exc())
        dev_null = e

    shift_to_start_of_window = True
    try:
        shift_to_start_of_window = algorithm_parameters['shift_to_start_of_window']
    except KeyError:
        shift_to_start_of_window = True
    except Exception as e:
        record_algorithm_error(current_skyline_app, parent_pid, algorithm_name, traceback.format_exc())
        dev_null = e

    save_plots_to = False
    try:
        save_plots_to = algorithm_parameters['save_plots_to']
    except KeyError:
        save_plots_to = False
    except Exception as e:
        record_algorithm_error(current_skyline_app, parent_pid, algorithm_name, traceback.format_exc())
        dev_null = e

    save_plots_to_absolute_dir = False
    try:
        save_plots_to_absolute_dir = algorithm_parameters['save_plots_to_absolute_dir']
    except KeyError:
        save_plots_to_absolute_dir = False
    except Exception as e:
        record_algorithm_error(current_skyline_app, parent_pid, algorithm_name, traceback.format_exc())
        dev_null = e
    filename_prefix = False
    try:
        filename_prefix = algorithm_parameters['filename_prefix']
    except KeyError:
        filename_prefix = False
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

    try:
        realtime_analysis = algorithm_parameters['realtime_analysis']
    except KeyError:
        realtime_analysis = False
    except Exception as e:
        record_algorithm_error(current_skyline_app, parent_pid, algorithm_name, traceback.format_exc())
        dev_null = e

    save_plots_to = False
    try:
        save_plots_to = algorithm_parameters['save_plots_to']
    except KeyError:
        save_plots_to = False
    except Exception as e:
        record_algorithm_error(current_skyline_app, parent_pid, algorithm_name, traceback.format_exc())
        dev_null = e

    save_plots_to_absolute_dir = False
    try:
        save_plots_to_absolute_dir = algorithm_parameters['save_plots_to_absolute_dir']
    except KeyError:
        save_plots_to = False
    except Exception as e:
        record_algorithm_error(current_skyline_app, parent_pid, algorithm_name, traceback.format_exc())
        dev_null = e
    filename_prefix = False
    try:
        filename_prefix = algorithm_parameters['filename_prefix']
    except KeyError:
        filename_prefix = False
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
        if current_skyline_app == 'webapp':
            return (anomalous, anomalyScore, anomalies, anomalies_dict)
        if return_anomalies:
            return (False, None, anomalies)
        return (False, None)
    if debug_logging:
        current_logger.debug('debug :: %s :: base_name - %s' % (
            algorithm_name, str(base_name)))

    anomalies_dict['metric'] = base_name
    anomalies_dict['anomalies'] = {}

    use_bottleneck = True
    if save_plots_to:
        use_bottleneck = False
    if use_bottleneck:
        import bottleneck as bn

    # ALWAYS WRAP YOUR ALGORITHM IN try and the BELOW except
    try:
        start_preprocessing = timer()

        # INFO: Sorting time series of 10079 data points took 0.002215 seconds
        timeseries = sorted(timeseries, key=lambda x: x[0])
        if debug_logging:
            current_logger.debug('debug :: %s :: time series of length - %s' % (
                algorithm_name, str(len(timeseries))))

        # Testing the data to ensure it meets minimum requirements, in the case
        # of Skyline's use of the m66 algorithm this means that:
        # - the time series must have at least 75% of its full_duration
        do_not_use_sparse_data = False
        if current_skyline_app == 'luminosity':
            do_not_use_sparse_data = True

        if minimum_sparsity == 0:
            do_not_use_sparse_data = False

        total_period = 0
        total_datapoints = 0

        calculate_variables = False
        if do_not_use_sparse_data:
            calculate_variables = True
        if determine_duration:
            calculate_variables = True

        if calculate_variables:
            try:
                start_timestamp = int(timeseries[0][0])
                end_timestamp = int(timeseries[-1][0])
                total_period = end_timestamp - start_timestamp
                total_datapoints = len(timeseries)
            except SystemExit as e:
                if debug_logging:
                    current_logger.debug('debug_logging :: %s :: SystemExit called, exiting - %s' % (
                        algorithm_name, e))
                if current_skyline_app == 'webapp':
                    return (anomalous, anomalyScore, anomalies, anomalies_dict)
                if return_anomalies:
                    return (anomalous, anomalyScore, anomalies)
                return (anomalous, anomalyScore)
            except:
                traceback_msg = traceback.format_exc()
                record_algorithm_error(current_skyline_app, parent_pid, algorithm_name, traceback_msg)
                if debug_logging:
                    current_logger.error(traceback_msg)
                    current_logger.error('error :: debug_logging :: %s :: failed to determine total_period and total_datapoints' % (
                        algorithm_name))
                timeseries = []
            if not timeseries:
                if current_skyline_app == 'webapp':
                    return (anomalous, anomalyScore, anomalies, anomalies_dict)
                if return_anomalies:
                    return (anomalous, anomalyScore, anomalies)
                return (anomalous, anomalyScore)

            if current_skyline_app == 'analyzer':
                # Default for analyzer at required period to 18 hours
                period_required = int(FULL_DURATION * 0.75)
            else:
                # Determine from timeseries
                if total_period < FULL_DURATION:
                    period_required = int(FULL_DURATION * 0.75)
                else:
                    period_required = int(total_period * 0.75)

            if determine_duration:
                period_required = int(total_period * 0.75)

        if do_not_use_sparse_data:
            # If the time series does not have 75% of its full_duration it does
            # not have sufficient data to sample
            try:
                if total_period < period_required:
                    if debug_logging:
                        current_logger.debug('debug :: %s :: time series does not have sufficient data' % (
                            algorithm_name))
                    if current_skyline_app == 'webapp':
                        return (anomalous, anomalyScore, anomalies, anomalies_dict)
                    if return_anomalies:
                        return (anomalous, anomalyScore, anomalies)
                    return (anomalous, anomalyScore)
            except SystemExit as e:
                if debug_logging:
                    current_logger.debug('debug_logging :: %s :: SystemExit called, exiting - %s' % (
                        algorithm_name, e))
                if current_skyline_app == 'webapp':
                    return (anomalous, anomalyScore, anomalies, anomalies_dict)
                if return_anomalies:
                    return (anomalous, anomalyScore, anomalies)
                return (anomalous, anomalyScore)
            except:
                traceback_msg = traceback.format_exc()
                record_algorithm_error(current_skyline_app, parent_pid, algorithm_name, traceback_msg)
                if debug_logging:
                    current_logger.error(traceback_msg)
                    current_logger.error('error :: debug_logging :: %s :: falied to determine if time series has sufficient data' % (
                        algorithm_name))
                if current_skyline_app == 'webapp':
                    return (anomalous, anomalyScore, anomalies, anomalies_dict)
                if return_anomalies:
                    return (anomalous, anomalyScore, anomalies)
                return (anomalous, anomalyScore)

            # If the time series does not have 75% of its full_duration
            # datapoints it does not have sufficient data to sample

            # Determine resolution from the last 30 data points
            # INFO took 0.002060 seconds
            if not resolution:
                resolution_timestamps = []
                metric_resolution = False
                for metric_datapoint in timeseries[-30:]:
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
                    except SystemExit as e:
                        if debug_logging:
                            current_logger.debug('debug_logging :: %s :: SystemExit called, exiting - %s' % (
                                algorithm_name, e))
                        if current_skyline_app == 'webapp':
                            return (anomalous, anomalyScore, anomalies, anomalies_dict)
                        if return_anomalies:
                            return (anomalous, anomalyScore, anomalies)
                        return (anomalous, anomalyScore)
                    except:
                        traceback_msg = traceback.format_exc()
                        record_algorithm_error(current_skyline_app, parent_pid, algorithm_name, traceback_msg)
                        if debug_logging:
                            current_logger.error(traceback_msg)
                            current_logger.error('error :: debug_logging :: %s :: failed to determine if time series has sufficient data' % (
                                algorithm_name))
                    try:
                        del timestamp_resolutions
                    except:
                        pass
            else:
                metric_resolution = resolution

            minimum_datapoints = None
            if metric_resolution:
                minimum_datapoints = int(period_required / metric_resolution)
            if minimum_datapoints:
                if total_datapoints < minimum_datapoints:
                    if debug_logging:
                        current_logger.debug('debug :: %s :: time series does not have sufficient data, minimum_datapoints required is %s and time series has %s' % (
                            algorithm_name, str(minimum_datapoints),
                            str(total_datapoints)))
                    if current_skyline_app == 'webapp':
                        return (anomalous, anomalyScore, anomalies, anomalies_dict)
                    if return_anomalies:
                        return (anomalous, anomalyScore, anomalies)
                    return (anomalous, anomalyScore)

            # Is the time series fully populated?
            # full_duration_datapoints = int(full_duration / metric_resolution)
            total_period_datapoints = int(total_period / metric_resolution)
            # minimum_percentage_sparsity = 95
            minimum_percentage_sparsity = 90
            sparsity = int(total_datapoints / (total_period_datapoints / 100))
            if sparsity < minimum_percentage_sparsity:
                if debug_logging:
                    current_logger.debug('debug :: %s :: time series does not have sufficient data, minimum_percentage_sparsity required is %s and time series has %s' % (
                        algorithm_name, str(minimum_percentage_sparsity),
                        str(sparsity)))
                if current_skyline_app == 'webapp':
                    return (anomalous, anomalyScore, anomalies, anomalies_dict)
                if return_anomalies:
                    return (anomalous, anomalyScore, anomalies)
                return (anomalous, anomalyScore)
            if len(set(item[1] for item in timeseries)) == 1:
                if debug_logging:
                    current_logger.debug('debug :: %s :: time series does not have sufficient variability, all the values are the same' % algorithm_name)
                anomalous = False
                anomalyScore = 0.0
                if current_skyline_app == 'webapp':
                    return (anomalous, anomalyScore, anomalies, anomalies_dict)
                if return_anomalies:
                    return (anomalous, anomalyScore, anomalies)
                return (anomalous, anomalyScore)

        end_preprocessing = timer()
        preprocessing_runtime = end_preprocessing - start_preprocessing
        if debug_logging:
            current_logger.debug('debug :: %s :: preprocessing took %.6f seconds' % (
                algorithm_name, preprocessing_runtime))

        if not timeseries:
            if debug_logging:
                current_logger.debug('debug :: %s :: m66 not run as no data' % (
                    algorithm_name))
            anomalies = []
            if current_skyline_app == 'webapp':
                return (anomalous, anomalyScore, anomalies, anomalies_dict)
            if return_anomalies:
                return (anomalous, anomalyScore, anomalies)
            return (anomalous, anomalyScore)
        if debug_logging:
            current_logger.debug('debug :: %s :: timeseries length: %s' % (
                algorithm_name, str(len(timeseries))))

        anomalies_dict['timestamp'] = int(timeseries[-1][0])
        anomalies_dict['from_timestamp'] = int(timeseries[0][0])

        start_analysis = timer()
        try:
            # bottleneck is used because it is much faster
            # pd dataframe method (1445 data point - 24hrs): took 0.077915 seconds
            # bottleneck method (1445 data point - 24hrs): took 0.005692 seconds
            # numpy and pandas rolling
            # 2021-07-30 12:37:31 :: 2827897 :: cloudbursts :: find_cloudbursts completed on 1530 metrics in 136.93 seconds
            # 2021-07-30 12:44:53 :: 2855884 :: cloudbursts :: find_cloudbursts completed on 1530 metrics in 148.82 seconds
            # 2021-07-30 12:48:41 :: 2870822 :: cloudbursts :: find_cloudbursts completed on 1530 metrics in 145.62 seconds
            # 2021-07-30 12:55:00 :: 2893634 :: cloudbursts :: find_cloudbursts completed on 1530 metrics in 139.00 seconds
            # 2021-07-30 12:59:31 :: 2910443 :: cloudbursts :: find_cloudbursts completed on 1530 metrics in 144.80 seconds
            # 2021-07-30 13:02:31 :: 2922928 :: cloudbursts :: find_cloudbursts completed on 1530 metrics in 143.35 seconds
            # 2021-07-30 14:12:56 :: 3132457 :: cloudbursts :: find_cloudbursts completed on 1530 metrics in 129.25 seconds
            # 2021-07-30 14:22:35 :: 3164370 :: cloudbursts :: find_cloudbursts completed on 1530 metrics in 125.72 seconds
            # 2021-07-30 14:28:24 :: 3179687 :: cloudbursts :: find_cloudbursts completed on 1530 metrics in 222.43 seconds
            # 2021-07-30 14:33:45 :: 3179687 :: cloudbursts :: find_cloudbursts completed on 1530 metrics in 244.00 seconds
            # 2021-07-30 14:36:27 :: 3214047 :: cloudbursts :: find_cloudbursts completed on 1530 metrics in 141.10 seconds
            # numpy and bottleneck
            # 2021-07-30 16:41:52 :: 3585162 :: cloudbursts :: find_cloudbursts completed on 1530 metrics in 73.92 seconds
            # 2021-07-30 16:46:46 :: 3585162 :: cloudbursts :: find_cloudbursts completed on 1530 metrics in 68.84 seconds
            # 2021-07-30 16:51:48 :: 3585162 :: cloudbursts :: find_cloudbursts completed on 1530 metrics in 70.55 seconds
            # numpy and bottleneck (passing resolution and not calculating in m66)
            # 2021-07-30 16:57:46 :: 3643253 :: cloudbursts :: find_cloudbursts completed on 1530 metrics in 65.59 seconds

            if use_bottleneck:
                if len(timeseries) < 10:
                    if current_skyline_app == 'webapp':
                        return (anomalous, anomalyScore, anomalies, anomalies_dict)
                    if return_anomalies:
                        return (anomalous, anomalyScore, anomalies)
                    return (anomalous, anomalyScore)

                x_np = np.asarray([x[1] for x in timeseries])
                # Fast Min-Max scaling
                data = (x_np - x_np.min()) / (x_np.max() - x_np.min())

                # m66 - calculate to nth_median
                median_count = 0
                while median_count < nth_median:
                    median_count += 1
                    rolling_median_s = bn.move_median(data, window=window)
                    median = rolling_median_s.tolist()
                    data = median
                    if median_count == nth_median:
                        break

                # m66 - calculate the moving standard deviation for the
                # nth_median array
                rolling_std_s = bn.move_std(data, window=window)
                std_nth_median_array = np.nan_to_num(rolling_std_s, copy=False, nan=0.0, posinf=None, neginf=None)
                std_nth_median = std_nth_median_array.tolist()
                if debug_logging:
                    current_logger.debug('debug :: %s :: std_nth_median calculated with bn' % (
                        algorithm_name))
            else:
                df = pd.DataFrame(timeseries, columns=['date', 'value'])
                df['date'] = pd.to_datetime(df['date'], unit='s')
                datetime_index = pd.DatetimeIndex(df['date'].values)
                df = df.set_index(datetime_index)
                df.drop('date', axis=1, inplace=True)
                original_df = df.copy()
                # MinMax scale
                df = (df - df.min()) / (df.max() - df.min())
                # window = 6
                data = df['value'].tolist()

                if len(data) < 10:
                    if current_skyline_app == 'webapp':
                        return (anomalous, anomalyScore, anomalies, anomalies_dict)
                    if return_anomalies:
                        return (anomalous, anomalyScore, anomalies)
                    return (anomalous, anomalyScore)

                # m66 - calculate to nth_median
                median_count = 0
                while median_count < nth_median:
                    median_count += 1
                    s = pd.Series(data)
                    rolling_median_s = s.rolling(window).median()
                    median = rolling_median_s.tolist()
                    data = median
                    if median_count == nth_median:
                        break

                # m66 - calculate the moving standard deviation for the
                # nth_median array
                s = pd.Series(data)
                rolling_std_s = s.rolling(window).std()

                nth_median_column = 'std_nth_median_%s' % str(nth_median)
                df[nth_median_column] = rolling_std_s.tolist()
                std_nth_median = df[nth_median_column].fillna(0).tolist()

            # m66 - calculate the standard deviation for the entire nth_median
            # array
            metric_stddev = np.std(std_nth_median)
            std_nth_median_n_sigma = []
            anomalies_found = False

            for value in std_nth_median:
                # m66 - if the value in the 6th median array is > six-sigma of
                # the metric_stddev the datapoint is anomalous
                if value > (metric_stddev * n_sigma):
                    std_nth_median_n_sigma.append(1)
                    anomalies_found = True
                else:
                    std_nth_median_n_sigma.append(0)
            std_nth_median_n_sigma_column = 'std_median_%s_%s_sigma' % (str(nth_median), str(n_sigma))
            if not use_bottleneck:
                df[std_nth_median_n_sigma_column] = std_nth_median_n_sigma

            anomalies = []
            # m66 - only label anomalous if the n_sigma triggers are persisted
            # for (window / 2)
            if anomalies_found:
                current_triggers = []
                for index, item in enumerate(timeseries):
                    if std_nth_median_n_sigma[index] == 1:
                        current_triggers.append(index)
                    else:
                        if len(current_triggers) > int(window / 2):
                            for trigger_index in current_triggers:
                                # Shift the anomaly back to the beginning of the
                                # window
                                if shift_to_start_of_window:
                                    anomalies.append(timeseries[(trigger_index - (window * int((nth_median / 2))))])
                                else:
                                    anomalies.append(timeseries[trigger_index])
                        current_triggers = []
                # Process any remaining current_triggers
                if len(current_triggers) > int(window / 2):
                    for trigger_index in current_triggers:
                        # Shift the anomaly back to the beginning of the
                        # window
                        if shift_to_start_of_window:
                            anomalies.append(timeseries[(trigger_index - (window * int((nth_median / 2))))])
                        else:
                            anomalies.append(timeseries[trigger_index])
            if not anomalies:
                anomalous = False

            if anomalies:
                anomalous = True
                anomalies_data = []
                anomaly_timestamps = [int(item[0]) for item in anomalies]
                for item in timeseries:
                    if int(item[0]) in anomaly_timestamps:
                        anomalies_data.append(1)
                    else:
                        anomalies_data.append(0)
                if not use_bottleneck:
                    df['anomalies'] = anomalies_data
                anomalies_list = []
                for ts, value in timeseries:
                    if int(ts) in anomaly_timestamps:
                        anomalies_list.append([int(ts), value])
                        anomalies_dict['anomalies'][int(ts)] = value

            if anomalies and save_plots_to:
                try:
                    from adtk.visualization import plot
                    metric_dir = base_name.replace('.', '/')
                    timestamp_dir = str(int(timeseries[-1][0]))
                    save_path = '%s/%s/%s/%s' % (
                        save_plots_to, algorithm_name, metric_dir,
                        timestamp_dir)
                    if save_plots_to_absolute_dir:
                        save_path = '%s' % save_plots_to
                    anomalies_dict['file_path'] = save_path
                    save_to_file = '%s/%s.%s.png' % (
                        save_path, algorithm_name, base_name)
                    if filename_prefix:
                        save_to_file = '%s/%s.%s.%s.png' % (
                            save_path, filename_prefix, algorithm_name,
                            base_name)
                    save_to_path = os_path_dirname(save_to_file)
                    title = '%s\n%s - median %s %s-sigma persisted (window=%s)' % (
                        base_name, algorithm_name, str(nth_median), str(n_sigma), str(window))

                    if not os_path_exists(save_to_path):
                        try:
                            mkdir_p(save_to_path)
                        except Exception as e:
                            current_logger.error('error :: %s :: failed to create dir - %s - %s' % (
                                algorithm_name, save_to_path, e))
                    if os_path_exists(save_to_path):
                        try:
                            plot(original_df['value'], anomaly=df['anomalies'], anomaly_color='red', title=title, save_to_file=save_to_file)
                            if debug_logging:
                                current_logger.debug('debug :: %s :: plot saved to - %s' % (
                                    algorithm_name, save_to_file))
                            anomalies_dict['image'] = save_to_file
                        except Exception as e:
                            current_logger.error('error :: %s :: failed to plot - %s - %s' % (
                                algorithm_name, base_name, e))
                    anomalies_file = '%s/%s.%s.anomalies_list.txt' % (
                        save_path, algorithm_name, base_name)
                    with open(anomalies_file, 'w') as fh:
                        fh.write(str(anomalies_list))
                        # os.chmod(anomalies_file, mode=0o644)
                    data_file = '%s/data.txt' % (save_path)
                    with open(data_file, 'w') as fh:
                        fh.write(str(anomalies_dict))
                except SystemExit as e:
                    if debug_logging:
                        current_logger.debug('debug_logging :: %s :: SystemExit called during save plot, exiting - %s' % (
                            algorithm_name, e))
                    if current_skyline_app == 'webapp':
                        return (anomalous, anomalyScore, anomalies, anomalies_dict)
                    if return_anomalies:
                        return (anomalous, anomalyScore, anomalies)
                    return (anomalous, anomalyScore)
                except Exception as e:
                    traceback_msg = traceback.format_exc()
                    record_algorithm_error(current_skyline_app, parent_pid, algorithm_name, traceback_msg)
                    if debug_logging:
                        current_logger.error(traceback_msg)
                        current_logger.error('error :: %s :: failed to plot or save anomalies file - %s - %s' % (
                            algorithm_name, base_name, e))

            try:
                del df
            except:
                pass
        except SystemExit as e:
            if debug_logging:
                current_logger.debug('debug_logging :: %s :: SystemExit called, during analysis, exiting - %s' % (
                    algorithm_name, e))
            if current_skyline_app == 'webapp':
                return (anomalous, anomalyScore, anomalies, anomalies_dict)
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
            if current_skyline_app == 'webapp':
                return (anomalous, anomalyScore, anomalies, anomalies_dict)
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
        except:
            pass
        if current_skyline_app == 'webapp':
            return (anomalous, anomalyScore, anomalies, anomalies_dict)
        if return_anomalies:
            return (anomalous, anomalyScore, anomalies)
        return (anomalous, anomalyScore)
    except SystemExit as e:
        if debug_logging:
            current_logger.debug('debug_logging :: %s :: SystemExit called (before StopIteration), exiting - %s' % (
                algorithm_name, e))
        if current_skyline_app == 'webapp':
            return (anomalous, anomalyScore, anomalies, anomalies_dict)
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
        if current_skyline_app == 'webapp':
            return (anomalous, anomalyScore, anomalies, anomalies_dict)
        if return_anomalies:
            return (False, None, anomalies)
        return (False, None)
    except:
        record_algorithm_error(current_skyline_app, parent_pid, algorithm_name, traceback.format_exc())
        # Return None and None as the algorithm could not determine True or False
        if current_skyline_app == 'webapp':
            return (anomalous, anomalyScore, anomalies, anomalies_dict)
        if return_anomalies:
            return (False, None, anomalies)
        return (False, None)

    if current_skyline_app == 'webapp':
        return (anomalous, anomalyScore, anomalies, anomalies_dict)
    if return_anomalies:
        return (anomalous, anomalyScore, anomalies)
    return (anomalous, anomalyScore)
