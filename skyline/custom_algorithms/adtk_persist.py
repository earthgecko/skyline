# @added 20210223 - Feature #3970: custom_algorithm - adtk_level_shift
#                   Task #3664:: POC with adtk
#                   Feature #3642: Anomaly type classification
#                   Info #3324: adtk
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
import datetime
import zlib
import numpy as np
import pandas as pd
from adtk.data import validate_series
from adtk.detector import PersistAD

from skyline_functions import get_redis_conn_decoded, mkdir_p

from settings import FULL_DURATION

# The name of the fucntion MUST be the same as the name declared in
# settings.CUSTOM_ALGORITHMS.
# It MUST have 3 parameters:
# current_skyline_app, timeseries, algorithm_parameters
# See https://earthgecko-skyline.readthedocs.io/en/latest/algorithms/custom-algorithms.html
# for a full explanation about each.
# ALWAYS WRAP YOUR ALGORITHM IN try and except


def adtk_persist(current_skyline_app, parent_pid, timeseries, algorithm_parameters):
    """
    A timeseries is anomalous if a level shift occurs in a 5 window period bound
    by a factor of 9 of the normal range based on historical interquartile range.

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
        custom_algorithm and algorithm itself.  For the matrixprofile custom
        algorithm the following parameters are required, example:
        ``algorithm_parameters={
            'c': 9.0,
            'run_every': 5,
            'side': 'both',
            'window': 5
        }``
    :type current_skyline_app: str
    :type parent_pid: int
    :type timeseries: list
    :type algorithm_parameters: dict
    :return: True, False or Non
    :rtype: boolean

    Performance is of paramount importance in Skyline, especially in terms of
    computational complexity, along with execution time and CPU usage. The
    adtk persistAD algortihm is not O(n) and it is not fast either, not when
    compared to the normal three-sigma triggered algorithms.  However it is
    useful if you care about detecting all level shifts.  The normal three-sigma
    triggered algorithms do not always detect a level shift, especially if the
    level shift does not breach the three-sigma limits.  Therefore you may find
    over time that you encounter alerts that contain level shifts that you
    thought should have been detected.  On these types of metrics and events,
    the adtk PersistAD algortihm can be implemented to detect and alert on
    these.  It is not recommended to run on all your metrics as it would
    immediately triple the analyzer runtime every if only run every 5 windows/
    minutes.

    Due to the computational complexity and long run time of the adtk
    PersistAD algorithm on the size of timeseries data used by Skyline.
    Therefore the analysis methodology implemented for the adtk_persist
    custom_algorithm is as folows:

    - When new metrics are added either to the configuration or by actual new
    metrics coming online that match the ``algorithm_parameters['namespace']``,
    Skyline implements sharding on new metrics into time slots to prevent a
    thundering herd situation from developing.  A newly added metrics will
    eventually be assigned into a time shard and be added and the last analysed
    timestamp will be added to the ``analyzer.last.adtk_persist`` Redis hash
    key to determine the next scheduled run with
    ``algorithm_parameters['namespace']``

    - A ``run_every`` parameter is implemented so that the algorithm can be
    configured to run on a metric once every ``run_every`` minutes.  The default
    is to run it every 5 minutes using window 5 (rolling) and trigger as
    anomalous if the algorithm labels any of the last 5 datapoints as anomalous.
    This means that there could be up to a 5 minute delay on an alert on the
    60 second, 168 SECOND_ORDER_RESOLUTION_HOURS metrics in the example, but a
    ``c=9.0`` level shift would be detected and would be alerted on (if both
    analyzer and mirage triggered on it).  This periodic running of the
    algorithm is a tradeoff so that the adtk_persist load and runtime can be
    spread over ``run_every`` minutes.

    - The algorithm is not run against metrics that are sparsely populated.
    When the algorithm is run on sparsely populated metrics it results in lots
    of false positives and noise.

    The Skyline CUSTOM_ALGORITHMS implementation of the adtk PersistAD
    algorithm is configured as the example shown below.  However please note
    that the algorithm_parameters shown in this example configuration are
    suitiable for metrics that have a 60 second relation and have a
    :mod:`settings.ALERTS` Mirage SECOND_ORDER_RESOLUTION_HOURS of 168 (7 days).
    For metrics with a different resolution/frequency may require different
    values appropriate for metric resolution.

    :
    Example CUSTOM_ALGORITHMS configuration:

    'adtk_persist': {
        'namespaces': [
            'skyline.analyzer.run_time', 'skyline.analyzer.total_metrics',
            'skyline.analyzer.exceptions'
        ],
        'algorithm_source': '/opt/skyline/github/skyline/skyline/custom_algorithms/adtk_persist.py',
        'algorithm_parameters': {'c': 9.0, 'run_every': 5, 'side': 'both', 'window': 5},
        'max_execution_time': 0.5,
        'consensus': 1,
        'algorithms_allowed_in_consensus': ['adtk_persist'],
        'run_3sigma_algorithms': True,
        'run_before_3sigma': True,
        'run_only_if_consensus': False,
        'use_with': ["analyzer", "mirage"],
        'debug_logging': False,
    },

    """

    # You MUST define the algorithm_name
    algorithm_name = 'adtk_persist'

    # Define the default state of None and None, anomalous does not default to
    # False as that is not correct, False is only correct if the algorithm
    # determines the data point is not anomalous.  The same is true for the
    # anomalyScore.
    anomalous = None
    anomalyScore = None

    # @aded 20210308 - Feature #3978: luminosity - classify_metrics
    #                  Feature #3642: Anomaly type classification
    return_anomalies = False
    anomalies = []
    realtime_analysis = True

    current_logger = None

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
            return (False, None)

    # Allow the PersistAD window parameter to be passed in the
    # algorithm_parameters
    window = 5
    try:
        window = algorithm_parameters['window']
    except:
        pass

    # Allow the PersistAD c parameter to be passed in the
    # algorithm_parameters
    c = 9.0
    try:
        c = algorithm_parameters['c']
    except:
        pass

    run_every = window
    try:
        run_every = algorithm_parameters['run_every']
    except:
        pass

    side = 'both'
    try:
        side = algorithm_parameters['side']
    except:
        pass

    if debug_logging:
        current_logger.debug('debug :: algorithm_parameters :: %s' % (
            str(algorithm_parameters)))

    # @added 20210308 - Feature #3978: luminosity - classify_metrics
    #                   Feature #3642: Anomaly type classification
    try:
        return_anomalies = algorithm_parameters['return_anomalies']
    except:
        return_anomalies = False
    try:
        realtime_analysis = algorithm_parameters['realtime_analysis']
    except:
        realtime_analysis = True

    # @added 20210316 - Feature #3978: luminosity - classify_metrics
    #                   Feature #3642: Anomaly type classification
    save_plots_to = False
    try:
        save_plots_to = algorithm_parameters['save_plots_to']
    except:
        pass

    # @added 20210323 - Feature #3978: luminosity - classify_metrics
    #                   Feature #3642: Anomaly type classification
    save_plots_to_absolute_dir = False
    try:
        save_plots_to_absolute_dir = algorithm_parameters['save_plots_to_absolute_dir']
    except:
        pass
    filename_prefix = False
    try:
        filename_prefix = algorithm_parameters['filename_prefix']
    except:
        pass

    if debug_logging:
        current_logger.debug('debug :: algorithm_parameters :: %s' % (
            str(algorithm_parameters)))

    try:
        base_name = algorithm_parameters['base_name']
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
        if return_anomalies:
            return (False, None, anomalies)
        else:
            return (False, None)
    if debug_logging:
        current_logger.debug('debug :: %s :: base_name - %s' % (
            algorithm_name, str(base_name)))

    # Due to the load and runtime of PersistAD it is only run in analyzer
    # periodically
    if current_skyline_app == 'analyzer':
        redis_conn_decoded = get_redis_conn_decoded(current_skyline_app)
        last_hash_key = 'analyzer.last.%s' % algorithm_name
        last_check = None
        try:
            raw_last_check = redis_conn_decoded.hget(last_hash_key, base_name)
            last_check = int(raw_last_check)
        except:
            last_check = None
        last_window_timestamps = [int(item[0]) for item in timeseries[-run_every:]]
        if last_check in last_window_timestamps:
            if debug_logging:
                current_logger.debug('debug :: %s :: run_every period is not over yet, skipping base_name - %s' % (
                    algorithm_name, str(base_name)))
            if return_anomalies:
                return (False, None, anomalies)
            else:
                return (False, None)

        # If there is no last timestamp, shard the metric, it will eventually
        # be added.
        if not last_check:
            now = datetime.datetime.now()
            now_seconds = int(now.second)
            if now_seconds == 0:
                now_seconds = 1
            period_seconds = int(60 / run_every)
            shard = int(period_seconds)
            last_shard = 60
            shard = int(period_seconds)
            shards = [shard]
            while shard < last_shard:
                shard = shard + period_seconds
                shards.append((shard))
            shard_value = round(now_seconds / shards[0]) * shards[0]
            if shard_value <= shards[0]:
                shard_value = shards[0]
            metric_as_bytes = str(base_name).encode()
            value = zlib.adler32(metric_as_bytes)
            shard_index = [(index + 1) for index, s_value in enumerate(shards) if s_value == shard_value][0]
            modulo_result = value % shard_index
            if modulo_result == 0:
                if debug_logging:
                    current_logger.debug('debug :: %s :: skipping as not sharded into this run - %s' % (
                        algorithm_name, str(base_name)))
            if return_anomalies:
                return (False, None, anomalies)
            else:
                return (False, None)
        if debug_logging:
            current_logger.debug('debug :: %s :: analysing %s' % (
                algorithm_name, str(base_name)))

        try:
            int_metric_timestamp = int(timeseries[-1][0])
        except:
            int_metric_timestamp = 0
        if int_metric_timestamp:
            try:
                redis_conn_decoded.hset(
                    last_hash_key, base_name,
                    int_metric_timestamp)
            except:
                pass

    # ALWAYS WRAP YOUR ALGORITHM IN try and the BELOW except
    try:
        start_preprocessing = timer()

        # INFO: Sorting time series of 10079 data points took 0.002215 seconds
        timeseries = sorted(timeseries, key=lambda x: x[0])
        if debug_logging:
            current_logger.debug('debug :: %s :: time series of length - %s' % (
                algorithm_name, str(len(timeseries))))

        # Testing the data to ensure it meets minimum requirements, in the case
        # of Skyline's use of the PersistAD algorithm this means that:
        # - the time series must have at least 75% of its full_duration
        # - the time series must have at least 99% of the data points for the
        #   in the sample being analysed.
        do_not_use_sparse_data = False
        if current_skyline_app == 'analyzer':
            do_not_use_sparse_data = True

        # @added 20210305 - Feature #3970: custom_algorithm - adtk_level_shift
        #                   Task #3664:: POC with adtk
        # With mirage also do not run PersistAD on sparsely populated data
        if current_skyline_app == 'mirage':
            do_not_use_sparse_data = True

        # @aded 20210309 - Feature #3978: luminosity - classify_metrics
        #                  Feature #3642: Anomaly type classification
        if current_skyline_app == 'luminosity':
            do_not_use_sparse_data = True

        if do_not_use_sparse_data:

            total_period = 0
            total_datapoints = 0
            try:
                start_timestamp = int(timeseries[0][0])
                end_timestamp = int(timeseries[-1][0])
                total_period = end_timestamp - start_timestamp
                total_datapoints = len(timeseries)
            except SystemExit as e:
                if debug_logging:
                    current_logger.debug('debug_logging :: %s :: SystemExit called, exiting - %s' % (
                        algorithm_name, e))
                if return_anomalies:
                    return (anomalous, anomalyScore, anomalies)
                else:
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
                if return_anomalies:
                    return (anomalous, anomalyScore, anomalies)
                else:
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

            # If the time series does not have 75% of its full_duration it does not
            # have sufficient data to sample
            try:
                if total_period < period_required:
                    if debug_logging:
                        current_logger.debug('debug :: %s :: time series does not have sufficient data' % (
                            algorithm_name))
                    if return_anomalies:
                        return (anomalous, anomalyScore, anomalies)
                    else:
                        return (anomalous, anomalyScore)
            except SystemExit as e:
                if debug_logging:
                    current_logger.debug('debug_logging :: %s :: SystemExit called, exiting - %s' % (
                        algorithm_name, e))
                if return_anomalies:
                    return (anomalous, anomalyScore, anomalies)
                else:
                    return (anomalous, anomalyScore)
            except:
                traceback_msg = traceback.format_exc()
                record_algorithm_error(current_skyline_app, parent_pid, algorithm_name, traceback_msg)
                if debug_logging:
                    current_logger.error(traceback_msg)
                    current_logger.error('error :: debug_logging :: %s :: falied to determine if time series has sufficient data' % (
                        algorithm_name))
                if return_anomalies:
                    return (anomalous, anomalyScore, anomalies)
                else:
                    return (anomalous, anomalyScore)

            # If the time series does not have 75% of its full_duration data points
            # it does not have sufficient data to sample

            # Determine resolution from the last 30 data points
            # INFO took 0.002060 seconds
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
                    if return_anomalies:
                        return (anomalous, anomalyScore, anomalies)
                    else:
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
            minimum_datapoints = None
            if metric_resolution:
                minimum_datapoints = int(period_required / metric_resolution)
            if minimum_datapoints:
                if total_datapoints < minimum_datapoints:
                    if debug_logging:
                        current_logger.debug('debug :: %s :: time series does not have sufficient data, minimum_datapoints required is %s and time series has %s' % (
                            algorithm_name, str(minimum_datapoints),
                            str(total_datapoints)))
                    if return_anomalies:
                        return (anomalous, anomalyScore, anomalies)
                    else:
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
                if return_anomalies:
                    return (anomalous, anomalyScore, anomalies)
                else:
                    return (anomalous, anomalyScore)
            if len(set(item[1] for item in timeseries)) == 1:
                if debug_logging:
                    current_logger.debug('debug :: %s :: time series does not have sufficient variability, all the values are the same' % algorithm_name)
                anomalous = False
                anomalyScore = 0.0
                if return_anomalies:
                    return (anomalous, anomalyScore, anomalies)
                else:
                    return (anomalous, anomalyScore)

        end_preprocessing = timer()
        preprocessing_runtime = end_preprocessing - start_preprocessing
        if debug_logging:
            current_logger.debug('debug :: %s :: preprocessing took %.6f seconds' % (
                algorithm_name, preprocessing_runtime))

        if not timeseries:
            if debug_logging:
                current_logger.debug('debug :: %s :: PersistAD not run as no data' % (
                    algorithm_name))
            anomalies = []
            if return_anomalies:
                return (anomalous, anomalyScore, anomalies)
            else:
                return (anomalous, anomalyScore)
        else:
            if debug_logging:
                current_logger.debug('debug :: %s :: timeseries length: %s' % (
                    algorithm_name, str(len(timeseries))))

        if len(timeseries) < 100:
            if debug_logging:
                current_logger.debug('debug :: %s :: time series does not have sufficient data' % (
                    algorithm_name))
            if return_anomalies:
                return (anomalous, anomalyScore, anomalies)
            else:
                return (anomalous, anomalyScore)

        start_analysis = timer()
        try:
            df = pd.DataFrame(timeseries, columns=['date', 'value'])
            df['date'] = pd.to_datetime(df['date'], unit='s')
            datetime_index = pd.DatetimeIndex(df['date'].values)
            df = df.set_index(datetime_index)
            df.drop('date', axis=1, inplace=True)
            s = validate_series(df)
            persist_ad = PersistAD(c=c, side=side, window=window)
            anomaly_df = persist_ad.fit_detect(s)
            anomalies = anomaly_df.loc[anomaly_df['value'] > 0]
            anomalous = False
            if len(anomalies) > 0:
                anomaly_timestamps = list(anomalies.index.astype(np.int64) // 10**9)
                if realtime_analysis:
                    last_window_timestamps = [int(item[0]) for item in timeseries[-window:]]
                    # if timeseries[-1][0] in anomaly_timestamps:
                    for timestamp in last_window_timestamps:
                        if timestamp in anomaly_timestamps:
                            anomalous = True
                            break
                else:
                    anomalous = True
                    # Convert anomalies dataframe to anomalies_list
                    anomalies_list = []

                    # @added 20210316 - Feature #3978: luminosity - classify_metrics
                    #                   Feature #3642: Anomaly type classification
                    # Convert anomalies dataframe to anomalies_dict
                    anomalies_dict = {}
                    anomalies_dict['metric'] = base_name
                    anomalies_dict['timestamp'] = int(timeseries[-1][0])
                    anomalies_dict['from_timestamp'] = int(timeseries[0][0])
                    anomalies_dict['algorithm'] = algorithm_name
                    anomalies_dict['anomalies'] = {}

                    for ts, value in timeseries:
                        if int(ts) in anomaly_timestamps:
                            anomalies_list.append([int(ts), value])
                            anomalies_dict['anomalies'][int(ts)] = value
                    anomalies = list(anomalies_list)

                    # @added 20210316 - Feature #3978: luminosity - classify_metrics
                    #                   Feature #3642: Anomaly type classification
                    if save_plots_to:
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
                            title = '%s\n%s' % (algorithm_name, base_name)
                            if not os_path_exists(save_to_path):
                                try:
                                    mkdir_p(save_to_path)
                                except Exception as e:
                                    current_logger.error('error :: %s :: failed to create dir - %s - %s' % (
                                        algorithm_name, save_to_path, e))
                            if os_path_exists(save_to_path):
                                try:
                                    plot(s, anomaly=anomaly_df, anomaly_color='red', title=title, save_to_file=save_to_file)
                                    if debug_logging:
                                        current_logger.debug('debug :: %s :: plot saved to - %s' % (
                                            algorithm_name, save_to_file))
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
                            if return_anomalies:
                                return (anomalous, anomalyScore, anomalies)
                            else:
                                return (anomalous, anomalyScore)
                        except Exception as e:
                            traceback_msg = traceback.format_exc()
                            record_algorithm_error(current_skyline_app, parent_pid, algorithm_name, traceback_msg)
                            if debug_logging:
                                current_logger.error(traceback_msg)
                                current_logger.error('error :: %s :: failed to plot or save anomalies file - %s - %s' % (
                                    algorithm_name, base_name, e))
            else:
                anomalies = []

            try:
                del df
            except:
                pass
        except SystemExit as e:
            if debug_logging:
                current_logger.debug('debug_logging :: %s :: SystemExit called, during analysis, exiting - %s' % (
                    algorithm_name, e))
            if return_anomalies:
                return (anomalous, anomalyScore, anomalies)
            else:
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
            else:
                return (anomalous, anomalyScore)

        end_analysis = timer()
        analysis_runtime = end_analysis - start_analysis

        if debug_logging:
            current_logger.debug('debug :: %s :: PersistAD took %.6f seconds' % (
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
            current_logger.info('%s :: completed analysis in %.6f seconds' % (
                algorithm_name, processing_runtime))
        try:
            del timeseries
        except:
            pass
        if return_anomalies:
            return (anomalous, anomalyScore, anomalies)
        else:
            return (anomalous, anomalyScore)

    except SystemExit as e:
        if debug_logging:
            current_logger.debug('debug_logging :: %s :: SystemExit called (before StopIteration), exiting - %s' % (
                algorithm_name, e))
        if return_anomalies:
            return (anomalous, anomalyScore, anomalies)
        else:
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
        else:
            return (False, None)
    except:
        record_algorithm_error(current_skyline_app, parent_pid, algorithm_name, traceback.format_exc())
        # Return None and None as the algorithm could not determine True or False
        if return_anomalies:
            return (False, None, anomalies)
        else:
            return (False, None)

    if return_anomalies:
        return (anomalous, anomalyScore, anomalies)
    else:
        return (anomalous, anomalyScore)
