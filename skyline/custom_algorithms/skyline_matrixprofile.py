"""
THIS IS A MORE FEATUREFUL CUSTOM ALGORITHM to provide a skeleton to develop your
own custom algorithms.  The algorithm itself, although viable, is not
recommended for production or general use, it is simply a toy algorithm here to
demonstrate the structure of a more complex custom algorithm that has
``algorithm_parameters`` passed and can also log if enabled.
It is documented via comments #
"""

# REQUIRED Skyline imports.  All custom algorithms MUST have the following two
# imports.  These are required for exception handling and to record algorithm
# errors regardless of debug_logging setting for the custom_algorithm
import traceback
from timeit import default_timer as timer
from custom_algorithms import record_algorithm_error

import logging

# Import ALL modules that the custom algorithm requires.  Remember that if a
# requirement is not one that is provided by the Skyline requirements.txt you
# must ensure it is installed in the Skyline virtualenv
from collections import Counter
import matrixprofile as mp
import numpy as np


# The name of the fucntion MUST be the same as the name declared in
# settings.CUSTOM_ALGORITHMS.
# It MUST have 3 parameters:
# current_skyline_app, timeseries, algorithm_parameters
# See https://earthgecko-skyline.readthedocs.io/en/latest/algorithms/custom-algorithms.html
# for a full explanation about each.
# ALWAYS WRAP YOUR ALGORITHM IN try and except
def skyline_matrixprofile(current_skyline_app, parent_pid, timeseries, algorithm_parameters):
    """
    The skyline_matrixprofile algorithm uses matrixprofile to identify discords.

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
            'check_details': {<empty_dict|check_details dict>},
            'full_duration': full_duration,
            'windows': int
        }``
    :type current_skyline_app: str
    :type parent_pid: int
    :type timeseries: list
    :type algorithm_parameters: dict
    :return: True, False or Non
    :rtype: boolean

    """

    # You MUST define the algorithm_name
    algorithm_name = 'matrixprofile'

    # Define the default state of None and None, anomalous does not default to
    # False as that is not correct, False is only correct if the algorithm
    # determines the data point is not anomalous.  The same is true for the
    # anomalyScore.
    anomalous = None
    anomalyScore = None

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
            return (None, None)

    # Use the algorithm_parameters to determine if there are check_details
    check_details = {}
    try:
        check_details = algorithm_parameters['check_details']
        if debug_logging:
            current_logger.debug('debug :: %s :: snab check_details - %s' % (
                algorithm_name, str(check_details)))
    except:
        traceback_msg = traceback.format_exc()
        if current_skyline_app == 'snab':
            if not traceback_msg:
                traceback_msg = 'None'
            current_logger.error(traceback_msg)
            record_algorithm_error(current_skyline_app, parent_pid, algorithm_name, traceback_msg)
            # Return None and None as the algorithm could not determine True or False
            if debug_logging:
                current_logger.error('error :: debug :: could not determine check_details from algorithm_parameters - %s' % (
                    str(algorithm_parameters)))
                current_logger.debug('debug :: %s :: snab check_details - %s' % (
                    algorithm_name, str(check_details)))
            return (None, None)
        else:
            pass

    # Allow the matrixprofile windows parameter to be passed in the
    # check_details for snab as well
    windows = None
    if check_details:
        try:
            windows = check_details['windows']
            if debug_logging:
                current_logger.debug('debug :: %s :: windows - %s - determined from check_details' % (
                    algorithm_name, str(windows)))
        except:
            windows = None
    if not windows:
        try:
            windows = algorithm_parameters['windows']
            if debug_logging:
                current_logger.debug('debug :: %s :: windows - %s' % (
                    algorithm_name, str(windows)))
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
            return (None, None)

    # Allow the matrixprofile k discords parameter to be passed in the
    # check_details for snab as well
    k_discords = None
    if check_details:
        try:
            k_discords = int(check_details['k_discords'])
            if debug_logging:
                current_logger.debug('debug :: %s :: k_discords - %s - determined from check_details' % (
                    algorithm_name, str(k_discords)))
        except:
            k_discords = None
    if not k_discords:
        try:
            k_discords = algorithm_parameters['k_discords']
            if debug_logging:
                current_logger.debug('debug :: %s :: k_discords - %s' % (
                    algorithm_name, str(k_discords)))
        except:
            # Default to discovering 20 discords
            k_discords = 20

    # ALWAYS WRAP YOUR ALGORITHM IN try and the BELOW except
    try:

        start_preprocessing = timer()

        # INFO: Sorting time series of 10079 data points took 0.002215 seconds
        timeseries = sorted(timeseries, key=lambda x: x[0])
        if debug_logging:
            current_logger.debug('debug :: %s :: time series of length - %s' % (
                algorithm_name, str(len(timeseries))))

        # Testing the data to ensure it meets minimum requirements, in the case
        # of Skyline's use of the matrixprofile algorithm this means that:
        # - the time series must have at least 75% of its full_duration
        # - the time series must have at least 99% of the data points for the
        #   in the sample being analysed.
        do_not_use_sparse_data = False

        if do_not_use_sparse_data:
            # Default for analyzer at required period to 18 hours
            period_required = int(86400 * 0.75)
            total_period = 0
            total_datapoints = 0
            try:
                start_timestamp = int(timeseries[0][0])
                end_timestamp = int(timeseries[-1][0])
                total_period = end_timestamp - start_timestamp
                total_datapoints = len(timeseries)
            except:
                traceback_msg = traceback.format_exc()
                record_algorithm_error(current_skyline_app, parent_pid, algorithm_name, traceback_msg)
                if debug_logging:
                    current_logger.error(traceback_msg)
                    current_logger.error('error :: debug_logging :: %s :: falied to determine total_period and total_datapoints' % (
                        algorithm_name))
                timeseries = []
            if not timeseries:
                return (anomalous, anomalyScore)

            if current_skyline_app == 'snab':
                try:
                    full_duration = check_details['full_duration']
                    period_required = int(full_duration * 0.75)
                except:
                    traceback_msg = traceback.format_exc()
                    record_algorithm_error(current_skyline_app, parent_pid, algorithm_name, traceback_msg)
                    if debug_logging:
                        current_logger.error(traceback_msg)
                        current_logger.error('error :: debug_logging :: %s :: falied to determine total_period and total_datapoints' % (
                            algorithm_name))
                    return (anomalous, anomalyScore)

            # If the time series does not have 75% of its full_duration it does not
            # have sufficient data to sample
            try:
                if total_period < period_required:
                    if debug_logging:
                        current_logger.debug('debug :: %s :: time series does not have sufficient data' % (
                            algorithm_name))
                    return (anomalous, anomalyScore)
            except:
                traceback_msg = traceback.format_exc()
                record_algorithm_error(current_skyline_app, parent_pid, algorithm_name, traceback_msg)
                if debug_logging:
                    current_logger.error(traceback_msg)
                    current_logger.error('error :: debug_logging :: %s :: falied to determine if time series has sufficient data' % (
                        algorithm_name))
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
                    return (anomalous, anomalyScore)

            # Is the time series fully populated?
            # full_duration_datapoints = int(full_duration / metric_resolution)
            total_period_datapoints = int(total_period / metric_resolution)
            minimum_percentage_sparsity = 95
            sparsity = int(total_datapoints / (total_period_datapoints / 100))
            if sparsity < minimum_percentage_sparsity:
                if debug_logging:
                    current_logger.debug('debug :: %s :: time series does not have sufficient data, minimum_percentage_sparsity required is %s and time series has %s' % (
                        algorithm_name, str(minimum_percentage_sparsity),
                        str(sparsity)))
                return (anomalous, anomalyScore)

        # Preprocess the data into the required matrixprofile format and run the
        # data through the matrixprofile algorithm

        # Initially a POC was attempted using a reversed time series to validate
        # whether matrixprofile was identifying discords in the last windows of
        # the timeseries
        # reversed_timeseries = timeseries[::-1]

        try:
            dataset = [float(item[1]) for item in timeseries]

            # Do not reverse - after the right yssiM
            # dataset = dataset[::-1]

            ts = np.array(dataset)
        except:
            traceback_msg = traceback.format_exc()
            record_algorithm_error(current_skyline_app, parent_pid, algorithm_name, traceback_msg)
            if debug_logging:
                current_logger.error(traceback_msg)
                current_logger.error('error :: debug_logging :: %s :: failed to create ts' % (
                    algorithm_name))
            return (anomalous, anomalyScore)

        end_preprocessing = timer()
        preprocessing_runtime = end_preprocessing - start_preprocessing
        if debug_logging:
            current_logger.debug('debug :: %s :: preprocessing took %.6f seconds' % (
                algorithm_name, preprocessing_runtime))

        profile = None
        start_compute = timer()
        try:
            profile = mp.compute(ts, windows=windows)
        except:
            traceback_msg = traceback.format_exc()
            record_algorithm_error(current_skyline_app, parent_pid, algorithm_name, traceback_msg)
            if debug_logging:
                current_logger.error(traceback_msg)
                current_logger.error('error :: debug_logging :: %s :: failed to run mp.compute on ts' % (
                    algorithm_name))
            return (anomalous, anomalyScore)
        end_compute = timer()
        compute_runtime = end_compute - start_compute
        if debug_logging:
            current_logger.debug('debug :: %s :: mp.compute took %.6f seconds' % (
                algorithm_name, compute_runtime))

        start_discord = timer()
        if profile:
            try:
                profile = mp.discover.discords(profile, k=k_discords)
            except:
                traceback_msg = traceback.format_exc()
                record_algorithm_error(current_skyline_app, parent_pid, algorithm_name, traceback_msg)
                if debug_logging:
                    current_logger.error(traceback_msg)
                    current_logger.error('error :: debug_logging :: %s :: failed to run mp.discover.discords on profile' % (
                        algorithm_name))
                return (anomalous, anomalyScore)
        end_discord = timer()
        discord_runtime = end_discord - start_discord
        if debug_logging:
            current_logger.debug('debug :: %s :: mp.discover.discords for %s k discords took %.6f seconds' % (
                algorithm_name, str(k_discords), discord_runtime))

        discords = []
        if profile:
            try:
                for discord in profile['discords']:
                    discords.append(discord)
            except KeyError:
                anomalous = False
                anomalyScore = 0.0
                if debug_logging:
                    current_logger.debug('debug :: %s :: no discords discovered, not anomalous' % (
                        algorithm_name))
                return (anomalous, anomalyScore)
            except Exception:
                traceback_msg = traceback.format_exc()
                record_algorithm_error(current_skyline_app, parent_pid, algorithm_name, traceback_msg)
                if debug_logging:
                    current_logger.error(traceback_msg)
                    current_logger.error('error :: debug_logging :: %s :: failed to determine discords' % (
                        algorithm_name))
                return (anomalous, anomalyScore)

        if discords:
            anomaly_timestamp = int(timeseries[-1][0])
            anomaly_index = 0
            for index, item in enumerate(timeseries):
                if int(item[0]) == int(anomaly_timestamp):
                    anomaly_index = index
                    break
            anonamlous_period_indices = []
            for index, item in enumerate(timeseries):
                # @modified 20210630
                # if index in range((anomaly_index - 10), anomaly_index):
                if index in range((anomaly_index - windows), anomaly_index):
                    anonamlous_period_indices.append(index)
            anomalous = False
            discord_anomalies = []
            for discord in discords:
                if discord in anonamlous_period_indices:
                    anomalous = True
                    for index in anonamlous_period_indices:
                        if discord == index:
                            discord_anomalies.append(index)
                            if debug_logging:
                                current_logger.debug('debug :: %s :: anomalous :: anomalous_timeseries index - %s' % (
                                    algorithm_name, str(index)))
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
        return (anomalous, anomalyScore)
    except StopIteration:
        # This except pattern MUST be used in ALL custom algortihms to
        # facilitate the traceback from any errors.  The algorithm we want to
        # run super fast and without spamming the log with lots of errors.
        # But we do not want the function returning and not reporting
        # anything to the log, so the pythonic except is used to "sample" any
        # algorithm errors to a tmp file and report once per run rather than
        # spewing tons of errors into the log e.g. analyzer.log
        pass
        return (None, None)
    except:
        record_algorithm_error(current_skyline_app, parent_pid, algorithm_name, traceback.format_exc())
        # Return None and None as the algorithm could not determine True or False
        return (None, None)

    return (anomalous, anomalyScore)
