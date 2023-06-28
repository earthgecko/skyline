"""
irregular_unstable.py
"""
# REQUIRED Skyline imports.  All custom algorithms MUST have the following two
# imports.  These are required for exception handling and to record algorithm
# errors regardless of debug_logging setting for the custom_algorithm
import logging
from time import time
import traceback
from custom_algorithms import record_algorithm_error

# Import ALL modules that the custom algorithm requires.  Remember that if a
# requirement is not one that is provided by the Skyline requirements.txt you
# must ensure it is installed in the Skyline virtualenv
import numpy as np

from custom_algorithm_sources.sigma.sigma import run_sigma_algorithms
from custom_algorithm_sources import stumpy
from custom_algorithm_sources.spectral_residual.spectral_residual import SpectralResidual
from skyline_functions import get_graphite_metric
from functions.victoriametrics.get_victoriametrics_metric import get_victoriametrics_metric
from functions.timeseries.downsample import downsample_timeseries

# The name of the function MUST be the same as the name declared in
# settings.CUSTOM_ALGORITHMS.
# It MUST have 3 parameters:
# current_skyline_app, timeseries, algorithm_parameters
# See https://earthgecko-skyline.readthedocs.io/en/latest/algorithms/custom-algorithms.html
# for a full explanation about each.
# ALWAYS WRAP YOUR ALGORITHM IN try and except


# @added 20230414 - Feature #4848: mirage - analyse.irregular.unstable.timeseries.at.30days
def irregular_unstable(current_skyline_app, parent_pid, timeseries, algorithm_parameters):

    """
    A timeseries is NOT anomalous if it has low variance over 30 days and does
    not trigger multiple algorithms.

    Only timeseries that are thought to be anomalous AND have a low variance at
    7 days should be run through this algorithm.  It is meant to be run with
    Mirage after all algorithms, inlcuding custom algorithms have been run and
    found a metric to be ANOMALOUS.  This algorithm does a final check to see if
    the metric has low variance and if so analyses the data at 30 days.

    On irregular, unstable metrics that exhibit low variance at 7 days and 30
    days, the algorithm generally results in ~63% of anomalies on these
    timeseries at 7 days, being correctly identified as false positives when the
    data is analysed at 30 days.

    The irregular_unstable algorithm takes on average 3.861 seconds to run,
    however if the timeseries is discarded because at 30 days it does not have
    low variance, the average discard time is 0.213 seconds.

    :param current_skyline_app: the Skyline app executing the algorithm.  This
        will be passed to the algorithm by Skyline.  This is **required** for
        error handling and logging.  You do not have to worry about handling the
        argument in the scope of the custom algorithm itself,  but the algorithm
        must accept it as the first agrument.
    :param parent_pid: the parent pid which is executing the algorithm, this is
        **required** for error handling and logging.  You do not have to worry
        about handling this argument in the scope of algorithm, but the
        algorithm must accept it as the second argument.
    :param timeseries: the time series as a list e.g. ``[[1667608854, 1269121024.0],
        [1667609454, 1269174272.0], [1667610054, 1269174272.0]]``
    :param algorithm_parameters: a dictionary of any required parameters for the
        custom_algorithm and algorithm itself.  For the irregular_unstable
        custom algorithm no specific algorithm_parameters are required apart
        from an empty dict, example: ``algorithm_parameters={}``.  But the
        number_of_daily_peaks can be passed define how many peaks must exist in
        the window period to be classed as normal.  If this is set to 3 and say
        that we are checking a possible anomaly at 00:05, there need to be 3
        peaks that occur over the past 7 days in the dialy 23:35 to 00:05 window
        if there are not at least 3 then this is considered as anomalous.
        ``algorithm_parameters={'number_of_daily_peaks': 3}``
    :type current_skyline_app: str
    :type parent_pid: int
    :type timeseries: list
    :type algorithm_parameters: dict
    :return: anomalous, anomalyScore
    :rtype: tuple(boolean, float)

    """

    # You MUST define the algorithm_name
    algorithm_name = 'irregular_unstable'

    start = time()

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

    def normalised_variance(values):
        normalised_var = np.nan
        try:
            np_values = np.array(values)
            np_max = np.amax(np_values)
            np_min = np.amin(np_values)
            norm_np_values = (np_values - np_min) / (np_max - np_min)
            normalised_var = round(np.var(norm_np_values), 4)
        except:
            normalised_var = np.nan
        return normalised_var
    
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

    print_debug = False
    try:
        print_debug = algorithm_parameters['print_debug']
    except:
        print_debug = False

    base_name = None
    try:
        base_name = algorithm_parameters['metric']
        if print_debug:
            print("algorithm_parameters['metric']:", base_name)
    except:
        record_algorithm_error(current_skyline_app, parent_pid, algorithm_name, traceback.format_exc())
        # Return None and None as the algorithm could not determine True or False
        return (None, None)

    # @added 20230419 - Feature #4892: SNAB - labelled_metrics
    labelled_metric_name = None
    try:
        labelled_metric_name = algorithm_parameters['labelled_metric_name']
        if print_debug:
            print("algorithm_parameters['labelled_metric_name']:", labelled_metric_name)
    except:
        labelled_metric_name = None

    low_variance = 0.009
    try:
        low_variance = algorithm_parameters['low_variance']
        if print_debug:
            print("algorithm_parameters['low_variance']:", low_variance)
    except:
        low_variance = 0.009

    # General
    anomaly_window = 1
    try:
        anomaly_window = int(algorithm_parameters['anomaly_window'])
    except:
        anomaly_window = 1
    # spectral_residual
    threshold = None
    try:
        threshold = float(algorithm_parameters['threshold'])
    except:
        threshold = None
    threshold_perc = 99
    try:
        threshold_perc = float(algorithm_parameters['threshold_perc'])
    except:
        threshold_perc = 99
    # matrixprofile
    windows = 5
    k_discords = 20
    # sigma
    sigma_value = 3
    try:
        sigma_value = int(algorithm_parameters['sigma'])
    except:
        sigma_value = 3
    sigma_consensus = 6
    try:
        sigma_consensus = algorithm_parameters['consensus']
    except:
        sigma_consensus = 6

    # @added 20230429 - Feature #4848: mirage - analyse.irregular.unstable.timeseries.at.30days
    # Use downsampled_timeseries data
    downsampled = False
    try:
        downsampled = algorithm_parameters['downsample_data']
    except:
        downsampled = False
    downsampled_timeseries = []
    if downsampled:
        downsampled_timeseries = list(timeseries)

    if print_debug:
        print('irregular_unstable checking %s with %s datapoints' % (base_name, str(len(timeseries))))
    if debug_logging:
        current_logger.debug('debug :: irregular_unstable :: checking %s with %s datapoints' % (base_name, str(len(timeseries))))        

    try:
        # Check the normalised variance at 7 days
        normalised_var = np.nan
        try:
            timestamps = [int(item[0]) for item in timeseries]
            values = [item[1] for item in timeseries]
            np_timestamps = np.array(timestamps)
            ts_diffs = np.diff(np_timestamps)
            resolution_counts = np.unique(ts_diffs, return_counts=True)
            resolution = resolution_counts[0][np.argmax(resolution_counts[1])]
            if resolution > 900:
                # Not suited to low resolution data
                return (True, 1.0)
            duration = timestamps[-1] - timestamps[0]
            if duration < 446400:
                # Not suitable for less than 5.25 days worth of data
                return (True, 1.0)
            normalised_var = normalised_variance(values)
        except:
            if print_debug:
                print('error :: normalised_variance')
                print(traceback.format_exc())
            record_algorithm_error(current_skyline_app, parent_pid, algorithm_name, traceback.format_exc())
            # Return None and None as the algorithm could not determine True or False
            return (None, None)

        if debug_logging:
            current_logger.debug('debug :: irregular_unstable :: normalised_var: %s' % str(normalised_var))

        if print_debug:
            print('irregular_unstable normalised_var: %s' % (str(normalised_var)))

        if not normalised_var:
            return (None, None)
        if normalised_var > low_variance:
            return (True, 1.0)

        timeseries = []
        until_timestamp = timestamps[-1]
        from_timestamp = until_timestamp - (86400 * 30)
        try:
            if not labelled_metric_name:
                timeseries = get_graphite_metric(current_skyline_app, base_name, from_timestamp, until_timestamp, 'list', 'object')
            else:
                timeseries = get_victoriametrics_metric(current_skyline_app, base_name, from_timestamp, until_timestamp, 'list', 'object')
        except:
            if print_debug:
                print('error :: no timeseries fetched')
                print(traceback.format_exc())
            record_algorithm_error(current_skyline_app, parent_pid, algorithm_name, traceback.format_exc())
            # Return None and None as the algorithm could not determine True or False
            return (None, None)
        if not timeseries:
            return (None, None)
        if print_debug:
            print('irregular_unstable long timeseries length: %s' % (str(len(timeseries))))

        # @added 20230429 - Feature #4848: mirage - analyse.irregular.unstable.timeseries.at.30days
        # Use downsampled_timeseries data (the FULL_DURATION and Graphite data) and align and merge
        # with the 30 day data using the downsampled_timeseries data values
        if downsampled:
            aligned_timeseries = []
            for ts, value in timeseries:
                aligned_timeseries.append([int(int(ts) // resolution * resolution), value])
            aligned_downsampled_timeseries = []
            aligned_downsampled_timestamps = []
            for ts, value in downsampled_timeseries:
                aligned_ts = int(int(ts) // resolution * resolution)
                aligned_downsampled_timeseries.append([aligned_ts, value])
                aligned_downsampled_timestamps.append(aligned_ts)
            reduced_aligned_timeseries = [item for item in aligned_timeseries if item[0] not in aligned_downsampled_timestamps]
            if reduced_aligned_timeseries:
                timeseries = reduced_aligned_timeseries + downsampled_timeseries
                if print_debug:
                    print('irregular_unstable long timeseries aligned and merged with downsampled timeseries')
                if debug_logging:
                    current_logger.debug('debug :: irregular_unstable :: long timeseries aligned and merged with downsampled timeseries')

        normalised_var = np.nan
        try:
            normalised_var = normalised_variance([item[1] for item in timeseries])
        except:
            record_algorithm_error(current_skyline_app, parent_pid, algorithm_name, traceback.format_exc())
            # Return None and None as the algorithm could not determine True or False
            return (None, None)
        if print_debug:
            print('irregular_unstable long timeseries normalised_var: %s' % (str(normalised_var)))

        if debug_logging:
            current_logger.debug('debug :: irregular_unstable :: long timeseries normalised_var: %s' % str(normalised_var))

        # @modified 20230505 - Feature #4848: mirage - analyse.irregular.unstable.timeseries.at.30days
        # After analysing and assessing the variance values for 251 tN SNAB results and
        # 7 fN results and 1 unsure result, it was determined that at 30 days the
        # normalised_var should be < 0.0065.  Of the 251 tN result only 7 of those were
        # >= 0.0065 (0.0065, 0.0065, 0.0066, 0.0066, 0.0081, 0.0096, 0.0104).  These
        # were reassessed and found that they could be deemed either way.  Further the
        # unsure metric was 0.0065, therefore for the sake of perhaps 2.5% fPs we ensure
        # less fNs or unsures (with current knowledge)
        # if normalised_var > low_variance:
        if normalised_var >= 0.0065:
            return (True, 1.0)

        X = np.array([v for t, v in timeseries])
        t = np.array([t for t, v in timeseries])

        consensus = []
        # Check spectral_residual at 30 days (or greater than 7 day)
        anomalous = False
        try:
            if print_debug:
                print('running SpectralResidual on X with %s datapoints' % str(len(X)))
            if debug_logging:
                current_logger.debug('debug :: running SpectralResidual on X with %s datapoints' % str(len(X)))
            start_sr = time()
            od = SpectralResidual(
                threshold=threshold,             # threshold for outlier score
                window_amp=20,                   # window for the average log amplitude
                window_local=20,                 # window for the average saliency map
                n_est_points=20,                 # nb of estimated points padded to the end of the sequence
                padding_amp_method='reflect',    # padding method to be used prior to each convolution over log amplitude.
                padding_local_method='reflect',  # padding method to be used prior to each convolution over saliency map.
                padding_amp_side='bilateral'     # whether to pad the amplitudes on both sides or only on one side.
            )
            if not threshold:
                od.infer_threshold(X, t, threshold_perc=threshold_perc)
            od_preds = od.predict(X, t, return_instance_score=True, threshold_perc=threshold_perc)
            if print_debug:
                try:
                    print('infer_threshold returned: %s' % str(od_preds['data']['threshold']))
                except:
                    print('infer_threshold no determined')
            # @modified 20230516
            # If the anomaly_window is 1, give spectral_residual more because
            # it often triggers on the far leading side rather than the trailing
            # side or the peak
            if anomaly_window == 1:
                use_anomaly_window = anomaly_window + 2
                anomaly_sum = sum(od_preds['data']['is_outlier'][-use_anomaly_window:])
            else:
                anomaly_sum = sum(od_preds['data']['is_outlier'][-anomaly_window:])
            if anomaly_sum > 0:
                consensus.append('spectral_residual')
                anomalous = True
            if print_debug:
                print('spectral_residual - anomalous: %s, took %s seconds' % (str(anomalous), (time() - start_sr)))
        except:
            if print_debug:
                print('error :: spectral_residual')
                print(traceback.format_exc())
            record_algorithm_error(current_skyline_app, parent_pid, algorithm_name, traceback.format_exc())

        if debug_logging:
            current_logger.debug('debug :: irregular_unstable :: spectral_residual - anomalous: %s, took %s seconds' % (str(anomalous), (time() - start_sr)))

        # Check matrixprofile at 30 days (or greater than 7 day)
        profile = None
        discords = []
        anomalous = False
        start_mp = time()
        try:
            profile = stumpy.stump(X, m=windows)
            if isinstance(profile, np.ndarray):
                profile = np.argsort(profile[:, 0])[-k_discords:]
                for discord in np.sort(profile):
                    discords.append(discord)
            if discords:
                anomaly_timestamp = int(timeseries[-1][0])
                anomaly_index = 0
                for index, item in enumerate(timeseries):
                    if int(item[0]) == int(anomaly_timestamp):
                        anomaly_index = index
                        break
                anonamlous_period_indices = []
                for index, item in enumerate(timeseries):
                    if index in range((anomaly_index - windows), anomaly_index):
                        anonamlous_period_indices.append(index)
                discord_anomalies = []
                for discord in discords:
                    if discord in anonamlous_period_indices:
                        anomalous = True
                        for index in anonamlous_period_indices:
                            if discord == index:
                                discord_anomalies.append(index)
            if anomalous:
                consensus.append('matrixprofile')
            if print_debug:
                print('matrixprofile - anomalous: %s, took %s seconds' % (str(anomalous), (time() - start_mp)))
        except:
            if print_debug:
                print('error :: matrixprofile')
                print(traceback.format_exc())
            record_algorithm_error(current_skyline_app, parent_pid, algorithm_name, traceback.format_exc())

        if debug_logging:
            current_logger.debug('debug :: irregular_unstable :: matrixprofile - anomalous: %s, took %s seconds' % (str(anomalous), (time() - start_sr)))

        if len(consensus) == 2:
            if print_debug:
                print('irregular_unstable consensus: %s, took %s seconds' % (str(consensus), (time() - start)))
            return (True, 1.0)

        anomalous = False
        start_s = time()
        try:
            anomalous, anomalies = run_sigma_algorithms(current_skyline_app, timeseries, sigma_value, sigma_consensus, anomaly_window)
            if anomalous:
                consensus.append('sigma')
        except:
            if print_debug:
                print('error :: sigma')
                print(traceback.format_exc())
            record_algorithm_error(current_skyline_app, parent_pid, algorithm_name, traceback.format_exc())
        if debug_logging:
            current_logger.debug('debug :: irregular_unstable :: sigma - anomalous: %s, took %s seconds' % (str(anomalous), (time() - start_sr)))

        if print_debug:
            print('sigma - anomalous: %s, took %s seconds' % (str(anomalous), (time() - start_s)))

        if print_debug:
            print('irregular_unstable consensus: %s, took %s seconds' % (str(consensus), (time() - start)))

        if len(consensus) >= 2:
            anomalous = True
            anomalyScore = 1.0
            if debug_logging:
                current_logger.debug('debug :: irregular_unstable :: anomalous: %s, consensus: %s, took %s seconds' % (
                    str(anomalous), str(consensus), (time() - start)))
            return (True, 1.0)
        else:
            anomalous = False
            anomalyScore = 0.0
            if debug_logging:
                current_logger.debug('debug :: irregular_unstable :: anomalous: %s, consensus: %s, took %s seconds' % (
                    str(anomalous), str(consensus), (time() - start)))
            return (False, 0.0)

    except StopIteration:
        # This except pattern MUST be used in ALL custom algortihms to
        # facilitate the traceback from any errors.  The algorithm we want to
        # run super fast and without spamming the log with lots of errors.
        # But we do not want the function returning and not reporting
        # anything to the log, so the pythonic except is used to "sample" any
        # algorithm errors to a tmp file and report once per run rather than
        # spewing tons of errors into the log e.g. analyzer.log
        return (None, None)
    except:
        record_algorithm_error(current_skyline_app, parent_pid, algorithm_name, traceback.format_exc())
        # Return None and None as the algorithm could not determine True or False
        return (None, None)

    return (anomalous, anomalyScore)
