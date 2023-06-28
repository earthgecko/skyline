"""
spectral_residual.py
"""
# REQUIRED Skyline imports.  All custom algorithms MUST have the following two
# imports.  These are required for exception handling and to record algorithm
# errors regardless of debug_logging setting for the custom_algorithm
import logging
import traceback
from timeit import default_timer as timer

import numpy as np

from custom_algorithms import record_algorithm_error

# Import ALL modules that the custom algorithm requires.  Remember that if a
# requirement is not one that is provided by the Skyline requirements.txt you
# must ensure it is installed in the Skyline virtualenv
from custom_algorithm_sources.spectral_residual.spectral_residual import SpectralResidual

# The name of the function MUST be the same as the name declared in
# settings.CUSTOM_ALGORITHMS.
# It MUST have 3 parameters:
# current_skyline_app, timeseries, algorithm_parameters
# See https://earthgecko-skyline.readthedocs.io/en/latest/algorithms/custom-algorithms.html
# for a full explanation about each.
# ALWAYS WRAP YOUR ALGORITHM IN try and except


# @added 20221119 - Feature #4744: custom_algorithms - spectral_residual
def spectral_residual(current_skyline_app, parent_pid, timeseries, algorithm_parameters):

    """
    Outlier detector for time-series data using the spectral residual algorithm.
    Based on the alibi-detect implementation of "Time-Series Anomaly Detection
    Service at Microsoft" (Ren et al., 2019) https://arxiv.org/abs/1906.03821
    For Mirage this algorithm is FAST
    For Analyzer this algorithm is SLOW
    Although this algorithm is fast, it is not fast enough to be run in Analyzer,
    even if only deployed against a subset of metrics.  In testing
    spectral_residual took between 0.134828 and 0.698201 seconds to run per
    metrics, which is much too long for Analyzer

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
        custom_algorithm and algorithm itself.  For the anomalous_daily_peak
        custom algorithm no specific algorithm_parameters are required apart
        from an empty dict, example:
        ``algorithm_parameters={'return_instance_score': True}``
    :type current_skyline_app: str
    :type parent_pid: int
    :type timeseries: list
    :type algorithm_parameters: dict
    :return: anomalous, anomalyScore, instance_scores
    :rtype: tuple(boolean, float, instance_scores)

    """

    # You MUST define the algorithm_name
    algorithm_name = 'spectral_residual'

    # Define the default state of None and None, anomalous does not default to
    # False as that is not correct, False is only correct if the algorithm
    # determines the data point is not anomalous.  The same is true for the
    # anomalyScore.
    anomalous = None
    anomalyScore = None
    results = {}

    current_logger = None

    # If you wanted to log, you can but this should only be done during
    # testing and development
    def get_log(current_skyline_app):
        current_skyline_app_logger = current_skyline_app + 'Log'
        current_logger = logging.getLogger(current_skyline_app_logger)
        return current_logger

    return_results = False
    try:
        return_results = algorithm_parameters['return_results']
    except:
        return_results = False

    if not return_results:
        try:
            return_results = algorithm_parameters['return_anomalies']
        except:
            return_results = False

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
            if return_results:
                return (None, None, None)
            return (None, None)

    # Use the algorithm_parameters to determine variables
    debug_print = None
    try:
        debug_print = algorithm_parameters['debug_print']
    except:
        debug_print = False

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

    window_amp = 20
    try:
        window_amp = int(algorithm_parameters['window_amp'])
    except:
        window_amp = 20

    window_local = 20
    try:
        window_local = int(algorithm_parameters['window_local'])
    except:
        window_local = 20

    n_est_points = 20
    try:
        n_est_points = int(algorithm_parameters['estimate_points'])
    except:
        n_est_points = 20

    n_grad_points = 5
    try:
        n_grad_points = int(algorithm_parameters['gradient_points'])
    except:
        n_grad_points = 5

    anomaly_window = 1
    try:
        anomaly_window = int(algorithm_parameters['anomaly_window'])
    except:
        anomaly_window = 1

    if debug_print:
        print('running SpectralResidual with threshold: %s, window_amp: %s, window_local: %s, n_est_points: %s, n_grad_points: %s' % (
            str(threshold), str(window_amp), str(window_local),
            str(n_est_points), str(n_grad_points)))
    if debug_logging:
        current_logger.debug('debug :: running SpectralResidual with threshold: %s, window_amp: %s, window_local: %s, n_est_points: %s, n_grad_points: %s' % (
            str(threshold), str(window_amp), str(window_local),
            str(n_est_points), str(n_grad_points)))

    try:
        X = np.array([v for t, v in timeseries])
        t = np.array([t for t, v in timeseries])
        if debug_print:
            print('running SpectralResidual on X with %s datapoints' % str(len(X)))
        if debug_logging:
            current_logger.debug('debug :: running SpectralResidual on X with %s datapoints' % str(len(X)))
        start = timer()
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
        if debug_print:
            try:
                print('infer_threshold returned: %s' % str(od_preds['data']['threshold']))
            except:
                print('infer_threshold no determined')
        if debug_logging:
            try:
                current_logger.debug('debug :: infer_threshold returned: %s' % str(od_preds['data']['threshold']))
            except:
                current_logger.debug('debug :: infer_threshold no determined')

        anomaly_sum = sum(od_preds['data']['is_outlier'][-anomaly_window:])
        if anomaly_sum > 0:
            anomalous = True
        else:
            anomalous = False

        # Always convert from numpy.int64 and numpy.float64 type
        anomalies = {}
        anomalyScore_list = []
        sr_scores = []
        for index, item in enumerate(timeseries):
            anomaly_score = 0
            sr_score = 0.0
            try:
                sr_score = float(od_preds['data']['instance_score'][index])
            except:
                sr_score = 0.0
            if str(sr_score) in ['nan', 'NaN']:
                sr_score = 0.0
            try:
                anomaly_score = int(od_preds['data']['is_outlier'][index])
            except:
                anomaly_score = 0
            if anomaly_score == 1:
                ts = int(item[0])
                anomalies[ts] = {'value': item[1], 'index': index, 'score': sr_score}
            anomalyScore_list.append(anomaly_score)
            sr_scores.append(sr_score)

        results = {
            'anomalous': anomalous,
            # 'anomalies': od_preds['data']['is_outlier'],
            # 'sr_scores': od_preds['data']['instance_score'],
            'anomalies': anomalies,
            'anomalyScore_list': anomalyScore_list,
            'scores': sr_scores,
            'SpectralResidual results': od_preds,
        }
        if debug_print:
            print('ran SpectralResidual OK in %.6f seconds' % (timer() - start))
        if debug_logging:
            current_logger.debug('debug :: ran SpectralResidual OK in %.6f seconds' % (timer() - start))
        if results:
            if results['anomalous']:
                anomalous = True
                anomalyScore = 1.0
            else:
                anomalous = False
                anomalyScore = 0.0
            if debug_print:
                print('anomalous: %s' % str(anomalous))
            if debug_logging:
                current_logger.debug('debug :: anomalous: %s' % str(anomalous))
        else:
            if debug_print:
                print('error - no results')
            if debug_logging:
                current_logger.debug('debug :: error - no results')

    except StopIteration:
        if debug_print:
            print('warning - StopIteration called on SpectralResidual')
        if debug_logging:
            current_logger.debug('debug :: warning - StopIteration called on SpectralResidual')

        # This except pattern MUST be used in ALL custom algortihms to
        # facilitate the traceback from any errors.  The algorithm we want to
        # run super fast and without spamming the log with lots of errors.
        # But we do not want the function returning and not reporting
        # anything to the log, so the pythonic except is used to "sample" any
        # algorithm errors to a tmp file and report once per run rather than
        # spewing tons of errors into the log e.g. analyzer.log
        if return_results:
            return (None, None, None)
        return (None, None)
    except Exception as err:
        record_algorithm_error(current_skyline_app, parent_pid, algorithm_name, traceback.format_exc())
        if debug_print:
            print('error:', traceback.format_exc())
        if debug_logging:
            current_logger.debug('debug :: error - on SpectralResidual - %s' % err)
            current_logger.debug(traceback.format_exc())

        # Return None and None as the algorithm could not determine True or False
        if return_results:
            return (None, None, None)
        return (None, None)

    if return_results:
        return (anomalous, anomalyScore, results)

    return (anomalous, anomalyScore)
