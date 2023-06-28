"""
mstl.py
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
import pandas as pd
from statsforecast import StatsForecast
from statsforecast.models import MSTL

from skyline_functions import get_graphite_metric
from functions.victoriametrics.get_victoriametrics_metric import get_victoriametrics_metric

# The name of the function MUST be the same as the name declared in
# settings.CUSTOM_ALGORITHMS.
# It MUST have 3 parameters:
# current_skyline_app, timeseries, algorithm_parameters
# See https://earthgecko-skyline.readthedocs.io/en/latest/algorithms/custom-algorithms.html
# for a full explanation about each.
# ALWAYS WRAP YOUR ALGORITHM IN try and except


# @added 20230427 - Feature #4896: custom_algorithms - mstl
def mstl(current_skyline_app, parent_pid, timeseries, algorithm_parameters):

    """
    EXPERIMENTAL

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
        custom_algorithm and algorithm itself.  For the mstl
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
    algorithm_name = 'mstl'

    start = time()

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

    print_debug = False
    try:
        print_debug = algorithm_parameters['print_debug']
    except:
        print_debug = False

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

    base_name = None
    try:
        if 'base_name' in list(algorithm_parameters.keys()):
            base_name = algorithm_parameters['base_name']
        if print_debug:
            print("algorithm_parameters['base_name']:", base_name)
        if 'metric' in list(algorithm_parameters.keys()):
            base_name = algorithm_parameters['metric']
        if print_debug:
            print("algorithm_parameters['metric']:", base_name)
        if not base_name:
            base_name = algorithm_parameters['metric']
    except:
        record_algorithm_error(current_skyline_app, parent_pid, algorithm_name, traceback.format_exc())
        # Return None and None as the algorithm could not determine True or False
        if return_results:
            return (None, None, None)
        return (None, None)

    # @added 20230419 - Feature #4892: SNAB - labelled_metrics
    labelled_metric_name = None
    try:
        labelled_metric_name = algorithm_parameters['labelled_metric_name']
        if print_debug:
            print("algorithm_parameters['labelled_metric_name']:", labelled_metric_name)
    except:
        labelled_metric_name = None

    # General
    anomaly_window = 1
    try:
        anomaly_window = int(algorithm_parameters['anomaly_window'])
    except:
        anomaly_window = 1

    if print_debug:
        print('mstl checking %s with %s datapoints' % (base_name, str(len(timeseries))))
    if debug_logging:
        current_logger.debug('debug :: mstl :: checking %s with %s datapoints' % (base_name, str(len(timeseries))))

    try:
        timestamps = [int(item[0]) for item in timeseries]
        values = [item[1] for item in timeseries]
        np_timestamps = np.array(timestamps)
        ts_diffs = np.diff(np_timestamps)
        resolution_counts = np.unique(ts_diffs, return_counts=True)
        resolution = resolution_counts[0][np.argmax(resolution_counts[1])]
        duration = timestamps[-1] - timestamps[0]
    except Exception as err:
        if print_debug:
            print('error :: resolution and duration')
            print(traceback.format_exc())
        if debug_logging:
            current_logger.error('error :: debug :: mstl :: checking resolution and duration for %s - %s' % (base_name, err))
        record_algorithm_error(current_skyline_app, parent_pid, algorithm_name, traceback.format_exc())
        # Return None and None as the algorithm could not determine True or False
        if return_results:
            return (None, None, None)
        return (None, None)

    if debug_logging:
        current_logger.debug('debug :: mstl :: determined resolution: %s for %s' % (str(resolution), base_name))

    res_minute = int(resolution / 60)
    freq_str = '%sT' % str(res_minute)

    number_of_predictions = 1
    level = 99
    try:
        level = int(algorithm_parameters['level'])
    except:
        level = 99
    
    season_hours = 24
    try:
        season_hours = int(algorithm_parameters['season_hours'])
    except:
        season_hours = 24

    season_days = 7
    try:
        season_days = int(algorithm_parameters['season_days'])
    except:
        season_days = 7

    if debug_logging:
        current_logger.debug('debug :: mstl :: forecasting for %s' % (base_name))

    # forecast_window
    try:
        uid_value = [base_name] * len(timeseries)
        df = pd.DataFrame(timeseries, columns=['ds', 'y'])
        df['ds'] = pd.to_datetime(df['ds'], unit='s')
        df['unique_id'] = uid_value
        # Create a list of models and instantiation parameters 
        # models = [MSTL(season_length=[24, 24*7])]
        models = [MSTL(season_length=[season_hours, (season_hours * season_days)])]
        if debug_logging:
            current_logger.debug('debug :: mstl :: defined MSTL model for %s' % (base_name))
        quick_sf = StatsForecast(
            df=df[-60:], 
            models=models, 
            freq=freq_str, 
            n_jobs=2,
        )
        horizon = number_of_predictions
        levels = [level]

        start = time()
        if debug_logging:
            current_logger.debug('debug :: mstl :: calculating quick_fcst for %s' % (base_name))
        quick_fcst = quick_sf.forecast(h=horizon, level=levels, fitted=True)
        if debug_logging:
            current_logger.debug('debug :: mstl :: calculated quick_fcst for %s' % (base_name))
        sf = StatsForecast(
            df=df, 
            models=models, 
            freq=freq_str, 
            n_jobs=2,
        )
        if debug_logging:
            current_logger.debug('debug :: mstl :: defined sf for %s' % (base_name))
        start = time()
        if debug_logging:
            current_logger.debug('debug :: mstl :: calculating fcst for %s' % (base_name))
        fcst = sf.forecast(h=horizon, level=levels, fitted=True)
        if debug_logging:
            current_logger.debug('debug :: mstl :: calculated fcst for %s' % (base_name))

        fcst = fcst.reset_index()
        hi_column = 'MSTL-hi-%s' % str(level)
        lo_column = 'MSTL-lo-%s' % str(level)

        insample_forecasts = sf.forecast_fitted_values().reset_index()
        anomalies = insample_forecasts.loc[(insample_forecasts['y'] >= insample_forecasts[hi_column]) | (insample_forecasts['y'] <= insample_forecasts[lo_column])]

        aa_insample_forecasts = insample_forecasts.copy()
        aa_insample_forecasts['ds'] = pd.to_datetime(aa_insample_forecasts['ds']).astype(int) / 10**9
        aa_anomalies = anomalies.copy()
        aa_anomalies['ds'] = pd.to_datetime(aa_anomalies['ds']).astype(int) / 10**9
        timestamps = [int(t) for t in aa_insample_forecasts['ds'].tolist()]
        values = [float(v) for v in aa_insample_forecasts['y'].tolist()]
        anomaly_timestamps = [int(t) for t in aa_anomalies['ds'].tolist()]
        anomaly_values = aa_anomalies['y'].tolist()
        aa_insample_forecasts = insample_forecasts.copy()
        aa_insample_forecasts['ds'] = pd.to_datetime(aa_insample_forecasts['ds']).astype(int) / 10**9
        aa_insample_forecasts_timestamps = [int(t) for t in aa_insample_forecasts['ds'].tolist()]
        values = [float(v) for v in aa_insample_forecasts['y'].tolist()]
        if debug_logging:
            current_logger.debug('debug :: mstl :: determining mstl anomalies for %s' % (base_name))

        scores = []
        anomalies = {}
        for index, item in enumerate(timeseries):
            if item[0] in anomaly_timestamps:
                score = 1.0
                ts = int(item[0])
                anomalies[ts] = {'value': item[1], 'index': index, 'score': score}
            else:
                score = 0.0
            scores.append(score)
        if debug_logging:
            current_logger.debug('debug :: mstl :: calculated %s scores for %s' % (str(len(scores)), base_name))

        if debug_logging:
            current_logger.debug('debug :: mstl :: determining anomaly_sum on anomaly_window: %s for %s' % (str(anomaly_window), base_name))

        anomaly_sum = sum(scores[-anomaly_window:])
        if anomaly_sum > 0:
            anomalous = True
            anomalyScore = 1.0
        else:
            anomalous = False
            anomalyScore = 0.0
        results = {
            'anomalous': anomalous,
            'anomalies': anomalies,
            'scores': scores,
            'anomalyScore_list': scores,
        }
        if debug_logging:
            current_logger.debug('debug :: mstl :: anomalous: %s, took %s seconds' % (
                str(anomalous), (time() - start)))

    except StopIteration:
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
        if debug_logging:
            current_logger.error('error :: debug :: mstl :: analysis failed on %s - %s' % (base_name, err))
        record_algorithm_error(current_skyline_app, parent_pid, algorithm_name, traceback.format_exc())
        # Return None and None as the algorithm could not determine True or False
        if return_results:
            return (None, None, None)
        return (None, None)

    if return_results:
        return (anomalous, anomalyScore, results)

    return (anomalous, anomalyScore)
