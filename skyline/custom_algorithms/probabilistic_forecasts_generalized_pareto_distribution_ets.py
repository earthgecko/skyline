"""
probabilistic_forecasts_generalized_pareto_distribution_ets.py
"""
# REQUIRED Skyline imports.  All custom algorithms MUST have the following two
# imports.  These are required for exception handling and to record algorithm
# errors regardless of debug_logging setting for the custom_algorithm
import logging
import traceback
from custom_algorithms import record_algorithm_error

# Import ALL modules that the custom algorithm requires.  Remember that if a
# requirement is not one that is provided by the Skyline requirements.txt you
# must ensure it is installed in the Skyline virtualenv
from time import time
import numpy as np
import pandas as pd
from statsmodels.tsa.statespace.exponential_smoothing import ExponentialSmoothing as ETS
from scipy.stats import norm, genpareto
import warnings
from statsmodels.tools.sm_exceptions import ConvergenceWarning
warnings.simplefilter('ignore', ConvergenceWarning)

# The name of the function MUST be the same as the name declared in
# settings.CUSTOM_ALGORITHMS.
# It MUST have 3 parameters:
# current_skyline_app, timeseries, algorithm_parameters
# See https://earthgecko-skyline.readthedocs.io/en/latest/algorithms/custom-algorithms.html
# for a full explanation about each.
# ALWAYS WRAP YOUR ALGORITHM IN try and except


# @added 20240912 - Feature #5467: custom_algorithm - probabilistic_forecasts_generalized_pareto_distribution_ets
#                   Info #5380: Probabilistic forecasts for anomaly detection - robjhyndman
#                   Task #5382: POC - Probabilistic forecasts for anomaly detection
def probabilistic_forecasts_generalized_pareto_distribution_ets(current_skyline_app, parent_pid, timeseries, algorithm_parameters):

    """
    A basic Python implementation of Probabilistic forecasts for anomaly detection
    as proposed by Rob J Hyndman - 3 July 2024
    
    https://robjhyndman.com/seminars/isf2024.html International Symposium on Forecasting, Dijon, France

    https://raw.githubusercontent.com/robjhyndman/forecast-anomalies-talk/main/forecast_anomalies.pdf
    
    When a forecast is very inaccurate, it is sometimes because a poor
    forecasting model is used, but it can also occur when an unusual observation
    occurs. A good forecasting model can be used to identify anomalies. The
    approach taken is to use a probabilistic forecast, and to compute the
    "density scores" equal to the negative log likelihood of the observations
    based on the forecast distributions. The density scores provide a measure of
    how anomalous each observation is, given the forecast density. A large
    density score indicates that the observation is unlikely, and so is a
    potential anomaly. On the other hand, typical values will have low density
    scores. A Generalized Pareto Distribution is fitted to the largest density
    scores to estimate the probability of each observation being an anomaly.

    probabilistic_forecasts_generalized_pareto_distribution_ets designated as
    pfgpde.

    This implementation uses statespace ExponentialSmoothing.  Although
    holtwinters ExponentialSmoothing could also be used, a seasonal parameter
    needs to be defined therefore holtwinters ExponentialSmoothing is not suited
    to zero knowledge, unsupervised analysis.
    
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
        custom_algorithm and algorithm itself.  For the
        probabilistic_forecasts_generalized_pareto_distribution_ets custom
        algorithm no specific algorithm_parameters are required apart
        from an empty dict but the algorithm_parameters that can be passed are:

        - ``'anomaly_window'`` (int): The anomaly_window value.
            This specifies how many of the last data points should be considered
            when determining if the metric is anomalous. Only the last
            ``anomaly_window`` data points in the time series will be used to
            determine if the metric is anomalous.  Default is ``1``.
        - ``'threshold'`` (float): The percent for the threshold.
            A cutoff value above which density scores are considered for
            fitting the Generalized Pareto Distribution (GPD).  This value is
            calculated based on the ``p_value`` parameter as the percentile of
            the density scores.   For example, if ``p_value`` is 95, the
            `threshold` corresponds to the 95th percentile of the density
            scores.  All density scores above than this threshold are used as
            exceedances to fit the GPD.  Default is ``0.95``.
        - ``'p_value'`` (int): [0, 100] The p_value.
            The percentile threshold used to determine the cutoff for selecting
            the **largest** density scores.  This value represents the percentile
            (e.g., 95 for the 95th percentile) used to compute the threshold for
            fitting the Generalized Pareto Distribution (GPD). Must be in the
            range [0, 100].  Default is ``95``.
        - ``'return_results'`` (bool): Optional.
            If ``True``, returns the results dict in addition to anomalous and
            anomalyScore.  Default is ``False``.
        - ``'debug_logging'`` (bool): Optional.
            If ``True``, enables debug logging.
        - ``'debug_print'`` (bool): Optional.
            If ``True``, enables debug printing  (for Jupyter testing). Default
            is ``False``.
        - ``'return_full_results'`` (bool): Optional.
            If ``True``, returns the addition data in the results dict.  Default
            is ``False``.

        Example usage:
        
            algorithm_parameters={
                'anomaly_window': 1,
                'threshold': 0.95,
                'p_value': 95,
                'debug_logging': True,
                'return_results': True,
            }

    :type current_skyline_app: str
    :type parent_pid: int
    :type timeseries: list
    :type algorithm_parameters: dict
    :return: anomalous, anomalyScore, results
    :rtype: tuple(bool, float, dict)

    """

    # You MUST define the algorithm_name
    algorithm_name = 'probabilistic_forecasts_generalized_pareto_distribution_ets'

    algo_name = 'pfgpde'

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

    start = time()

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

    return_full_results = False
    try:
        return_full_results = algorithm_parameters['return_full_results']
    except:
        return_full_results = False
        
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
                algo_name, str(algorithm_parameters)))
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

    threshold = 0.95
    try:
        threshold = int(algorithm_parameters['threshold'])
    except:
        threshold = 0.95

    p_value = 95
    try:
        p_value = int(algorithm_parameters['p_value'])
    except:
        p_value = 95

    anomaly_window = 1
    try:
        anomaly_window = int(algorithm_parameters['anomaly_window'])
    except:
        anomaly_window = 1

    if debug_print:
        print('running %s with threshold: %s, p_value: %s' % (
            algo_name, str(threshold), str(p_value)))
    if debug_logging:
        current_logger.debug('debug :: running %s with threshold: %s, p_value: %s with anomaly_window: %s' % (
            algo_name, str(threshold), str(p_value), str(anomaly_window)))

    data = {}
    errors = []
    try:
        df = pd.DataFrame(timeseries, columns=['ts','value'])
        # Fit the state space Exponential Smoothing model to the entire time series
        model = ETS(df['value'], trend=True, initialization_method="estimated")
        #fit = model.fit(optimized=True)
        fit = model.fit()
        if debug_logging:
            current_logger.debug('debug :: %s model fitted' % algo_name)
        # Calculate the residuals
        residuals = df['value'] - fit.fittedvalues
        if debug_logging:
            current_logger.debug('debug :: %s residuals calculated, len(residuals): %s' % (
                algo_name, str(len(residuals))))
        if return_full_results:
            data['residuals'] = [float(v) for v in residuals] 
        # Calculate the standard deviation of the residuals
        sigma = residuals.std()
        if debug_logging:
            current_logger.debug('debug :: %s sigma calculated, sigma: %s' % (
                algo_name, str(sigma)))
        if return_full_results:
            data['sigma'] = float(sigma)

        # Generate one-step ahead forecasts and their standard deviations
        forecasts = fit.fittedvalues
        if return_full_results:
            data['forecasts'] = [float(v) for v in forecasts] 

        if debug_logging:
            current_logger.debug('debug :: %s forecasts calculated, len(forecasts): %s' % (
                algo_name, str(len(forecasts))))
        forecast_stds = np.full_like(forecasts, sigma)
        if return_full_results:
            data['forecast_stds'] = [float(v) for v in forecast_stds] 

        # Calculate the density scores (negative log likelihoods)
        density_scores = -norm.logpdf(df['value'], loc=forecasts, scale=forecast_stds)
        if debug_logging:
            current_logger.debug('debug :: %s density_scores calculated, len(density_scores): %s' % (
                algo_name, str(len(density_scores))))
        if return_full_results:
            data['density_scores'] = [float(v) for v in density_scores] 

        # Identify the largest density scores (e.g., above the 95th percentile)
        p_threshold = np.percentile(density_scores, p_value)
        if return_full_results:
            data['p_threshold'] = float(p_threshold)
        if debug_logging:
            current_logger.debug('debug :: %s set p_threshold: %s' % (
                algo_name, str(p_threshold)))
        large_density_scores = density_scores[density_scores > p_threshold] - p_threshold
        if return_full_results:
            data['large_density_scores'] = [float(v) for v in large_density_scores] 

        # Fit the Generalized Pareto Distribution to the largest density scores
        params = genpareto.fit(large_density_scores)
        # Estimate the probability of each observation being an anomaly
        probabilities = genpareto.cdf(density_scores - p_threshold, *params)
        if return_full_results:
            data['probabilities'] = [float(v) for v in probabilities] 

        if debug_logging:
            current_logger.debug('debug :: %s probabilities calculated, len(probabilities): %s' % (
                algo_name, str(len(probabilities))))
        # Identify anomalies based on the estimated probability (e.g., probability > 0.95)
        anomaly_labels = (probabilities > threshold).astype(int)
        if return_full_results:
            data['anomaly_labels'] = [float(v) for v in anomaly_labels] 

        if debug_logging:
            current_logger.debug('debug :: %s anomaly_labels calculated, len(anomaly_labels): %s' % (
                algo_name, str(len(anomaly_labels))))
        # Coerce results from numpy.int64 to ints
        probabilistic_forecasts_generalized_pareto_distribution_ets_scores = []
        for i in list(anomaly_labels):
            probabilistic_forecasts_generalized_pareto_distribution_ets_scores.append(int(i))
        if return_full_results:
            data['probabilistic_forecasts_generalized_pareto_distribution_ets_scores'] = [float(v) for v in probabilistic_forecasts_generalized_pareto_distribution_ets_scores] 

        if debug_logging:
            current_logger.debug('debug :: %s probabilistic_forecasts_generalized_pareto_distribution_ets_scores calculated, len(probabilistic_forecasts_generalized_pareto_distribution_ets_scores): %s' % (
                algo_name, str(len(probabilistic_forecasts_generalized_pareto_distribution_ets_scores))))
        anomalies = {}
        anomalyScore_list = []
        for index, item in enumerate(timeseries):
            try:
                probabilistic_forecasts_generalized_pareto_distribution_ets_score = probabilistic_forecasts_generalized_pareto_distribution_ets_scores[index]
            except Exception as err:
                probabilistic_forecasts_generalized_pareto_distribution_ets_score = 0
                errors.append(['determining score', index, err])
            if probabilistic_forecasts_generalized_pareto_distribution_ets_score == 0:
                anomalyScore_list.append(0)
            else:
                anomalyScore_list.append(1)
                ts = int(item[0])
                anomalies[ts] = {'value': item[1], 'index': index, 'score': 1}

        if errors:
            if debug_logging:
                current_logger.debug('error :: debug :: %s - errors reported in determining scores, last recorded err: %s' % (
                    algo_name, str(errors[-1])))

        anomaly_sum = sum(anomalyScore_list[-anomaly_window:])
        if anomaly_sum > 0:
            anomalous = True
        else:
            anomalous = False
        runtime = (time() - start)
        results = {
            'anomalous': anomalous,
            'anomalies': anomalies,
            'anomalyScore_list': anomalyScore_list,
            'scores': probabilistic_forecasts_generalized_pareto_distribution_ets_scores,
            'runtime': runtime,
        }
        if debug_print:
            print('ran %s OK in %.6f seconds' % (algo_name, runtime))
        if debug_logging:
            current_logger.debug('debug :: %s anomalies_in_window: %s, total_anomalies: %s' % (
                algo_name, str(anomaly_sum), str(len(anomalies))))
            current_logger.debug('debug :: ran %s OK in %.6f seconds' % (algo_name, runtime))
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
                current_logger.debug('debug :: %s anomalous: %s' % (algo_name, str(anomalous)))

            if return_full_results:
                results['data'] = data

        else:
            if debug_print:
                print('error - no results')
            if debug_logging:
                current_logger.debug('debug :: %s error - no results' % algo_name)

    except StopIteration:
        if debug_print:
            print('warning - StopIteration called on %s' % algo_name)
        if debug_logging:
            current_logger.debug('debug :: warning - StopIteration called on %s' % algo_name)

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
            current_logger.debug('debug :: error - %s, err: %s' % (algo_name, err))
            current_logger.debug(traceback.format_exc())

        # Return None and None as the algorithm could not determine True or False
        if return_results:
            return (None, None, None)
        return (None, None)

    if return_results:
        return (anomalous, anomalyScore, results)

    return (anomalous, anomalyScore)
