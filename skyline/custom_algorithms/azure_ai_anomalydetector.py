"""
azure_ai_anomalydetector.py
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
import pandas as pd
import numpy as np

from azure.ai.anomalydetector import AnomalyDetectorClient
from azure.ai.anomalydetector.models import *
from azure.core.credentials import AzureKeyCredential

# The name of the function MUST be the same as the name declared in
# settings.CUSTOM_ALGORITHMS.
# It MUST have 3 parameters:
# current_skyline_app, timeseries, algorithm_parameters
# See https://earthgecko-skyline.readthedocs.io/en/latest/algorithms/custom-algorithms.html
# for a full explanation about each.
# ALWAYS WRAP YOUR ALGORITHM IN try and except


# @added 20230829 - Feature #5054: custom_algorithm - azure_ai_anomalydetector
def azure_ai_anomalydetector(current_skyline_app, parent_pid, timeseries, algorithm_parameters):

    """
    Outlier detection for time-series data using azure_ai_anomalydetector.
    Requires pip install --upgrade azure.ai.anomalydetector
    EXPERIMENTAL.

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
        azure_ai_anomalydetector custom algorithm no specific
        algorithm_parameters are required apart from an empty dict but the
        algorithm_parameters that can be passed are:

        - ``'anomaly_window'`` (int): The anomaly_window value.
            This specifies how many of the last data points should be considered
            when determining if the metric is anomalous. Only the last
            ``anomaly_window`` data points in the time series will be used to
            determine if the metric is anomalous.  Default is ``1``.
        - ``'sensitivity'`` (int):
            Between 0 and 99, the lower the value is, the larger the margin
            value will be which means less anomalies will be accepted.  Default
            is ``99``.
        - ``'max_anomaly_ratio'`` (float):
            The max anomaly ratio in a time series. Default is ``0.1``.
        - ``'apikey'`` (str):  Your Azure API key.
        - ``'endpoint'`` (str):  Your Azure endpoint.
            For example https://<YOUR_ENDPOINT>.cognitiveservices.azure.com/
         - ``'return_results'`` (bool): Optional.
            If ``True``, returns the results dict in addition to anomalous and
            anomalyScore.  Default is ``False``.
        - ``'debug_logging'`` (bool): Optional.
            If ``True``, enables debug logging.
        - ``'debug_print'`` (bool): Optional.
            If ``True``, enables debug printing  (for Jupyter testing). Default
            is ``False``.

        Example usage::
        
            algorithm_parameters={
                'anomaly_window': 1,
                'window': 3,
                'min_samples': 4,
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
    algorithm_name = 'azure_ai_anomalydetector'

    # Define the default state of None and None, anomalous does not default to
    # False as that is not correct, False is only correct if the algorithm
    # determines the data point is not anomalous.  The same is true for the
    # anomalyScore.
    anomalous = None
    anomalyScore = False
    # A list of binary results for each data point, either 1 == anomalous or
    # 0 == not_anomalous.  This is not the algorithm score, just whether the
    # algorithm determined the data point as anomalous or not.
    anomalyScore_list = []
    # A dict for anomalies
    anomalies = {}
    # @added 20230707 - custom_algorithms - handle unreliable results
    # To test the reliability of the result
    unreliable = False
    # A reason if unreliabile
    unreliable_reason = 'None'

    # A dict to add results to
    results = {'anomalous': False, 'anomalies': [], 'anomalyScore_list': [], 'scores': []}

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

    # If any data points in the last anomaly_window data points are
    # anomalous, the instance is classed as anomalous.  For example
    # if set to 1 the instance would only be anomalous if the last
    # data point was anomalous.  If it is set to 5, the instance
    # would be classed as anomalous if ANY one of the last 5 data
    # points were classed as anomalous.
    # How many data points?
    anomaly_window = 1
    try:
        anomaly_window = int(algorithm_parameters['anomaly_window'])
    except:
        anomaly_window = 1

    sensitivity = 99
    try:
        sensitivity = int(algorithm_parameters['sensitivity'])
    except:
        sensitivity = 99

    max_anomaly_ratio = 0.01
    try:
        max_anomaly_ratio = float(algorithm_parameters['max_anomaly_ratio'])
    except:
        max_anomaly_ratio = 0.01

    apikey = None
    try:
        apikey = algorithm_parameters['apikey']
    except:
        apikey = None

    endpoint = None
    try:
        endpoint = algorithm_parameters['endpoint']
    except:
        endpoint = None

    if debug_print:
        print('running azure_ai_anomalydetector with sensitivity: %s, max_anomaly_ratio: %s' % (
            str(sensitivity), str(max_anomaly_ratio)))
    if debug_logging:
        current_logger.debug('debug :: running azure_ai_anomalydetector with sensitivity: %s, max_anomaly_ratio: %s' % (
            str(sensitivity), str(max_anomaly_ratio)))

    if debug_print:
        print('running azure_ai_anomalydetector on timeseries with %s datapoints' % str(len(timeseries)))
    if debug_logging:
        current_logger.debug('debug :: running azure_ai_anomalydetector on timeseries with %s datapoints' % str(len(timeseries)))

    algorithm_parameters_used = {
        'anomaly_window': anomaly_window, 'sensitivity': sensitivity,
        'max_anomaly_ratio': max_anomaly_ratio
    }

    try:
        client = AnomalyDetectorClient(endpoint, AzureKeyCredential(apikey))
    except Exception as err:
        record_algorithm_error(current_skyline_app, parent_pid, algorithm_name, traceback.format_exc())
        if debug_print:
            print('error:', traceback.format_exc())
        if debug_logging:
            current_logger.debug('debug :: error - on %s - %s' % (algorithm_name, err))
            current_logger.debug(traceback.format_exc())
        # Return None and None as the algorithm could not determine True or False
        if return_results:
            results['error'] = err
            results['anomalous'] = None
            return (None, None, results)
        return (None, None)

    try:

        df = pd.DataFrame(timeseries, columns=['date', 'value'])
        df['date'] = pd.to_datetime(df['date'], unit='s')

        # client = AnomalyDetectorClient(endpoint, AzureKeyCredential(apikey))

        series = []
        for index, row in df.iterrows():
            series.append(TimeSeriesPoint(timestamp=row[0], value=row[1]))

        request = UnivariateDetectionOptions(series=series, granularity=TimeGranularity.PER_MINUTE)
        anomaly_response = client.detect_univariate_entire_series(request)

        anomalous_list = list(anomaly_response.is_anomaly)
        for i in range(len(df.values)):
            score = 0.0
            ts = timeseries[i][0]
            if anomalous_list[i]:
                score = 1.0
                anomalies[ts] = {'value': timeseries[i][1], 'index': index, 'score': 1}
            anomalyScore_list.append(score)

        anomaly_sum = sum(anomalyScore_list[-anomaly_window:])
        if anomaly_sum > 0:
            anomalous = True
        else:
            anomalous = False
        success = True
        total_anomaly_sum = sum(anomalyScore_list)
        if total_anomaly_sum > (len(timeseries) / 5):
            # None is not json friendly
            # anomalous = None
            anomalous = False
            unreliable = True
            unreliable_reason = '%s triggered %s data points of the total %s data points as anomalous, discarding as this algorithm does not suit this data.' % (
                algorithm_name, str(total_anomaly_sum), str(len(timeseries))            )

        results['anomalous'] = anomalous
        results['anomalies'] = anomalies
        results['anomalyScore_list'] = anomalyScore_list
        results['scores'] = list(anomalyScore_list)
        results['unreliable'] = unreliable
        results['unreliable_reason'] = unreliable_reason
        results['success'] = success
        results['algorithm_parameters'] = algorithm_parameters
        results['algorithm_parameters_used'] = algorithm_parameters_used

        if debug_print:
            print('ran %s OK in %.6f seconds' % (algorithm_name, (time() - start)))
        if debug_logging:
            current_logger.debug('debug :: ran %s OK in %.6f seconds' % (algorithm_name, (time() - start)))
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

        if unreliable:
            anomalous = False
            anomalyScore = 0.0

    except StopIteration:
        if debug_print:
            print('warning - StopIteration called on %s' % algorithm_name)
        if debug_logging:
            current_logger.debug('debug :: warning - StopIteration called on %s' % algorithm_name)

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
            current_logger.debug('debug :: error - on %s - %s' % (algorithm_name, err))
            current_logger.debug(traceback.format_exc())

        # Return None and None as the algorithm could not determine True or False
        if return_results:
            return (None, None, None)
        return (None, None)

    if return_results:
        return (anomalous, anomalyScore, results)

    return (anomalous, anomalyScore)
