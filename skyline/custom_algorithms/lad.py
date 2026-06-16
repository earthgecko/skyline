"""
lad.py
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

# The name of the function MUST be the same as the name declared in
# settings.CUSTOM_ALGORITHMS.
# It MUST have 3 parameters:
# current_skyline_app, timeseries, algorithm_parameters
# See https://earthgecko-skyline.readthedocs.io/en/latest/algorithms/custom-algorithms.html
# for a full explanation about each.
# ALWAYS WRAP YOUR ALGORITHM IN try and except


# @added 20240911 - Feature #5466: custom_algorithm - lad
#                   Info #5465: LAD - Large Deviations Anomaly Detection
def lad(current_skyline_app, parent_pid, timeseries, algorithm_parameters):

    """
    Large Deviations Anomaly Detection
    Univariate implementation of:
    Large Deviations Anomaly Detection (LAD) for collection of multivariate
    time series data: Applications to COVID-19 data (Sreelekha Guggilam, Varun Chandola, Abani K. Patra)
    Journal of Computational Science 72 (2023) 102101
    https://www.sciencedirect.com/science/article/pii/S1877750323001618
    https://pdf.sciencedirectassets.com/280179/1-s2.0-S1877750323X00076/1-s2.0-S1877750323001618/main.pdf
    https://doi.org/10.1016/j.jocs.2023.102101
    At 95 percentile may be the noisiest algorithm yet...
    If threshold is set at 95, will detect step changes, etc.
    If threshold is set at 99 will only detect most severe spike/dip/point anomalies.
    Fast but too noisy.

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
        custom_algorithm and algorithm itself.  For the lad custom algorithm no
        specific algorithm_parameters are required apart from an empty dict but
        the algorithm_parameters that can be passed are:

        - ``'anomaly_window'`` (int): The anomaly_window value.
            This specifies how many of the last data points should be considered
            when determining if the metric is anomalous. Only the last
            ``anomaly_window`` data points in the time series will be used to
            determine if the metric is anomalous.  Default is ``1``.
        - ``'threshold'`` (int):
            The percentile value for the threshold.  Default is ``95``.
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
                'threshold': 99,
                'debug_logging': True,
                'return_results': True,
            }


    :type current_skyline_app: str
    :type parent_pid: int
    :type timeseries: list
    :type algorithm_parameters: dict
    :return: anomalous, anomalyScore, results
    :rtype: tuple(boolean, float, dict)

    """

    # You MUST define the algorithm_name
    algorithm_name = 'lad'

    # Define the default state of None and None, anomalous does not default to
    # False as that is not correct, False is only correct if the algorithm
    # determines the data point is not anomalous.  The same is true for the
    # anomalyScore.
    anomalous = None
    anomalyScore = None
    anomalies = {}
    scores = []
    anomalyScore_list = []
    results = {
        'anomalous': False, 'anomalies': anomalies,
        'anomalyScore_list': anomalyScore_list, 'scores': scores
    }

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

    threshold = 95
    try:
        threshold = int(algorithm_parameters['threshold'])
    except:
        threshold = 95

    anomaly_window = 1
    try:
        anomaly_window = int(algorithm_parameters['anomaly_window'])
    except:
        anomaly_window = 1

    if debug_print:
        print('running lad with threshold: %s' % (
            str(threshold)))
    if debug_logging:
        current_logger.debug('debug :: running lad with threshold: %s' % (
            str(threshold)))

    try:
        values = np.array([v for t, v in timeseries])
        steps = len(values)
        # Initialize anomaly score array, label array, and entropy matrix
        anomaly_scores = np.zeros(steps)  # Anomaly Score Array (𝑎)
        anomaly_labels = np.zeros(steps)  # Anomaly Label Array (𝐼)
        entropy_matrix = np.zeros(steps)  # Entropy Matrix (𝐸)
        # Normalize
        mean_value = np.mean(values)
        std_value = np.std(values)
        # Avoid division by zero
        if std_value != 0:
            normalized_values = (values - mean_value) / std_value
        else:
            # If std is 0, center the data
            normalized_values = values - mean_value  
        # Calculate the entropy (rate function) values
        # Formula: Entropy = -(normalized_value^2) / 2
        entropy_matrix = -np.square(normalized_values) / 2
        # Calculate the anomaly scores as the negative maximum of the entropy
        # (rate function) values
        anomaly_scores = -entropy_matrix
        # Normalize the anomaly scores to be between 0 and 1
        min_score = np.min(anomaly_scores)
        max_score = np.max(anomaly_scores)
        # Avoid division by zero
        if max_score != min_score:
            anomaly_scores = (anomaly_scores - min_score) / (max_score - min_score)
        else:
            anomaly_scores = np.zeros_like(anomaly_scores)  # If all scores are the same, set them to 0
        # Determine the anomaly threshold percentile
        threshold = np.percentile(anomaly_scores, threshold)
        # Label anomalies with 1
        anomaly_labels = (anomaly_scores > threshold).astype(int)

        # Coerce results from numpy.int64 to ints
        lad_scores = []
        for i in list(anomaly_labels):
            lad_scores.append(int(i))

        anomalies = {}
        anomalyScore_list = []
        for index, item in enumerate(timeseries):
            try:
                lad_score = lad_scores[index]
            except:
                lad_score = 0
            if lad_score == 0:
                anomalyScore_list.append(0)
            else:
                anomalyScore_list.append(1)
                ts = int(item[0])
                anomalies[ts] = {'value': float(item[1]), 'index': index, 'score': 1}

        anomaly_sum = sum(anomalyScore_list[-anomaly_window:])
        if anomaly_sum > 0:
            anomalous = True
        else:
            anomalous = False
        results = {
            'anomalous': anomalous,
            'anomalies': anomalies,
            'anomalyScore_list': anomalyScore_list,
            'scores': lad_scores,
        }
        if debug_print:
            print('ran lad OK in %.6f seconds' % (time() - start))
        if debug_logging:
            current_logger.debug('debug :: ran lad OK in %.6f seconds' % (time() - start))
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
            print('warning - StopIteration called on lad')
        if debug_logging:
            current_logger.debug('debug :: warning - StopIteration called on lad')

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
            current_logger.debug('debug :: error - on lad - %s' % err)
            current_logger.debug(traceback.format_exc())

        # Return None and None as the algorithm could not determine True or False
        if return_results:
            return (None, None, None)
        return (None, None)

    if return_results:
        return (anomalous, anomalyScore, results)

    return (anomalous, anomalyScore)
