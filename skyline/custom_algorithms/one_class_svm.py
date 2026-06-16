"""
one_class_svm.py
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
from numpy.lib.stride_tricks import sliding_window_view
from sklearn.preprocessing import StandardScaler
from sklearn.svm import OneClassSVM

# The name of the function MUST be the same as the name declared in
# settings.CUSTOM_ALGORITHMS.
# It MUST have 3 parameters:
# current_skyline_app, timeseries, algorithm_parameters
# See https://earthgecko-skyline.readthedocs.io/en/latest/algorithms/custom-algorithms.html
# for a full explanation about each.
# ALWAYS WRAP YOUR ALGORITHM IN try and except


# @added 20221114 - Feature #4750: custom_algorithm - one_class_svm
def one_class_svm(current_skyline_app, parent_pid, timeseries, algorithm_parameters):

    """
    Outlier detector for time-series data using One Class SVM base on the moving
    mean and variance, unless the variance is low in which case the standard
    deviation will be used in place of variance.  The algorithm parameters to
    be concerned with are ``'window'`` which defines the length of sliding
    window to use, ``nu`` which defines the percentage that can be considered as
    outliers e.g. 0.1 would be 10%.  Do note that if the variance is low each
    spike or trough will probably be identified as an outlier.

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
        custom_algorithm and algorithm itself.  For the one_class_svm custom
        algorithm no specific algorithm_parameters are required apart from an
        empty dict but the algorithm_parameters that can be passed are:

        - ``'anomaly_window'`` (int): The anomaly_window value.
            This specifies how many of the last data points should be considered
            when determining if the metric is anomalous. Only the last
            ``anomaly_window`` data points in the time series will be used to
            determine if the metric is anomalous.  Default is ``1``.
        - ``'window'`` (int): The sliding window size.
            Default is ``3``.
        - ``'nu'`` (float): The threshold value.
            The value for nu which defines the percentage that can be considered
            as outliers e.g. 0.1 would be 10%.  Default is ``0.01``.
        - ``'gamma'`` (str): Kernel coefficient for ``rbf``, ``poly`` and ``sigmoid``.
            Default is ``scale``.
            Possible values: `scale` | `auto`.
                - `scale` - uses 1 / (n_features * X.var()) as value of gamma.
                - `auto` - uses 1 / n_features as value of gamma.
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
                'nu': 0.01,
                'gamma': 'scale',
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

    def normalised_variance(np_values):
        normalised_var = np.nan
        err = None
        try:
            # np_values = np.array(values)
            np_max = np.amax(np_values)
            np_min = np.amin(np_values)

            # @modified 20241114 - Task #5526: Build v5.0.0 and upgrade deps
            #                      Branch #5532: v5.0.0-alpha
            # Prevent invalid value encountered in divide errors
            # norm_np_values = (np_values - np_min) / (np_max - np_min)
            if np_max == np_min:
                norm_np_values = np.zeros_like(np_values)
            else:
                norm_np_values = (np_values - np_min) / (np_max - np_min)

            normalised_var = round(np.var(norm_np_values), 4)
        except Exception as err:
            normalised_var = np.nan
        return normalised_var, err

    # You MUST define the algorithm_name
    algorithm_name = 'one_class_svm'

    # Define the default state of None and None, anomalous does not default to
    # False as that is not correct, False is only correct if the algorithm
    # determines the data point is not anomalous.  The same is true for the
    # anomalyScore.
    anomalous = None
    anomalyScore = None
    anomalies = {}
    anomalyScore_list = []
    one_class_svm_scores = []
    results = {
        'anomalous': anomalous,
        'anomalies': anomalies,
        'anomalyScore_list': anomalyScore_list,
        'scores': one_class_svm_scores,
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
    print_debug = None
    try:
        print_debug = algorithm_parameters['print_debug']
    except:
        print_debug = False

    # nu = 0.01
    nu = 0.09
    try:
        nu = float(algorithm_parameters['nu'])
    except:
        # nu = 0.01
        nu = 0.09
    window_shape = 3
    try:
        window_shape = int(algorithm_parameters['window'])
    except:
        window_shape = 3
    gamma = 'scale'
    try:
        gamma = algorithm_parameters['gamma']
    except:
        gamma = 'scale'
    anomaly_window = 1
    try:
        anomaly_window = int(algorithm_parameters['anomaly_window'])
    except:
        anomaly_window = 1

    if print_debug:
        print('running one_class_svm with nu: %s, window_shape: %s, gamma: %s' % (
            str(nu), str(window_shape), str(gamma)))
    if debug_logging:
        current_logger.debug('debug :: running one_class_svm with nu: %s, window_shape: %s, gamma: %s' % (
            str(nu), str(window_shape), str(gamma)))

    if print_debug:
        print('running one_class_svm on timeseries with %s datapoints' % str(len(timeseries)))
    if debug_logging:
        current_logger.debug('debug :: running one_class_svm on timeseries with %s datapoints' % str(len(timeseries)))

    results['algorithm_parameters'] = algorithm_parameters.copy()
    results['algorithm_parameters_used'] = {
        'anomaly_window': anomaly_window, 'nu': nu, 'gamma': gamma, 'window': window_shape,
    }

    # @added 20230707 - Feature #4750: custom_algorithm - one_class_svm
    # Use standard deviation instead of variance if the variance is low
    results['components'] = ['mean', 'variance']
    low_variance = 0.009
    use_column = 'variance'

    try:
        X = np.array([v for t, v in timeseries])
        norm_var, err = normalised_variance(X)
        if err:
            results['normalised_variance_error'] = err
            if debug_logging:
                current_logger.debug('debug :: normalised_variance error: %s' % str(err))
        # Only use json friendly values in results
        if isinstance(normalised_variance, float):
            results['normalised_variance'] = norm_var
        else:
            results['normalised_variance'] = str(norm_var)

        Xmean = np.average(sliding_window_view(X, window_shape=window_shape), axis=1)

        # @modified 20230707 - Feature #4750: custom_algorithm - one_class_svm
        # Use standard deviation instead of variance if the variance is low
        # Xvar = np.var(sliding_window_view(X, window_shape=window_shape), axis=1)
        if norm_var <= low_variance:
            Xvar = np.std(sliding_window_view(X, window_shape=window_shape), axis=1)
            results['components'] = ['mean', 'std']
            use_column = 'std'
        else: 
            Xvar = np.var(sliding_window_view(X, window_shape=window_shape), axis=1)

        xx = []
        xmeans = list(Xmean)
        xvars = list(Xvar)
        for index, value in enumerate(xmeans):
            xx.append([value, xvars[index]])

        # @modified 20230707 - Feature #4750: custom_algorithm - one_class_svm
        # Use standard deviation instead of variance if the variance is low
        # df = pd.DataFrame(xx, columns=['mean', 'variance'])
        df = pd.DataFrame(xx, columns=['mean', use_column])

        # Standardise
        st = StandardScaler()
        stdDf = pd.DataFrame(st.fit_transform(df), columns=df.columns)
        stdMean = stdDf['mean'].tolist()
        #stdVar = stdDf['variance'].tolist()
        stdVar = stdDf[use_column].tolist()
        xx = []
        for index, value in enumerate(stdMean):
            xx.append([value, stdVar[index]])

        XX = np.array(xx)
        clf = OneClassSVM(gamma=gamma, nu=nu).fit(XX)
        one_class_svm_scores = list(clf.predict(XX))

        # Pad the beginning so that the lists align because the sliding_window
        # results in len(X - window) as the window points are not calculated
        insert_count = len(X) - len(xmeans)
        for i in list(range(0, insert_count)):
            one_class_svm_scores.insert(i, 1)

        # Coerce numpy.int64 to int
        one_class_svm_scores = [int(x) for x in one_class_svm_scores]
        results['scores'] = one_class_svm_scores

        anomalyScore_list = []
        anomalies = {}
        for index, item in enumerate(timeseries):
            try:
                if one_class_svm_scores[index] == -1:
                    ts = int(item[0])
                    anomalies[ts] = {'value': item[1], 'index': index, 'score': -1}
                    anomalyScore_list.append(1)
                else:
                    anomalyScore_list.append(0)
            except:
                anomalyScore_list.append(0)
        results['anomalyScore_list'] = anomalyScore_list

        # @added 20230707
        # This would be to handle the algorithm occasionally flagging all the
        # values of 0 as anomalies
        # if sum(anomalyScore_list) >= len(timeseries) / 2:

        anomaly_sum = sum(anomalyScore_list[-anomaly_window:])
        if anomaly_sum > 0:
            anomalous = True
            results['anomalous'] = True
        else:
            anomalous = False
            results['anomalous'] = False

        # @added 20230801
        # If the algorithm identifies almost all of the data points as
        # anomalous, class the results as invalid
        if sum(anomalyScore_list) >= int((len(timeseries) / 100) * 95):
            anomalous = False
            results['anomalous'] = anomalous
            results['unreliable'] = True
            if print_debug:
                print('one_class_svm results unreliable, %s anomalies in timeseries of length %s' % (
                    str(sum(anomalyScore_list)), str(len(timeseries))))
            if debug_logging:
                current_logger.debug('debug :: one_class_svm results unreliable, %s anomalies in timeseries of length %s' % (
                    str(sum(anomalyScore_list)), str(len(timeseries))))

        if print_debug:
            print('ran one_class_svm OK in %.6f seconds' % (time() - start))
        if debug_logging:
            current_logger.debug('debug :: ran one_class_svm OK in %.6f seconds' % (time() - start))
        if results:
            results['anomalies'] = anomalies
            if results['anomalous']:
                anomalous = True
                anomalyScore = 1.0
            else:
                anomalous = False
                anomalyScore = 0.0
            if print_debug:
                print('anomalous: %s' % str(anomalous))
            if debug_logging:
                current_logger.debug('debug :: one_class_svm - anomalous: %s' % str(anomalous))
        else:
            if print_debug:
                print('error - no results')
            if debug_logging:
                current_logger.debug('debug :: error - no results')

    except StopIteration:
        if print_debug:
            print('warning - StopIteration called on one_class_svm')
        if debug_logging:
            current_logger.debug('debug :: warning - StopIteration called on one_class_svm')

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
        if print_debug:
            print('error:', traceback.format_exc())
        if debug_logging:
            current_logger.debug('debug :: error - on one_class_svm - %s' % err)
            current_logger.debug(traceback.format_exc())

        # Return None and None as the algorithm could not determine True or False
        if return_results:
            return (None, None, None)
        return (None, None)

    if return_results:
        return (anomalous, anomalyScore, results)

    return (anomalous, anomalyScore)
