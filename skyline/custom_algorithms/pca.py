"""
pca.py
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
from sklearn.preprocessing import StandardScaler
from scipy.spatial.distance import cdist
from sklearn.decomposition import PCA

# The name of the function MUST be the same as the name declared in
# settings.CUSTOM_ALGORITHMS.
# It MUST have 3 parameters:
# current_skyline_app, timeseries, algorithm_parameters
# See https://earthgecko-skyline.readthedocs.io/en/latest/algorithms/custom-algorithms.html
# for a full explanation about each.
# ALWAYS WRAP YOUR ALGORITHM IN try and except


# @added 20221114 - Feature #4750: custom_algorithm - pca
# Based on https://andrewm4894.com/2021/10/11/time-series-anomaly-detection-using-pca/
# https://github.com/andrewm4894/colabs/blob/master/time_series_anomaly_detection_with_pca.ipynb
def pca(current_skyline_app, parent_pid, timeseries, algorithm_parameters):

    """
    Outlier detector for time-series data using PCA - EXPERIMENTAL

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
        custom_algorithm and algorithm itself.  Example:
        "algorithm_parameters": {
            "n_neighbors": 2,
            "anomaly_window": 5,
            "return_results": True,
        }
    :type current_skyline_app: str
    :type parent_pid: int
    :type timeseries: list
    :type algorithm_parameters: dict
    :return: anomalous, anomalyScore, results
    :rtype: tuple(boolean, float, dict)

    """

    # You MUST define the algorithm_name
    algorithm_name = 'pca'

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

    def pca_anomaly_scores(n_pca, X):
        """
        Given a fitted pca model and some X feature vectors, compute an anomaly
        score as the sum of weighted euclidean distance between each sample to
        the hyperplane constructed by the selected eigenvectors.
        """
        return np.sum(cdist(X, n_pca.components_) / n_pca.explained_variance_ratio_, axis=1).ravel()

    def pca_preprocess_df(df, lags_n, diffs_n, smooth_n, diffs_abs=False, abs_features=True):
        """
        Given a pandas dataframe preprocess it to take differences, add
        smoothing and lags as specified.
        """
        if diffs_n >= 1:
            # take differences
            df = df.diff(diffs_n).dropna()
            # abs diffs if defined
            if diffs_abs:
                df = abs(df)
        if smooth_n >= 2:
            # apply a rolling average to smooth out the data a bit
            df = df.rolling(smooth_n).mean().dropna()
        do_lags = True
        if do_lags:
            if lags_n >= 1:
                # for each dimension add a new columns for each of lags_n lags
                # of the differenced and smoothed values for that dimension
                df_columns_new = [f'{col}_lag{n}' for n in range(lags_n + 1) for col in df.columns]
                df = pd.concat([df.shift(n) for n in range(lags_n + 1)], axis=1).dropna()
                df.columns = df_columns_new
        # sort columns to have lagged values next to each other for clarity when
        # looking at the feature vectors
        df = df.reindex(sorted(df.columns), axis=1)

        # abs all features if specified
        if abs_features:
            df = abs(df)
        return df

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

    threshold = 0.7
    try:
        threshold = float(algorithm_parameters['threshold'])
    except:
        threshold = 0.7
    n_test = 10
    try:
        n_test = int(algorithm_parameters['n_test'])
    except:
        n_test = 10
    diffs = 1
    try:
        diffs = int(algorithm_parameters['diffs'])
    except:
        diffs = 1
    lags = 3
    try:
        lags = int(algorithm_parameters['lags'])
    except:
        lags = 3
    smooth = 3
    try:
        smooth = int(algorithm_parameters['smooth'])
    except:
        smooth = 3
    anomaly_window = 1
    try:
        anomaly_window = int(algorithm_parameters['anomaly_window'])
    except:
        anomaly_window = 1

    if debug_print:
        print('running pca with threshold: %s, n_test: %s, diffs: %s, lags: %s, smooth: %s' % (
            str(threshold), str(n_test), str(diffs), str(lags), str(smooth)))
    if debug_logging:
        current_logger.debug('debug :: running pca with threshold: %s, n_test: %s, diffs: %s, lags: %s, smooth: %s' % (
            str(threshold), str(n_test), str(diffs), str(lags), str(smooth)))

    if debug_print:
        print('running PCA on timeseries with %s datapoints' % str(len(timeseries)))
    if debug_logging:
        current_logger.debug('debug :: running PCA on timeseries with %s datapoints' % str(len(timeseries)))

    try:
        # Use the last n_test data points to train on
        n_train = len(timeseries) - n_test
        # Difference between diff number of data points
        diffs_n = diffs
        # Lags to include in the the feature vector
        lags_n = lags
        # Smooth the latest values to be included in the feature vector
        smooth_n = smooth
        df = pd.DataFrame(timeseries, columns=['date', 'value'])
        df['date'] = pd.to_datetime(df['date'], unit='s')
        datetime_index = pd.DatetimeIndex(df['date'].values)
        df = df.set_index(datetime_index)
        df.drop('date', axis=1, inplace=True)
        df_train = df.head(n_train)
        # preprocess or 'featurize' the training data
        train_data = pca_preprocess_df(df_train, lags_n, diffs_n, smooth_n)
        df_anomalous = df.tail((len(timeseries) - len(train_data)))
        anomalous_data = pca_preprocess_df(df_anomalous, lags_n, diffs_n, smooth_n)
        # build PCA model
        n_pca = PCA(n_components=2)
        # scale based on training data
        scaler = StandardScaler()
        scaler.fit(train_data)
        # fit model
        n_pca.fit(scaler.transform(train_data))
        # get anomaly scores for training data
        train_scores = pca_anomaly_scores(n_pca, scaler.transform(train_data))
        df_train_scores = pd.DataFrame(train_scores, columns=['anomaly_score'], index=train_data.index)
        df_train_scores_min = df_train_scores.min()
        df_train_scores_max = df_train_scores.max()
        # normalize anomaly scores on based training data
        df_train_scores = (df_train_scores - df_train_scores_min) / (df_train_scores_max - df_train_scores_min)
        # score anomalous data
        scores = pca_anomaly_scores(n_pca, scaler.transform(anomalous_data))
        df_scores = pd.DataFrame(scores, columns=['anomaly_score'], index=anomalous_data.index)
        # normalize based on train data scores
        df_scores = (df_scores - df_train_scores_min) / (df_train_scores_max - df_train_scores_min)
        df_anomalies = df.copy()
        df_anomalies['anomaly_score'] = pd.concat([df_train_scores, df_scores])
        pca_scores = df_anomalies['anomaly_score'].tolist()

        # Coerce scores to floats, removing nans and replace with 0 so they are
        # json safe
        json_safe_pca_scores = []
        for score in pca_scores:
            value = score
            if str(value) in ['Nan', 'nan', 'NaN']:
                value = 0.0
            json_safe_pca_scores.append(value)
        pca_scores = list(json_safe_pca_scores)

        anomalyScore_list = []
        anomalies = {}
        for index, item in enumerate(timeseries):
            try:
                if pca_scores[index] >= threshold:
                    ts = int(item[0])
                    anomalies[ts] = {'value': item[1], 'index': index, 'score': float(pca_scores[index])}
                    anomalyScore_list.append(1)
                else:
                    anomalyScore_list.append(0)
            except:
                anomalyScore_list.append(0)

        anomaly_sum = sum(anomalyScore_list[-anomaly_window:])
        if anomaly_sum > 0:
            anomalous = True
        else:
            anomalous = False
        results = {
            'anomalous': anomalous,
            'anomalies': anomalies,
            'anomalyScore_list': anomalyScore_list,
            'scores': pca_scores,
            'threshold': threshold,
        }
        if debug_print:
            print('ran pca OK in %.6f seconds' % (time() - start))
        if debug_logging:
            current_logger.debug('debug :: ran pca OK in %.6f seconds' % (time() - start))
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
            print('warning - StopIteration called on pca')
        if debug_logging:
            current_logger.debug('debug :: warning - StopIteration called on pca')

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
            current_logger.debug('debug :: error - on pca - %s' % err)
            current_logger.debug(traceback.format_exc())

        # Return None and None as the algorithm could not determine True or False
        if return_results:
            return (None, None, None)
        return (None, None)

    if return_results:
        return (anomalous, anomalyScore, results)

    return (anomalous, anomalyScore)
