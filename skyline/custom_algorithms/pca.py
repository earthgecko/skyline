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
        custom_algorithm and algorithm itself.  For the pca custom algorithm
        no specific algorithm_parameters are required apart from an empty dict
        but the algorithm_parameters that can be passed are:

        - ``'anomaly_window'`` (int): The anomaly_window value.
            This specifies how many of the last data points should be considered
            when determining if the metric is anomalous. Only the last
            ``anomaly_window`` data points in the time series will be used to
            determine if the metric is anomalous.  Default is ``1``.
        - ``'threshold'`` (float): The threshold value.
            The value for the threshold to use, >= to this value is anomalous.
            Default is ``0.7``.
        - ``'n_test'`` (int): Size of test sample.
            The number of samples in the test data. Default is ``10``.
        - ``'diffs'`` (int): Number of differences to calculate.
            The number of differences to calculate. Default is ``1``.
        - ``'lags'`` (int): Number of lags to calculate.
            The number of lags to calculate. Default is ``3``.
        - ``'smooth'`` (int): Number of data points used in rolling average for smoothing.
            Default is ``3``.
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
                'threshold': 0.7,
                'n_test': 10,
                'diffs': 1,
                'lags': 3,
                'smooth': 3,
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

    def deduplicate_timeseries(timeseries):
        """
        Use a set to track unique (timestamp, value) pairs and deduplicate
        items that are the same
        """
        seen = set()
        deduplicated_timeseries = []
        for ts, v in timeseries:
            if (ts, v) not in seen:
                seen.add((ts, v))
                deduplicated_timeseries.append([ts, v])
        return deduplicated_timeseries

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

    original_timeseries_len = len(timeseries)
    timestamps = [t for t, _ in timeseries]
    timestamps_set = set(timestamps)
    if len(timestamps) > len(timestamps_set):
        try:
            timeseries = deduplicate_timeseries(timeseries)
            if debug_logging:
                current_logger.debug('debug :: pca deduplicated timeseries from on %s data points to %s data points' % (
                    str(original_timeseries_len), str(len(timeseries))))
        except Exception as err:
            record_algorithm_error(current_skyline_app, parent_pid, algorithm_name, traceback.format_exc())
            if debug_print:
                print('error:', traceback.format_exc())
            if debug_logging:
                current_logger.debug('debug :: error on pca deduplicate_timeseries, err: %s' % err)
                current_logger.debug(traceback.format_exc())

    # @added 20241114 - Task #5526: Build v5.0.0 and upgrade deps
    #                   Branch #5532: v5.0.0-alpha
    # Check that the data suits the PCA algorithm and if not do not run and
    # fail/error with 
    # /opt/python_virtualenv/projects/skyline-py31015/lib/python3.10/site-packages/sklearn/decomposition/_pca.py:653: RuntimeWarning: invalid value encountered in divide
    #    explained_variance_ratio_ = explained_variance_ / total_var
    def check_pca_suitability(data, dataset='train'):
        reason = 'ok'
        suitable = True
        try:
            variances = np.var(data, axis=0)
            if np.any(variances == 0):
                suitable = False
                reason = '%s has features with zero variance, which are unsuitable for PCA' % dataset
                if debug_logging:
                    current_logger.info('warning :: %s' % reason)
            if suitable:
                # Low variability threshold
                if np.all(variances < 1e-5):
                    suitable = False
                    reason = '%s has very low variance overall, which may make PCA ineffective' % dataset
                    if debug_logging:
                        current_logger.info('warning :: %s' % reason)
            if suitable:
                if data.shape[0] < data.shape[1]:
                    suitable = False
                    reason = '%s has more features than samples, which may not be ideal for PCA' % dataset
                    if debug_logging:
                        current_logger.info('warning :: %s' % reason)
            if suitable:
                # Low correlation threshold
                corr_matrix = pd.DataFrame(data).corr()
                if corr_matrix.abs().values.max() < 0.1:
                    suitable = False
                    reason = '%s has low correlation between features, which may reduce PCA effectiveness' % dataset
                    if debug_logging:
                        current_logger.info('warning :: %s' % reason)
        except Exception as err:
            if debug_logging:
                current_logger.error('error :: debug :: check_pca_suitability failed on %s, err: %s' % (
                    dataset, err))
        return suitable, reason

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

        # @added 20241107 - Task #5526: Build v5.0.0 and upgrade deps
#        reduction = diffs_n + max(smooth_n - 1, 0) + lags_n
#        df_train = df_train.iloc[reduction:]

        # preprocess or 'featurize' the training data
        train_data = pca_preprocess_df(df_train, lags_n, diffs_n, smooth_n)

        # @added 20241114 - Task #5526: Build v5.0.0 and upgrade deps
        #                   Branch #5532: v5.0.0-alpha
        # Check that the data suits the PCA algorithm and if not do not run and
        check_dataset = 'train_data'
        suitable_for_pca = True
        reason = 'OK'
        try:
            suitable_for_pca, reason = check_pca_suitability(train_data, dataset=check_dataset)
        except Exception as err:
            if debug_logging:
                current_logger.error('error :: debug :: check_pca_suitability on %s, err: %s' % (
                    check_dataset, err))
        if not suitable_for_pca:
            if debug_logging:
                current_logger.debug('debug :: not suitable for pca returning, took %.6f seconds' % (time() - start))
            results['anomalies'] = {}
            results['error'] = reason
            if return_results:
                return (None, None, results)
            return (None, None)

        # @modified 20241107 - Task #5526: Build v5.0.0 and upgrade deps
        df_anomalous = df.tail((len(timeseries) - len(train_data)))
#        df_anomalous_start = len(df) - len(train_data) - reduction  # Starting point for df_anomalous
#        df_anomalous = df.iloc[df_anomalous_start:]  # Tail slice after train_data

        anomalous_data = pca_preprocess_df(df_anomalous, lags_n, diffs_n, smooth_n)

        # @added 20241114 - Task #5526: Build v5.0.0 and upgrade deps
        #                   Branch #5532: v5.0.0-alpha
        # Check that the data suits the PCA algorithm and if not do not run and
        check_dataset = 'anomalous_data'
        suitable_for_pca = True
        reason = 'OK'
        try:
            suitable_for_pca, reason = check_pca_suitability(anomalous_data, dataset=check_dataset)
        except Exception as err:
            if debug_logging:
                current_logger.error('error :: debug :: check_pca_suitability on %s, err: %s' % (
                    check_dataset, err))
        if not suitable_for_pca:
            if debug_logging:
                current_logger.debug('debug :: not suitable for pca returning, took %.6f seconds' % (time() - start))
            results['anomalies'] = {}
            results['error'] = reason
            if return_results:
                return (None, None, results)
            return (None, None)

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

        if debug_logging:
            # Find duplicated indices in each DataFrame
            duplicated_train_indices = df_train_scores.index[df_train_scores.index.duplicated()]
            duplicated_score_indices = df_scores.index[df_scores.index.duplicated()]
            duplicated_anomalies_indices = df_anomalies.index[df_anomalies.index.duplicated()]

            duplicated_df_train_scores = df_train_scores[df_train_scores.index.duplicated(keep=False)]
            duplicated_df_scores = df_scores[df_scores.index.duplicated(keep=False)]
            duplicated_df_anomalies = df_anomalies[df_anomalies.index.duplicated(keep=False)]

            log_data = {
                'version': 20,
                'n_train': n_train, 'len(df)': len(df), 'len(df_train)': len(df_train),
                'len(train_data)': len(train_data), 'len(df_anomalous)': len(df_anomalous),
                'len(anomalous_data)': len(anomalous_data), 'len(train_scores)': len(train_scores),
                'len(df_train_scores)': len(df_train_scores),
                'len(df_scores)': len(df_scores),
                'Duplicates in df_anomalies index': df_anomalies.index.duplicated().any(),
                'Duplicates in df_train_scores index': df_train_scores.index.duplicated().any(),
                'Duplicates in df_scores index': df_scores.index.duplicated().any(),
#                'duplicated_train_indices': duplicated_train_indices,
#                'duplicated_score_indices': duplicated_score_indices,
#                'duplicated_anomalies_indices': duplicated_anomalies_indices,
#                'duplicated_df_train_scores': df_train_scores[df_train_scores.index.duplicated(keep=False)],
#                'duplicated_df_scores': df_scores[df_scores.index.duplicated(keep=False)],
#                'duplicated_df_anomalies': df_anomalies[df_anomalies.index.duplicated(keep=False)],
#                'duplicated_df_train_scores': duplicated_df_train_scores.to_dict(orient="records"),
#                'duplicated_df_scores': duplicated_df_scores.to_dict(orient="records"),
#                'duplicated_df_anomalies': duplicated_df_anomalies.to_dict(orient="records"),
                'duplicated_df_train_scores': str(duplicated_df_train_scores),
                'duplicated_df_scores': str(duplicated_df_scores),
                'duplicated_df_anomalies': str(duplicated_df_anomalies),
            }
            current_logger.debug('debug :: pca - data attributes: %s' % str(log_data))

        # @added 20241107 - Task #5526: Build v5.0.0 and upgrade deps
#        df_anomalies = df_anomalies[~df_anomalies.index.duplicated()]
#        df_train_scores = df_train_scores[~df_train_scores.index.duplicated()]
#        df_scores = df_scores[~df_scores.index.duplicated()]

        # @modified 20241107 - Task #5526: Build v5.0.0 and upgrade deps
        # Testing possible difference between pandas 2.0.3 and 2.2.3 in terms
        # over stricter handling of things with concat, in which the following
        # errors are being reported occassionally
        # File "/opt/python_virtualenv/projects/skyline/lib/python3.10/site-packages/pandas/core/indexes/base.py", line 4429, in reindex
        #   raise ValueError("cannot reindex on an axis with duplicate labels")
        # ValueError: cannot reindex on an axis with duplicate labels
        reindex_data = False
        try:
            # @modified 20241107 - Task #5526: Build v5.0.0 and upgrade deps
            df_anomalies['anomaly_score'] = pd.concat([df_train_scores, df_scores])
            #df_anomalies['anomaly_score'] = pd.concat([df_train_scores, df_scores]).reindex(df_anomalies.index).values
        except ValueError:
            reindex_data = True
            if debug_logging:
                current_logger.error(traceback.format_exc())
                current_logger.info('warning - ValueError encountered on pca from pd.concat([df_train_scores, df_scores])')
        if reindex_data:
            if debug_logging:
                current_logger.debug('debug :: pca - resetting indexes to avoid duplicates')
            try:
                use_first_method = False
                if use_first_method:
                    # Reset indexes to avoid duplicates
                    df_train_scores_reset = df_train_scores.reset_index(drop=True)
                    df_scores_reset = df_scores.reset_index(drop=True)
                    # Concatenate the scores with a continuous index
                    concatenated_scores = pd.concat([df_train_scores_reset, df_scores_reset], ignore_index=True)
                    # Reset index of df_anomalies to align with concatenated_scores
                    df_anomalies = df_anomalies.reset_index(drop=True)
                else:
                    # concatenated_scores = pd.concat([df_train_scores, df_scores]).reset_index(drop=True)
                    df_train_scores = df_train_scores[~df_train_scores.index.duplicated()]
                    df_scores = df_scores[~df_scores.index.duplicated()]
                    concatenated_scores = pd.concat([df_train_scores, df_scores], ignore_index=True)

                # Assign the anomaly scores
                df_anomalies['anomaly_score'] = concatenated_scores['anomaly_score'].values
            except Exception as err:
                record_algorithm_error(current_skyline_app, parent_pid, algorithm_name, traceback.format_exc())
                if debug_print:
                    print('error:', traceback.format_exc())
                if debug_logging:
                    current_logger.debug('debug :: error - on pca reindexing data, err: %s' % err)
                    current_logger.debug(traceback.format_exc())

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
            'algorithm_parameters': algorithm_parameters,
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
