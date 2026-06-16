"""
dbscan.py
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
from numpy.lib.stride_tricks import sliding_window_view
import numpy.matlib
from sklearn.neighbors import NearestNeighbors
from sklearn.cluster import DBSCAN
from kneed import KneeLocator

# @added 20241116 - Feature #4746: custom_algorithm - dbscan
#                   Task #5526: Build v5.0.0 and upgrade deps
#                   Branch #5532: v5.0.0-alpha
from scipy.stats import kurtosis, shapiro, skew, zscore
from sklearn.preprocessing import MinMaxScaler

# The name of the function MUST be the same as the name declared in
# settings.CUSTOM_ALGORITHMS.
# It MUST have 3 parameters:
# current_skyline_app, timeseries, algorithm_parameters
# See https://earthgecko-skyline.readthedocs.io/en/latest/algorithms/custom-algorithms.html
# for a full explanation about each.
# ALWAYS WRAP YOUR ALGORITHM IN try and except


# @added 20221111 - Feature #4746: custom_algorithm - dbscan
def dbscan(current_skyline_app, parent_pid, timeseries, algorithm_parameters):
    """
    Outlier detector based on DBSCAN.  EXPERIMENTAL

    A shit algorithm don't waste your time.

    UNRELIABLE as it is very sensitive to input parameters which make it difficult
    to automatically determine suitable parameters.  Automatically determined
    parameters can sometimes be very effective, but often they do not have the
    desired results.  Seeing as there is a single epsilon value for all clusters
    the algorithm fails when varying density clusters are present in the data.

    Therefore if DBSCAN identifies more than 33% of the data points in a
    timeseries as outliers, this algorithm will return an inconclusive results.

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
        - ``'window'`` (int):
            The number of data points to use in the sliding window to calculate
            Xmean and Xvar.  Default is ``3``.
        - ``'min_samples'`` (int):
            The number of samples (or total weight) in a neighborhood for a
            point to be considered as a core point. This includes the point
            itself. If min_samples is set to a higher value, DBSCAN will find
            denser clusters, whereas if it is set to a lower value, the found
            clusters will be more sparse.  Default is ``4``.
        - ``'use_skewness'`` (bool):
            Whether to use Skewness as a feature.  For backwards compatibility
            to the original implementation it defaults to ``False``.  Testing
            the use of Skewness to cptures the asymmetry of the data
            distribution in the sliding window and help identify anomalies where
            the data distribution has shifted toward one tail, which mean and
            variance may not detect.
        - ``'use_kurtosis'`` (bool):
            Whether to use Kurtosis as a feature.  For backwards compatibility
            to the original implementation it defaults to ``False``.  Testing
            the use of the use of Kurtosis which measures the "tailedness" of
            the data distribution in the sliding window, to determine whether it
            helps detecting sharp peaks or heavy tails.
        - ``'normalize_data'`` (bool):
            Whether to normalize the data.  If ``True`` the appropriate method
            to normalize the data is determined from the data and either
            Z-score normalization or Min-Max scaling is used.  Default is ``False``.
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
                'use_skewness': True,
                'use_kurtosis': True,
                'normalize_data': True,
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
    algorithm_name = 'dbscan'

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

    window_shape = 3
    try:
        window_shape = int(algorithm_parameters['window'])
    except:
        window_shape = 3
    min_samples = 4
    try:
        min_samples = int(algorithm_parameters['min_samples'])
    except:
        min_samples = 4

    # @added 20241116 - Feature #4746: custom_algorithm - dbscan
    #                   Task #5526: Build v5.0.0 and upgrade deps
    #                   Branch #5532: v5.0.0-alpha
    use_skewness = False
    try:
        use_skewness = bool(algorithm_parameters['use_skewness'])
    except:
        use_skewness = False
    use_kurtosis = False
    try:
        use_kurtosis = bool(algorithm_parameters['use_kurtosis'])
    except:
        use_kurtosis = False
    eps_percentile = 95
    try:
        eps_percentile = int(algorithm_parameters['eps_percentile'])
    except:
        eps_percentile = 95
    normalize_data = False
    try:
        normalize_data = bool(algorithm_parameters['normalize_data'])
    except:
        normalize_data = False

    anomaly_window = 1
    try:
        anomaly_window = int(algorithm_parameters['anomaly_window'])
    except:
        anomaly_window = 1

    if debug_print:
        print('running dbscan with window_shape: %s, min_samples: %s' % (
            str(window_shape), str(min_samples)))
    if debug_logging:
        current_logger.debug('debug :: running dbscan with window_shape: %s, min_samples: %s' % (
            str(window_shape), str(min_samples)))


    def check_feature_range_difference(features, threshold=10):
        """
        Check if feature ranges are drastically different.

        Parameters:
            features (list of np.ndarray): List of feature arrays.
            threshold (float): Ratio threshold to determine drastic difference.

        Returns:
            bool: True if ranges are drastically different, False otherwise.
        """
        ranges = [np.max(f) - np.min(f) for f in features]
        max_range = max(ranges)
        min_range = min(ranges)
        range_ratio = max_range / min_range if min_range > 0 else float('inf')

        print(f"Feature ranges: {ranges}")
        print(f"Max range: {max_range}, Min range: {min_range}, Ratio: {range_ratio}")

        return range_ratio > threshold

    try:
        X = np.array([v for t, v in timeseries])
        Xmean = np.average(sliding_window_view(X, window_shape=window_shape), axis=1)
        Xvar = np.var(sliding_window_view(X, window_shape=window_shape), axis=1)
        xx = []

        # @modified 20241116 - Feature #4746: custom_algorithm - dbscan
        # Just use numpy arrays
        #xmeans = list(Xmean)
        #xvars = list(Xvar)

        # @added 20241116 - Feature #4746: custom_algorithm - dbscan
        #                   Task #5526: Build v5.0.0 and upgrade deps
        #                   Branch #5532: v5.0.0-alpha
        # Adding Skewness and Kurtosis with the addition of these two simple
        # features the dimensionality of the feature space increases from 2 to 4
        # keeping the computational cost low while enhancing anomaly detection.
        # Mean and variance summarize central tendency and spread.
        # Skewness and kurtosis capture asymmetry and extreme values.
        # NOW leave and do not waste any more time trying to get it to work in a
        # coherent fashion.
        Xskew = np.array([])
        if use_skewness:
            Xskew = skew(sliding_window_view(X, window_shape=window_shape), axis=1)
        Xkurt = np.array([])
        if use_kurtosis:
            Xkurt = kurtosis(sliding_window_view(X, window_shape=window_shape), axis=1)
        # Determine what normalization best suits the data
        use_z_score = False
        use_minmax_scale = False
        if normalize_data:
            if use_skewness and use_kurtosis:
                features = [Xmean, Xvar, Xskew, Xkurt]
            elif use_skewness and not use_kurtosis:
                features = [Xmean, Xvar, Xskew]
            elif not use_skewness and use_kurtosis:
                features = [Xmean, Xvar, Xkurt]
            else:
                features = [Xmean, Xvar]
            try:
                _, shapiro_p_value = shapiro(Xmean)
                if shapiro_p_value > 0.05:
                    use_z_score = True
            except Exception as err:
                record_algorithm_error(current_skyline_app, parent_pid, algorithm_name, traceback.format_exc())
                if debug_print:
                    print('error:', traceback.format_exc())
                if debug_logging:
                    current_logger.debug('debug :: error - on dbscan, shapiro failed, err: %s' % err)
                    current_logger.debug(traceback.format_exc())
            try:
                use_minmax_scale = check_feature_range_difference(features)
            except Exception as err:
                record_algorithm_error(current_skyline_app, parent_pid, algorithm_name, traceback.format_exc())
                if debug_print:
                    print('error:', traceback.format_exc())
                if debug_logging:
                    current_logger.debug('debug :: error - on dbscan, check_feature_range_difference failed, err: %s' % err)
                    current_logger.debug(traceback.format_exc())
            if use_z_score and not use_minmax_scale:
                if debug_logging:
                    current_logger.debug('debug :: dbscan using Z-score normalization')
                # Normalize each feature with Z-score normalization
                Xmean = zscore(Xmean)
                Xvar = zscore(Xvar)
                Xskew = zscore(Xskew)
                Xkurt = zscore(Xkurt)
            if use_minmax_scale:
                if debug_logging:
                    current_logger.debug('debug :: dbscan using Min-Max scaling for normalization')
                # Create a scaler instance
                scaler = MinMaxScaler()
                # Normalize each feature with Min-Max scaling
                Xmean = scaler.fit_transform(Xmean.reshape(-1, 1)).flatten()
                Xvar = scaler.fit_transform(Xvar.reshape(-1, 1)).flatten()
                if use_skewness:
                    Xskew = scaler.fit_transform(Xskew.reshape(-1, 1)).flatten()
                if use_kurtosis:
                    Xkurt = scaler.fit_transform(Xkurt.reshape(-1, 1)).flatten()
            if debug_logging:
                current_logger.debug('debug :: dbscan data normalization')

        # @modified 20241116 - Feature #4746: custom_algorithm - dbscan
        #                      Task #5526: Build v5.0.0 and upgrade deps
        #                      Branch #5532: v5.0.0-alpha
        #for index, value in enumerate(xmeans):
        #    xx.append([value, xvars[index]])
        for index, value in enumerate(Xmean):
            if use_skewness and use_kurtosis:
                xx.append([value, Xvar[index], Xskew[index], Xkurt[index]])
            elif use_skewness and not use_kurtosis:
                xx.append([value, Xvar[index], Xskew[index]])
            elif not use_skewness and use_kurtosis:
                xx.append([value, Xvar[index], Xkurt[index]])
            else:
                xx.append([value, Xvar[index]])

        # Approximate most suitable epsilon value to use by determining the
        # the elbow point on the k-NN distance curve.
        try:
            neighbors = NearestNeighbors(n_neighbors=min_samples)
            neighbors_fit = neighbors.fit(xx)
            distances, indices = neighbors_fit.kneighbors(xx)
            try:
                del indices
            except:
                pass
            distances = np.sort(distances, axis=0)
            distances = distances[:, 1]
            x = np.array(range(len(distances)))
            kl = KneeLocator(x, distances, curve='convex')
            eps = kl.elbow
            if eps is None:
                if debug_logging:
                    current_logger.debug('debug :: error - on dbscan eps is set to None after kl.elbow, setting to %s percentile' % (
                        str(eps_percentile)))
                # Assume the outliers on in the 95th percentile
                #eps = int((len(xx) / 100) * 95)
                eps = int((len(xx) / 100) * eps_percentile)
        except Exception as err:
            record_algorithm_error(current_skyline_app, parent_pid, algorithm_name, traceback.format_exc())
            if debug_print:
                print('error:', traceback.format_exc())
            if debug_logging:
                current_logger.debug('debug :: error - on dbscan - %s' % err)
                current_logger.debug(traceback.format_exc())
            # Assume the outliers on in the 95th percentile
            #eps = int((len(xx) / 100) * 95)
            eps = int((len(xx) / 100) * eps_percentile)

        if debug_logging:
            current_logger.debug('debug :: dbscan - len(XX): %s, eps: %s' % (
                str(len(xx)), str(eps)))

        try:
            XX = np.array(xx)
            if debug_logging:
                current_logger.debug('debug :: XX.shape: %s' % (
                    str(XX.shape)))
            db = DBSCAN(eps=eps, min_samples=min_samples)
            db.fit(XX)
            dbscan_scores = list(db.labels_)
            # Coerce scores from numpy.int64 to float
            dbscan_scores = [float(v) for v in dbscan_scores]
        except Exception as err:
            record_algorithm_error(current_skyline_app, parent_pid, algorithm_name, traceback.format_exc())
            if debug_print:
                print('error:', traceback.format_exc())
            if debug_logging:
                current_logger.debug('debug :: error - on dbscan - %s' % err)
                current_logger.debug(traceback.format_exc())
            if return_results:
                return (None, None, None)
            return (None, None)

        if debug_logging:
            current_logger.debug('debug :: dbscan - len(dbscan_scores): %s' % str(len(dbscan_scores)))

        outlier_count = len([x for x in dbscan_scores if x == -1.0])
        if debug_logging:
            current_logger.debug('debug :: dbscan - anomalous count: %s, not_anomalous count: %s' % (
                str(len([x for x in dbscan_scores if x == -1.0])),
                str(len([x for x in dbscan_scores if x != -1.0]))))

        # if outlier_count == 0:
        if outlier_count == 0 or outlier_count > (len(timeseries) / 3):
            if debug_logging:
                eps2 = eps * 2
                current_logger.debug('debug :: dbscan - eps: %s, did not work, running again with eps*2: %s' % (
                    str(eps), str(eps2)))
            try:
                # Assume the outliers on in the 95th percentile
                #eps = int((len(xx) / 100) * 95)
                eps = int((len(xx) / 100) * eps_percentile)
                XX = np.array(xx)
#                db = DBSCAN(eps=eps, min_samples=min_samples)

                db = DBSCAN(eps=eps2, min_samples=min_samples)

                db.fit(XX)
                dbscan_scores = list(db.labels_)
                # Coerce scores from numpy.int64 to float
                dbscan_scores = [float(v) for v in dbscan_scores]
            except Exception as err:
                record_algorithm_error(current_skyline_app, parent_pid, algorithm_name, traceback.format_exc())
                if debug_print:
                    print('error:', traceback.format_exc())
                if debug_logging:
                    current_logger.debug('debug :: error - on dbscan - %s' % err)
                    current_logger.debug(traceback.format_exc())
                if return_results:
                    return (None, None, None)
                return (None, None)
            if debug_logging:
                current_logger.debug('debug :: dbscan - eps %s percentile - len(dbscan_scores): %s' % (
                    str(eps_percentile), str(len(dbscan_scores))))
                current_logger.debug('debug :: dbscan - eps %s percentile - anomalous count: %s, not_anomalous count: %s' % (
                    str(eps_percentile),
                    str(len([x for x in dbscan_scores if x == -1.0])),
                    str(len([x for x in dbscan_scores if x != -1.0]))))

        #insert_count = len(X) - len(Xmeans)
        insert_count = len(X) - len(Xmean)

        for i in list(range(0, insert_count)):
            dbscan_scores.insert(i, 0.0)

        anomalyScore_list = []
        anomalies = {}
        for index, item in enumerate(timeseries):
            score = 0
            try:
                dbscan_score = dbscan_scores[index]
            except:
                dbscan_score = 0.0
            if dbscan_score == -1:
                ts = int(item[0])
                anomalies[ts] = {'value': item[1], 'index': index, 'score': -1}
                score = 1
            anomalyScore_list.append(score)

        anomaly_sum = sum(anomalyScore_list[-anomaly_window:])
        if anomaly_sum:
            anomalous = True
        else:
            anomalous = False

        # REMOVE UNRELIABLE ANALYSIS
        # If the number of outliers detected is greater than a third of timeseries
        # the automatic determination of the parameters did not fit the data
        # well and the results are probably unreliable
        unreliable = False
        if len(anomalies) > (len(timeseries) / 3):
            unreliable = True
            anomalous = None
            # anomalies = {}
            # dbscan_scores = []

        results = {
            'anomalous': anomalous,
            'anomalies': anomalies,
            'anomalyScore_list': anomalyScore_list,
            'scores': dbscan_scores,
        }
        if debug_print:
            print('ran dbscan OK in %.6f seconds' % (time() - start))
        if debug_logging:
            current_logger.debug('debug :: ran dbscan OK in %.6f seconds' % (time() - start))
        if results:
            if results['anomalous']:
                anomalous = True
                anomalyScore = 1.0
            else:
                anomalous = False
                anomalyScore = 0.0
            if unreliable:
                anomalous = None
                anomalyScore = None
                results['error'] = 'parameters produced unreliable results, removed'
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
            print('warning - StopIteration called on dbscan')
        if debug_logging:
            current_logger.debug('debug :: warning - StopIteration called on dbscan')

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
            current_logger.debug('debug :: error - on dbscan - %s' % err)
            current_logger.debug(traceback.format_exc())

        # Return None and None as the algorithm could not determine True or False
        if return_results:
            return (None, None, None)
        return (None, None)

    if return_results:
        return (anomalous, anomalyScore, results)

    return (anomalous, anomalyScore)
