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

    UNRELIABLE as it is very sensitive to input parameters which make it difficult
    to automatically determine suitable parameters.  Automatically determined
    parameters can sometimes be very effective, but often they do not have the
    desired results.  Seeing as there is a single epsilon value for all clusters
    the algorithm fails when varying density clusters are present in the data.

    Therefore if DBSCAN identifies more that 33% of the data points in a
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
        custom_algorithm and algorithm itself.  Example:
        ``algorithm_parameters={'window_shape'=3, 'min_samples'=4, 'anomaly_window'=5, 'return_results'=True}``
    :type current_skyline_app: str
    :type parent_pid: int
    :type timeseries: list
    :type algorithm_parameters: dict
    :return: anomalous, anomalyScore, instance_scores
    :rtype: tuple(boolean, float, instance_scores)

    """

    # You MUST define the algorithm_name
    algorithm_name = 'dbscan'

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

    try:
        X = np.array([v for t, v in timeseries])
        Xmean = np.average(sliding_window_view(X, window_shape=window_shape), axis=1)
        Xvar = np.var(sliding_window_view(X, window_shape=window_shape), axis=1)
        xx = []
        xmeans = list(Xmean)
        xvars = list(Xvar)
        for index, value in enumerate(xmeans):
            xx.append([value, xvars[index]])

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
        except Exception as err:
            record_algorithm_error(current_skyline_app, parent_pid, algorithm_name, traceback.format_exc())
            if debug_print:
                print('error:', traceback.format_exc())
            if debug_logging:
                current_logger.debug('debug :: error - on dbscan - %s' % err)
                current_logger.debug(traceback.format_exc())
            # Assume the outliers on in the 95th percentile
            eps = int((len(xx) / 100) * 95)

        if debug_logging:
            current_logger.debug('debug :: dbscan - len(XX): %s, eps: %s' % (
                str(len(xx)), str(eps)))

        try:
            XX = np.array(xx)
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

        if outlier_count == 0:
            if debug_logging:
                current_logger.debug('debug :: dbscan - running again with 95th percentile for eps')
            try:
                # Assume the outliers on in the 95th percentile
                eps = int((len(xx) / 100) * 95)
                XX = np.array(xx)
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
                current_logger.debug('debug :: dbscan - eps 95th percentile - len(dbscan_scores): %s' % str(len(dbscan_scores)))
                current_logger.debug('debug :: dbscan - eps 95th percentile - anomalous count: %s, not_anomalous count: %s' % (
                    str(len([x for x in dbscan_scores if x == -1.0])),
                    str(len([x for x in dbscan_scores if x != -1.0]))))

        insert_count = len(X) - len(xmeans)
        for i in list(range(0, insert_count)):
            dbscan_scores.insert(i, 0.0)

        anomalyScore_list = []
        anomalies = {}
        for index, item in enumerate(timeseries):
            try:
                dbscan_score = dbscan_scores[index]
            except:
                dbscan_score = 0.0
            if dbscan_score == -1:
                ts = int(item[0])
                anomalies[ts] = {'value': item[1], 'index': index, 'score': -1}
                anomalyScore_list.append(1)
            else:
                anomalyScore_list.append(0)

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
