"""
laoccfdlpnc.py
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
import sys
from time import time

import cvxpy as cp
import numpy as np
from scipy.stats import zscore
from scipy.sparse import csc_matrix
from sklearn.svm import OneClassSVM
from sklearn.ensemble import IsolationForest
from sklearn.decomposition import KernelPCA
from sklearn.kernel_approximation import Nystroem
from sklearn.mixture import GaussianMixture

# The name of the function MUST be the same as the name declared in
# settings.CUSTOM_ALGORITHMS.
# It MUST have 3 parameters:
# current_skyline_app, timeseries, algorithm_parameters
# See https://earthgecko-skyline.readthedocs.io/en/latest/algorithms/custom-algorithms.html
# for a full explanation about each.
# ALWAYS WRAP YOUR ALGORITHM IN try and except


# @added 20241112 - Feature #5553: custom_algorithm - laoccfdlpnc
#                   Info #5544: Locally Adaptive One-Class Classifier Fusion
def laoccfdlpnc(current_skyline_app, parent_pid, timeseries, algorithm_parameters):

    """
    EXPERIMENTAL

    An implementation of:

    Locally Adaptive One-Class Classifier Fusion with Dynamic ℓp-Norm
    Constraints for Robust Anomaly Detection

    https://arxiv.org/pdf/2411.06406

    Using a TCPDBench type naming convention here - laoccfdlpnc

    Not super fast, but not too slow.
    
    Should be limited to time series with <= 2000 data points.

    If used for SNAB or in Mirage as a custom_algorithm should be used via the
    skyline_laoccfdlpnc custom_algorithm which preloads and is serves it via
    flux/tornado.  See
    https://earthgecko-skyline.readthedocs.io/en/latest/skyline.custom_algorithms.html#module-custom_algorithms.skyline_laoccfdlpnc


    :param current_skyline_app: the Skyline app executing the algorithm.  This
        will be passed to the algorithm by Skyline.  This is **required** for
        error handling and logging.  You do not have to worry about handling the
        argument in the scope of the custom algorithm itself,  but the algorithm
        must accept it as the first agrument.
    :param parent_pid: the parent pid which is executing the algorithm, this is
        **required** for error handling and logging.  You do not have to worry
        about handling this argument in the scope of algorithm, but the
        algorithm must accept it as the second argument.
    :param timeseries: the time series as a list e.g. ``[[1731401580, 1269121024.0],
        [1731402180, 1269174272.0], ..., [1732006380, 1269174272.0]]``
    :param algorithm_parameters: a dictionary of any required parameters for the
        custom_algorithm and algorithm itself.  For the laoccfdlpnc custom algorithm no
        specific algorithm_parameters are required apart from an empty dict but
        the algorithm_parameters that can be passed are:

        - ``'anomaly_window'`` (int): The anomaly_window value.
            This specifies how many of the last data points should be considered
            when determining if the metric is anomalous. Only the last
            ``anomaly_window`` data points in the time series will be used to
            determine if the metric is anomalous.  Default is ``1``.
        - ``'oc_svm_nu'`` (float):
            The OneClassSVM nu for an upper bound on the fraction of training
            errors and a lower bound of the fraction of support vectors. Should
            be in the interval (0, 1].  Default ``0.05``.
        - ``'if_contamination'`` (float or "auto"):
            The proportion of the dataset that is expected to be anomalies or
            outliers. This can be either:

            - A ``float`` value between ``0.0`` and ``0.5`` representing the
                expected proportion of anomalies in the data. For example,
                ``0.01`` would mean that 1% of the data is anticipated to be
                anomalous.
            - The string ``"auto"``: This option allows the algorithm to 
                automatically determine an appropriate contamination level based
                on the characteristics of the data. Useful when the proportion
                of outliers is unknown.
            Default is ``0.05``.
        - ``'interior_point_p_value'`` (float):
            Variations in the norm parameter p significantly affect the method's
            behavior. As p increases towards infinity, weights among classifiers
            uniformize, resembling the sum rule in classifier fusion. Conversely,
            as p approaches slightly above 1 (p → 1+), the method selectively
            prioritizes the classifier with the highest correct classifications.
            Setting p = 2 aligns the method with a soft-margin linear SVM,
            underlining its adaptability. Adjusting p within [1, ∞) allows
            transitioning from focusing on the strongest classifier to equally
            weighting all, optimizing based on unique learner characteristics
            and data.
            Assuming all classifiers surpass random performance, adjusting p to
            a more selective value can diminish the influence of under-performing
            classifiers, streamlining decision-making by reducing their weight.
            Default is ``1.5``.
        - ``'interior_point_max_epochs'`` (int):
            The maximum number of iterations allowed for the optimization
            process.  Default is ``100``.
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
                'oc_svm_nu': 0.05,
                'if_contamination': 0.05,
                'interior_point_p_value': 1.5,
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
    algorithm_name = 'laoccfdlpnc'

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
    timings = {}

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

    oc_svm_nu = 0.05
    try:
        oc_svm_nu = float(algorithm_parameters['oc_svm_nu'])
    except:
        oc_svm_nu = 0.05

    if_contamination = 0.05
    try:
        if_contamination = float(algorithm_parameters['if_contamination'])
    except:
        if_contamination = 0.05

    interior_point_p_value = 1.5
    try:
        interior_point_p_value = float(algorithm_parameters['interior_point_p_value'])
    except:
        interior_point_p_value = 1.5

    interior_point_max_epochs = 100
    try:
        interior_point_max_epochs = float(algorithm_parameters['interior_point_max_epochs'])
    except:
        interior_point_max_epochs = 100

    anomaly_window = 1
    try:
        anomaly_window = int(algorithm_parameters['anomaly_window'])
    except:
        anomaly_window = 1

    if debug_print:
        print('passed algorithm_parameters: %s' % (
            str(algorithm_parameters)))
        print('running laoccfdlpnc with oc_svm_nu: %s, if_contamination: %s' % (
            str(oc_svm_nu), str(if_contamination)))
    if debug_logging:
        current_logger.debug('debug :: laoccfdlpnc passed algorithm_parameters: %s' % (
            str(algorithm_parameters)))
        current_logger.debug('debug :: running laoccfdlpnc with oc_svm_nu: %s, if_contamination: %s' % (
            str(oc_svm_nu), str(if_contamination)))

    def return_result(anomalous, anomalyScore, results, return_results):
        if return_results:
            return (anomalous, anomalyScore, results)
        return (anomalous, anomalyScore)

    # Preprocessing
    def preprocess_data(timeseries):
        """
        :param timeseries: the time series data as a list of lists
            e.g. ``[[1731401580, 1269121024.0], [1731402180, 1269174272.0], ...,
            [1732006380, 1269174272.0]]``
        :type timeseries: list
        :return scaled_values
        """

        values = np.array([float(item[1]) for item in timeseries])

        # Check if there are any NaN values before attempting to fill
        if np.isnan(values).any():
            # Backward fill NaNs by reversing, forward filling, then reversing back.
            # This is required as OneClassSVM does not tolerate NaNs.  This may not
            # be the most appropriate manner in which to achieve this with all data,
            # but given that a lot of metric data has sudden jumps or discrete
            # shifts.  It assumes small gaps.  A min percentage data points strategy
            # would probably be sutiable here, to ensure that only mostly fully
            # populated data sets have the method applied.
            reversed_values = values[::-1]
            mask = np.isnan(reversed_values)
            reversed_values[mask] = np.where(mask, np.maximum.accumulate(np.where(mask, 0, reversed_values)), reversed_values)
            values = reversed_values[::-1]

        # Scaling with Z-score normalization.  To be verified with the author if
        # this included as it is not specifically clear in the paper.  Testing
        # with and without shows little difference in the results and does seem
        # to solve some cvxpy solver issues that have been encountered during
        # testing.
        scaled_values = (values - values.mean()) / values.std()
        return scaled_values

    # Calculate gamma_values for RBF kernel
    def calculate_gamma_values(data, method='squared_diffs'):
        # The paper states:
        # "we use RBF kernels with widths 0.25, 0.5, 1 × M
        # (M: average squared Euclidean distance among training samples)"
        # Calculate the average squared Euclidean distance (M) among training
        # samples
        # loop
        if method == 'loop':
            n = len(data)
            M = np.sum([np.linalg.norm(x - y)**2 for i, x in enumerate(data) for j, y in enumerate(data) if i < j]) / (n * (n - 1) / 2)

        # HOW TO DO FASTER?
        # In this use case for metrics with between 1000 and 2000 data
        # points, it is much faster to use a matrix based calculation of M.
        #M (loop): 2.001988071570577 | took: 14.001994609832764
        #M (vector): 2.001988071570577 | took: 0.0978384017944336
        #M (squared_diffs): 2.001988071570577 | took: 0.031850576400756836
        # vector
        if method == 'vector':
            # Reshape data to a 2D array as it only has one feature per data point
            data = data.reshape(-1, 1) if data.ndim == 1 else data
            # Calculate the squared Euclidean distance matrix
            distance_matrix = np.sum((data[:, np.newaxis] - data[np.newaxis, :]) ** 2, axis=-1)
            # Use np.triu_indices to avoid double-counting pairs
            i_upper = np.triu_indices(len(data), k=1)
            # Take the upper triangle of the matrix, excluding the diagonal, to
            # avoid double-counting
            upper_triangle_values = distance_matrix[i_upper]
            # Calculate the mean of the upper triangle values
            average_squared_distance = np.mean(upper_triangle_values)
            M = average_squared_distance
        # squared_diffs
        if method == 'squared_diffs':
            squared_diffs = (data[:, np.newaxis] - data[np.newaxis, :]) ** 2
            # Get the average of the upper triangle (excluding the diagonal)
            n_samples = data.shape[0]
            M = np.sum(squared_diffs) / (n_samples * (n_samples - 1))

        # Check for NaN or zero to prevent invalid gamma values which end up
        # resulting in encountering:
        # InvalidParameterError: The 'gamma' parameter of OneClassSVM must be a str among {'scale', 'auto'} or a float in the range [0.0, inf). Got np.float64(nan) instead.
        if np.isnan(M) or M == 0:
            # Use default gamma values if M is invalid
            return [1.0, 0.5, 0.1]
        # Define gamma_values for RBF kernels based on specified widths of
        # (0.25, 0.5, 1 * M)
        return [1 / (2 * width * M) for width in [0.25, 0.5, 1]]

    # Initialize and fit One-Class SVM models with different gamma values
    def initialize_and_fit_svms(data, gamma_values, oc_svm_nu):
        oc_svms = []
        for gamma in gamma_values:
            oc_svm = OneClassSVM(kernel='rbf', gamma=gamma, nu=oc_svm_nu)
            oc_svm.fit(data.reshape(-1, 1))
            oc_svms.append(oc_svm)
        return oc_svms

    # Initialize and fit other models: Isolation Forest, KPCA + SVM, and GMM
    def initialize_and_fit_models(data, gamma_values):
        fit_timings = {}
        # Isolation Forest
        started = time()
        isolation_forest = IsolationForest(contamination=if_contamination, random_state=42)
        isolation_forest.fit(data.reshape(-1, 1))
        fit_timings['isolation_forest'] = time() - started

        # KPCA with Nystroem approximation and SVM
        # Use Nyström kernel approximate method to reduce computational
        # complexity, much faster and good enough.
        #kpca = KernelPCA(kernel="rbf", gamma=gamma_value)
        ## Transform the data using KPCA
        #transformed_values = kpca.fit_transform(scaled_values.reshape(-1, 1))
        started_kpca = time()
        started_kpca_svm = float(started_kpca)
        kpca_svm = OneClassSVM(kernel='linear', nu=oc_svm_nu)
        fit_timings['kpca_svm - OneClassSVM'] = time() - started_kpca_svm
        started_kpca_Nystroem_fit_transform = time()
        transformed_data = Nystroem(kernel='rbf', gamma=gamma_values[1], n_components=300).fit_transform(data.reshape(-1, 1))
        fit_timings['kpca_svm - Nystroem fit_transform'] = time() - started_kpca_Nystroem_fit_transform
        started_kpca_fit = time()
        kpca_svm.fit(transformed_data)
        fit_timings['kpca_svm - fit'] = time() - started_kpca_fit
        fit_timings['kpca_svm'] = time() - started_kpca

        # GMM for anomaly detection
        started_gmm = time()
        gmm = GaussianMixture(n_components=1, covariance_type='full', random_state=42)
        gmm.fit(data.reshape(-1, 1))
        fit_timings['GaussianMixture'] = time() - started_gmm

        return isolation_forest, kpca_svm, gmm, transformed_data, fit_timings

    # Score Matrix Calculation
    def calculate_score_matrix(data, oc_svms, isolation_forest, kpca_svm, gmm, transformed_data):
        # Calculate anomaly scores for each model
        svm_scores = np.column_stack([model.decision_function(data.reshape(-1, 1)) for model in oc_svms])
        if_scores = isolation_forest.decision_function(data.reshape(-1, 1)).reshape(-1, 1)
        # kpca_svm needs to use transformed_data
        # kpca_svm_scores = kpca_svm.decision_function(data.reshape(-1, 1)).reshape(-1, 1)
        kpca_svm_scores = kpca_svm.decision_function(transformed_data).reshape(-1, 1)
        gmm_scores = gmm.score_samples(data.reshape(-1, 1)).reshape(-1, 1)
        
        # Combine scores into score matrix
        score_matrix = np.hstack([svm_scores, if_scores, kpca_svm_scores, gmm_scores])

        # @added 20241113 - added missing Z-score standardization of classifier scores
        # Standardize each classifier's scores
        zscored_scores = np.apply_along_axis(zscore, 0, score_matrix)
        score_matrix = zscored_scores.copy()
        return score_matrix

    # Two-sided min-max normalization with trimmed range
    def two_sided_minmax(data, rho):
        lower_bound = np.percentile(data, rho)
        upper_bound = np.percentile(data, 100 - rho)
        data_clipped = np.clip(data, lower_bound, upper_bound)
        # Handle case where all values are the same e.g. constant rows in
        # score_matrix where min == max
        if np.min(data_clipped) == np.max(data_clipped):
            return np.zeros_like(data_clipped)
        return (data_clipped - np.min(data_clipped)) / (np.max(data_clipped) - np.min(data_clipped))

    def apply_two_sided_minmax(score_matrix, rho=5):
        return np.array([two_sided_minmax(score, rho=rho) for score in score_matrix.T]).T

    # Locally Adaptive Weight Optimization with Interior-Point Method
    def optimize_weights_with_interior_point(
            score_matrix, labels, p_value=interior_point_p_value,
            max_epochs=interior_point_max_epochs, debug_logging=False):
        num_models = score_matrix.shape[1]
        # Initial barrier parameter
        mu = 10
        # Decay factor for mu
        beta = 0.5
        weights = cp.Variable(num_models)
        
        for epoch in range(max_epochs):
            hinge_losses = cp.maximum(0, 1 - cp.multiply(labels, score_matrix @ weights))
            barrier_term = -mu * cp.sum(cp.log(1 - cp.abs(weights)))
            objective = cp.Minimize(cp.sum(hinge_losses) + barrier_term)
            constraints = [cp.norm(weights, p_value) <= 1]
            
            problem = cp.Problem(objective, constraints)

            #if problem.is_dcp():
            #    print("The problem is DCP compliant.")
            #else:
            #    print("The problem is NOT DCP compliant.")

            try:
                problem.solve()
            except Exception as err:
                if debug_print:
                    print('CLARABEL could not solve, err: %s, trying SCS' % err)
                if debug_logging:
                    current_logger.debug('debug :: laoccfdlpnc CLARABEL could not solve, err: %s, trying SCS' % err)
                try:
                    # Regularization parameter (small value to avoid affecting
                    # the solution too much)
                    epsilon = 1e-5
                    regularization = epsilon * cp.norm(weights, 2)
                    objective = cp.Minimize(cp.sum(hinge_losses) + barrier_term + regularization)
                    constraints = [cp.norm(weights, p_value) <= 1]
                    problem = cp.Problem(objective, constraints)        
    #                problem.solve(solver=cp.SCS, verbose=True)
                    problem.solve(solver=cp.SCS)
                    # problem.solve(solver=cp.ECOS, verbose=True)  # ecos not installed, can install via pip
                except Exception as err:
                    print('SCS could not solve with Regularization, err: %s, trying csc_matrix with SCS' % err)
                    if debug_print:
                        print('SCS could not solve with Regularization, err: %s, trying csc_matrix with SCS' % err)
                    if debug_logging:
                        current_logger.debug('debug :: laoccfdlpnc SCS could not solve with Regularization, err: %s, trying csc_matrix with SCS' % err)
                    # Convert score matrix to sparse format if it contains many
                    # zeros
                    score_matrix_sparse = csc_matrix(score_matrix)
                    hinge_losses = cp.maximum(0, 1 - cp.multiply(labels, score_matrix_sparse @ weights))
                    barrier_term = -mu * cp.sum(cp.log(1 - cp.abs(weights)))
                    objective = cp.Minimize(cp.sum(hinge_losses) + barrier_term)
                    constraints = [cp.norm(weights, p_value) <= 1]
                    problem = cp.Problem(objective, constraints)
                    problem.solve(solver=cp.SCS)

            mu *= beta
            if problem.status == 'optimal':
                break
        
        return weights.value

    # Calculate Final Anomaly Scores and Predictions
    def calculate_anomaly_scores(score_matrix, optimized_weights):
        #print("Length of score_matrix:", len(score_matrix))
        #print("Length of optimized_weights:", len(optimized_weights))
        weighted_scores = score_matrix @ optimized_weights
        #print("Length of weighted_scores after recalculation:", len(weighted_scores))
        threshold = np.percentile(weighted_scores, 5)
        predictions = np.where(weighted_scores < threshold, -1, 1)
        return predictions, weighted_scores

    # Preprocess data the data
    scaled_values = None
    try:
        started = time()
        scaled_values = preprocess_data(timeseries)
        timings['preprocess_data'] = time() - started
    except Exception as err:
        record_algorithm_error(current_skyline_app, parent_pid, algorithm_name, traceback.format_exc())
        if debug_print:
            print('error:', traceback.format_exc())
        if debug_logging:
            current_logger.debug('debug :: error :: laoccfdlpnc - preprocess_data, err: %s' % err)
            current_logger.debug(traceback.format_exc())
        return_result(anomalous, anomalyScore, results, return_results)

    # Calculate gamma values for SVMs
    gamma_values = None
    try:
        started = time()
        gamma_values = calculate_gamma_values(scaled_values)
        timings['calculate_gamma_values'] = time() - started
    except Exception as err:
        record_algorithm_error(current_skyline_app, parent_pid, algorithm_name, traceback.format_exc())
        if debug_print:
            print('error:', traceback.format_exc())
        if debug_logging:
            current_logger.debug('debug :: error :: laoccfdlpnc - calculate_gamma_values, err: %s' % err)
            current_logger.debug(traceback.format_exc())
        return_result(anomalous, anomalyScore, results, return_results)

    # Initialize and fit SVMs with different gamma values
    oc_svms = None
    try:
        started = time()
        oc_svms = initialize_and_fit_svms(scaled_values, gamma_values, oc_svm_nu)
        timings['oc_svms'] = time() - started
    except Exception as err:
        record_algorithm_error(current_skyline_app, parent_pid, algorithm_name, traceback.format_exc())
        if debug_print:
            print('error:', traceback.format_exc())
        if debug_logging:
            current_logger.debug('debug :: error :: laoccfdlpnc - oc_svms, err: %s' % err)
            current_logger.debug(traceback.format_exc())
        return_result(anomalous, anomalyScore, results, return_results)

    # Initialize and fit models
    try:
        started = time()
        isolation_forest, kpca_svm, gmm, transformed_data, fit_timings = initialize_and_fit_models(scaled_values, gamma_values)
        timings['initialize_and_fit_models'] = time() - started
        timings['initialize_and_fit_models_fit_timings'] = fit_timings
    except Exception as err:
        record_algorithm_error(current_skyline_app, parent_pid, algorithm_name, traceback.format_exc())
        if debug_print:
            print('error:', traceback.format_exc())
        if debug_logging:
            current_logger.debug('debug :: error :: laoccfdlpnc - initialize_and_fit_models, err: %s' % err)
            current_logger.debug(traceback.format_exc())
        return_result(anomalous, anomalyScore, results, return_results)

    # Calculate the score matrix
    try:
        started = time()
        score_matrix = calculate_score_matrix(scaled_values, oc_svms, isolation_forest, kpca_svm, gmm, transformed_data)
        timings['calculate_score_matrix'] = time() - started
    except Exception as err:
        record_algorithm_error(current_skyline_app, parent_pid, algorithm_name, traceback.format_exc())
        if debug_print:
            print('error:', traceback.format_exc())
        if debug_logging:
            current_logger.debug('debug :: error :: laoccfdlpnc - calculate_score_matrix, err: %s' % err)
            current_logger.debug(traceback.format_exc())
        return_result(anomalous, anomalyScore, results, return_results)

    # Apply two-sided min-max normalization to scores
    try:
        started = time()
        trimmed_scores = apply_two_sided_minmax(score_matrix, rho=5)
        timings['apply_two_sided_minmax'] = time() - started
    except Exception as err:
        record_algorithm_error(current_skyline_app, parent_pid, algorithm_name, traceback.format_exc())
        if debug_print:
            print('error:', traceback.format_exc())
        if debug_logging:
            current_logger.debug('debug :: error :: laoccfdlpnc - apply_two_sided_minmax, err: %s' % err)
            current_logger.debug(traceback.format_exc())
        return_result(anomalous, anomalyScore, results, return_results)
        
    # Optimize weights with the interior-point method
    try:
        # Assume all data points are normal
        started = time()
        labels = np.ones(len(scaled_values))  
        optimized_weights = optimize_weights_with_interior_point(trimmed_scores, labels)
        timings['optimize_weights_with_interior_point'] = time() - started
    except Exception as err:
        record_algorithm_error(current_skyline_app, parent_pid, algorithm_name, traceback.format_exc())
        if debug_print:
            print('error:', traceback.format_exc())
        if debug_logging:
            current_logger.debug('debug :: error :: laoccfdlpnc - optimize_weights_with_interior_point, err: %s' % err)
            current_logger.debug(traceback.format_exc())
        return_result(anomalous, anomalyScore, results, return_results)

    # Calculate final anomaly scores and make predictions
    try:
        started = time()
        predictions, weighted_scores = calculate_anomaly_scores(trimmed_scores, optimized_weights)
        timings['calculate_anomaly_scores'] = time() - started
    except Exception as err:
        record_algorithm_error(current_skyline_app, parent_pid, algorithm_name, traceback.format_exc())
        if debug_print:
            print('error:', traceback.format_exc())
        if debug_logging:
            current_logger.debug('debug :: error :: laoccfdlpnc - calculate_anomaly_scores, err: %s' % err)
            current_logger.debug(traceback.format_exc())
        return_result(anomalous, anomalyScore, results, return_results)

    # Identify indices and scores of detected anomalies
    anomaly_indices = []
    try:
        anomaly_indices_array = np.where(predictions == -1)[0]
        anomalies_final_scores = weighted_scores[anomaly_indices_array]
        anomaly_indices = list(anomaly_indices_array)
    except Exception as err:
        record_algorithm_error(current_skyline_app, parent_pid, algorithm_name, traceback.format_exc())
        if debug_print:
            print('error:', traceback.format_exc())
        if debug_logging:
            current_logger.debug('debug :: error :: laoccfdlpnc - calculate_anomaly_scores, err: %s' % err)
            current_logger.debug(traceback.format_exc())
        return_result(anomalous, anomalyScore, results, return_results)

#    print("Final Anomaly Indices:", anomaly_indices_final)
#    print("Final Anomaly Scores:", anomalies_final_scores)

    scores = []
    anomalies = {}
    anomalyScore_list = []
    try:
        for index, item in enumerate(timeseries):
            score = 0
            if index in anomaly_indices:
                score = 1
                ts = int(item[0])
                # Coerce NaN to None or float for valid JSON
                value = item[1]
                if str(value) in ['NaN','nan','Nan','none']:
                    value = None
                else:
                    try:
                        value = float(item[1])
                    except:
                        value = None
                anomalies[ts] = {'value': value, 'index': index, 'score': score}
            anomalyScore_list.append(score)
            anomScore = None
            try:
                anomScore = float(weighted_scores[index])
            except:
                anomScore = None
            scores.append(anomScore)

        anomaly_sum = sum(anomalyScore_list[-anomaly_window:])
        if anomaly_sum > 0:
            anomalous = True
        else:
            anomalous = False

        #print("Length of timeseries:", len(timeseries))
        #print("Length of scaled_values:", len(scaled_values))  # Should match timeseries
        #print("Length of transformed_data:", len(transformed_data))  # Should also match
        #print("Length of score_matrix:", len(score_matrix))  # Should also match
        #print("Length of score_matrix:", len(score_matrix))
        #print("Length of optimized_weights:", len(optimized_weights))
        #print("Length of weighted_scores after recalculation:", len(weighted_scores))
        laoccfdlpnc_attributes = None
        try:
            laoccfdlpnc_attributes = {
                'len(timeseries_length)': len(timeseries),
                'len(scaled_values)': len(scaled_values),
                'len(transformed_data)': len(transformed_data),
                'len(score_matrix)': len(score_matrix),
                'len(optimized_weights)': len(optimized_weights),
                'len(weighted_scores)': len(weighted_scores),
            }
        except:
            laoccfdlpnc_attributes = None

        runtime = time() - start
        timings['total'] = runtime

        results = {
            'anomalous': anomalous,
            'anomalies': anomalies,
            'anomalyScore_list': anomalyScore_list,
            'scores': scores,
            'algorithm_parameters': algorithm_parameters,
            'laoccfdlpnc_attributes': laoccfdlpnc_attributes,
            'timings': timings,
        }
        if debug_print:
            print('ran laoccfdlpnc OK in %.6f seconds' % runtime)
        if debug_logging:
            current_logger.debug('debug :: laoccfdlpnc :: attributes: %s' % str(results['laoccfdlpnc_attributes']))
            current_logger.debug('debug :: laoccfdlpnc :: anomalous: %s, timings: %s' % (
                str(anomalous), str(results['timings'])))
            current_logger.debug('debug :: ran laoccfdlpnc OK in %.6f seconds' % runtime)
        if results:
            if results['anomalous']:
                anomalous = True
                anomalyScore = 1.0
            else:
                anomalous = False
                anomalyScore = 0.0
            results['anomalyScore'] = anomalyScore
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
            print('warning - StopIteration called on laoccfdlpnc')
        if debug_logging:
            current_logger.debug('debug :: warning - StopIteration called on laoccfdlpnc')

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
            current_logger.debug('debug :: error - on laoccfdlpnc - %s' % err)
            current_logger.debug(traceback.format_exc())

        # Return None and None as the algorithm could not determine True or False
        if return_results:
            return (None, None, None)
        return (None, None)

    if return_results:
        return (anomalous, anomalyScore, results)

    return (anomalous, anomalyScore)
