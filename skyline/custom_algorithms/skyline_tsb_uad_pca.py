"""
skyline_tsb_uad_pca.py
"""

# REQUIRED Skyline imports.  All custom algorithms MUST have the following two
# imports.  These are required for exception handling and to record algorithm
# errors regardless of debug_logging setting for the custom_algorithm
import traceback
from timeit import default_timer as timer
import logging
import sys

from custom_algorithms import record_algorithm_error


# Import ALL modules that the custom algorithm requires.  Remember that if a
# requirement is not one that is provided by the Skyline requirements.txt you
# must ensure it is installed in the Skyline virtualenv
import copy
import os
import numpy as np

from contextlib import nullcontext
from sklearn.preprocessing import MinMaxScaler

sys.path.append(os.path.join(os.path.dirname(os.path.realpath(__file__)), os.pardir))
sys.path.insert(0, os.path.dirname(__file__))

from functions.timeseries.downsample import downsample_timeseries
from custom_algorithm_sources.TSB_UAD.pca import PCA as tsb_uad_pca


FLUX_TORNADO_ENABLED = False
FLUX_TORNADO_URL = None
FLUX_SELF_API_KEY = None
try:
    from settings import FLUX_TORNADO_ENABLED
except Exception as outerr:
    print('err: %s' % outerr)
    FLUX_TORNADO_ENABLED = False

if FLUX_TORNADO_ENABLED:
    import json
    import requests
    try:
        from settings import FLUX_TORNADO_URL
    except:
        FLUX_TORNADO_URL = None
    try:
        from settings import FLUX_SELF_API_KEY
    except:
        FLUX_SELF_API_KEY = None

# @added 20251018 - Feature #5655: custom_algorithm - skyline_tsb_uad_pca
def skyline_tsb_uad_pca(current_skyline_app, parent_pid, timeseries, algorithm_parameters):
    """

    The skyline_tsb_uad_pca algorithm uses a version of PCA that is based on a
    version implemented in the TSB-UAD library.

    It is convience wrapper to run TSB_UAD.models.pca
    This algorithm has not been added as a tornado implementation, however if it
    is to be used in mirage as a general algorithm it should only be used via a
    tornado method as the algorithm load time and initialisation takes 2.3 seconds
    which is much too high for each mirage process to load every run.  Once loaded
    and initialised the algorithm has a very decent runtime of around
    0.019312143325805664 seconds.

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
    :param algorithm_parameters: a dictionary of the required parameters for
        the skyline_tsb_uad_pca custom_algorithm.  These are documented in the
        custom_algorithm code itself so they will not be redocumented here.
        Refer to the algorithm_parameters and example usage in the skyline_tsb_uad_pca
        docstrings
        https://earthgecko-skyline.readthedocs.io/en/latest/skyline.custom_algorithms.html#module-custom_algorithms.skyline_tsb_uad_pca

        - ``'base_name'`` (str): The metric base_name.
        - ``'anomaly_window'`` (int): The anomaly_window value.
            This specifies how many of the last data points should be considered
            when determining if the metric is anomalous. Only the last
            ``anomaly_window`` data points in the time series will be used to
            determine if the metric is anomalous.  Default is ``1``.
        - ``length_check`` (bool): Optional.
            If ``True``, does not check to determine if the length of the
            timeseries if sufficient to run the algorithm against.  This check
            is required in the real time analysis context, however in the adhoc
            analysis or testing context it is not required and results is not
            analysing some test samples due to them be less than the required
            amount of data points. Default is ``False``.
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
                'length_check': True,
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
    algorithm_name = 'skyline_tsb_uad_pca'
    func_name = str(algorithm_name)

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
        'anomalyScore_list': anomalyScore_list, 'scores': scores, 'results': {}
    }
    pca_scores = []
    pca_anomalies = []

    current_logger = None

    # If you wanted to log, you can but this should only be done during
    # testing and development
    def get_log(current_skyline_app):
        current_skyline_app_logger = current_skyline_app + 'Log'
        current_logger = logging.getLogger(current_skyline_app_logger)
        return current_logger

    # @added 20260514 - Feature #5655: custom_algorithm - skyline_tsb_uad_pca
    def is_constant(np_values):
        """
        Determine if the series has effectively zero variance.

        :param np_values: the array of values
        :type np_values: np.array

        """
        return np.nanstd(np_values) < 1e-10

    # @added 20260514 - Bug #5745: tsb_uad - statsmodels and sklearn divide warnings
    #                   Feature #5655: custom_algorithm - skyline_tsb_uad_pca
    def fill_nans(np_values):
        """
        Linear interpolation for interior NaNs, forward/back fill for edge NaNs.
        This is equivalent to pd.Series(x).interpolate('linear').ffill().bfill()
        but pure numpy without having the overhead of importing pandas

        :param np_values: the array of values
        :type np_values: np.array
        :return: np_values_nan_filled
        :rtype: np.array

        """
        x = np_values.copy().astype(float)
        nans = np.isnan(x)
        if not nans.any():
            return x
        # If nans are present interpolate using non-NaN values
        indices = np.arange(len(x))
        x[nans] = np.interp(indices[nans], indices[~nans], x[~nans])
        return x

    start = timer()

    # Use the algorithm_parameters to determine the sample_period
    debug_logging = None
    try:
        debug_logging = algorithm_parameters['debug_logging']
    except:
        debug_logging = False

    debug_logging = True

    # Determine if a context is passed with algorithm_parameters
    context = None
    try:
        context = algorithm_parameters['context']
    except KeyError:
        context = None
    if context:
        func_name = '%s :: %s' % (context, algorithm_name)
    # TSB_UAD algorithm parameters

    anomaly_window = 1
    try:
        anomaly_window = int(algorithm_parameters['anomaly_window'])
    except:
        anomaly_window = 1

    return_results = False
    try:
        return_results = algorithm_parameters['return_results']
    except:
        return_results = False

    # tornado and some custom_algorithms requires a skyline_app and base_name
    # parameters to be passed
    algorithm_parameters['skyline_app'] = str(current_skyline_app)
    base_name = 'none'
    try:
        base_name = algorithm_parameters['base_name']
    except:
        try:
            base_name = algorithm_parameters['metric']
        except:
            try:
                 base_name = algorithm_parameters['labelled_metric_name']
            except:
                base_name = 'none'
    algorithm_parameters['base_name'] = base_name

    # @added 20260528 - Feature #4734: mirage_vortex
    # Allow the downsample parameter
    downsample = True
    try:
        downsample = algorithm_parameters['downsample']
    except:
        downsample = True

    # @added 20260612 - Feature #5751: custom_algorithm - skyline_fast
    # Allow to disable length_check for skyline_fast testing and general
    # testing of adhoc timeseries data.  The minimum_datapoints length_check
    # is required for realtime analysis
    length_check = True
    try:
        length_check = algorithm_parameters['length_check']
    except:
        length_check = True

    if debug_logging:
        try:
            current_logger = get_log(current_skyline_app)
            current_logger.debug('debug :: %s :: debug_logging enabled with algorithm_parameters - %s' % (
                func_name, str(algorithm_parameters)))
            current_logger.debug('debug :: %s :: FLUX_TORNADO_ENABLED: %s' % (
                func_name, str(FLUX_TORNADO_ENABLED)))

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
                return (None, None, results)
            return (None, None)

    if not FLUX_TORNADO_ENABLED:
        if debug_logging:
            current_logger.debug('debug :: %s :: cannot run because FLUX_TORNADO_ENABLED: %s' % (
                func_name, str(FLUX_TORNADO_ENABLED)))
        if return_results:
            return (None, None, results)
        return (None, None)

    # Use the algorithm_parameters to determine if there are check_details added
    # by snab
    check_details = {}
    try:
        check_details = algorithm_parameters['check_details']
        if debug_logging:
            current_logger.debug('debug :: %s :: snab check_details - %s' % (
                func_name, str(check_details)))
    except KeyError:
        check_details = {}
    except Exception as err:
        traceback_msg = traceback.format_exc()
        if current_skyline_app == 'snab':
            if not traceback_msg:
                traceback_msg = 'None'
            current_logger.error(traceback_msg)
            if debug_logging:
                current_logger.error('error :: debug :: could not determine check_details from algorithm_parameters - %s - %s' % (
                    str(algorithm_parameters), err))
                current_logger.debug('debug :: %s :: snab check_details - %s' % (
                    func_name, str(check_details)))
            record_algorithm_error(current_skyline_app, parent_pid, algorithm_name, traceback_msg)
            # Return None and None as the algorithm could not determine True or False
            if return_results:
                return (None, None, results)
            return (None, None)
        else:
            pass

    # flux - tornado
    flux_tornado_dict = {}
    tornado_url = None
    try:
        tornado_url = algorithm_parameters['tornado_url']
    except:
        tornado_url = None
    tornado_api_key = None

    # @added 20260514 - Bug #5745: tsb_uad - statsmodels and sklearn divide warnings
    #                   Feature #5655: custom_algorithm - skyline_tsb_uad_pca
    # There is no tornado implementation
    if tornado_url:
        tornado_url = None
        if debug_logging:
            current_logger.warning('%s :: tornado_url passed in algorithm_parameters and there is no tornado implementation' % func_name)

    if tornado_url:
        try:
            tornado_api_key = algorithm_parameters['tornado_api_key']
        except:
            tornado_api_key = None
    if tornado_url and tornado_api_key:
        headers = {
            'Accept': 'application/json',
            'Content-Type': 'application/json',
        }
        if debug_logging:
            current_logger.info('%s :: using tornado' % func_name)

    # Allow the windows parameter to be passed in the
    # check_details for snab as well
    windows = None
    if check_details:
        try:
            windows = check_details['windows']
            if debug_logging:
                current_logger.debug('debug :: %s :: windows - %s - determined from check_details' % (
                    func_name, str(windows)))
        except:
            windows = None

    metric_resolution = 0

    # ALWAYS WRAP YOUR ALGORITHM IN try and the BELOW except
    try:

        start_preprocessing = timer()

        # INFO: Sorting time series of 10079 data points took 0.002215 seconds
        timeseries = sorted(timeseries, key=lambda x: x[0])
        if debug_logging:
            current_logger.debug('debug :: %s :: time series of length - %s' % (
                func_name, str(len(timeseries))))

        # Testing the data to ensure it meets minimum requirements, in the case
        # of Skyline's use of the matrixprofile algorithm this means that:
        # - the time series must have at least 75% of its full_duration
        # - the time series must have at least 99% of the data points for the
        #   in the sample being analysed.
        do_not_use_sparse_data = True

        # @added 20260528 - Feature #4734: mirage_vortex
        # Allow the downsample parameter
        if not downsample:
            do_not_use_sparse_data = False

        if do_not_use_sparse_data:
            # Default for analyzer at required period to 18 hours
            period_required = int(86400 * 0.75)
            total_period = 0
            total_datapoints = 0
            try:
                start_timestamp = int(timeseries[0][0])
                end_timestamp = int(timeseries[-1][0])
                total_period = end_timestamp - start_timestamp
                current_logger.debug('debug :: %s :: start_timestamp: %s, end_timestamp: %s, total_period: %s' % (
                    func_name, str(start_timestamp), str(end_timestamp),
                    str(total_period)))
                total_datapoints = len(timeseries)
            except:
                traceback_msg = traceback.format_exc()
                if debug_logging:
                    current_logger.error(traceback_msg)
                    current_logger.error('error :: debug_logging :: %s :: falied to determine total_period and total_datapoints' % (
                        func_name))
                record_algorithm_error(current_skyline_app, parent_pid, algorithm_name, traceback_msg)
                timeseries = []
            if not timeseries:
                if return_results:
                    return (anomalous, anomalyScore, results)
                return (anomalous, anomalyScore)

            if current_skyline_app == 'snab':
                try:
                    full_duration = check_details['full_duration']
                    period_required = int(full_duration * 0.75)
                except:
                    traceback_msg = traceback.format_exc()
                    if debug_logging:
                        current_logger.error(traceback_msg)
                        current_logger.error('error :: debug_logging :: %s :: falied to determine total_period and total_datapoints' % (
                            func_name))
                    record_algorithm_error(current_skyline_app, parent_pid, algorithm_name, traceback_msg)
                    if return_results:
                        return (anomalous, anomalyScore, results)
                    return (anomalous, anomalyScore)

            # If the time series does not have 75% of its full_duration it does not
            # have sufficient data to sample
            try:
                if total_period < period_required:
                    if debug_logging:
                        current_logger.debug('debug :: %s :: time series does not have sufficient data, total_period: %s < period_required: %s' % (
                            func_name, str(total_period), str(period_required)))
                    if return_results:
                        return (anomalous, anomalyScore, results)
                    return (anomalous, anomalyScore)
            except:
                traceback_msg = traceback.format_exc()
                if debug_logging:
                    current_logger.error(traceback_msg)
                    current_logger.error('error :: debug_logging :: %s :: falied to determine if time series has sufficient data' % (
                        func_name))
                record_algorithm_error(current_skyline_app, parent_pid, algorithm_name, traceback_msg)
                if return_results:
                    return (anomalous, anomalyScore, results)
                return (anomalous, anomalyScore)

            # If the time series does not have 75% of its full_duration data points
            # it does not have sufficient data to sample

            # Determine resolution
            metric_resolution = 0
            try:
                np_timestamps_array = np.array([int(t) for t, _ in timeseries])
                ts_diffs = np.diff(np_timestamps_array)
                resolution_counts = np.unique(ts_diffs, return_counts=True)
                metric_resolution = resolution_counts[0][np.argmax(resolution_counts[1])]
            except Exception as err:
                if debug_logging:
                    current_logger.error('error :: %s :: failed to determine metric_resolution, err: %s' % (
                        func_name, err))

            minimum_datapoints = None
            if metric_resolution:
                minimum_datapoints = int(period_required / metric_resolution)
            # @modified 20260612 - Feature #5751: custom_algorithm - skyline_fast
            # if minimum_datapoints:
            if minimum_datapoints and length_check:
                if total_datapoints < minimum_datapoints:
                    if debug_logging:
                        current_logger.debug('debug :: %s :: time series does not have sufficient data, minimum_datapoints required is %s and time series has %s' % (
                            func_name, str(minimum_datapoints),
                            str(total_datapoints)))
                    if return_results:
                        return (anomalous, anomalyScore, results)
                    return (anomalous, anomalyScore)

            # Is the time series fully populated?
            # full_duration_datapoints = int(full_duration / metric_resolution)
            total_period_datapoints = int(total_period / metric_resolution)
            minimum_percentage_sparsity = 95
            sparsity = int(total_datapoints / (total_period_datapoints / 100))
            # @modified 20260612 - Feature #5751: custom_algorithm - skyline_fast
            #if sparsity < minimum_percentage_sparsity:
            if sparsity < minimum_percentage_sparsity and length_check:
                if debug_logging:
                    current_logger.debug('debug :: %s :: time series does not have sufficient data, minimum_percentage_sparsity required is %s and time series has %s' % (
                        func_name, str(minimum_percentage_sparsity),
                        str(sparsity)))
                if return_results:
                    return (anomalous, anomalyScore, results)
                return (anomalous, anomalyScore)

        # Determine resolution
        if not metric_resolution:
            try:
                np_timestamps_array = np.array([int(t) for t, _ in timeseries])
                ts_diffs = np.diff(np_timestamps_array)
                resolution_counts = np.unique(ts_diffs, return_counts=True)
                metric_resolution = resolution_counts[0][np.argmax(resolution_counts[1])]
            except Exception as err:
                if debug_logging:
                    current_logger.error('error :: %s :: failed to determine metric_resolution, err: %s' % (
                        func_name, err))

        original_timeseries = []
        downsample_data = False
        if current_skyline_app == 'mirage' and len(timeseries) > 1600:
            if metric_resolution < 600:
                downsample_data = True

        # @added 20260528 - Feature #4734: mirage_vortex
        # Allow the downsample parameter
        if not downsample:
            downsample_data = False

        downsampled_timeseries = []
        if downsample_data:
            original_timeseries = list(timeseries)
            try:
                downsampled_timeseries = downsample_timeseries(current_skyline_app, timeseries, metric_resolution, 600, 'mean', 'end', bfill=True)
                timeseries = list(downsampled_timeseries)
                if debug_logging:
                    current_logger.debug('debug :: %s :: downsampled timeseries from resolution %s with %s datapoints to resolution 600 with %s datapoints' % (
                        func_name, str(metric_resolution), str(len(original_timeseries)),
                        str(len(timeseries))))
            except Exception as err:
                traceback_msg = traceback.format_exc()
                record_algorithm_error(current_skyline_app, parent_pid, algorithm_name, traceback_msg)
                if debug_logging:
                    current_logger.error(traceback_msg)
                    current_logger.error('error :: debug_logging :: %s :: downsample_timeseries failed - %s' % (
                        func_name, err))


        # @added 20260514 - Bug #5745: tsb_uad - statsmodels and sklearn divide warnings
        #                   Feature #5655: custom_algorithm - skyline_tsb_uad_pca
        # Handle near zero variance and constant time series
        np_values = np.array([value for _, value in timeseries], dtype=float)
        # Skip if too many nans
        if np.isnan(np_values).mean() > 0.1:
            if debug_logging:
                current_logger.debug('debug :: %s :: time series has too many nans, skipping' % (
                    func_name))
            if return_results:
                return (anomalous, anomalyScore, results)
            return (anomalous, anomalyScore)
        # Replace nans
        np_values = fill_nans(np_values)
        # If the series is constant PCA and statsmodels/tsa/stattools.acf will
        # encounter a divide by zero
        if np.std(np_values) < 1e-10:
            anomalous = False
            results['anomalous'] = anomalous
            results['constant_series'] = True
            if debug_logging:
                current_logger.debug('debug :: %s :: time series is constant, skipping' % (
                    func_name))
            if return_results:
                return (anomalous, anomalyScore, results)
            return (anomalous, anomalyScore)

        end_preprocessing = timer()
        preprocessing_runtime = end_preprocessing - start_preprocessing
        if debug_logging:
            current_logger.debug('debug :: %s :: preprocessing took %.6f seconds' % (
                func_name, preprocessing_runtime))

        # flux - tornado
        if tornado_url and tornado_api_key:
            if debug_logging:
                current_logger.debug('debug :: %s :: using tornado tsb_uad_pca' % (
                    func_name))
            # tornado requires the skyline_app and base_name k/v pairs to be
            # passed
            flux_tornado_dict = {
                'skyline_app': str(current_skyline_app),
                'base_name': base_name,
                'key': tornado_api_key,
                'algorithm': 'tsb_uad_pca',
                'timeseries': timeseries,
                'algorithm_parameters': algorithm_parameters,
            }
            start_compute = timer()
            r = None
            try:
                connect_timeout = 5
                read_timeout = 300
                use_timeout = (int(connect_timeout), int(read_timeout))
                r = requests.post(
                        tornado_url, data=json.dumps(flux_tornado_dict),
                        timeout=use_timeout, headers=headers)
            except Exception as err:
                traceback_msg = traceback.format_exc()
                current_logger.error(traceback_msg)
                current_logger.error('error :: %s :: failed to get response from %s, err: %s' % (
                    func_name, str(tornado_url), err))
            response = None
            # These get overridden they only hold the last request and bad
            # response
            debug_request_file = '/tmp/skyline/%s.tornado.request.debug.txt' % func_name  # nosec B108
            debug_response_file = '/tmp/skyline/%s.tornado.response.debug.txt' % func_name  # nosec B108

            if not r:
                with open(debug_request_file, 'w') as file:
                    file.write(str(flux_tornado_dict))
                current_logger.debug('debug :: %s :: no r created debug_request_file: %s' % (
                    func_name, debug_request_file))

            if r:
                try:
                    response = r.json()
                except Exception as err:
                    traceback_msg = traceback.format_exc()
                    current_logger.error(traceback_msg)
                    current_logger.error('error :: %s :: failed to get json from the response from %s, err: %s' % (
                        func_name, str(tornado_url), err))
                    with open(debug_request_file, 'w') as file:
                        file.write(str(flux_tornado_dict))
                    with open(debug_response_file, 'wb') as file:
                        file.write(r.content)
                    current_logger.debug('debug :: %s :: created debug_request_file: %s, debug_response_file: %s' % (
                        func_name, debug_request_file, debug_response_file))
            if response:
                try:
                    if 'data' in response.keys():
                        anomalous = response['data']['anomalous']
                        anomalyScore = response['data']['anomalyScore']
                        if 'results' in response['data']:
                            results = copy.deepcopy(response['data']['results'])
                except Exception as err:
                    traceback_msg = traceback.format_exc()
                    current_logger.error(traceback_msg)
                    current_logger.error('error :: %s :: failed to determine details from [\'data\'] from the json from %s, err: %s' % (
                        func_name, str(tornado_url), err))
                    with open(debug_request_file, 'w') as file:
                        file.write(str(flux_tornado_dict))
                    with open(debug_response_file, 'wb') as file:
                        file.write(r.content)
                    current_logger.debug('debug :: %s :: created debug_request_file: %s, debug_response_file: %s' % (
                        func_name, debug_request_file, debug_response_file))
            end_compute = timer()
            compute_runtime = end_compute - start_compute
            if debug_logging:
                current_logger.debug('debug :: %s :: tornado laoccfdlpnc took %.6f seconds' % (
                    func_name, compute_runtime))

            if debug_logging:
                current_logger.debug('debug :: %s :: anomalous - %s, anomalyScore - %s' % (
                    func_name, str(anomalous), str(anomalyScore)))
        else:
            if debug_logging:
                current_logger.debug('debug :: %s :: no tornado_url or tornado_api_key passed, not running via tornado' % (
                    func_name))

        if not tornado_url and not tornado_api_key:
            try:

                clf = tsb_uad_pca()
                # @modified 20260514 - Bug #5745: tsb_uad - statsmodels and sklearn divide warnings
                #                      Feature #5655: custom_algorithm - skyline_tsb_uad_pca
                #x = np.array([value for _, value in timeseries])
                clf.fit(np_values)
                score = clf.decision_scores_

                # @added 20260514 - Bug #5745: tsb_uad - statsmodels and sklearn divide warnings
                #                   Feature #5655: custom_algorithm - skyline_tsb_uad_pca
                # Handle if the score array is all nans before MinMaxScaler
                # to mitigate All-NaN slice encountered
                # RuntimeWarning: All-NaN slice encountered
                # on return xp.asarray(numpy.nanmax(X, axis=axis))
                if np.all(np.isnan(score)):
                    if debug_logging:
                        current_logger.debug('debug :: %s :: decision_scores_ are all nans, skipping' % (
                            func_name))
                    if return_results:
                        return (anomalous, anomalyScore, results)
                    return (anomalous, anomalyScore)

                score = MinMaxScaler(feature_range=(0,1)).fit_transform(score.reshape(-1,1)).ravel()

                # Using 6-sigma as the threshold unless > 1 then use the 95th
                # percentile
                threshold = np.mean(score) + (6 * np.std(score))
                if threshold > 1:
                    threshold = 0.95
                if debug_logging:
                    current_logger.info('%s :: using threshold: %s' % (
                        func_name, str(threshold)))

                #pca_scores = score.tolist()
                # Coerce NaN
                pca_scores = []
                for value in score.tolist():
                    if str(value) in ['NaN','nan','Nan']:
                        value = 0
                    else:
                        try:
                            value = float(value)
                        except:
                            value = 0
                    pca_scores.append(value)

                for index, item in enumerate(timeseries):
                    if pca_scores[index] > threshold:
                        anomalies[item[0]] = {'index': index, 'value': item[1], 'score': 1, 'anomalyScore': pca_scores[index]}
                        pca_anomalies.append(1)
                    else:
                        pca_anomalies.append(0)
                results = {'anomalies': anomalies}
                if debug_logging:
                    current_logger.info('%s :: len(anomalies): %s' % (
                        func_name, str(len(anomalies))))

                anomaly_sum = sum(pca_anomalies[-anomaly_window:])
                if debug_logging:
                    current_logger.info('%s :: anomalies_in_window: %s' % (
                        func_name, str(anomaly_sum)))

                if not anomalous:
                    if anomaly_sum > 0:
                        anomalous = True
                        anomalyScore = 1.0
                    else:
                        anomalous = False
                        anomalyScore = 0.0
            except Exception as err:
                traceback_msg = traceback.format_exc()
                record_algorithm_error(current_skyline_app, parent_pid, algorithm_name, traceback_msg)
                if debug_logging:
                    current_logger.error(traceback_msg)
                    current_logger.error('error :: debug_logging :: %s :: tsb_uad_pca processing failed, err: %s' % (
                        func_name, err))

        if return_results:
            results = {
                'anomalous': anomalous,
                'anomalies': anomalies,
                'anomalyScore_list': pca_anomalies,
                'scores': pca_scores,
            }
            # Allow for custom_algorithm results to Mirage and plots
            if downsampled_timeseries and downsample_data:
                results['downsampled_timeseries'] = downsampled_timeseries

        if debug_logging:
            current_logger.info('%s :: anomalous: %s' % (
                func_name, str(anomalous)))
            end = timer()
            processing_runtime = end - start
            current_logger.debug('debug :: %s :: completed analysis in %.6f seconds' % (
                func_name, processing_runtime))

        try:
            del timeseries
        except:
            pass
        if return_results:
            return (anomalous, anomalyScore, results)
        return (anomalous, anomalyScore)
    except StopIteration:
        # This except pattern MUST be used in ALL custom algortihms to
        # facilitate the traceback from any errors.  The algorithm we want to
        # run super fast and without spamming the log with lots of errors.
        # But we do not want the function returning and not reporting
        # anything to the log, so the pythonic except is used to "sample" any
        # algorithm errors to a tmp file and report once per run rather than
        # spewing tons of errors into the log e.g. analyzer.log
        if return_results:
            return (anomalous, anomalyScore, results)
        return (None, None)
    except Exception as err:
        traceback_msg = traceback.format_exc()
        if debug_logging:
            try:
                current_logger.error(traceback_msg)
                current_logger.error('error :: debug_logging :: %s :: failed - %s' % (
                    func_name, err))
            except:
                pass
        record_algorithm_error(current_skyline_app, parent_pid, algorithm_name, traceback_msg)
        # Return None and None as the algorithm could not determine True or False
        if return_results:
            return (anomalous, anomalyScore, results)
        return (None, None)
