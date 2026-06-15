"""
THIS IS A MORE FEATUREFUL CUSTOM ALGORITHM to provide a skeleton to develop your
own custom algorithms.  This algorithm demonstrates the structure of a more
complex custom algorithm that has ``algorithm_parameters`` passed and can also
log if enabled.
It is documented via comments #
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

sys.path.append(os.path.join(os.path.dirname(os.path.realpath(__file__)), os.pardir))
sys.path.insert(0, os.path.dirname(__file__))

from functions.timeseries.downsample import downsample_timeseries

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

# The name of the fucntion MUST be the same as the name declared in
# settings.CUSTOM_ALGORITHMS.
# It MUST have 3 parameters:
# current_skyline_app, timeseries, algorithm_parameters
# See https://earthgecko-skyline.readthedocs.io/en/latest/algorithms/custom-algorithms.html
# for a full explanation about each.
# ALWAYS WRAP YOUR ALGORITHM IN try and except
def skyline_laoccfdlpnc(current_skyline_app, parent_pid, timeseries, algorithm_parameters):
    """
    EXPERIMENTAL

    The skyline_laoccfdlpnc algorithm uses laoccfdlpnc (Locally Adaptive
    One-Class Classifier Fusion with Dynamic ℓp-Norm Constraints) to identify
    anomalies.

    It is convience wrapper to run laoccfdlpnc preloaded via a flux/tornado
    method.  Due to the initiatialisation time of the imports required by
    laoccfdlpnc, laoccfdlpnc suits being preloaded and available to compute the
    results immediately, without having to incur import load times of about 4.5s
    per analysis.  Making it available via a flux/tornado method makes using
    laoccfdlpnc a viable option as a custom_algorithm option in second stage
    Mirage analysis.  It's accuracy warrants it's inclusion, given that it is
    much more accurate than many of the other SOTA custom_algorithms that are
    available for use in Skyline.
    
    Whether it's overall runtimes are suffice for this purpose remains to be
    seen.  Unfortunately it varies in compute time complexity based on the data.
    This algorithm is very complex O(n^3), though it can often perform closer to
    O(n^2) depending on the data and with the use of approximations.  Factors
    such as the time series with high internal complexity, due to variability,
    distribution, etc, significantly influence the runtime.  One-Class SVM and
    KPCA take longer to find an optimal boundary subspace when the data is
    spread across a wide range or has many complex patterns.  So lots of server
    type metrics will perform closer to O(n^3).
    runtimes seem to vary from 1.623307228088379 to 20.396100997924805 seconds
    on a Skyline server running a production workload.

    Unlike some other skyline_ custom_algorithms which failover to running the
    custom_algorithm locally if the flux/tornado request does not respond with
    a result, skyline_laoccfdlpnc ONLY accesses laoccfdlpnc via flux and will
    not run locally if flux does not respond with a result.

    However skyline_laoccfdlpnc does however validate and preprocess the data to
    ensure that it is suitable be run through laoccfdlpnc from a Skyline point
    of view, e.g. it is sufficiently populated, etc.

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
        the laoccfdlpnc custom_algorithm.  These are documented in the
        custom_algorithm code itself so they will not be redocumented here.
        Refer to the algorithm_parameters and example usage in the laoccfdlpnc
        docstrings
        https://earthgecko-skyline.readthedocs.io/en/latest/skyline.custom_algorithms.html#module-custom_algorithms.laoccfdlpnc
    :type current_skyline_app: str
    :type parent_pid: int
    :type timeseries: list
    :type algorithm_parameters: dict
    :return: anomalous, anomalyScore, results
    :rtype: tuple(bool, float, dict)

    """

    # You MUST define the algorithm_name
    algorithm_name = 'skyline_laoccfdlpnc'
    func_name = str(algorithm_name)

    # Define the default state of None and None, anomalous does not default to
    # False as that is not correct, False is only correct if the algorithm
    # determines the data point is not anomalous.  The same is true for the
    # anomalyScore.
    anomalous = None
    anomalyScore = None
    anomalies = {}
    scores = []
    results = {'anomalous': False, 'anomalies': anomalies, 'scores': scores, 'results': {}}

    current_logger = None

    # If you wanted to log, you can but this should only be done during
    # testing and development
    def get_log(current_skyline_app):
        current_skyline_app_logger = current_skyline_app + 'Log'
        current_logger = logging.getLogger(current_skyline_app_logger)
        return current_logger

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
    # laoccfdlpnc algorithm parameters
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
                        current_logger.debug('debug :: %s :: time series does not have sufficient data' % (
                            func_name))
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
            if minimum_datapoints:
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
            if sparsity < minimum_percentage_sparsity:
                if debug_logging:
                    current_logger.debug('debug :: %s :: time series does not have sufficient data, minimum_percentage_sparsity required is %s and time series has %s' % (
                        func_name, str(minimum_percentage_sparsity),
                        str(sparsity)))
                if return_results:
                    return (anomalous, anomalyScore, results)
                return (anomalous, anomalyScore)

        original_timeseries = []
        downsample_data = False
        if current_skyline_app == 'mirage' and len(timeseries) > 1600:
            if metric_resolution < 600:
                downsample_data = True

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

        end_preprocessing = timer()
        preprocessing_runtime = end_preprocessing - start_preprocessing
        if debug_logging:
            current_logger.debug('debug :: %s :: preprocessing took %.6f seconds' % (
                func_name, preprocessing_runtime))

        # flux - tornado
        if tornado_url and tornado_api_key:
            if debug_logging:
                current_logger.debug('debug :: %s :: using tornado laoccfdlpnc' % (
                    func_name))
            # tornado requires the skyline_app and base_name k/v pairs to be
            # passed
            flux_tornado_dict = {
                'skyline_app': str(current_skyline_app),
                'base_name': base_name,
                'key': tornado_api_key,
                'algorithm': 'laoccfdlpnc',
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
                current_logger.debug('debug :: %s :: no tornado_url or tornado_api_key passed, nothing run' % (
                    func_name))

        if return_results:
            # Allow for custom_algorithm results to Mirage and plots
            if downsampled_timeseries and downsample_data:
                results['downsampled_timeseries'] = downsampled_timeseries

        if debug_logging:
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

    if return_results:
        return (anomalous, anomalyScore, results)
    return (anomalous, anomalyScore)
