import logging
import traceback
import os
import time
import copy
from time import time
from ast import literal_eval
import json

from flask import request, jsonify
from werkzeug.utils import secure_filename
import gzip
import requests

import settings
import skyline_version
from skyline_functions import mkdir_p, write_data_to_file, get_graphite_metric
from functions.pandas.csv_to_timeseries import csv_to_timeseries
from functions.metrics.get_base_name_from_labelled_metrics_name import get_base_name_from_labelled_metrics_name
from functions.victoriametrics.get_victoriametrics_metric import get_victoriametrics_metric
from functions.redis.get_metric_timeseries import get_metric_timeseries

skyline_app = 'webapp'
skyline_app_logger = '%sLog' % skyline_app
logger = logging.getLogger(skyline_app_logger)
skyline_app_logfile = '%s/%s.log' % (settings.LOG_PATH, skyline_app)
logfile = '%s/%s.log' % (settings.LOG_PATH, skyline_app)

skyline_version = skyline_version.__absolute_version__

try:
    DATA_UPLOADS_PATH = '%s_webapp_vortex' % settings.DATA_UPLOADS_PATH
except:
    # @modified 20230110 - Task #4778: v4.0.0 - update dependencies
    # DATA_UPLOADS_PATH = '/tmp/skyline/data_uploads_webapp_vortex'
    DATA_UPLOADS_PATH = '%s/data_uploads_webapp_vortex' % settings.SKYLINE_TMP_DIR

try:
    VORTEX_ALGORITHMS = copy.deepcopy(settings.VORTEX_ALGORITHMS)
except Exception as outer_err:
    logger.error('error :: vortex_request :: CRITICAL error failed to load VORTEX_ALGORITHMS from settings - %s' % outer_err)
    VORTEX_ALGORITHMS = {}
ALLOWED_ALGORITHMS = list(VORTEX_ALGORITHMS.keys())

# The keys that are allowed in flux.listen.VortexDataPost
vortex_data = {
    'key': settings.FLUX_SELF_API_KEY,
    'metric_namespace_prefix': None,
    'metric': None,
    'timeout': 60,
    'timeseries': None,
    'reference': 'submitted via webapp Vortex page',
    'consensus': [],
    'no_downsample': False,
    'save_training_data_on_false': True,
    'send_to_ionosphere': False,
    'return_image_urls': True,
    'trigger_anomaly': False,
    'algorithms_test_only': True,
    # @added 20230129
    'override_7_day_limit': False,
    # @added 20230613 - Feature #4948: vortex - adtk algorithms
    # Changed the adtk algorithms to return a results dict
    # like other custom algorithms that vortex can run
    'realtime_analysis': False,
    'return_results': True,
    # @added 20230616 - Feature #4952: vortex - consensus_count
    'check_all_consensuses': False,
    'consensus_count': 0,
}


def get_run_algorithms():
    function_str = 'vortex_request :: get_run_algorithms'
    run_algorithms = {}
    try:
        for key in list(request.form.keys()):
            logger.info('%s :: post key: %s, value: %s' % (
                function_str, str(key), str(request.form[key])))
        for algorithm in list(settings.VORTEX_ALGORITHMS.keys()):
            if algorithm in list(request.form.keys()):
                run_algorithms[algorithm] = {'algorithm_parameters': {}}
                for key in list(request.form.keys()):
                    if key.startswith(algorithm) and key != algorithm:
                        algorithm_str = '%s_' % algorithm
                        algorithm_parameter = key.replace(algorithm_str, '')
                        value = request.form[key]
                        if value.isdigit():
                            value = int(value)
                        elif value.replace('.', '', 1).isdigit() and value.count('.') < 2:
                            value = float(value)

                        # @added 20230613 - Feature #4948: vortex - adtk algorithms
                        # Handle true and false
                        elif value == 'true':
                            value = True
                        elif value == 'false':
                            value = False

                        else:
                            value = str(value)
                        run_algorithms[algorithm]['algorithm_parameters'][algorithm_parameter] = value
    except Exception as err:
        logger.error('error :: %s :: failed to parse form data - %s' % (
            function_str, err))
        raise
    return run_algorithms


def save_local_csv():
    function_str = 'vortex_request :: save_local_csv'
    local_csv_file = None
    csv_file = None
    try:
        csv_file = request.files['csv_file']
    except Exception as err:
        logger.error('error :: %s :: no csv file uploaded - %s' % (
            function_str, err))
        raise
    csv_filename = csv_file.filename
    if not csv_filename.endswith('.csv'):
        logger.error('error :: %s :: only .csv files accepted, rejecting %s' % (
            function_str, csv_filename))
        raise ValueError('error :: only .csv files are accepted')
    try:
        data_filename = secure_filename(csv_filename)
        upload_data_dir = '%s/%s' % (DATA_UPLOADS_PATH, data_filename)
        logger.info('%s :: saving uploaded csv_file - %s' % (
            function_str, str(data_filename)))
        if not os.path.exists(upload_data_dir):
            mkdir_p(upload_data_dir)
        local_data_file = '%s/%s' % (upload_data_dir, data_filename)
        csv_file.save(local_data_file)
        logger.info('%s :: saved data file - %s' % (function_str, local_data_file))
        local_csv_file = str(local_data_file)
    except Exception as err:
        logger.error('error :: %s :: failed to %s - %s' % (
            function_str, csv_filename, err))
        raise
    return local_csv_file


def get_data_source():
    function_str = 'vortex_request :: get_data_source'
    data_source = None
    try:
        data_source = request.form['data_source']
    except Exception as err:
        logger.error('error :: %s :: data_source not passed in POST data - %s' % (
            function_str, err))
        raise
    if data_source not in ['graphite', 'victoriametrics', 'redis', 'training_data', 'csv']:
        raise ValueError('error :: invalid data_source passed - %s' % str(data_source))
    return data_source


def get_consensus():
    function_str = 'vortex_request :: get_consensus'
    consensus = []
    try:
        consensus_str = request.form['consensus']
    except Exception as err:
        logger.error('error :: %s :: consensus not passed in POST data - %s' % (
            function_str, err))
        raise
    try:
        consensus = literal_eval(consensus_str)
    except Exception as err:
        logger.error('error :: %s :: invalid consensus passed in POST data %s - %s' % (
            function_str, consensus_str, err))
        raise
    # Coerce strings into lists
    for item in consensus:
        if isinstance(item, str):
            consensus.remove(item)
            consensus.append([item])
    if not consensus:
        logger.error('error :: %s :: no items passed in consensus' % (
            function_str))
        raise ValueError('error :: no items passed in consensus')
    unknown_algorithms = []
    consensus_algorithms = []
    for item in consensus:
        if isinstance(item, list):
            for i_item in item:
                consensus_algorithms.append(i_item)
                if i_item not in ALLOWED_ALGORITHMS:
                    unknown_algorithms.append(i_item)
        else:
            consensus_algorithms.append(i_item)
            if item not in ALLOWED_ALGORITHMS:
                unknown_algorithms.append(item)
    if unknown_algorithms:
        unknown_algorithms = list(set(unknown_algorithms))
        logger.error('error :: %s :: unknown algorithm/s passed in consensus - %s' % (
            function_str, str(unknown_algorithms)))
        raise ValueError('error :: unknown algorithm/s passed in consensus - %s' % str(unknown_algorithms))
    return consensus


def get_timeseries_from_training_data():
    function_str = 'vortex_request :: get_timeseries_from_training_data'
    timeseries = []
    try:
        training_data_json = request.form['training_data_json']
    except Exception as err:
        logger.error('error :: %s :: training_data_json not passed in POST data - %s' % (
            function_str, err))
        raise
    if not os.path.isfile(training_data_json):
        logger.error('error :: %s :: training_data_json not passed in POST data - %s' % (
            function_str, err))
        raise ValueError('error :: file not found %s' % training_data_json)
    try:
        with open(training_data_json, 'r') as f:
            raw_timeseries = f.read()
            timeseries_array_str = str(raw_timeseries).replace('(', '[').replace(')', ']')
            timeseries = literal_eval(timeseries_array_str)
        logger.info('%s :: loaded time series from - %s' % (function_str, training_data_json))
    except Exception as err:
        logger.error('error :: %s :: failed to load timeseries from training_data_json %s  - %s' % (
            function_str, training_data_json, err))
        raise
    return timeseries


def get_timeseries_from_data_source(data_source, metric, from_timestamp, until_timestamp):
    function_str = 'vortex_request :: save_local_csv'
    timeseries = []
    use_metric = str(metric)
    if metric.startswith('labelled_metrics.'):
        try:
            use_metric = get_base_name_from_labelled_metrics_name(skyline_app, metric)
        except Exception as err:
            logger.error('error :: %s :: get_base_name_from_labelled_metrics_name failed with %s - %s' % (
                function_str, metric, err))
            raise

    if data_source == 'graphite':
        try:
            timeseries = get_graphite_metric(
                skyline_app, metric, from_timestamp,
                until_timestamp, 'list', 'object')
        except Exception as err:
            logger.error('error :: %s :: get_graphite_metric failed with %s - %s' % (
                function_str, metric, err))
            raise
    if data_source == 'victoriametrics':
        try:
            timeseries = get_victoriametrics_metric(
                skyline_app, use_metric, from_timestamp,
                until_timestamp, 'list', 'object')
        except Exception as err:
            logger.error('error :: %s :: get_victoriametrics_metric failed with %s - %s' % (
                function_str, use_metric, err))
            raise
    if data_source == 'redis':
        try:
            timeseries = get_metric_timeseries(skyline_app, use_metric, int(from_timestamp), int(until_timestamp))
        except Exception as err:
            logger.error('error :: %s :: get_metric_timeseries failed with %s - %s' % (
                function_str, use_metric, err))
            raise
    if not timeseries:
        logger.error('error :: %s :: failed to get timeseries for %s' % (
            function_str, use_metric))
        raise ValueError('error :: failed to get timeseries for %s' % use_metric)
    return timeseries


def get_vortex_results(vortex_post_data):
    function_str = 'vortex_request :: get_vortex_results'
    vortex_results = {}
    headers = {'content-encoding': 'gzip', "content-type": "application/json"}
    url = '%s/flux/vortex' % settings.SKYLINE_URL
    logger.info('%s :: sending vortex request to %s' % (
        function_str, url))
    try:
        request_body = gzip.compress(json.dumps(vortex_post_data).encode('utf-8'))
        r = requests.post(url, data=request_body, headers=headers, timeout=60, verify=settings.VERIFY_SSL)
        if r.status_code != 200:
            logger.warning('%s :: got response with status code %s and reason %s' % (
                function_str, str(r.status_code), str(r.reason)))
        else:
            logger.info('%s :: got vortex response' % function_str)
    except Exception as err:
        logger.error('error :: %s :: failed to get response from %s - %s' % (
            function_str, url, err))
        raise
    try:
        vortex_results = r.json()
    except Exception as err:
        logger.error('error :: %s :: failed to get json response from %s - %s' % (
            function_str, url, err))
        raise
    return vortex_results


def vortex_request():
    """
    Process a vortex request.
    """
    function_str = 'vortex_request'
    run_algorithms = {}
    results = None
    metric = None

    if request.method == 'GET':
        return metric, results

    metric_data = {'anomalous': None, 'success': False, 'results': None}
    logger.info('%s :: handling %s request' % (function_str, request.method))
    try:
        metric = request.form['metric']
        metric_data['metric'] = str(metric)
    except Exception as err:
        logger.error('error :: %s :: post data has no metric - %s' % (
            function_str, err))
        raise
    if metric == '':
        raise ValueError('error :: no metric passed')
    try:
        run_algorithms = get_run_algorithms()
        metric_data['algorithms'] = run_algorithms
    except Exception as err:
        logger.error('error :: %s :: get_run_algorithms failed - %s' % (
            function_str, err))
        raise
    if not run_algorithms:
        raise ValueError('error :: %s :: no algorithms passed to run' % function_str)
    logger.info('vortex_request :: run_algorithms: %s' % str(run_algorithms))

    check_all_consensuses = False
    try:
        check_all_consensuses = request.form['check_all_consensuses']
        if check_all_consensuses == 'true':
            metric_data['check_all_consensuses'] = True
    except:
        pass

    consensus = []
    try:
        consensus = get_consensus()
        metric_data['consensus'] = consensus
    except Exception as err:
        logger.error('error :: %s :: get_consensus failed - %s' % (
            function_str, err))
        raise
    if not consensus:
        logger.error('error :: %s :: no consensus determined' % (
            function_str))
        raise ValueError('error :: no consensus determined')

    timeseries = []
    local_csv_file = None
    if request.form['data_source'] == 'csv':
        try:
            local_csv_file = save_local_csv()
        except Exception as err:
            logger.error('error :: %s :: save_local_csv failed - %s' % (
                function_str, err))
            raise
        if local_csv_file:
            try:
                timeseries = csv_to_timeseries(skyline_app, local_csv_file)
            except Exception as err:
                logger.error('error :: %s :: csv_to_timeseries failed with %s - %s' % (
                    function_str, local_csv_file, err))
                raise
        if not timeseries:
            logger.error('error :: %s :: no timeseries' % (
                function_str))
            raise ValueError('error :: no timeseries after processing uploaded csv')

    data_source = None
    try:
        data_source = get_data_source()
    except Exception as err:
        logger.error('error :: %s :: get_data_source() failed - %s' % (
            function_str, err))
        raise
    c_time = int(time()) - 60
    try:
        until_timestamp = int(request.form['until_timestamp'])
    except:
        until_timestamp = c_time
    try:
        from_timestamp = int(request.form['from_timestamp'])
    except:
        from_timestamp = until_timestamp - (86400 * 7)

    if data_source == 'csv':
        data_source = None

    if data_source == 'training_data':
        try:
            timeseries = get_timeseries_from_training_data()
        except Exception as err:
            logger.error('error :: %s :: get_timeseries_from_training_data() failed - %s' % (
                function_str, err))
            raise
        if not timeseries:
            logger.error('error :: %s :: failed to load timeseries from training_data_json' % (
                function_str))
            raise ValueError('error :: failed to load timeseries from training_data_json')

    if data_source in ['graphite', 'victoriametrics', 'redis']:
        try:
            timeseries = get_timeseries_from_data_source(data_source, metric, from_timestamp, until_timestamp)
        except Exception as err:
            logger.error('error :: %s :: get_timeseries_from_data_source(%s) failed - %s' % (
                function_str, data_source, err))
            raise

    if not timeseries:
        logger.error('error :: %s :: no timeseries' % (
            function_str))
        raise ValueError('error :: no timeseries found for %s' % metric)
    logger.info('vortex_request :: got timeseries with length: %s' % str(len(timeseries)))

    metric_data['timeseries'] = list(timeseries)

    for key in [
        'no_downsample', 'send_to_ionosphere', 'trigger_anomaly', 'reference',
        # @added 20230129
        'override_7_day_limit',
        # @added 20230613 - Feature #4948: vortex - adtk algorithms
        # Changed the adtk algorithms to return a results dict
        # like other custom algorithms that vortex can run
        'realtime_analysis', 'return_results', 'check_all_consensuses']:
        try:
            if request.form[key] == 'true':
                metric_data[key] = True
            if request.form[key] == 'false':
                metric_data[key] = False
            if key == 'reference':
                if request.form[key] != 'none':
                    metric_data[key] = request.form[key]
        except:
            pass

    # @added 20230616 - Feature #4952: vortex - consensus_count
    try:
        consensus_count = int(request.form['consensus_count'])
    except:
        consensus_count = 0
    metric_data['consensus_count'] = consensus_count

    vortex_post_data = copy.deepcopy(vortex_data)
    for key in list(metric_data.keys()):
        try:
            vortex_post_data[key] = copy.deepcopy(metric_data[key])
        except KeyError:
            continue

    logger.debug('debug :: %s :: get_vortex_results POSTing - %s' % (
        function_str, str(vortex_post_data)))

    try:
        results = get_vortex_results(vortex_post_data)
    except Exception as err:
        logger.error('error :: %s :: get_vortex_results failed - %s' % (
            function_str, err))
        raise

    successfully_processed = False
    status_code = 0
    if results:
        try:
            status_code = results['status_code']
        except Exception as err:
            logger.error('error :: %s :: failed to determine status_code from results: %s - %s' % (
                function_str, str(results), err))
            status_code = 0

        if status_code == 200:
            successfully_processed = True

    logger.info('vortex_request :: status_code: %s, successfully_processed: %s' % (
        str(status_code), str(successfully_processed)))

    if results and successfully_processed:
        try:
            timestamp = results['metric_timestamp']
            use_metric = results['labelled_metric_name']
            training_data_url = '%s/ionosphere?timestamp=%s&metric=%s&requested_timestamp=%s' % (
                settings.SKYLINE_URL, str(timestamp), use_metric, str(timestamp))
            results['training_data_url'] = training_data_url
        except Exception as err:
            logger.error('error :: %s :: failed to add training_data_url - %s' % (
                function_str, err))
            raise
        image_files = []
        replace_str = '%s/ionosphere_images?image=' % settings.SKYLINE_URL
        try:
            for image_url in results['results']['image_urls']:
                image_file = image_url.replace(replace_str, '')
                image_files.append(image_file)
            results['results']['image_files'] = image_files
        except Exception as err:
            logger.error('error :: %s :: failed to add image_files - %s' % (
                function_str, err))
            raise

        # @added 20230601 - Feature #4734: mirage_vortex
        #                   Branch #4728: vortex
        # Add results to training_data_dir to be loaded in the Ionosphere
        # training page
        training_data_dir = None
        try:
            training_data_dir = results['training_data']
        except Exception as err:
            logger.error('error :: %s :: failed to determine training_data from results dict - %s' % (
                function_str, err))
        logger.info('vortex_request :: training_data_dir: %s' % (
            str(training_data_dir)))

        if training_data_dir:
            try:
                if os.path.exists(training_data_dir):
                    data_file = '%s/vortex.results.dict' % (training_data_dir)
                    with open(data_file, 'w') as fh:
                        fh.write(str(results))
                    results['results_dict_file'] = data_file
                logger.info('vortex_request :: results_dict_file: %s' % (
                    str(data_file)))
            except Exception as err:
                logger.error('error :: %s :: failed to save results.dict.txt to %s - %s' % (
                    function_str, training_data_dir, err))

    if not results:
        results = copy.deepcopy(metric_data)
        results['anomalous'] = None
        results['success'] = False

    return metric, results
