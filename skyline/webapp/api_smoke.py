"""
api_smoke.py
"""
import copy
import os
import datetime
import logging
from time import time, strftime, gmtime, localtime
from ast import literal_eval
from flask import request

import settings
from backend import get_cluster_data
from skyline_functions import get_redis_conn_decoded, mirage_load_metric_vars
from functions.database.queries.query_anomalies import get_anomalies_for_period
from functions.metrics.get_metric_id_from_base_name import get_metric_id_from_base_name
from functions.metrics.get_base_name_from_labelled_metrics_name import get_base_name_from_labelled_metrics_name
from functions.metrics.get_base_name_from_metric_id import get_base_name_from_metric_id
# @added 20240212 - Feature #5270: webapp - panorama smoke
# Allow namespaces to be passed
from functions.metrics.get_metric_ids_and_base_names import get_metric_ids_and_base_names
from matched_or_regexed_in_list import matched_or_regexed_in_list

from functions.metrics.get_authoritative_node import get_authoritative_node

# @added 20231225 - Feature #5270: webapp - panorama smoke
# @modified 20240212 - Feature #5270: webapp - panorama smoke
# Added namespaces
def get_api_smoke(
        current_skyline_app, from_timestamp=None, cluster_data=False,
        max_items=50, namespaces=[],
        # @added 20250329 - Feature #5611: custom_algorithm_only
        # Allow smoke to include training for do_not_train instances and added
        # until_timestamp
        do_not_train=False, until_timestamp=None,
        # @added 20260228
        llm_output=False):
    """
    Return the training_data_dict to populate the smoke table.

    :param current_skyline_app: the app calling the function
    :param from_timestamp: the from_timestamp to return entries from
    :param until_timestamp: the until_timestamp to return entries from
    :param cluster_data: the cluster_data parameter from the request
    :param max_items: the maximum number of items to return
    :param namespaces: a list of namespaces to match
    :param do_not_train: a boolean whether to include do_not_train instances in
        the returned training data
    :type current_skyline_app: str
    :type from_timestamp: int
    :type until_timestamp: int
    :type cluster_data: bool
    :type max_items: int
    :type namespaces: list
    :type do_not_train: bool

    :return: training_data_dict
    :rtype: dict

    """
    function_str = 'api_smoke'
    training_data_dict = {}

    current_skyline_app_logger = current_skyline_app + 'Log'
    current_logger = logging.getLogger(current_skyline_app_logger)

    this_host = str(os.uname()[1])

    try:
        redis_conn_decoded = get_redis_conn_decoded(current_skyline_app)
    except Exception as err:
        current_logger.error('error :: %s :: get_redis_conn_decoded failed - %s' % (
            function_str, err))
        raise

    current_logger.info('%s :: from_timestamp: %s, until_timestamp: %s, cluster_data: %s, max_items: %s, namespaces: %s' % (
        function_str, str(from_timestamp), str(until_timestamp),
        str(cluster_data), str(max_items), str(namespaces)))

    if not from_timestamp:
        from_timestamp = int(time()) - 86400

    # @added 20240212 - Feature #5270: webapp - panorama smoke
    # Allow namespaces to be passed
    metric_ids_with_base_names = {}
    base_names_with_metric_ids = {}
    metric_ids_to_lookup_errors = []
    pattern_match_errors = []
    skip_metrics = []
    process_metrics = []
    try:
        metric_ids_with_base_names = get_metric_ids_and_base_names(current_skyline_app)
        for mid, base_name in metric_ids_with_base_names.items():
            base_names_with_metric_ids[base_name] = mid
    except Exception as err:
        current_logger.error('error :: %s :: get_metric_ids_and_base_names failed, err: %s' % (
            function_str, err))

    all_db_metric_id_strs_with_names = {}
    try:
        all_db_metric_id_strs_with_names = redis_conn_decoded.hgetall('metrics_manager.all_db_metric_ids_with_names')
    except Exception as err:
        current_logger.error('error :: api_smoke failed to get metrics_manager.all_db_metric_ids_with_names, err: %s' % err)
        raise
    if all_db_metric_id_strs_with_names:
        for id_str, base_name in all_db_metric_id_strs_with_names.items():
            metric_id = int(id_str)
            base_names_with_metric_ids[base_name] = metric_id
            metric_ids_with_base_names[metric_id] = base_name

    # @added 20240227 - Feature #5270: webapp - panorama smoke
    # Label ionosphere alerts
    ionosphere_unique_metrics = []
    try:
        ionosphere_unique_metrics = redis_conn_decoded.smembers('ionosphere.unique_metrics')
    except Exception as err:
        current_logger.error('error :: api_smoke failed to get ionosphere.unique_metrics - %s' % err)
        raise

    # @added 20250329 - Feature #5611: custom_algorithm_only
    # Allow smoke to include training for do_not_train instances
    redis_ionosphere_training_data_set = 'ionosphere.training_data'
    if do_not_train:
        redis_ionosphere_training_data_set = 'ionosphere.training_data_with_do_not_train'

    training_data_raw = []
    try:
        # @modified 20250329 - Feature #5611: custom_algorithm_only
        # Allow smoke to include training for do_not_train instances
        #training_data_raw = list(redis_conn_decoded.smembers('ionosphere.training_data'))
        training_data_raw = list(redis_conn_decoded.smembers(redis_ionosphere_training_data_set))
    except Exception as err:
        #current_logger.error('error :: api_smoke failed to get ionosphere.training_data - %s' % err)
        current_logger.error('error :: api_smoke failed to get %s, err: %s' % (
            redis_ionosphere_training_data_set, err))
        raise
    #current_logger.info('api_smoke got %s entries from ionosphere.training_data' % str(len(training_data_raw)))
    current_logger.info('api_smoke got %s entries from %s' % (
        str(len(training_data_raw)), redis_ionosphere_training_data_set))
    training_data = []
    for training_data_str in training_data_raw:
        try:
            training_data_item = literal_eval(training_data_str)

            # @added 20240212 - Feature #5270: webapp - panorama smoke
            # Allow namespaces to be passed
            if namespaces:
                metric = None
                try:
                    metric = training_data_item[0]
                except Exception as err:
                    metric_ids_to_lookup_errors.append(['error metric_name not found in training_data_item', str(training_data_item), err])
                if not metric:
                    continue
                use_base_name = str(metric)
                if metric.startswith('labelled_metrics.'):
                    try:
                        metric_id_str = metric.replace('labelled_metrics.', '', 1)
                        metric_id = int(metric_id_str)
                        use_base_name = metric_ids_with_base_names[metric_id]
                    except Exception as err:
                        metric_ids_to_lookup_errors.append(['error metric_name lookup', str(training_data_item), err])
                if use_base_name in set(skip_metrics):
                    continue
                if use_base_name not in set(process_metrics):
                    pattern_match = None
                    metric_matched_by = None
                    try:
                        pattern_match, metric_matched_by = matched_or_regexed_in_list(current_skyline_app, use_base_name, namespaces)
                    except Exception as err:
                        pattern_match_errors.append(['error matched_or_regexed_in_list', str(use_base_name), err])
                    if pattern_match:
                        process_metrics.append(use_base_name)
                    else:
                        skip_metrics.append(use_base_name)
                        continue

            # if int(training_data_item[1]) < from_timestamp:
            #    continue
            training_data.append(training_data_item)

        except Exception as err:
            current_logger.error('error :: api_smoke failed to get ionosphere.training_data - %s' % err)
            raise

    training_data = sorted(training_data, key=lambda x: x[1], reverse=True)
    current_logger.info('api_smoke filtered %s entries from ionosphere.training_data' % str(len(training_data)))
    # current_logger.info('api_smoke training_data: %s' % str(training_data))

    # Remove ionosphere.untrainable_metrics from the training data
    ionosphere_untrainable_metrics_list = []
    ionosphere_untrainable_metrics = []
    ionosphere_untrainable_metrics_redis_set = 'ionosphere.untrainable_metrics'
    try:
        ionosphere_untrainable_metrics_list = list(redis_conn_decoded.smembers(ionosphere_untrainable_metrics_redis_set))
    except Exception as err:
        current_logger.error('error :: could not get the ionosphere.untrainable_metrics set from Redis, err: %s' % err)
        ionosphere_untrainable_metrics_list = []

    if ionosphere_untrainable_metrics_list:
        for ionosphere_untrainable_metric_str in ionosphere_untrainable_metrics_list:
            try:
                ionosphere_untrainable_metric = literal_eval(ionosphere_untrainable_metric_str)
                ium_metric_name = ionosphere_untrainable_metric[0]
                ionosphere_untrainable_metrics.append(ium_metric_name)
            except Exception as err:
                current_logger.error('error :: failed to add to ionosphere_untrainable_metrics - %s' % err)
    if ionosphere_untrainable_metrics:
        ionosphere_untrainable_metrics = set(ionosphere_untrainable_metrics)

    added_events = 0
    for base_name, ts, full_duration in training_data:
        if added_events > 10:
            if ts < from_timestamp:
                continue

            # @added 20250329 - Feature #5611: custom_algorithm_only
            # Added until_timestamp
            if until_timestamp:
                if ts > until_timestamp:
                    continue

        if len(training_data_dict) >= max_items:
             break
        labelled_metric_name = None
        untrainable = False
        try:
            use_base_name = str(base_name)
            metric_id = 0

            # @added 20240212 - Feature #5270: webapp - panorama smoke
            # Allow namespaces to be passed, rather use the dict to look up
            # than hitting the DB function
            if base_name.startswith('labelled_metrics.'):
                got_base_name = None
                try:
                    metric_id_str = base_name.replace('labelled_metrics.', '', 1)
                    metric_id = int(metric_id_str)
                    got_base_name = metric_ids_with_base_names[metric_id]
                except Exception as err:
                    metric_ids_to_lookup_errors.append(['error got_base_name lookup', str(base_name), err])
                    got_base_name = None
                if got_base_name:
                    base_name = str(got_base_name)

            if base_name.startswith('labelled_metrics.'):
                try:
                    base_name = get_base_name_from_labelled_metrics_name(current_skyline_app, use_base_name)
                except Exception as err:
                    current_logger.error('error :: %s :: get_base_name_from_labelled_metrics_name failed for %s - %s' % (
                        function_str, str(base_name), err))
                metric_id_str = use_base_name.replace('labelled_metrics.', '')
                metric_id = int(metric_id_str)
                labelled_metric_name = 'labelled_metrics.%s' % str(metric_id)
            if '{' in use_base_name and '}' in use_base_name and '_tenant_id="' in use_base_name:
                # @added 20240212 - Feature #5270: webapp - panorama smoke
                # Allow namespaces to be passed, rather use the dict to look up
                # than hitting the DB function
                if not metric_id:
                    try:
                        metric_id = base_names_with_metric_ids[use_base_name]
                    except KeyError:
                        metric_id = 0
                if not metric_id:
                    try:
                        metric_id = get_metric_id_from_base_name(current_skyline_app, use_base_name)
                        if metric_id:
                            base_names_with_metric_ids[use_base_name] = metric_id
                    except Exception as err:
                        current_logger.error('error :: %s :: get_metric_id_from_base_name failed with base_name: %s - %s' % (
                            function_str, str(base_name), err))
                if metric_id:
                    labelled_metric_name = 'labelled_metrics.%s' % str(metric_id)
                    use_base_name = str(labelled_metric_name)

            # Remove ionosphere.untrainable_metrics from the training data
            if labelled_metric_name in ionosphere_untrainable_metrics:
                untrainable = True
            if base_name in ionosphere_untrainable_metrics:
                untrainable = True

            metric_host = str(this_host)
            if settings.HORIZON_SHARDS:
                try:
                    metric_host = get_authoritative_node(current_skyline_app, base_name, return_fqdn=False)
                except Exception as err:
                    current_logger.error('error :: %s :: get_metric_shard_host failed for %s, err: %s' % (
                        function_str, base_name, err))
                if metric_host != this_host:
                    continue

            human_date = strftime('%Y-%m-%d %H:%M:%S %Z (%A)', localtime(int(ts)))
            try:
                training_data_dict[ts][base_name] = {'date': human_date, 'training_data_dir': None, 'data_dir': None, 'image': None, 'image_url': None, 'thunder': None, 'host': metric_host}
                added_events += 1
            except:
                training_data_dict[ts] = {}
                training_data_dict[ts][base_name] = {'date': human_date, 'training_data_dir': None, 'data_dir': None, 'image': None, 'image_url': None, 'thunder': None, 'host': metric_host}
                added_events += 1
            training_data_dict[ts][base_name]['added_to_smoke_by'] = this_host

            timestamp = str(ts)
            timeseries_dir = use_base_name.replace('.', '/')
            training_data_dir = '%s/%s/%s' % (settings.IONOSPHERE_DATA_FOLDER, timestamp, timeseries_dir)
            trained = False

            matched = False
            image_url = None
            timeseries_json = None

            if os.path.exists(training_data_dir):
                training_data_dict[ts][base_name]['training_data_dir'] = training_data_dir
                training_data_uri = '/ionosphere?timestamp=%s&metric=%s&requested_timestamp=%s' % (
                    timestamp, use_base_name, timestamp)
                training_data_dict[ts][base_name]['training_data_uri'] = training_data_uri
                anomaly_txt_file = '%s.txt' % use_base_name
                anomaly_file = '%s/%s.txt' % (training_data_dir, use_base_name)
                trained_txt_file = '%s.%s.fp.created.txt' % (timestamp, use_base_name)

                timeseries_json = '%s.json' % use_base_name

                # @added 20250329 - Feature #5611: custom_algorithm_only
                # Allow to show short_period 
                short_period_image = None

                # @added 20250223 - Feature #5270: webapp - panorama smoke
                #                   Feature #5712: skyline.dawn
                # Added the learnt variable
                learnt = False

                for path, folders, files in os.walk(training_data_dir):
                    if files:
                        for file in files:
                            if file.endswith('h.png'):
                                if 'mirage.graphite' in str(file):
                                    # @added 20250329 - Feature #5611: custom_algorithm_only
                                    # Allow to show short_period 
                                    if file.endswith('0_75h.png'):
                                        short_period_image = '%s/%s' % (training_data_dir, file)
                                        continue

                                    image = '%s/%s' % (training_data_dir, file)
                                    current_logger.debug('debug :: api_smoke image: %s' % image)
                                    training_data_dict[ts][base_name]['image'] = image
                                    image_url = '%s/ionosphere_images?image=%s' % (
                                        settings.SKYLINE_URL, image)
                                    training_data_dict[ts][base_name]['image_url'] = image_url
                                    current_logger.debug('debug :: training_data_dict image_url: %s' % training_data_dict[ts][base_name]['image_url'])
                            if file == anomaly_txt_file:
                                metric_vars_dict = mirage_load_metric_vars(current_skyline_app, anomaly_file, return_dict=True, log=False)
                                training_data_dict[ts][base_name]['details'] = metric_vars_dict['metric_vars']
                                training_data_dict[ts][base_name]['details']['base_name'] = base_name

                                # @added 20240227 - Feature #5270: webapp - panorama smoke
                                # Label ionosphere alerts
                                details_base_name = training_data_dict[ts][base_name]['details']['base_name']
                                details_metric = training_data_dict[ts][base_name]['details']['metric']
                                check_name = str(details_metric)
                                if details_base_name == details_metric:
                                    check_name = '%s%s' % (settings.FULL_NAMESPACE, details_metric)
                                    if 'added_by' in training_data_dict[ts][base_name]['details']:
                                        if training_data_dict[ts][base_name]['details']['added_by'] == 'mirage':
                                            if check_name in ionosphere_unique_metrics:
                                                training_data_dict[ts][base_name]['details']['added_by'] = 'ionosphere'

                                # @added 20241111 - Feature #5270: webapp - panorama smoke
                                if 'added_by' in training_data_dict[ts][base_name]['details']:
                                    if training_data_dict[ts][base_name]['details']['added_by'] == 'analyzer':
                                        untrainable = True

                            if file == trained_txt_file:
                                trained = True
                                # @added 20250223 - Feature #5270: webapp - panorama smoke
                                #                   Feature #5712: skyline.dawn
                                # Added the learnt variable
                                features_profile_created_file = '%s/%s' % (training_data_dir, trained_txt_file)
                                try:
                                    with open(features_profile_created_file, 'r') as f:
                                        fp_created_str = f.read()
                                    fp_created_array = literal_eval(fp_created_str)
                                    generation = fp_created_array[8]
                                    if generation > 1:
                                        learnt = True
                                except Exception as err:
                                    current_logger.error(
                                        'error :: api_smoke failed to read from %s, err: %s' % (
                                            features_profile_created_file, err))

                            if file.endswith('profile_id_matched.fp_id'):
                                matched = True
                        # @added 20240514 - Feature #5270: webapp - panorama smoke
                        # Handle Analyzer only anomalies if the mirage image is
                        # not available
                        if not training_data_dict[ts][base_name]['image_url']:
                            for file in files:
                                if 'analyzer.graphite' in str(file) and file.endswith('h.png'):
                                    image = '%s/%s' % (training_data_dir, file)
                                    training_data_dict[ts][base_name]['image'] = image
                                    image_url = '%s/ionosphere_images?image=%s' % (
                                        settings.SKYLINE_URL, image)
                                    training_data_dict[ts][base_name]['image_url'] = image_url
                                    break
                        # @added 20241108 - Feature #5270: webapp - panorama smoke
                        # Handle new VictoriaMetrics metrics
                        if not training_data_dict[ts][base_name]['image_url']:
                            for file in files:
                                if 'mirage.redis.plot.24h' in str(file) and file.endswith('.png'):
                                    #current_logger.debug('debug :: no image_url in training_data_dict keys: %s' % (
                                    #    str(training_data_dict[ts][base_name].keys())))
                                    current_logger.debug('debug :: api_smoke image: %s' % image)
                                    image = '%s/%s' % (training_data_dir, file)
                                    training_data_dict[ts][base_name]['image'] = image
                                    image_url = '%s/ionosphere_images?image=%s' % (
                                        settings.SKYLINE_URL, image)
                                    training_data_dict[ts][base_name]['image_url'] = image_url
                                    current_logger.debug('debug :: training_data_dict image_url: %s' % training_data_dict[ts][base_name]['image_url'])
                                    break
                        # Handle metrics submitted via vortex
                        if not training_data_dict[ts][base_name]['image_url']:
                            for file in files:
                                if 'vortex.analysed' in str(file) and file.endswith('h.png'):
                                    image = '%s/%s' % (training_data_dir, file)
                                    training_data_dict[ts][base_name]['image'] = image
                                    image_url = '%s/ionosphere_images?image=%s' % (
                                        settings.SKYLINE_URL, image)
                                    training_data_dict[ts][base_name]['image_url'] = image_url
                                    break

                        # @added 20250329 - Feature #5611: custom_algorithm_only
                        # Allow to show short_period 
                        training_data_dict[ts][base_name]['short_period_image'] = short_period_image
                        short_period_image_url = None
                        if short_period_image:
                            short_period_image_url = '%s/ionosphere_images?image=%s' % (
                                settings.SKYLINE_URL, short_period_image)
                        training_data_dict[ts][base_name]['short_period_image_url'] = short_period_image_url

            training_data_dict[ts][base_name]['trained'] = trained
            training_data_dict[ts][base_name]['untrainable'] = untrainable

            # @added 20250223 - Feature #5270: webapp - panorama smoke
            #                   Feature #5712: skyline.dawn
            # Added the learnt variable
            training_data_dict[ts][base_name]['learnt'] = learnt

            data_dir = '%s/%s/%s' % (settings.IONOSPHERE_PROFILES_FOLDER, timestamp, timeseries_dir)
            if os.path.exists(data_dir):
                training_data_dict[ts][base_name]['data_dir'] = data_dir

            # @added 20231231 - Feature #5270: webapp - panorama smoke
            # Handle matched which have no images and removed
            training_data_dict[ts][base_name]['matched'] = matched
            if matched and not image_url:
                training_data_dict[ts][base_name]['image_url'] = None

        except Exception as err:
            current_logger.error('error :: api_smoke failed to evaluate training_data item, err: %s' % err)
    # current_logger.debug('debug :: api_smoke training_data_dict: %s' % str(training_data_dict))

    # @added 20231227 - Feature #5270: webapp - panorama smoke
    # Add anomalies as well to display boundary anomalies too
    dict_timestamps = list(training_data_dict.keys())

    # @added 20250329 - Feature #5611: custom_algorithm_only
    # Added until_timestamp as an argument so only use current if not set
    if not until_timestamp:
        until_timestamp = int(time())

    metric_ids = []
    try:
        anomalies = get_anomalies_for_period(current_skyline_app, metric_ids, from_timestamp, until_timestamp)
    except Exception as err:
        current_logger.error('error :: api_smoke get_anomalies_for_period failed, err: %s' % err)
        raise
    # current_logger.debug('debug :: api_smoke surfaced %s anomalies' % str(len(anomalies)))
    for anomaly_id in list(anomalies.keys()):
        anomaly_timestamp = anomalies[anomaly_id]['anomaly_timestamp']
        base_name = None
        try:
            metric_id = anomalies[anomaly_id]['metric_id']
            # @modified 20240212 - Feature #5270: webapp - panorama smoke
            # Try the dict to look up before hitting the DB function
            # base_name = get_base_name_from_metric_id(current_skyline_app, int(metric_id))
        except Exception as err:
            current_logger.error('error :: api_smoke :: get_base_name_from_metric_id failed to determine base_name, err: %s' % (
                err))
            base_name = None

        # @added 20240212 - Feature #5270: webapp - panorama smoke
        # Allow namespaces to be passed, rather use the dict to look up
        # than hitting the DB function
        try:
            base_name = metric_ids_with_base_names[metric_id]
        except KeyError:
            base_name = None
        if not base_name:
            try:
                base_name = get_base_name_from_metric_id(current_skyline_app, int(metric_id))
                if base_name:
                    metric_ids_with_base_names[int(metric_id)] = base_name
            except Exception as err:
                current_logger.error('error :: api_smoke :: get_base_name_from_metric_id failed to determine base_name, err: %s' % (
                    err))
                base_name = None

        if anomaly_timestamp in dict_timestamps:
            if base_name in list(training_data_dict[anomaly_timestamp].keys()):
                # current_logger.debug('debug :: api_smoke skipping anomaly for %s at %s as already in training data' % (
                #     base_name, str(anomaly_timestamp)))
                del anomalies[anomaly_id]
                continue
        if not anomalies[anomaly_id]['alert']:
            # current_logger.debug('debug :: api_smoke skipping anomaly for %s at %s as not alerted' % (
            #     base_name, str(anomaly_timestamp)))
            del anomalies[anomaly_id]
            continue
        labelled_metric_name = None
        if '{' in base_name and '}' in base_name and '_tenant_id="' in base_name:
            labelled_metric_name = 'labelled_metrics.%s' % str(metric_id)
        anomalies[anomaly_id]['metric'] = base_name
        anomalies[anomaly_id]['labelled_metric_name'] = labelled_metric_name
        
    current_logger.info('api_smoke len(anomalies): %s' % str(len(anomalies)))

    # @added 20240101 - Feature #5270: webapp - panorama smoke
    # Add thunder stale metrics alerts
    thunder_alert_keys = []
    try:
        thunder_alert_keys = list(redis_conn_decoded.scan_iter('thunder.stale_metrics.alert.*'))
    except Exception as err:
        current_logger.error('error :: api_smoke failed to get ionosphere.training_data - %s' % err)
        raise
    for thunder_alert_key in thunder_alert_keys:
        if 'thunder.stale_metrics.alert.times_attempted' in thunder_alert_key:
            continue
        try:
            alert_sent_at = '.'.join(thunder_alert_key.split('.')[3:])
        except Exception as err:
            current_logger.error('error :: api_smoke failed to determine alert_sent_at from %s, err: %s' % (
                str(thunder_alert_key), err))
            continue
        try:
            ts = int(thunder_alert_key.split('.')[3])
        except Exception as err:
            current_logger.error('error :: api_smoke failed to determine ts from %s, err: %s' % (
                str(thunder_alert_key), err))
            continue
        if ts < from_timestamp:
            continue

        # @added 20250329 - Feature #5611: custom_algorithm_only
        # Added until_timestamp
        if until_timestamp:
            if ts > until_timestamp:
                continue

        thunder_alert = None
        try:
            thunder_alert_str = redis_conn_decoded.get(thunder_alert_key)
            if thunder_alert_str:
                thunder_alert = literal_eval(thunder_alert_str)
        except Exception as err:
            current_logger.error('error :: %s :: failed to interpolate thunder_alert_key: %s, err: %s' % (
                function_str, str(thunder_alert_key), err))
        if thunder_alert:
            human_date = strftime('%Y-%m-%d %H:%M:%S %Z (%A)', localtime(int(ts)))
            try:
                training_data_dict[ts]['thunder_alert'] = {'date': human_date, 'training_data_dir': None, 'data_dir': None, 'image': None, 'image_url': None}
                added_events += 1
            except:
                training_data_dict[ts] = {}
                training_data_dict[ts]['thunder_alert'] = {'date': human_date, 'training_data_dir': None, 'data_dir': None, 'image': None, 'image_url': None}
                added_events += 1
            try:
                training_data_dict[ts]['thunder_alert']['status'] = thunder_alert['status']
                training_data_dict[ts]['thunder_alert']['metrics'] = thunder_alert['metrics']
                training_data_dict[ts]['thunder_alert']['trained'] = None
                training_data_dict[ts]['thunder_alert']['matched'] = None
            except Exception as err:
                current_logger.error('error :: %s :: failed to add details from thunder_alert_key: %s, err: %s' % (
                    function_str, str(thunder_alert_key), err))
            stale_statuses = [
                'not recieving data for some metrics', 'recovered'
            ]
            try:
                if thunder_alert['status'] in stale_statuses:
                    key_link = '%s/api?thunder_stale_metrics=true&timestamp=%s' % (
                        settings.SKYLINE_URL, str(alert_sent_at))
                    training_data_dict[ts]['thunder_alert']['url'] = key_link
            except Exception as err:
                current_logger.error('error :: %s :: failed to add stale_metrics key_link from thunder_alert_key: %s, err: %s' % (
                    function_str, str(thunder_alert_key), err))

    # Add thunder no data alerts
    # Add thunder no update alerts

    remote_training_data = []
    if settings.REMOTE_SKYLINE_INSTANCES and cluster_data:
        current_logger.info('%s :: calling /smoke on other cluster nodes' % function_str)
        # @added 20250329 - Feature #5611: custom_algorithm_only
        # Added until_timestamp
        api_uri = 'smoke=true&from_timestamp=%s&until_timestamp=%s&cluster_data=false&cluster_call=true' % (
            str(from_timestamp), str(until_timestamp))

        # @added 20240212 - Feature #5270: webapp - panorama smoke
        # Allow max_items and namespaces to be passed
        if max_items:
            api_uri = '%s&max_items=%s' % (api_uri, str(max_items))
        if namespaces:
            namespaces_str = ','.join(namespaces)
            api_uri = '%s&namespaces=%s' % (api_uri, str(namespaces_str))

        # @added 20250329 - Feature #5611: custom_algorithm_only
        # Allow smoke to include training for do_not_train instances
        if do_not_train:
            api_uri = '%s&do_not_train=true' % api_uri

        try:
            remote_training_data = get_cluster_data(api_uri, 'training_data')
        except Exception as err:
            current_logger.error('error :: %s :: failed to get skip_analysis from remote instances - %s' % (
                function_str, err))
            raise
        for remote_training_data_dict in remote_training_data:
            for ts in list(remote_training_data_dict.keys()):
                for base_name in list(remote_training_data_dict[ts].keys()):
                    add_data = True
                    if ts in training_data_dict.keys():
                        if base_name in training_data_dict[ts].keys():
                            add_data = False
                    if add_data:
                        try:
                            training_data_dict[ts][base_name] = copy.deepcopy(remote_training_data_dict[ts][base_name])
                        except:
                            training_data_dict[ts] = {}
                            training_data_dict[ts][base_name] = copy.deepcopy(remote_training_data_dict[ts][base_name])
    sorted_training_data_dict = {}    
    dict_timestamps = list(training_data_dict.keys())
    for ts in dict_timestamps:
        sorted_training_data_dict[ts] = copy.deepcopy(training_data_dict[ts])
    # current_logger.debug('debug :: api_smoke sorted_training_data_dict: %s' % str(sorted_training_data_dict))

    if sorted_training_data_dict:
        training_data_dict = dict(sorted_training_data_dict)

    # @added 20260228
    if llm_output:
        remove_keys = [
            'training_data_dir', 'data_dir', 'image', 'image_url', 'host',
            'added_to_smoke_by', 'training_data_uri', 'short_period_image',
            'short_period_image_url'
        ]
        keep_details_keys = [
            #"value", "metric_timestamp", "added_by"
            "value", "metric_timestamp", "added_by"
        ]
#"details": {"metric": "victronenergy.vebus.Ac.Out.L2.P", "value": 1161.375, "from_timestamp": 1771701600, "metric_timestamp": 1772305772, "algorithms": ["histogram_bins", "first_hour_average", "stddev_from_average", "grubbs", "ks_test", "mean_subtraction_cumulation", "median_absolute_deviation", "stddev_from_moving_average", "least_squares"], "triggered_algorithms": ["mirage_nirvana"], "graphite_metric": false, "run_crucible_tests": false, "added_by": "ionosphere", "added_at": 1772305893, "full_duration": 604800, "algorithms_run": ["mirage_nirvana"], "base_name": "victronenergy.vebus.Ac.Out.L2.P"},
        timestamps = list(training_data_dict.keys())
        for ts in timestamps:
            ts_items = list(training_data_dict[ts].keys())
            for metric in ts_items:
                for key in remove_keys:
                    try:
                        del training_data_dict[ts][metric][key]
                    except:
                        pass
                if 'details' in training_data_dict[ts][metric]:
                    for key in list(training_data_dict[ts][metric]['details'].keys()):
                        if key not in keep_details_keys:
                            try:
                                del training_data_dict[ts][metric]['details'][key]
                            except:
                                pass
                


    return training_data_dict
