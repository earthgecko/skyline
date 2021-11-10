import logging
import traceback
from os import walk
import time
from ast import literal_eval

import settings
from backend import get_cluster_data
from skyline_functions import get_redis_conn_decoded
from matched_or_regexed_in_list import matched_or_regexed_in_list


# @added 20210727 - Feature #4206: webapp - saved_training_data page
def get_saved_training_data(
        current_skyline_app, webapp_endpoint, metrics, namespaces,
        label_includes, cluster_data=False):
    """
    Determine all details of all saved training data.

    :param current_skyline_app: the app calling the function
    :param webapp_endpoint: the endpoint calling the function either ionosphere
        or api
    :param metrics: a list of base_names
    :param namespaces: a list of namespace pattern to match
    :param label_includes: a string in the label to filter on
    :param cluster_data: whether this is a cluster_data request, optional,
        defaults to False
    :type current_skyline_app: str
    :type webapp_endpoint: str
    :type metrics: list
    :type namespace: list
    :type label_includes: str
    :return: saved_training_data_dict
    :rtype: dict

    """

    saved_training_data_dict = {}
    function_str = 'get_saved_training_data'

    filter_by_metrics = []
    current_skyline_app_logger = current_skyline_app + 'Log'
    current_logger = logging.getLogger(current_skyline_app_logger)
    current_logger.info('%s :: %s :: saved training data' % (
        current_skyline_app, function_str))

    saved_training_data_dir = '%s_saved' % settings.IONOSPHERE_DATA_FOLDER

    for root, dirs, files in walk(saved_training_data_dir):
        for file in files:
            if file.endswith('.saved_training_data_label.txt'):
                file_name_elements = file.split('.')
                timestamp = file_name_elements[0]
                metric = '.'.join(file_name_elements[1:-2])

                saved_training_data_dict[metric] = {}
                saved_training_data_dict[metric][timestamp] = {}
                saved_training_data_dict[metric][timestamp]['label'] = None
                saved_hdate = time.strftime('%Y-%m-%d %H:%M:%S %Z (%A)', time.localtime(int(timestamp)))
                saved_training_data_dict[metric][timestamp]['saved_date'] = saved_hdate
                url = '%s/ionosphere?saved_training_data=true&metric=%s&timestamp=%s' % (
                    settings.SKYLINE_URL, metric, str(timestamp))
                saved_training_data_dict[metric][timestamp]['url'] = url

                details_file = '%s/%s' % (root, file)
                saved_training_data_details = []
                try:
                    with open(details_file) as f:
                        for line in f:
                            saved_training_data_details.append(line)
                except Exception as e:
                    current_logger.error(traceback.format_exc())
                    current_logger.error('%s :: error :: failed to read details file %s - %s' % (function_str, details_file, e))
                    continue

                # Handle formating the plaintext to a list of dicts as the
                # original output was only meant to be passed as <code> in HTML
                # Going forward the saved_training_data_label.txt created in
                # skyline/webapp/ionosphere_backend.py has been modified to
                # output a list of dicts.  The below handles the old format.
                details_str = str(saved_training_data_details[0])
                new_details_str = details_str.replace("[label:", "{'label':")
                new_details_str = new_details_str.replace("], [saved_date:", "}, {'saved_date':")
                details_str = new_details_str.replace("]]", "}]")

                saved_data_info = None
                label = None
                saved_date = None
                try:
                    saved_data_info = literal_eval(details_str)
                    label = str(saved_data_info[0]['label'])
                    saved_date = str(saved_data_info[1]['saved_date'])
                except Exception as e:
                    current_logger.error(traceback.format_exc())
                    current_logger.error('%s :: error :: failed to literal_eval data from %s - %s' % (function_str, details_file, e))
                if label and saved_date:
                    saved_training_data_dict[metric][timestamp]['label'] = label
                    saved_training_data_dict[metric][timestamp]['date'] = saved_date

    if webapp_endpoint == 'api':
        return saved_training_data_dict

    if settings.REMOTE_SKYLINE_INSTANCES and cluster_data:
        saved_training_data_uri = 'saved_training_data'
        try:
            remote_saved_training_data = get_cluster_data(saved_training_data_uri, 'saved_training_data')
        except Exception as e:
            current_logger.error(traceback.format_exc())
            current_logger.error('error :: %s :: failed to get boundary_metrics from remote instances - %s' % (function_str, e))
            raise
        if remote_saved_training_data:
            current_logger.info('got %s remote saved_training_data from the remote Skyline instances' % str(len(remote_saved_training_data)))
            for remote_data in remote_saved_training_data:
                for base_name in list(remote_data.keys()):
                    saved_training_data_dict[base_name] = remote_data[base_name]

    if metrics:
        for metric in metrics:
            filter_by_metrics.append(metric)

    unique_base_names = []
    if namespaces:
        try:
            redis_conn_decoded = get_redis_conn_decoded(current_skyline_app)
        except Exception as e:
            current_logger.error(traceback.format_exc())
            current_logger.error('error :: %s :: %s :: get_redis_conn_decoded failed - %s' % (
                current_skyline_app, function_str, e))
            raise
        redis_key = 'analyzer.metrics_manager.db.metric_names'
        try:
            unique_base_names = list(redis_conn_decoded.smembers(redis_key))
            if unique_base_names:
                current_logger.info('%s :: %s :: got %s unique_base_names to filter' % (
                    current_skyline_app, function_str,
                    str(len(unique_base_names))))
        except Exception as e:
            current_logger.error(traceback.format_exc())
            current_logger.error('error :: %s :: %s :: failed to get Redis key %s - %s' % (
                current_skyline_app, function_str, redis_key, e))
            raise
        for base_name in unique_base_names:
            try:
                pattern_match, metric_matched_by = matched_or_regexed_in_list(current_skyline_app, base_name, namespaces)
                if pattern_match:
                    filter_by_metrics.append(base_name)
            except Exception as e:
                current_logger.error(traceback.format_exc())
                current_logger.error('error :: %s :: %s :: failed to get Redis key %s - %s' % (
                    current_skyline_app, function_str, redis_key, e))

    if filter_by_metrics:
        current_logger.info('%s :: %s :: filtering on %s metrics' % (
            current_skyline_app, function_str,
            str(len(filter_by_metrics))))

        filtered_saved_training_data_dict = {}
        for base_name in list(set(filter_by_metrics)):
            metric_training_data_dict = None
            try:
                metric_training_data_dict = saved_training_data_dict[base_name]
            except:
                continue
            if metric_training_data_dict:
                filtered_saved_training_data_dict[base_name] = metric_training_data_dict
        if filtered_saved_training_data_dict:
            saved_training_data_dict = filtered_saved_training_data_dict.copy()
            current_logger.info('%s :: %s :: filtered %s saved training data' % (
                current_skyline_app, function_str,
                str(len(saved_training_data_dict))))

    if label_includes:
        current_logger.info('%s :: %s :: filtering on %s metrics by label_includes %s' % (
            current_skyline_app, function_str,
            str(len(filter_by_metrics)), str(label_includes)))
        filtered_saved_training_data_dict = {}
        for metric in list(saved_training_data_dict.keys()):
            for timestamp in list(saved_training_data_dict[metric].keys()):
                if label_includes in saved_training_data_dict[metric][timestamp]['label']:
                    exists = None
                    try:
                        exists = filtered_saved_training_data_dict[metric]
                    except KeyError:
                        exists = False
                    if not exists:
                        filtered_saved_training_data_dict[metric] = {}
                    filtered_saved_training_data_dict[metric][timestamp] = saved_training_data_dict[metric][timestamp]
        if filtered_saved_training_data_dict:
            saved_training_data_dict = filtered_saved_training_data_dict.copy()
            current_logger.info('%s :: %s :: filtered %s saved training data on label_includes' % (
                current_skyline_app, function_str,
                str(len(saved_training_data_dict))))

    return saved_training_data_dict
