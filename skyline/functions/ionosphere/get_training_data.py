"""
get_training_data.py
"""
import logging
import traceback
import os
import copy

import requests

import settings

from matched_or_regexed_in_list import matched_or_regexed_in_list

# @added 20221018 - Feature #4650: ionosphere.bulk.training
from functions.metrics.get_base_name_from_labelled_metrics_name import get_base_name_from_labelled_metrics_name

verify_ssl = True
try:
    running_on_docker = settings.DOCKER
except:
    running_on_docker = False
if running_on_docker:
    verify_ssl = False
try:
    overall_verify_ssl = settings.VERIFY_SSL
except:
    overall_verify_ssl = True
if not overall_verify_ssl:
    verify_ssl = False
user = None
password = None
if settings.WEBAPP_AUTH_ENABLED:
    user = str(settings.WEBAPP_AUTH_USER)
    password = str(settings.WEBAPP_AUTH_USER_PASSWORD)
url = '%s/api?training_data' % settings.SKYLINE_URL


def get_training_data(
        current_skyline_app, metrics=None, namespaces=None, key_by='metric'):
    """
    Return a dictionary of training_data keyed by the key_by parameter.

    :param current_skyline_app: the app calling the function
    :param metric: training data for a specific metric/s, if [] all metrics are
        returned.
    :param namespace: training data for a specific namespace/s or labels/values
        if [] all metrics are returned.
    :param key_by: whether to the dictionary by metric, timestamp or interval,
        if the dictionary is to be keyed by an interval pass the seconds value
        of the interval as an int.
    :type current_skyline_app: str
    :type metrics: list
    :type namespaces: list
    :type key_by: str or int
    :return: dictionary of training data
    :rtype: dict

    """

    function_str = 'functions.ionosphere.get_training_data'
    current_skyline_app_logger = current_skyline_app + 'Log'
    current_logger = logging.getLogger(current_skyline_app_logger)

    training_data_dict = {}
    current_logger.info('%s :: getting training_data for metrics: %s, namespaces: %s, key_by: %s' % (
        function_str, str(metrics), str(namespaces), str(key_by)))

    if not metrics:
        metrics = []
    if not namespaces:
        namespaces = []
    key_by_interval = 0
    if isinstance(key_by, int):
        if key_by > 0:
            key_by_interval = int(key_by)
            key_by = 'interval'

    use_url = str(url)
    if len(metrics) == 1:
        use_url = '%s&metric=%s' % (url, metrics[0])

    try:
        if user and password:
            r = requests.get(use_url, timeout=settings.GRAPHITE_READ_TIMEOUT, auth=(user, password), verify=verify_ssl)
        else:
            r = requests.get(use_url, timeout=settings.GRAPHITE_READ_TIMEOUT, verify=verify_ssl)
    except Exception as err:
        current_logger.error(traceback.format_exc())
        current_logger.error('error :: %s :: failed get training_data from %s - %s' % (
            function_str, str(url), err))
    json_response = None
    try:
        json_response = r.json()
    except Exception as err:
        current_logger.error(traceback.format_exc())
        current_logger.error('error :: %s :: failed get json from the reponse from %s - %s' % (
            function_str, str(url), err))
    training_data = []
    if json_response:
        try:
            training_data = json_response['data']['metrics']
        except Exception as err:
            current_logger.error(traceback.format_exc())
            current_logger.error('error :: %s :: failed get training_data list from json_response - %s' % (function_str, err))
    if not training_data:
        return training_data_dict

    for item in training_data:
        discard = False
        metric_name = item[0]
        if metrics and metric_name not in metrics:
            discard = True

        # @added 20221018 - Feature #4650: ionosphere.bulk.training
        base_name = str(metric_name)
        if metric_name.startswith('labelled_metrics.'):
            try:
                base_name = get_base_name_from_labelled_metrics_name(current_skyline_app, metric_name)
            except Exception as err:
                current_logger.error('error :: get_base_name_from_labelled_metrics_name failed for %s - %s' % (
                    str(base_name), err))

        if namespaces:
            # @modified 20221018 - Feature #4650: ionosphere.bulk.training
            # pattern_match, metric_matched_by = matched_or_regexed_in_list(current_skyline_app, metric_name, namespaces)
            pattern_match, metric_matched_by = matched_or_regexed_in_list(current_skyline_app, base_name, namespaces)
            del metric_matched_by
            if pattern_match:
                discard = False
            else:
                discard = True
        # Internally the labelled_metrics. name item is used
        if '_tenant_id="' in metric_name or discard:
            continue
        timestamp = item[1]
        primary_key = metric_name
        secondary_key = timestamp
        if key_by in ['timestamp', 'interval']:
            primary_key = timestamp
            secondary_key = metric_name
        metric_timeseries_dir = metric_name.replace('.', '/')
        training_dir = '%s/%s/%s' % (
            str(settings.IONOSPHERE_DATA_FOLDER), str(timestamp),
            metric_timeseries_dir)
        try:
            dir_list = os.listdir(training_dir)
        except:
            continue
        if primary_key not in training_data_dict:
            training_data_dict[primary_key] = {}
        training_data_dict[primary_key][secondary_key] = {'training_data_dir': training_dir}
        trained = False
        for f in dir_list:
            if f.endswith('.fp.created.txt'):
                trained = True
        training_data_dict[primary_key][secondary_key]['files'] = dir_list
        training_data_dict[primary_key][secondary_key]['trained'] = trained

        # @added 20221018 - Feature #4650: ionosphere.bulk.training
        training_data_dict[primary_key][secondary_key]['metric'] = base_name

    if key_by == 'interval':
        new_training_data_dict = {}
        for timestamp in list(training_data_dict.keys()):
            interval_aligned_timestamp = int(timestamp // key_by_interval * key_by_interval)
            if interval_aligned_timestamp not in new_training_data_dict:
                new_training_data_dict[interval_aligned_timestamp] = {}
            for metric_name in list(training_data_dict[timestamp].keys()):
                if metric_name not in new_training_data_dict[interval_aligned_timestamp]:
                    new_training_data_dict[interval_aligned_timestamp][metric_name] = {}
                new_training_data_dict[interval_aligned_timestamp][metric_name][timestamp] = training_data_dict[timestamp][metric_name]
        training_data_dict = copy.deepcopy(new_training_data_dict)
    return training_data_dict
