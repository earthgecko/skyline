"""
learn_repetitive_patterns.py
"""
import logging
import traceback
import os
import copy
from time import time
from ast import literal_eval

import requests

import settings
from skyline_functions import get_redis_conn_decoded

from functions.ionosphere.get_training_data import get_training_data
from features_profile import calculate_features_profile
from functions.numpy.percent_different import get_percent_different
from ionosphere_functions import create_features_profile
# @added 20221215 - Feature #4658: ionosphere.learn_repetitive_patterns
from functions.ionosphere.get_repetitive_patterns_metrics_list_to_evaluate import get_repetitive_patterns_metrics_list_to_evaluate
from functions.metrics.get_base_names_and_metric_ids import get_base_names_and_metric_ids

skyline_app = 'ionosphere'
skyline_app_logger = '%sLog' % skyline_app
logger = logging.getLogger(skyline_app_logger)
skyline_app_logfile = '%s/%s.log' % (settings.LOG_PATH, skyline_app)
skyline_app_loglock = '%s.lock' % skyline_app_logfile
skyline_app_logwait = '%s.wait' % skyline_app_logfile

this_host = str(os.uname()[1])

# Converting one settings variable into a local variable, just because it is a
# long string otherwise.
try:
    ENABLE_IONOSPHERE_DEBUG = settings.ENABLE_IONOSPHERE_DEBUG
except:
    logger.error('error :: learn :: cannot determine ENABLE_IONOSPHERE_DEBUG from settings')
    ENABLE_IONOSPHERE_DEBUG = False

try:
    SERVER_METRIC_PATH = '.%s' % settings.SERVER_METRICS_NAME
    if SERVER_METRIC_PATH == '.':
        SERVER_METRIC_PATH = ''
except:
    SERVER_METRIC_PATH = ''

try:
    learn_full_duration = int(settings.IONOSPHERE_LEARN_DEFAULT_FULL_DURATION_DAYS) * 86400
except:
    learn_full_duration = 86400 * 30  # 2592000

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

function_str = 'learn_repetitive_patterns'

redis_conn_decoded = get_redis_conn_decoded(skyline_app)


def get_training_to_evaluate(metrics_to_evaluate):
    """
    Return a dictionary of training data to evaluate.

    :param metrics_to_evaluate: the dictionary of metrics_to_evaluate
    :type metrics_to_evaluate: dict
    :return: training_to_evaluate
    :rtype: dict
    """
    # Determine training data sets that are roughly aligned hourly or daily
    training_to_evaluate = {}
    hourly_tolerance = 180
    daily_tolerance = 900
    for metric in list(metrics_to_evaluate.keys()):
        # Sort timestamps oldest to latest
        timestamps = sorted(list(metrics_to_evaluate[metric].keys()))
        count = len(timestamps)
        aligned_count = 0

        # @added 20230105 - Feature #4658: ionosphere.learn_repetitive_patterns
        # Check that the timestamps are not within recent 1 hour periods of each
        # other.  This prevents a similar pattern occurring over short period
        # for being identified as a repetitive pattern, which is undesired.  For
        # instance if there are training data sets for 16:05, 17:06, 18:07 on
        # the same day we do not want to compare those.
        last_training_data_timestamp = None

        for index, t in enumerate(timestamps):
            # The last timestamp, we are done
            if index == (count - 1):
                break
            timestamp_difference = timestamps[(index + 1)] - t

            # @added 20230105 - Feature #4658: ionosphere.learn_repetitive_patterns
            # Check that the timestamps are not within recent 1 hour periods of
            # each other
            if last_training_data_timestamp and last_training_data_timestamp < t + (3600 * 3):
                last_training_data_timestamp = t
                continue
            last_training_data_timestamp = t

            if timestamp_difference in list(range((86400 - daily_tolerance), (86400 + daily_tolerance))):
                aligned_count += 1
                continue
            if timestamp_difference in list(range((3600 - hourly_tolerance), (3600 + hourly_tolerance))):
                aligned_count += 1
        if aligned_count == (count - 1):
            training_to_evaluate[metric] = copy.deepcopy(metrics_to_evaluate[metric])
    return training_to_evaluate


def get_metrics_to_evaluate(training_data_dict, last_evaluation_timestamp):
    """
    Return a dictionary of metrics_to_evaluate.

    :param training_data_dict: the dictionary of training data
    :param last_evaluation_timestamp: the last timestamp that training was
        evaluated
    :type training_data_dict: dict
    :type last_evaluation_timestamp: int
    :return: metrics_to_evaluate
    :rtype: dict
    """

    metrics_to_evaluate = {}
    training_data_dict_by_metric = {}
    training_to_evaluate = {}

    # @added 20221215 - Feature #4658: ionosphere.learn_repetitive_patterns
    # Added exclude_metrics list
    metrics = []
    base_names_with_ids = {}
    ids_with_base_names = {}
    try:
        base_names_with_ids = get_base_names_and_metric_ids(skyline_app)
        metrics = list(base_names_with_ids.keys())
    except Exception as err:
        logger.error('error :: %s :: get_base_names_and_metric_ids failed - %s' % (
            function_str, err))
    for metric in metrics:
        ids_with_base_names[base_names_with_ids[metric]] = metric

    include_metrics = exclude_metrics = []
    try:
        include_metrics, exclude_metrics = get_repetitive_patterns_metrics_list_to_evaluate(skyline_app, metrics)
    except Exception as err:
        logger.info('error :: %s :: get_repetitive_patterns_metrics_list_to_evaluate falied - %s' % (
            function_str, err))

    for t in list(training_data_dict.keys()):
        for metric in training_data_dict[t]:

            # @added 20221215 - Feature #4658: ionosphere.learn_repetitive_patterns
            # Added exclude_metrics list
            base_name = str(metric)
            if metric.startswith('labelled_metrics.'):
                metric_id_str = metric.replace('labelled_metrics.', '')
                metric_id = int(metric_id_str)
                try:
                    base_name = ids_with_base_names[metric_id]
                except:
                    pass
            if base_name in exclude_metrics:
                continue

            if metric not in training_data_dict_by_metric:
                training_data_dict_by_metric[metric] = {}
            training_data_dict_by_metric[metric][t] = training_data_dict[t][metric]
            if t >= last_evaluation_timestamp:
                # Do not add if already trained
                if training_data_dict[t][metric]['trained']:
                    continue
                if metric not in training_to_evaluate:
                    training_to_evaluate[metric] = {}
                training_to_evaluate[metric][t] = training_data_dict[t][metric]
    logger.info('%s :: there are %s training data set to consider for evaluation' % (
        function_str, str(len(training_to_evaluate))))
    # Only evalute metrics that have more than 2 training data sets present
    training_to_evaluate_counts = {}
    for metric in list(training_to_evaluate.keys()):
        count = len(list(training_data_dict_by_metric[metric].keys()))
        if count > 2:
            training_to_evaluate_counts[metric] = len(list(training_data_dict_by_metric[metric].keys()))
            metrics_to_evaluate[metric] = copy.deepcopy(training_data_dict_by_metric[metric])
    logger.info('%s :: there are %s metrics with multiple training data sets to check if the events align periodically' % (
        function_str, str(len(training_to_evaluate_counts))))
    return metrics_to_evaluate


def get_features_to_compare(training_to_evaluate):
    """
    Return a dictionary offeatures_to_compare.

    :param training_to_evaluate: the dictionary of training data to evaluate
    :type training_to_evaluate: dict
    :return: features_to_compare
    :rtype: dict
    """
    features_to_compare = {}
    for metric in list(training_to_evaluate.keys()):
        features_profile_sums = {}
        for t in list(training_to_evaluate[metric].keys()):
            features_profile_details_file = None
            fp_details = []
            training_data_dir = training_to_evaluate[metric][t]['training_data_dir']
            for f in training_to_evaluate[metric][t]['files']:
                if f.endswith('.fp.details.txt'):
                    features_profile_details_file = '%s/%s' % (training_to_evaluate[metric][t]['training_data_dir'], f)
                    break
            if not features_profile_details_file:
                try:
                    fp_csv, successful, fp_exists, fp_id, log_msg, traceback_format_exc, f_calc = calculate_features_profile(skyline_app, t, metric, 'training_data')
                except Exception as err:
                    logger.error(traceback.format_exc())
                    logger.error('error :: %s :: calculate_features_profile failed for metric: %s, timestamp: %s - %s' % (
                        function_str, metric, str(t), err))
                dir_list = os.listdir(training_data_dir)
                for f in dir_list:
                    if f.endswith('.fp.details.txt'):
                        features_profile_details_file = '%s/%s' % (training_data_dir, f)
            if os.path.isfile(features_profile_details_file):
                fp_details_str = None
                with open(features_profile_details_file, 'r') as f:
                    fp_details_str = f.read()
            try:
                fp_details = literal_eval(fp_details_str)
            except Exception as err:
                logger.error('error :: %s :: literal_eval failed for for fp_details for metric: %s - %s' % (
                    function_str, metric, err))
                fp_details = None
            if fp_details:
                try:
                    features_profile_sums[t] = fp_details[4]
                except Exception as err:
                    logger.error('error :: %s :: literal_eval failed for for fp_details for metric: %s - %s' % (
                        function_str, metric, err))
        if len(features_profile_sums) < 3:
            logger.info('%s :: %s has insufficient features sums to learn with, not evaluating' % (
                function_str, metric))
            continue
        features_to_compare[metric] = features_profile_sums
    return features_to_compare


def get_metrics_to_train(features_to_compare, timestamp):
    """
    Return a dictionary of metrics_to_train.

    :param features_to_compare: the dictionary of features_to_compare
    :type features_to_compare: dict
    :return: metrics_to_train
    :rtype: dict
    """
    metrics_to_train = {}
    metrics_to_train_key_data = {}
    for metric in list(features_to_compare.keys()):
        comparison_matrix = {
            'train': False,
            'training': {},
            'timestamps_to_train': [],
            'train_count': len(list(features_to_compare[metric].keys())),
        }
        training_timestamps = []
        for index, t in enumerate(list(features_to_compare[metric].keys())):
            comparison_matrix['training'][t] = {}
            features_sum = features_to_compare[metric][t]
            for i, it in [[index, t] for index, t in enumerate(list(features_to_compare[metric].keys()))]:
                similar_count = 0
                if i == index:
                    continue
                other_features_sum = features_to_compare[metric][it]
                percent_different = 100
                try:
                    percent_different = get_percent_different(features_sum, other_features_sum, True)
                except Exception as err:
                    logger.error('error :: %s :: failed to calculate percent_different for metric: %s - %s' % (
                        function_str, metric, err))
                    continue
                comparison_matrix['training'][t][it] = {'percent_different': percent_different, 'source_fp_sum': features_sum, 'fp_sum': other_features_sum}
                if float(percent_different) <= float(2):
                    comparison_matrix['training'][t][it]['similar'] = True
                    similar_count += 1
                    training_timestamps.append(t)
                    training_timestamps.append(it)
                else:
                    comparison_matrix['training'][t][it]['similar'] = False
        training_timestamps = list(set(training_timestamps))
        comparison_matrix['train_on_avg'] = False
        # If the average of the percentages is less than 4 train on
        # the timestamps where the percent_difference is less than 2
        precentages = []
        for t in list(comparison_matrix['training'].keys()):
            for tt in list(comparison_matrix['training'][t].keys()):
                precentages.append(comparison_matrix['training'][t][tt]['percent_different'])
        avg_percent_different = (sum(precentages) / len(precentages))
        comparison_matrix['avg_percent_different'] = avg_percent_different
        if len(training_timestamps) >= 3:
            comparison_matrix['train'] = True
        else:
            # If the average of the percentages is less than 4 train on
            # the timestamps where the percent_difference is less than 2
            if len(training_timestamps) > 0:
                if avg_percent_different <= 4:
                    comparison_matrix['train'] = True
                    comparison_matrix['train_on_avg'] = True
        comparison_matrix['timestamps_to_train'] = training_timestamps
        metrics_to_train_key_data[metric] = comparison_matrix
        if comparison_matrix['train']:
            metrics_to_train[metric] = comparison_matrix
    try:
        key = 'ionosphere.learn_repetitive_patterns.metrics_to_train_evaluation.%s' % str(timestamp)
        redis_conn_decoded.hset(key, timestamp, str(metrics_to_train_key_data))
        redis_conn_decoded.expire(key, (86400 * 3))
    except Exception as err:
        logger.error('error :: %s :: failed to hset ionosphere.learn_repetitive_patterns.metrics_to_train_evaluation in Redis - %s' % (
            function_str, err))
    return metrics_to_train


def ionosphere_learn_repetitive_patterns(timestamp):

    """
    Called by :class:`~skyline.skyline.learn_repetitive_patterns.ionosphere_learn_repetitive_patterns` to
    re-evaluate anomalies and such, like creating learning features profiles,
    when a human makes a features profile and the automated creation of learnt
    features profiles and learn features profiles.

    :param timestamp: timestamp at which learn was called
    :type timestamp: int
    :return: True or False
    :rtype: boolean

    """
    child_process_pid = os.getpid()
    logger.info('%s :: child_process_pid - %s' % (function_str, str(child_process_pid)))

    # Get a list of all the training_data and check the following:
    # Are there multiple metrics firing at certain times?
    # Are there groups of metrics that are aligned at these times?
    # If so compare the fp sums of each training data set for each metric that occurs multiple times and is aligned in a group

    last_evaluation_timestamp = 0
    try:
        last_evaluation_timestamp_data = redis_conn_decoded.get('ionosphere.learn_repetitive_patterns.last_evaluation_timestamp')
        if last_evaluation_timestamp_data:
            last_evaluation_timestamp = int(last_evaluation_timestamp_data)
    except Exception as err:
        logger.error('error :: %s :: failed to get ionosphere.learn_repetitive_patterns.last_evaluation_timestamp from Redis - %s' % (
            function_str, err))
    if last_evaluation_timestamp:
        next_evaluation = last_evaluation_timestamp + 3600
        if next_evaluation > timestamp:
            second_until = int(next_evaluation - timestamp)
            logger.info('%s :: next training_data evaluation scheduled to run in %s seconds, nothing to do' % (
                function_str, str(second_until)))
            return

    if not last_evaluation_timestamp:
        # Evaluate the last days data if not set
        last_evaluation_timestamp = timestamp - settings.FULL_DURATION
    else:
        last_evaluation_timestamp = last_evaluation_timestamp - 3600

    try:
        redis_conn_decoded.set('ionosphere.learn_repetitive_patterns.last_evaluation_timestamp', timestamp)
    except Exception as err:
        logger.error('error :: %s :: failed to set ionosphere.learn_repetitive_patterns.last_evaluation_timestamp from Redis - %s' % (
            function_str, err))

    training_data_dict = {}
    try:
        training_data_dict = get_training_data(skyline_app, metrics=None, namespaces=None, key_by='timestamp')
    except Exception as err:
        logger.error(traceback.format_exc())
        logger.error('error :: %s :: get_training_data failed - %s' % (function_str, err))

    if not training_data_dict:
        logger.info('%s :: no training_data_dict to evaluate, nothing to do' % function_str)
        return

    metrics_to_evaluate = {}
    try:
        metrics_to_evaluate = get_metrics_to_evaluate(training_data_dict, last_evaluation_timestamp)
    except Exception as err:
        logger.error(traceback.format_exc())
        logger.error('error :: %s :: get_metrics_to_evaluate failed - %s' % (function_str, err))

    if len(metrics_to_evaluate) == 0:
        logger.info('%s :: no metrics to evaluate' % function_str)
        return

    training_to_evaluate = {}
    try:
        training_to_evaluate = get_training_to_evaluate(metrics_to_evaluate)
    except Exception as err:
        logger.error('error :: %s :: get_training_to_evaluate failed - %s' % (
            function_str, err))

    logger.info('%s :: there are %s metrics with multiple training data sets that align periodically' % (
        function_str, str(len(training_to_evaluate))))

    if len(training_to_evaluate) == 0:
        logger.info('%s :: no metrics with periodically aligned events to evaluate' % function_str)
        return

    features_to_compare = {}
    try:
        features_to_compare = get_features_to_compare(training_to_evaluate)
    except Exception as err:
        logger.error('error :: %s :: get_features_to_compare failed - %s' % (
            function_str, err))

    if len(features_to_compare) == 0:
        logger.info('%s :: no metrics with features to compare' % function_str)
        return

    metrics_to_train = {}
    try:
        metrics_to_train = get_metrics_to_train(features_to_compare, timestamp)
    except Exception as err:
        logger.error('error :: %s :: get_metrics_to_train failed - %s' % (
            function_str, err))

    if len(metrics_to_train) == 0:
        logger.info('%s :: no metrics to train' % function_str)
        return

    logger.info('%s :: %s metrics to train based on repetitive patterns' % (
        function_str, str(len(metrics_to_train))))
    create_context = 'training_data'
    created_fp_ids = []
    for metric in list(metrics_to_train.keys()):

        # Do not hold up normal evaluation, run for 50 seconds and then set the
        # last evaluation time key appropriately and break so that as so as
        # Ionosphere is not busy with normal work the process will run again.
        now = int(time())
        if now > (timestamp + 50):
            new_last_evaluation_timestamp = last_evaluation_timestamp + 900
            logger.info('%s :: stopping due to run time and resetting ionosphere.learn_repetitive_patterns.last_evaluation_timestamp to %s' % (
                function_str, str(new_last_evaluation_timestamp)))
            try:
                redis_conn_decoded.set('ionosphere.learn_repetitive_patterns.last_evaluation_timestamp', new_last_evaluation_timestamp)
            except Exception as err:
                logger.error('error :: %s :: failed to set ionosphere.learn_repetitive_patterns.last_evaluation_timestamp in Redis - %s' % (
                    function_str, err))
            break

        logger.info('%s :: training metric: %s, training info: %s' % (
            function_str, metric, str(metrics_to_train[metric])))
        learn_parent_id = 0
        # Any generation of 0 or 1 is automatically set as validated
        # however this is handled in create_features_profile when the
        # ionosphere_job is 'learn_repetitive_patterns'
        generation = 2
        for ts in sorted(metrics_to_train[metric]['timestamps_to_train']):
            fetch_graphs = True
            for f in training_data_dict[ts][metric]['files']:
                if 'graphite_now' in f:
                    fetch_graphs = False
                    break
            if fetch_graphs:
                url = '%s/ionosphere?timestamp=%s&metric=%s&requested_timestamp=%s' % (
                    settings.SKYLINE_URL, str(ts), metric, str(ts))
                try:
                    if user and password:
                        r = requests.get(url, timeout=30, auth=(user, password), verify=verify_ssl)
                    else:
                        r = requests.get(url, timeout=30, verify=verify_ssl)
                    if r.status_code == 200:
                        logger.info('%s :: fetch graphs for features profile' % function_str)
                except Exception as err:
                    logger.error(traceback.format_exc())
                    logger.error('error :: %s :: failed get training to save graphs from %s - %s' % (
                        function_str, str(url), err))
            fp_in_successful = None
            try:
                fp_learn = False
                if learn_parent_id:
                    generation += 1
                ionosphere_job = 'learn_repetitive_patterns'
                slack_ionosphere_job = 'learn_repetitive_patterns'
                user_id = 1
                label = 'LEARNT - repetitive pattern'
                fp_id, fp_in_successful, fp_exists, fail_msg, traceback_format_exc = create_features_profile(skyline_app, ts, metric, create_context, ionosphere_job, learn_parent_id, generation, fp_learn, slack_ionosphere_job, user_id, label)
                if fp_exists:
                    logger.warning('warning :: %s :: failed to create a features profile for %s, %s as an fp already exists' % (
                        function_str, metric, str(ts)))
            except Exception as err:
                logger.error(traceback.format_exc())
                logger.error('error :: %s :: failed to create a features profile for %s, %s' % (
                    function_str, metric, str(timestamp)))
            if fp_in_successful is False:
                logger.error(traceback_format_exc)
                logger.error(fail_msg)
                logger.error('error :: %s :: failed to create a features profile for %s, %s' % (
                    function_str, metric, str(ts)))
                continue
            if not fp_exists:
                created_fp_ids.append(fp_id)
                logger.info('%s :: features profile with id %s created' % (function_str, str(fp_id)))
            learn_parent_id = int(fp_id)

    logger.info('%s :: created %s features profiles: %s' % (
        function_str, str(len(created_fp_ids)), str(created_fp_ids)))
    return
