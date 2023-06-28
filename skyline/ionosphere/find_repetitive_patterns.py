"""
find_repetitive_patterns.py
"""
import logging
import traceback
import os
import copy
from time import time
from ast import literal_eval
from shutil import rmtree
from timeit import default_timer as timer

import settings
from skyline_functions import get_redis_conn_decoded
from functions.ionosphere.get_training_data import get_training_data
from functions.database.queries.query_anomalies import get_anomalies_for_period
from functions.metrics.get_metric_id_from_base_name import get_metric_id_from_base_name
from functions.metrics.get_metric_ids_and_base_names import get_metric_ids_and_base_names
from skyline_functions import mkdir_p, write_data_to_file, get_graphite_metric
from features_profile import calculate_features_profile
from functions.numpy.percent_different import get_percent_different
from functions.victoriametrics.get_victoriametrics_metric import get_victoriametrics_metric
from functions.ionosphere.get_repetitive_patterns_metrics_list_to_evaluate import get_repetitive_patterns_metrics_list_to_evaluate

from functions.database.queries.get_apps import get_apps

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

redis_conn_decoded = get_redis_conn_decoded(skyline_app)


def get_features_profile_sum(until_timestamp, use_metric):
    """
    Determine the features profile sum for the metric
    """
    function_str = 'find_repetitive_patterns :: get_features_profile_sum'
    try:
        fp_csv, successful, fp_exists, fp_id, log_msg, traceback_format_exc, f_calc = calculate_features_profile(skyline_app, until_timestamp, use_metric, 'find_repetitive_patterns')
    except Exception as err:
        logger.error('error :: %s :: calculate_features_profile failed - %s' % (
            function_str, err))
    features_profile_details_file = None
    timeseries_dir = use_metric.replace('.', '/')
    training_data_dir = '%s/%s/%s' % (settings.SKYLINE_TMP_DIR, until_timestamp, timeseries_dir)
    dir_list = os.listdir(training_data_dir)
    for f in dir_list:
        if f.endswith('.fp.details.txt'):
            features_profile_details_file = '%s/%s' % (training_data_dir, f)
    fp_details_str = None
    if features_profile_details_file:
        if os.path.isfile(features_profile_details_file):
            with open(features_profile_details_file, 'r') as f:
                fp_details_str = f.read()
    fp_details = None
    if fp_details_str:
        try:
            fp_details = literal_eval(fp_details_str)
        except Exception as err:
            logger.error('error :: %s :: literal_eval failed - %s' % (
                function_str, err))
            fp_details = None
    features_profile_sum = None
    if fp_details:
        try:
            features_profile_sum = fp_details[4]
        except Exception as err:
            logger.error('error :: %s :: failed fp_details[4] - %s' % (
                function_str, err))
    return features_profile_sum


def get_features_profile_sums(metric, use_metric, anomalies, graphite):
    """
    For each anomaly generate the temporary training data and calculate the
    features profile sum for each
    """
    function_str = 'find_repetitive_patterns :: get_features_profile_sums'
    features_profile_sums = {}
    training_data_dirs = []
    timeseries_dir = use_metric.replace('.', '/')

    for a_id in list(anomalies.keys()):
        if anomalies[a_id]['full_duration'] < (604800 - 3600):
            continue
        start = timer()
        until_timestamp = anomalies[a_id]['anomaly_timestamp']
        from_timestamp = until_timestamp - (86400 * 7)
        training_data_dir = '%s/%s/%s' % (settings.SKYLINE_TMP_DIR, until_timestamp, timeseries_dir)
        try:
            mkdir_p(training_data_dir)
            # Append to the list of training dirs which is used to remove them at
            # the end of the process
            training_data_dirs.append(training_data_dir)
        except Exception as err:
            logger.error('error :: %s :: failed to create dir %s - %s' % (
                function_str, training_data_dir, err))

        anomaly_json = '%s/%s.json' % (training_data_dir, use_metric)
        metric_json_file_saved = None
        if graphite:
            metric_json_file_saved = get_graphite_metric(
                skyline_app, metric, from_timestamp,
                until_timestamp, 'json', anomaly_json)
        else:
            metric_data = {}
            metric_json_file_saved = get_victoriametrics_metric(
                skyline_app, use_metric, from_timestamp,
                until_timestamp, 'json', anomaly_json, metric_data)
        if not metric_json_file_saved:
            logger.error('error :: %s :: file not saved %s' % (
                function_str, anomaly_json))
            continue

        # To ensure that the features profile calculations are fast, if the
        # timeseries is long resample it.
        timeseries = []
        try:
            # Read the timeseries json file
            with open(anomaly_json, 'r') as f:
                raw_timeseries = f.read()
            timeseries_array_str = str(raw_timeseries).replace('(', '[').replace(')', ']')
            del raw_timeseries
            timeseries = literal_eval(timeseries_array_str)
        except:
            trace = traceback.format_exc()
            current_logger.error(trace)
            logger.error(
                'error: %s :: failed to read timeseries data from %s' % (log_context, anomaly_json))
            fail_msg = 'error: %s :: failed to read timeseries data from %s' % (log_context, anomaly_json)
        if len(timeseries) > 2000:
            try:
                downsampled_timeseries = downsample_timeseries(skyline_app, timeseries, 60, 600, 'mean', 'end')
            except Exception as err:
                errors.append([labelled_metric, 'downsample_timeseries', str(err)])
                downsampled_timeseries = list(timeseries)

        del timeseries
        anomaly_data = 'metric = \'%s\'\n' \
                       'full_duration = \'604800\'\n' \
            % (metric)
        anomaly_file = '%s/%s.txt' % (training_data_dir, use_metric)
        write_data_to_file(skyline_app, anomaly_file, 'w', anomaly_data)

        try:
            features_profile_sum = get_features_profile_sum(until_timestamp, use_metric)
        except Exception as err:
            logger.error('error :: %s :: get_features_profile_sum failed - %s' % (
                function_str, err))
        if not features_profile_sum:
            continue
        features_profile_sums[until_timestamp] = features_profile_sum

        # Only remove after training ... at the end
        # They need to be copied to the the IONOSPHERE_DATA_FOLDER so they
        # can be trained on
        #    if os.path.exists(training_data_dir):
        #        try:
        #            rmtree(training_data_dir)
        #        except Exception as err:
        #            print('error :: failed to rmtree - %s - %s' % (training_data_dir, err))
        timer_end = timer()
        logger.info('%s :: %s: %s (took %.6f seconds)' % (
            function_str, str(until_timestamp), str(features_profile_sum), (timer_end - start)))
    return features_profile_sums, training_data_dirs


def get_metrics_to_train(features_to_compare, metric):
    """
    For each anomaly generate the temporary training data and calculate the
    features profile sum for each
    """
    function_str = 'find_repetitive_patterns :: get_metrics_to_train'
    metrics_to_train = {}
    found_training = False
    metrics_to_train_key_data = {}
    for i_metric in list(features_to_compare.keys()):
        comparison_matrix = {
            'train': False,
            'training': {},
            'timestamps_to_train': [],
            'train_count': len(list(features_to_compare[i_metric].keys())),
        }
        training_timestamps = []
        for index, t in enumerate(list(features_to_compare[i_metric].keys())):
            comparison_matrix['training'][t] = {}
            features_sum = features_to_compare[i_metric][t]
            similar_count = 0
            t_training_timestamps = []
            t_training_timestamps = []
            for i, it in [[index, t] for index, t in enumerate(list(features_to_compare[i_metric].keys()))]:
                if t == it:
                    continue
                skip = False
                # skip if in the same week
                if it > t:
                    if (t + (86400 * 7)) > it:
                        skip = True
                if it < t:
                    if it > (t - (86400 * 7)):
                        skip = True
                if skip:
                    continue
                # similar_count = 0
                if i == index:
                    continue
                other_features_sum = features_to_compare[i_metric][it]
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
                    t_training_timestamps.append(t)
                    t_training_timestamps.append(it)
                else:
                    comparison_matrix['training'][t][it]['similar'] = False
            # If more than 3 are similar, train
            # But spread over what period...
            if similar_count >= 2:
                training_timestamps = training_timestamps + t_training_timestamps
                logger.info('%s :: %s similar to %s with %s' % (
                    function_str, str(t), str(similar_count), str(features_sum)))
        training_timestamps = list(set(training_timestamps))
        comparison_matrix['train_on_avg'] = False
        # If the average of the percentages is less than 4 train on
        # the timestamps where the percent_difference is less than 2
        percentages = []
        for t in list(comparison_matrix['training'].keys()):
            for tt in list(comparison_matrix['training'][t].keys()):
                percentages.append(comparison_matrix['training'][t][tt]['percent_different'])
        if percentages:
            avg_percent_different = (sum(percentages) / len(percentages))
            comparison_matrix['avg_percent_different'] = avg_percent_different
        else:
            avg_percent_different = 100
        if len(training_timestamps) >= 3:
            comparison_matrix['train'] = True
            found_training = True
        else:
            # If the average of the percentages is less than 4 train on
            # the timestamps where the percent_difference is less than 2
            if len(training_timestamps) > 0:
                if avg_percent_different <= 4:
                    comparison_matrix['train'] = True
                    comparison_matrix['train_on_avg'] = True
                    found_training = True
        comparison_matrix['timestamps_to_train'] = training_timestamps
        metrics_to_train_key_data[i_metric] = comparison_matrix
        # if comparison_matrix['train']:
        metrics_to_train[i_metric] = comparison_matrix
    return metrics_to_train, found_training


def get_anomalies_to_evaluate(include_metrics, metric_ids_with_base_names, anomalies_in_period):
    """
    Return anomalies that are for metrics in include_metrics
    """
    function_str = 'find_repetitive_patterns :: get_anomalies_to_evaluate'
    anomalies_to_evalute = {}

    apps = {}
    try:
        apps = get_apps(skyline_app)
    except Exception as err:
        logger.error('error :: %s :: get_apps failed - %s' % (
            function_str, err))
    boundary_id = 0
    try:
        boundary_id = apps['boundary']
    except Exception as err:
        logger.warning('warning :: %s :: failed to determine boundary app id - %s' % (
            function_str, err))

    for anomaly_id in list(anomalies_in_period.keys()):
        metric_id = anomalies_in_period[anomaly_id]['metric_id']
        base_name = None
        try:
            base_name = metric_ids_with_base_names[metric_id]
        except:
            base_name = None
        if base_name in include_metrics:
            # Do not include boundary anomalies
            if anomalies_in_period[anomaly_id]['app_id'] == boundary_id:
                continue

            anomalies_to_evalute[anomaly_id] = copy.deepcopy(anomalies_in_period[anomaly_id])
    return anomalies_to_evalute


def find_repetitive_patterns(metric, metric_id, anomaly_timestamp, plot=False):
    function_str = 'find_repetitive_patterns'
    found_training = False
    features_profile_sums = {}
    training_data_dirs = []
    metrics_to_train = {}

    use_metric = str(metric)
    graphite = True
    if '_tenant_id="' in metric:
        use_metric = 'labelled_metrics.%s' % str(metric_id)
        graphite = False

    until_timestamp = int(anomaly_timestamp)
    from_timestamp = until_timestamp - (86400 * 30)
    metric_ids = [metric_id]
    anomalies = get_anomalies_for_period(skyline_app, metric_ids, from_timestamp, until_timestamp)
    if not anomalies:
        logger.info('%s :: no anomalies found for %s' % (function_str, metric))
        return found_training, features_profile_sums, metrics_to_train, training_data_dirs

    logger.info('%s :: %s anomalies found for %s' % (
        function_str, str(len(anomalies)), metric))
    if len(anomalies) < 3:
        logger.info('%s :: insufficient anomalies data to evaluate for %s' % (function_str, metric))
        return found_training, features_profile_sums, metrics_to_train, training_data_dirs

    timeseries_dir = use_metric.replace('.', '/')

    try:
        features_profile_sums, training_data_dirs = get_features_profile_sums(metric, use_metric, anomalies, graphite)
    except Exception as err:
        logger.error('error :: %s :: get_features_profile_sums failed - %s' % (
            function_str, err))

    if not features_profile_sums:
        logger.info('%s :: no features_profile_sums calculated for %s' % (
            function_str, metric))
        return found_training, features_profile_sums, metrics_to_train, training_data_dirs

    logger.info('%s ::  %s features_profile_sums calculated' % (
        function_str, str(len(features_profile_sums))))

    features_to_compare = {}
    features_to_compare[use_metric] = features_profile_sums

    metrics_to_train_key_data = {}
    metrics_to_train, found_training = get_metrics_to_train(features_to_compare, metric)

    if found_training:
        logger.info('%s :: found %s similar patterns to train' % (
            function_str, str(len(metrics_to_train[use_metric]['timestamps_to_train']))))
        for t in sorted(metrics_to_train[use_metric]['timestamps_to_train']):
            print(metric, t, features_profile_sums[t])
            if plot:
                try:
                    raw_timeseries = []
                    training_data_dir = '%s/%s/%s' % (settings.SKYLINE_TMP_DIR, t, timeseries_dir)
                    anomaly_json = '%s/%s.json' % (training_data_dir, use_metric)

                    with open(anomaly_json, 'r') as f:
                        raw_timeseries = f.read()
                    timeseries_array_str = str(raw_timeseries).replace('(', '[').replace(')', ']')
                    del raw_timeseries
                    timeseries = literal_eval(timeseries_array_str)
                    plt.plot([item[0] for item in timeseries], [item[1] for item in timeseries])
                    plt.show()
                except Exception as err:
                    print('error :: %s :: failed to plot metric: %s - %s' % (
                        function_str, metric, err))
    else:
        print('No training found')
    return found_training, features_profile_sums, metrics_to_train, training_data_dirs


def ionosphere_find_repetitive_patterns(timestamp):

    """
    Called by :class:`~skyline.skyline.Ionosphere.spawn_learn_process` to
    re-evaluate anomalies and such, like creating learning features profiles,
    when a human makes a features profile and the automated creation of learnt
    features profiles and learn features profiles.

    :param timestamp: timestamp at which learn was called
    :type timestamp: int
    :return: True or False
    :rtype: boolean

    """
    function_str = 'ionosphere_find_repetitive_patterns'
    child_process_pid = os.getpid()
    logger.info('%s :: child_process_pid - %s' % (function_str, str(child_process_pid)))

    # Get the list of all metrics to find repetitive patterns on
    # Get a list of anomalies that are ready to check for the metrics which have find repetitive patterns enabled
    # Check the following:
    # Are there multiple metrics firing at certain times?
    # Are there groups of metrics that are aligned at these times?
    # If so compare the fp sums of each training data set for each metric that occurs multiple times and is aligned in a group

    last_evaluated_anomaly_timestamp = 0
    try:
        last_evaluated_anomaly_timestamp_data = redis_conn_decoded.get('ionosphere.find_repetitive_patterns.last_evaluated_anomaly_timestamp')
        if last_evaluated_anomaly_timestamp_data:
            last_evaluated_anomaly_timestamp = int(last_evaluated_anomaly_timestamp_data)
    except Exception as err:
        logger.error('error :: %s :: failed to get ionosphere.find_repetitive_patterns.last_evaluated_anomaly_timestamp from Redis - %s' % (
            function_str, err))

    if not last_evaluated_anomaly_timestamp:
        # Evaluate the last days data if not set
        last_evaluated_anomaly_timestamp = timestamp - settings.FULL_DURATION

    # Determine which anomalies need to be evaluated
    from_timestamp = int(last_evaluated_anomaly_timestamp)
    # Period after the normal ionosphere learn after period
    until_timestamp = int(time()) - (3600 + 900)
    anomalies_in_period = {}
    try:
        metric_ids = []
        anomalies_in_period = get_anomalies_for_period(skyline_app, metric_ids, from_timestamp, until_timestamp)
    except Exception as err:
        logger.error('error :: %s :: get_anomalies_for_period failed - %s' % (
            function_str, err))
    if not anomalies_in_period:
        logger.info('%s :: no anomalies in period from %s, until %s, nothing to do' % (
            function_str, str(from_timestamp), str(until_timestamp)))
        return

    # Get a list of all the metrics
    metrics = []
    try:
        metrics = list(redis_conn_decoded.smembers('aet.analyzer.metrics_manager.db.metric_names'))
    except Exception as err:
        logger.error('error :: %s :: smembers failed on aet.analyzer.metrics_manager.db.metric_names - %s' % (
            function_str, err))

    # Create the list of metrics to evalute based on IONOSPHERE_REPETITIVE_PATTERNS_INCLUDE
    # and IONOSPHERE_REPETITIVE_PATTERNS_EXCLUDE
    include_metrics, exclude_metrics = [], []
    try:
        include_metrics, exclude_metrics = get_repetitive_patterns_metrics_list_to_evaluate('ionosphere', metrics)
    except Exception as err:
        logger.error('error :: %s :: get_repetitive_patterns_metrics_list_to_evaluate failed - %s' % (
            function_str, err))
    if not include_metrics:
        logger.info('%s :: no metrics in include_metrics, nothing to do' % function_str)
        return

    logger.info('%s :: %s metrics found to include in find_repetitive_patterns, specifically excluding %s' % (
        function_str, str(len(include_metrics)), str(len(exclude_metrics))))

    metric_ids_with_base_names = {}
    try:
        metric_ids_with_base_names = get_metric_ids_and_base_names(skyline_app)
    except Exception as err:
        logger.error('error :: %s :: get_metric_ids_with_base_names failed - %s' % (
            function_str, str(err)))

    # Filter anomalies that are for include_metrics
    anomalies_to_evalute = {}
    try:
        anomalies_to_evalute = get_anomalies_to_evaluate(include_metrics, metric_ids_with_base_names, anomalies_in_period)
    except Exception as err:
        logger.error('error :: %s :: get_anomalies_to_evaluate failed - %s' % (
            function_str, str(err)))
    if not anomalies_to_evalute:
        logger.info('%s :: no anomalies to evaluate, nothing to do' % function_str)
        return

    # Iterate the anomalies and find repetitive patterns
    for anomaly_id in list(anomalies_to_evalute.keys()):
        if int(time()) > timestamp + 30:
            break
        found_training, features_profile_sums, metrics_to_train, training_data_dirs = False, {}, {}, []
        try:
            metric_id = anomalies_to_evalute[anomaly_id]['metric_id']
            anomaly_timestamp = anomalies_to_evalute[anomaly_id]['anomaly_timestamp']
            try:
                metric = metric_ids_with_base_names[metric_id]
            except:
                metric = None
            logger.info('%s :: processing %s for %s' % (
                function_str, str(anomaly_timestamp), metric))
            try:
                found_training, features_profile_sums, metrics_to_train, training_data_dirs = find_repetitive_patterns(metric, metric_id, anomaly_timestamp)
            except Exception as err:
                logger.error('error :: %s :: find_repetitive_patterns failed - %s' % (
                    function_str, str(err)))
        except Exception as err:
            logger.error('error :: %s :: find_repetitive_patterns failed - %s' % (
                function_str, str(err)))





    features_to_compare = {}
    try:
        features_to_compare = get_features_to_compare(training_to_evaluate)
    except Exception as err:
        logger.error('error :: %s :: get_training_to_evaluate failed - %s' % (
            function_str, err))

    if len(features_to_compare) == 0:
        logger.info('%s :: no metrics with features to compare' % function_str)
        return

    metrics_to_train = {}
    try:
        metrics_to_train = get_metrics_to_train(features_to_compare, timestamp)
    except Exception as err:
        logger.error('error :: %s :: get_training_to_evaluate failed - %s' % (
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
