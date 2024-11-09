"""
find_repetitive_patterns.py
"""
import logging
import traceback
import os
from datetime import datetime
from time import time
from ast import literal_eval
from shutil import rmtree
from timeit import default_timer as timer

import settings
from skyline_functions import (
    mkdir_p, write_data_to_file, get_graphite_metric, get_redis_conn_decoded)
from functions.database.queries.query_anomalies import get_anomalies_for_period
from functions.metrics.get_metric_id_from_base_name import get_metric_id_from_base_name
from functions.metrics.get_metric_ids_and_base_names import get_metric_ids_and_base_names
from features_profile import calculate_features_profile
from functions.numpy.percent_different import get_percent_different
from functions.victoriametrics.get_victoriametrics_metric import get_victoriametrics_metric
from functions.ionosphere.get_repetitive_patterns_metrics_list_to_evaluate import get_repetitive_patterns_metrics_list_to_evaluate
from functions.timeseries.load_timeseries_json import load_timeseries_json
from functions.timeseries.determine_data_frequency import determine_data_frequency
from functions.timeseries.downsample import downsample_timeseries
from functions.database.queries.get_fps_for_metric import get_fps_for_metric
from ionosphere_functions import create_features_profile
from functions.plots.plot_timeseries import plot_timeseries

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

# @added 20240308 - Feature #5304: ionosphere.find_repetitive_patterns
def prune_find_repetitive_patterns_dirs():

    pruned_dir_count = 0
    # Only prune once per hour
    recently_pruned = 0
    try:
        recently_pruned = redis_conn_decoded.exists('ionosphere.find_repetitive_patterns.recently_pruned')
    except Exception as err:
        logger.error('error :: exists failed on ionosphere.find_repetitive_patterns.recently_pruned, err: %s' % (
            err))
    if recently_pruned:
        logger.info('prune_find_repetitive_patterns_dirs :: recently pruned, nothing to do')
        return pruned_dir_count

    dir_path = '%s' % settings.SKYLINE_TMP_DIR
    until_timestamp_30_days_ago = int(time()) - ((86400 * 30) + 3600)
    try:
        for path, folders, files in os.walk(dir_path):
            for folder in folders[:]:
                timestamp_dir = False
                try:
                    timestamp_dir = int(folder)
                except:
                    continue
                folder_path = os.path.join(path, folder)
                if timestamp_dir < until_timestamp_30_days_ago:
                    try:
                        rmtree(folder_path)
                        logger.info('prune_find_repetitive_patterns_dirs :: removed %s' % (
                            folder_path))
                        pruned_dir_count += 1
                    except Exception as err:
                        logger.info(traceback.format_exc())
                        logger.error('error :: prune_find_repetitive_patterns_dirs :: rmtree failed on %s, err: %s' % (
                            folder_path, err))
            # Only do the parent path
            break
    except Exception as err:
        logger.error(traceback.format_exc())
        logger.error('error :: prune_find_repetitive_patterns_dirs :: failed, err: %s' % err)

    try:
        recently_pruned = redis_conn_decoded.setex('ionosphere.find_repetitive_patterns.recently_pruned', 3600, int(time()))
    except Exception as err:
        logger.error('error :: setex failed on ionosphere.find_repetitive_patterns.recently_pruned, err: %s' % (
            err))

    return pruned_dir_count


def update_last_processed_anomaly_timestamp(anomaly_timestamp):
    function_str = 'find_repetitive_patterns'
    timestamp_set = 0
    try:
        timestamp_set = redis_conn_decoded.set('ionosphere.find_repetitive_patterns.last_evaluated_anomaly_timestamp', anomaly_timestamp)
    except Exception as err:
        logger.error('error :: %s :: failed to set ionosphere.find_repetitive_patterns.last_evaluated_anomaly_timestamp, err: %s' % (
            function_str, err))
    return timestamp_set


# @added 20221013 - Feature #5304: ionosphere.find_repetitive_patterns
#                   Feature #4658: ionosphere.learn_repetitive_patterns
def find_repetitive_patterns(metric, anomaly_timestamp):
    function_str = 'find_repetitive_patterns'
    created_fp_ids = []
    found_training = False
    features_profile_sums = {}
    metrics_to_train = {}
    comparison_matrix = {
        'train': False,
        'training': {},
        'timestamps_to_train': [],
        'train_count': 0,
    }

    start = int(time())

    metric_id = get_metric_id_from_base_name(skyline_app, metric)
    use_metric = str(metric)
    graphite = True
    if '_tenant_id="' in metric:
        use_metric = 'labelled_metrics.%s' % str(metric_id) 
        graphite = False

    until_timestamp = int(anomaly_timestamp)
    from_timestamp = until_timestamp - (86400 * 30)
    datestr = datetime.utcfromtimestamp(until_timestamp).strftime('%Y-%m-%d %H:%M:%S')

    metric_ids = [metric_id]
    anomalies = get_anomalies_for_period(skyline_app, metric_ids, from_timestamp, until_timestamp)
    logger.info('%s :: %s anomalies found in 30 days since %s (%s) for metric_id: %s, metric: %s' % (
        function_str, str(len(anomalies)), datestr, str(until_timestamp), str(metric_id), metric))
    if not anomalies:
        return created_fp_ids
    if len(anomalies) < 3:
        logger.info('%s :: insufficient anomalies data to evaluate' % function_str)
        update_last_processed_anomaly_timestamp(anomaly_timestamp)
        return created_fp_ids

    # @added 20240306 - Feature #5304: ionosphere.find_repetitive_patterns
    # Remove anomalies that have a timestamp close to an existing
    # features_profile, these can be considered as trained and will prevent
    # lots of training being created that covers the same period
    fps_dict = {}
    try:
        fps_dict = get_fps_for_metric(skyline_app, metric_id)
    except Exception as err:
        logger.error('error :: %s :: get_fps_for_metric failed, err: %s' % (
            function_str, str(err)))
    fp_timestamps = []
    if fps_dict:
        for key, item in fps_dict.items():
            if item['enabled']:
                fp_timestamps.append(item['anomaly_timestamp'])
        if fp_timestamps:
            fp_timestamps = list(set(fp_timestamps))
    trained_period_anomalies_to_remove = []
    for i_anomaly_id, item in anomalies.items():
        trained_anomaly = False
        i_anomaly_timestamp = item['anomaly_timestamp']
        for fp_timestamp in fp_timestamps:
            before_fp = fp_timestamp - 86400
            after_fp = fp_timestamp + 14400
            if i_anomaly_timestamp > before_fp:
                if i_anomaly_timestamp < after_fp:
                    trained_anomaly = True
                    break
        if trained_anomaly:
            trained_period_anomalies_to_remove.append(i_anomaly_id)
    logger.info('%s :: removing %s anomalies from consideration as they are in the same period as existing feature profiles' % (
        function_str, str(len(trained_period_anomalies_to_remove))))
    for i_anomaly_id in trained_period_anomalies_to_remove:
        del anomalies[i_anomaly_id]

    # @added 20240311 - Feature #5304: ionosphere.find_repetitive_patterns
    # Handle sparsely populated metrics
    populated_sparse_timeseries = {}

    timeseries_dir = use_metric.replace('.', '/')
    features_profile_sums = {}
    for a_id in list(anomalies.keys()):
        if anomalies[a_id]['full_duration'] < (604800 - 3600):
            continue

        if int(time()) > start + 55:
            logger.info('%s :: not processing any further anomalies, max runtime reached' % function_str)
            return created_fp_ids

        start = timer()
        until_timestamp = anomalies[a_id]['anomaly_timestamp']
        datestr = datetime.utcfromtimestamp(until_timestamp).strftime('%Y-%m-%d %H:%M:%S')

        from_timestamp = until_timestamp - (86400 * 7)
        training_data_dir = '%s/%s/%s' % (settings.SKYLINE_TMP_DIR, until_timestamp, timeseries_dir)
        if not os.path.exists(training_data_dir):
            mkdir_p(training_data_dir)
        anomaly_json = '%s/%s.json' % (training_data_dir, use_metric)
        if not os.path.isfile(anomaly_json):
            if graphite:
                try:
                    metric_json_file_saved = get_graphite_metric(
                        skyline_app, metric, from_timestamp,
                        until_timestamp, 'json', anomaly_json)
                except Exception as err:
                    logger.error('error :: %s :: get_graphite_metric failed, err: %s' % (
                        function_str, str(err)))
            else:
                try:
                    metric_data = {}
                    metric_json_file_saved = get_victoriametrics_metric(
                        skyline_app, use_metric, from_timestamp,
                        until_timestamp, 'json', anomaly_json, metric_data)
                except Exception as err:
                    logger.error('error :: %s :: get_victoriametrics_metric failed, err: %s' % (
                        function_str, str(err)))
            if os.path.isfile(anomaly_json):
                logger.info('%s :: retrieved timeseries data for anomaly_timestamp: %s (%s), %s' % (
                    function_str, str(until_timestamp), datestr, metric_json_file_saved))

        timeseries = []
        try:
            timeseries = load_timeseries_json(skyline_app, anomaly_json)
        except Exception as err:
            logger.error('error :: %s :: load_timeseries_json failed on %s, err: %s' % (
                function_str, anomaly_json, err))
        original_timeseries = list(timeseries)
        downsampled_timeseries = []
        if timeseries:
            downsampled_anomaly_json = '%s/%s.downsampled.json' % (training_data_dir, use_metric)
            if not os.path.isfile(downsampled_anomaly_json):
                resolution = 0
                try:
                    resolution = determine_data_frequency(skyline_app, timeseries, False)
                except Exception as err:
                    logger.error('error :: %s :: determine_data_frequency failed, err: %s' % (function_str, err))
                if resolution < 600:
                    logger.info('%s :: timeseries resolution: %s, downsampling for %s' % (
                        function_str, str(resolution), metric))
                    try:
                        downsampled_timeseries = downsample_timeseries(skyline_app, timeseries, resolution, 600, 'mean', 'end')
                    except Exception as err:
                        logger.error('error :: %s :: downsample_timeseries failed, err: %s' % (function_str, err))
                if downsampled_timeseries:
                    timeseries = list(downsampled_timeseries)
                    timeseries_json = str(downsampled_timeseries).replace('[', '(').replace(']', ')')
                    try:
                        write_data_to_file(skyline_app, anomaly_json, 'w', timeseries_json)
                    except Exception as err:
                        logger.error('error :: %s :: write_data_to_file failed, err: %s' % (function_str, err))
                    try:
                        write_data_to_file(skyline_app, downsampled_anomaly_json, 'w', timeseries_json)
                    except Exception as err:
                        logger.error('error :: %s :: write_data_to_file failed, err: %s' % (function_str, err))

        if len(timeseries) < 360:
            # rmtree(training_data_dir)
            logger.info('%s :: timeseries too short to use, len(timeseries): %s, metric: %s' % (
                function_str, str(len(timeseries)), metric))
            continue

        # @added 20240311 - Feature #5304: ionosphere.find_repetitive_patterns
        # Handle sparsely populated metrics
        populated_sparse_metric = False
        if len(timeseries) < 800:
            first_timestamp = timeseries[0][0]
            if first_timestamp < (from_timestamp + (3600 * 2)):
                last_timestamp = timeseries[-1][0]
                if last_timestamp > (until_timestamp - (3600 * 2)):
                    populated_sparse_metric = True
                    populated_sparse_timeseries[until_timestamp] = {
                        'values': len(timeseries),
                    }

        if len(timeseries) < 800 and not populated_sparse_metric:
            # rmtree(training_data_dir)
            logger.info('%s :: timeseries too short to use, len(timeseries): %s, metric: %s' % (
                function_str, str(len(timeseries)), metric))
            continue
        if len(set(item[1] for item in timeseries[-settings.MAX_TOLERABLE_BOREDOM:])) == settings.BOREDOM_SET_SIZE:
            # rmtree(training_data_dir)
            logger.info('%s :: timeseries boring not using, metric: %s' % (
                function_str, metric))
            continue
                        
        anomaly_data = 'metric = \'%s\'\n' \
                       'full_duration = \'604800\'\n' \
            % (metric)
        anomaly_file = '%s/%s.txt' % (training_data_dir, use_metric)
        if not os.path.isfile(anomaly_file):
            write_data_to_file(skyline_app, anomaly_file, 'w', anomaly_data)
        fp_data_existed = True
        features_profile_details_file = '%s/%s.%s.fp.details.txt' % (
            training_data_dir, str(until_timestamp), use_metric)
        if not os.path.isfile(features_profile_details_file):
            try:
                fp_csv, successful, fp_exists, fp_id, log_msg, traceback_format_exc, f_calc = calculate_features_profile(skyline_app, until_timestamp, use_metric, 'adhoc')
            except Exception as err:
                logger.error('error :: %s :: calculate_features_profile failed - %s' % (
                    function_str, err))
            if not successful:
                continue
            fp_data_existed = False
            features_profile_details_file = None
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
        fp_sum = None
        if fp_details:
            try:
                fp_sum = float(fp_details[4])
                features_profile_sums[until_timestamp] = fp_sum
            except Exception as err:
                logger.error('error :: %s :: failed to determine fp_sum from fp_details, err: %s' % (
                    function_str, err))
                continue
        timer_end = timer()
        if anomaly_timestamp == until_timestamp:
            logger.info('%s :: %s: %s, %s (PARENT ANOMALY SUM), len(original_timeseries): %s, len(downsampled_timeseries): %s, fp_data_existed: %s, (took %.6f seconds)' % (
                function_str, str(until_timestamp), datestr, str(fp_sum),
                str(len(original_timeseries)), str(len(downsampled_timeseries)),
                str(fp_data_existed), (timer_end - start)))
        else:
            logger.info('%s :: %s: %s, %s, len(original_timeseries): %s, len(downsampled_timeseries): %s, fp_data_existed: %s, (took %.6f seconds)' % (
                function_str, str(until_timestamp), datestr, str(fp_sum), str(len(original_timeseries)),
                str(len(downsampled_timeseries)), str(fp_data_existed), (timer_end - start)))

    logger.info('%s :: features_profile_sums calculated for %s anomalies' % (
        function_str, str(len(features_profile_sums))))

    # @added 20240311 - Feature #5304: ionosphere.find_repetitive_patterns
    # Handle sparsely populated metrics
    if len(populated_sparse_timeseries) > 0:
        logger.info('%s :: checking the avg length of %s sparsely populated timeseries, %s' % (
            function_str, str(len(populated_sparse_timeseries)),
            str(populated_sparse_timeseries)))
        timeseries_lengths = []
        for until_timestamp, item in populated_sparse_timeseries.items():
            timeseries_lengths.append(item['values'])
        avg_length = int(sum(timeseries_lengths) / len(timeseries_lengths))
        ten_percent = int(avg_length * 0.1) 
        avg_length = avg_length - ten_percent
        logger.info('%s :: avg_length (-10%%): %s' % (
            function_str, str(avg_length)))
        for until_timestamp, item in populated_sparse_timeseries.items():
            if item['values'] < avg_length:
                try:
                    del features_profile_sums[until_timestamp]
                    logger.info('%s :: %s too short removing from features_profile_sums, less than avg_length: %s, len(timeseries): %s, metric: %s' % (
                        function_str, str(until_timestamp), str(avg_length),
                        str(item['values']), metric))
                except:
                    pass

    if not features_profile_sums:
        logger.info('%s :: no features_profile_sums calculated' % function_str)
        update_last_processed_anomaly_timestamp(anomaly_timestamp)
        return created_fp_ids

    logger.info('%s :: comparing features_profile_sums for %s anomalies' % (
        function_str, str(len(features_profile_sums))))

    features_to_compare = {}
    features_to_compare[use_metric] = features_profile_sums

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
                #similar_count = 0
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
            # If more than 3 are similar, train???
            # But spread over what period...
            if similar_count >= 2:
                training_timestamps = training_timestamps + t_training_timestamps
                logger.info('%s :: %s similar to %s, with %s' % (
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
        #if comparison_matrix['train']:
        metrics_to_train[i_metric] = comparison_matrix

    # @added 20240309 - Feature #5304: ionosphere.find_repetitive_patterns
    # Deduplicate training found within the same relative period
    if found_training:
        timestamps_to_deduplicate = []
        last_timestamp_to_train = None
        timestamps_to_train = sorted(comparison_matrix['timestamps_to_train'])
        for timestamp_to_train in timestamps_to_train:
            if not last_timestamp_to_train:
                last_timestamp_to_train = int(timestamp_to_train)
                continue            
            if last_timestamp_to_train > (timestamp_to_train - (3600 * 12)):
                timestamps_to_deduplicate.append(last_timestamp_to_train)
                last_timestamp_to_train_datestr = datetime.utcfromtimestamp(last_timestamp_to_train).strftime('%Y-%m-%d %H:%M:%S')
                timestamp_to_train_datestr = datetime.utcfromtimestamp(timestamp_to_train).strftime('%Y-%m-%d %H:%M:%S')
                logger.info('%s :: deduplicating training for %s as close to %s' % (
                    function_str, last_timestamp_to_train_datestr, timestamp_to_train_datestr))
            last_timestamp_to_train = int(timestamp_to_train)
        for timestamp_to_deduplicate in timestamps_to_deduplicate:
            del comparison_matrix['training'][timestamp_to_deduplicate]
            comparison_matrix['timestamps_to_train'].remove(timestamp_to_deduplicate)
        if len(comparison_matrix['timestamps_to_train']) < 2:
            found_training = False
            logger.info('%s :: no training after deduplicating training' % function_str)

    if found_training:
        learn_parent_id = 0
        generation = 2
        logger.info('%s :: found %s similar training_data sets' % (
            function_str, str(len(metrics_to_train[i_metric]['timestamps_to_train']))))
        for t in sorted(metrics_to_train[i_metric]['timestamps_to_train']):
            datestr = datetime.utcfromtimestamp(t).strftime('%Y-%m-%d %H:%M:%S')
            logger.info('%s :: creating features profile for training_data with fp_sum: %s, for anomaly_timestamp: %s (%s)' % (
                function_str, str(features_profile_sums[t]), str(until_timestamp), datestr))
            try:
                training_data_dir = '%s/%s/%s' % (settings.SKYLINE_TMP_DIR, t, timeseries_dir)
                anomaly_json = '%s/%s.json' % (training_data_dir, use_metric)
                timeseries = []
                try:
                    timeseries = load_timeseries_json(skyline_app, anomaly_json)
                except Exception as err:
                    logger.error('error :: %s :: load_timeseries_json failed on %s, err: %s' % (
                        function_str, anomaly_json, err))
                title = '%s\nLEARNT find_repetitive_patterns - %s' % (str(use_metric), datestr)
                plot_parameters = {
                    'title': title, 'line_color': 'blue', 'bg_color': 'black',
                    'figsize': (8, 4)
                }
                save_to_file = '%s/%s.graphite_now.168h.png' % (training_data_dir, use_metric)
                try:
                    success, graph_image = plot_timeseries(
                        skyline_app, metric, timeseries, save_to_file,
                        plot_parameters)
                    if success:
                        logger.info('%s :: plotted %s' % (function_str, str(graph_image)))
                except Exception as err:
                    logger.error('error :: %s :: plot_timeseries failed - %s' % (
                        function_str, err))
            except Exception as err:
                logger.error('error :: %s :: failed to plot metric: %s - %s' % (
                    function_str, metric, err))
            # @added 20240311 - Feature #5304: ionosphere.find_repetitive_patterns
            # Add a 30 day graph
            from_timestamp = t - (86400 * 30)
            until_timestamp = int(t)
            training_data_dir = '%s/%s/%s' % (settings.SKYLINE_TMP_DIR, t, timeseries_dir)
            graph_image_file = '%s/%s.graphite_now.720h.png' % (training_data_dir, use_metric)
            if not os.path.isfile(graph_image_file):
                if graphite:
                    try:
                        graph_image = get_graphite_metric(
                            skyline_app, metric, from_timestamp, until_timestamp, 'image',
                            graph_image_file)
                    except Exception as err:
                        logger.error('error :: %s :: get_graphite_metric failed - %s' % (
                            function_str, err))
                else:
                    title = '30 days (at 10 minutes)'
                    plot_parameters = {
                        'title': title, 'line_color': 'blue',
                        'bg_color': 'white', 'figsize': (8, 4)
                    }
                    try:
                        metric_data = {}
                        graph_image = get_victoriametrics_metric(
                            skyline_app, use_metric, from_timestamp,
                            until_timestamp, 'image', graph_image_file,
                            metric_data, plot_parameters)
                    except Exception as err:
                        logger.error('error :: %s :: get_victoriametrics_metric failed - %s' % (
                            function_str, err))

            # CREATE FEATURES_PROFILE
            logger.info('%s :: creating features profile for %s, anomaly_timestamp: %s (%s)' % (
                function_str, use_metric, str(t), datestr))
            fp_in_successful = None
            create_context = 'training_data'
            try:
                fp_learn = False
                if learn_parent_id:
                    generation += 1
                ionosphere_job = 'find_repetitive_patterns'
                slack_ionosphere_job = 'find_repetitive_patterns'
                user_id = 1
                label = 'LEARNT - repetitive pattern'
                fp_id, fp_in_successful, fp_exists, fail_msg, traceback_format_exc = create_features_profile(skyline_app, t, metric, create_context, ionosphere_job, learn_parent_id, generation, fp_learn, slack_ionosphere_job, user_id, label)
                if fp_exists:
                    logger.warning('warning :: %s :: failed to create a features profile for %s, %s as an fp already exists' % (
                        function_str, metric, str(t)))
            except Exception as err:
                logger.error(traceback.format_exc())
                logger.error('error :: %s :: failed to create a features profile for %s, %s, err: %s' % (
                    function_str, metric, str(t), err))
            if fp_in_successful is False:
                logger.error(traceback_format_exc)
                logger.error(fail_msg)
                logger.error('error :: %s :: failed to create a features profile for %s, %s' % (
                    function_str, metric, str(t)))
                continue
            if not fp_exists:
                created_fp_ids.append(fp_id)
                logger.info('%s :: features profile with id %s created for %s' % (
                    function_str, str(fp_id), metric))
            learn_parent_id = int(fp_id)
        logger.info('%s :: created %s learned feature profiles, created_fp_ids: %s' % (
            function_str, str(len(created_fp_ids)), str(created_fp_ids)))
    else:
        logger.info('%s :: no training found' % function_str)

    update_last_processed_anomaly_timestamp(anomaly_timestamp)

    return created_fp_ids


# @added 20221013 - Feature #5304: ionosphere.find_repetitive_patterns
#                   Feature #4658: ionosphere.learn_repetitive_patterns
def ionosphere_find_repetitive_patterns(timestamp):

    """
    Called by :class:`~skyline.skyline.Ionosphere.spawn_learn_process` to
    re-evaluate anomalies for a metric and determine if the current anomaly or
    previous anomalies in the last month are similar to other anomalies in last
    month, if there are more than 2, LEARNT features profiles are created for
    them.  To ensure that only the minimium number of features profiles are
    learnt a number of conditions are set:
    1. any anomalies that occur in the period of any existing features
    profile are excluded from consideration.
    2. any similar training data sets that are are found that occur less than 12
    hours apart are deduplicated and the latest of those is used only
    3. , the  and any that are
    up to 12 hours older are discarded.  Without these conditionals, there tends
    to be excessive features profile found to learn.

    Although it is possible to iterate over all the anomalies and check them
    all at the same time, here the function is doing one metric per run.
    repetitive learning 

    :param timestamp: timestamp at which learn was called
    :type timestamp: int
    :return: True or False
    :rtype: boolean

    """
    function_str = 'ionosphere_find_repetitive_patterns'
    child_process_pid = os.getpid()
    logger.info('%s :: child_process_pid - %s' % (function_str, str(child_process_pid)))
    start = time()

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
        # Evaluate the last 3 days of anomalies
        last_evaluated_anomaly_timestamp = timestamp - (settings.FULL_DURATION * 3)

    # Create the list of metrics to evalute based on the
    # IONOSPHERE_REPETITIVE_PATTERNS_INCLUDE,
    # IONOSPHERE_REPETITIVE_PATTERNS_EXCLUDE and external_settings.  This
    # function can take a number of seconds depending on the number of metrics
    # and number of exclude patterns.
    include_metrics = []
    exclude_metrics = []
    try:
        # Yes using an empty list of metrics the function itself then fetches them
        include_metrics, exclude_metrics = get_repetitive_patterns_metrics_list_to_evaluate('ionosphere', include_metrics)
    except Exception as err:
        logger.error('error :: %s :: get_repetitive_patterns_metrics_list_to_evaluate failed, err: %s' % (
            function_str, err))
    logger.info('%s :: %s include_metrics and %s exclude_metrics determined' % (
        function_str, str(len(include_metrics)), str(len(exclude_metrics))))

    if not include_metrics:
        logger.info('%s :: no metrics in include_metrics, nothing to do' % function_str)
        return

    # @modified 20241106 - Task #5526: Build v5.0.0 and upgrade deps
    # bandit - B108:hardcoded_tmp_directory - Probable insecure usage of temp file/directory
    if os.path.isfile('/tmp/skyline/find_repetitive_patterns.stop'):  # nosec B108
        logger.info('%s :: /tmp/skyline/find_repetitive_patterns.stop found, returning' % function_str)
        return

    metric_ids_and_base_names = {}
    try:
        metric_ids_and_base_names = get_metric_ids_and_base_names(skyline_app)
    except Exception as err:
        logger.error('error :: %s :: get_metric_ids_and_base_names failed, err: %s' % (
            function_str, err))
    base_names_and_metric_ids = {}
    for metric_id, base_name in metric_ids_and_base_names.items():
        base_names_and_metric_ids[base_name] = metric_id

    ionosphere_untrainable = []
    errors = []
    try:
        ionosphere_untrainable_metrics = list(redis_conn_decoded.smembers('ionosphere.untrainable_metrics'))
    except Exception as err:
        logger.error('error :: %s :: smembers failed on ionosphere.untrainable_metrics, err: %s' % (
            function_str, err))    
    for item in ionosphere_untrainable_metrics:
        untrainable_metric = None
        try:
            ionosphere_untrainable_metric_list = literal_eval(item)
            untrainable_metric = ionosphere_untrainable_metric_list[0]
            ionosphere_untrainable.append(untrainable_metric)
        except Exception as err:
            errors.append(['failed to determine untrainable metric from literal_eval', str(item), err])
        if untrainable_metric.startswith('labelled_metrics.'):
            try:
                untrainable_metric_id = int(untrainable_metric.split('.')[1])
                untrainable_metric = metric_ids_and_base_names[untrainable_metric_id]
                ionosphere_untrainable.append(untrainable_metric)
            except Exception as err:
                errors.append(['failed to determine untrainable metric', untrainable_metric, err])
    if errors:
        logger.warning('warning :: %s :: determine untrainable metric failed with %s errors, sample: %s' % (
            function_str, str(len(errors)), str(errors[0])))
    new_untrainable_metrics = []
    try:
        new_untrainable_metrics = list(redis_conn_decoded.hkeys('metrics_manager.untrainable_new_metrics'))
    except Exception as err:
        logger.error('error :: %s :: smembers failed on metrics_manager.untrainable_new_metrics, err: %s' % (
            function_str, err))    
    ionosphere_untrainable = set(ionosphere_untrainable + new_untrainable_metrics)
    logger.info('%s :: determined %s ionosphere_untrainable metrics to exclude' % (
        function_str, str(len(ionosphere_untrainable))))

    # THERE COULD BE AN ISSUE HERE WITH THE USE OF EXCLUDES IN EXTERNAL_SETTINGS
    # AS THOSE RULES MAY NOT INCLUDE THE NAMESPACE IN THE REGEX PATTERNS.
    # This means that the an external_settings namespace rule may override and
    # settings exclude.  However due to the primary_filter_keys used in
    # get_repetitive_patterns_metrics_list_to_evaluate, this may not be an issue
    # hard to say due the primary_filter_keys method being quite complicated but
    # it appears that it does as it adds external_settings as namespace_dot and
    # namespace_quote to the IONOSPHERE_REPETITIVE_PATTERNS_ dicts.
    learn_repetitive_pattern_metric_ids = []
    errors = []
    for metric in include_metrics:
        if metric in ionosphere_untrainable:
            continue
        try:
            learn_repetitive_pattern_metric_ids.append(base_names_and_metric_ids[metric])
        except Exception as err:
            errors.append(['falied to look up metric in base_names_and_metric_ids', metric, err])
    if errors:
        logger.warning('warning :: %s :: failed to find %s metrics in base_names_and_metric_ids, sample: %s' % (
            function_str, str(len(errors)), str(errors[0])))
    logger.info('%s :: determined %s metric_ids for include_metrics' % (
        function_str, str(len(learn_repetitive_pattern_metric_ids))))

    # Determine which anomalies need to be evaluated
    from_timestamp = int(last_evaluated_anomaly_timestamp) + 1
    # Period after the normal ionosphere learn after period
    until_timestamp = int(time()) - (3600 + 900)
    anomalies_in_period = {}
    try:
        anomalies_in_period = get_anomalies_for_period(skyline_app, learn_repetitive_pattern_metric_ids, from_timestamp, until_timestamp)
    except Exception as err:
        logger.error('error :: %s :: get_anomalies_for_period failed - %s' % (
            function_str, err))
    # Only use mirage and ionosphere anomalies
    remove_keys = []
    for key, item in anomalies_in_period.items():
        if item['app'] not in ['ionosphere', 'mirage']:
            remove_keys.append(key)
    for key in remove_keys:
        del anomalies_in_period[key]

    manage_find_repetitive_patterns_dir = False
    if len(anomalies_in_period) == 0:
        logger.info('%s :: no anomalies in period from %s, until %s, managing find_repetitive_patterns training dirs' % (
            function_str, str(from_timestamp), str(until_timestamp)))
        manage_find_repetitive_patterns_dir = True
    if manage_find_repetitive_patterns_dir:
        # Only prune once per hour
        recently_pruned = 0
        try:
            recently_pruned = redis_conn_decoded.exists('ionosphere.find_repetitive_patterns.recently_pruned')
        except Exception as err:
            logger.error('error :: %s :: exists failed on ionosphere.find_repetitive_patterns.recently_pruned, err: %s' % (
                function_str, err))
        if recently_pruned:
            logger.info('%s :: prune_find_repetitive_patterns_dirs :: recently pruned, nothing to do' % (
                function_str))
            return
        try:
            pruned_dir_count = prune_find_repetitive_patterns_dirs()
            logger.info('%s :: prune_find_repetitive_patterns_dirs removed %s old dirs' % (
                function_str, str(pruned_dir_count)))
        except Exception as err:
            logger.error('error :: %s :: get_anomalies_for_period failed - %s' % (
                function_str, err))
        logger.info('%s :: complete, took %s seconds' % (
            function_str, str((time() - start))))
        return

    logger.info('%s :: %s mirage and ionosphere anomalies in period found from %s, until %s' % (
        function_str, str(len(anomalies_in_period)), str(from_timestamp), str(until_timestamp)))

    checked_metric_anomalies = 0
    created_fps = {}
    # Iterate the anomalies and find repetitive patterns
    for anomaly_id in list(anomalies_in_period.keys()):
        if int(time()) > timestamp + 55:
            logger.info('%s :: not processing any further anomalies, max runtime reached' % function_str)
            break

        # @modified 20241106 - Task #5526: Build v5.0.0 and upgrade deps
        # bandit - B108:hardcoded_tmp_directory - Probable insecure usage of temp file/directory
        if os.path.isfile('/tmp/skyline/find_repetitive_patterns.stop'):  # nosec B108
            logger.info('%s :: /tmp/skyline/find_repetitive_patterns.stop found, stopping' % function_str)
            break

        try:
            metric_id = anomalies_in_period[anomaly_id]['metric_id']
            anomaly_timestamp = anomalies_in_period[anomaly_id]['anomaly_timestamp']
            try:
                metric = metric_ids_and_base_names[metric_id]
            except:
                metric = None
            logger.info('%s :: processing %s for %s' % (
                function_str, str(anomaly_timestamp), metric))
        except Exception as err:
            logger.error('error :: %s :: failed to determine details from anomaly_id: %s, err: %s' % (
                function_str, str(anomaly_id), err))
            continue
        checked_metric_anomalies += 1
        created_fp_ids = []
        try:
            created_fp_ids = find_repetitive_patterns(metric, anomaly_timestamp)
            if created_fp_ids:
                created_fps[metric] = created_fp_ids
        except Exception as err:
            logger.error('error :: %s :: find_repetitive_patterns failed for metric: %s, anomaly_timestamp: %s, err: %s' % (
                function_str, metric, str(anomaly_timestamp), err))

        # @added 20240402 - Feature #5318: motif_annihilation
        # If nothing was learnt add the anomaly for motif_annihilation to
        # attempt to learn
        if len(created_fp_ids) == 0:

            done_key = 'ionosphere.find_repetitive_patterns.motif_annihilation.%s' % str(anomaly_id)
            done_exists = False
            try:
                done_exists = redis_conn_decoded.exists(done_key)
            except Exception as err:
                logger.error('error :: %s :: failed to exists %s Redis key, err: %s' % (
                    function_str, done_key, err))
            if not done_exists:
                key = str(time())
                annihilation_data = {
                    'metric_id': metric_id, 'metric': metric,
                    'anomaly_id': anomaly_id,
                    'anomaly_timestamp': anomaly_timestamp
                }
                logger.info('%s :: adding motif_annihilation work for anomaly_id: %s' % (
                    function_str, str(anomaly_id)))
                try:
                    redis_conn_decoded.hset('ionosphere.find_repetitive_patterns.motif_annihilation.work', key, str(annihilation_data))
                except Exception as err:
                    logger.error('error :: could not add data to Redis hash ionosphere.find_repetitive_patterns.motif_annihilation.work, err: %s' % err)
            else:
                logger.info('%s :: motif_annihilation done for anomaly_id: %s, not adding' % (
                    function_str, str(anomaly_id)))
 
    if created_fps:
        logger.info('%s :: created feature profiles: %s' % (
            function_str, str(created_fps)))

    logger.info('%s :: complete - checked %s metric anomalies, took %s seconds' % (
        function_str, str(checked_metric_anomalies), str(time() - start)))
    return
