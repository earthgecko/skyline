from __future__ import division
import logging
from os import path, walk
import os
import datetime as dt
from time import time
from sys import version_info
from ast import literal_eval
import re
import traceback
from sqlalchemy.sql import text

import settings
import skyline_version
from skyline_functions import (
    mkdir_p, write_data_to_file, filesafe_metricname, is_derivative_metric)
from database import (get_engine, metrics_table_meta)

skyline_version = skyline_version.__absolute_version__
skyline_app = 'webapp'
skyline_app_logger = '%sLog' % skyline_app
logger = logging.getLogger(skyline_app_logger)
skyline_app_logfile = '%s/%s.log' % (settings.LOG_PATH, skyline_app)
logfile = '%s/%s.log' % (settings.LOG_PATH, skyline_app)
try:
    ENABLE_WEBAPP_DEBUG = settings.ENABLE_WEBAPP_DEBUG
except EnvironmentError as err:
    logger.error('error :: cannot determine ENABLE_WEBAPP_DEBUG from settings')
    ENABLE_WEBAPP_DEBUG = False

this_host = str(os.uname()[1])


def get_an_engine():
    try:
        engine, fail_msg, trace = get_engine(skyline_app)
        return engine, fail_msg, trace
    except:
        trace = traceback.format_exc()
        logger.error('%s' % trace)
        fail_msg = 'error :: failed to get MySQL engine for'
        logger.error('%s' % fail_msg)
        # return None, fail_msg, trace
        raise  # to webapp to return in the UI


def engine_disposal(engine):
    if engine:
        try:
            engine.dispose()
        except:
            logger.error(traceback.format_exc())
            logger.error('error :: calling engine.dispose()')
    return


# @added 20200420 - Feature #3500: webapp - crucible_process_metrics
#                   Feature #1448: Crucible web UI
#                   Branch #868: crucible
# @modified 20200421 - Feature #3500: webapp - crucible_process_metrics
#                      Feature #1448: Crucible web UI
# Added add_to_panorama
# @modified 20200422 - Feature #3500: webapp - crucible_process_metrics
#                      Feature #1448: Crucible web UI
# Added pad_timeseries
# @added 20200607 - Feature #3630: webapp - crucible_process_training_data
# Added training_data_json
# def submit_crucible_job(from_timestamp, until_timestamp, metrics_list, namespaces_list, source, alert_interval, user_id, user, add_to_panorama, pad_timeseries, training_data_json):
# @added 20200817 - Feature #3682: SNAB - webapp - crucible_process - run_algorithms
# Added run_algorithms
def submit_crucible_job(
        from_timestamp, until_timestamp, metrics_list, namespaces_list, source,
        alert_interval, user_id, user, add_to_panorama, pad_timeseries,
        training_data_json, run_algorithms):
    """
    Get a list of all the metrics passed and generate Crucible check files for
    each

    :param from_timestamp: the timestamp at which to start the time series
    :param until_timestamp: the timestamp at which to end the time series
    :param metrics_list: a list of metric names to analyse
    :param namespaces_list: a list of metric namespaces to analyse
    :param source: the source webapp making the request
    :param alert_interval: the alert_interval at which Crucible should trigger
        anomalies
    :param user_id: the user id of the user making the request
    :param user: the username making the request
    :param add_to_panorama: whether Crucible should add Skyline CONSENSUS
        anomalies to Panorama
    :param pad_timeseries: the amount of data to pad the time series with
    :param training_data_json: the full path to the training_data json file if
        source is training_data
    :param run_algorithms: list of algorithms to run
    :type from_timestamp: int
    :type until_timestamp: int
    :type metrics_list: list
    :type namespaces_list: list
    :type source: str
    :type alert_interval: int
    :type user_id: int
    :type user: str
    :type add_to_panorama: boolean
    :type pad_timeseries: str
    :type training_data_json: str
    :type run_algorithms: list
    :return: tuple of lists
    :rtype:  (list, list, list, list)

    Returns (crucible_job_id, metrics_submitted_to_process, fail_msg, trace)

    """

    fail_msg = None
    trace = None
    crucible_job_id = None
    metrics_submitted_to_process = 0

    # Generate a job id based on the YMDHMS.user_id and a job directory
    try:
        jobid_timestamp = int(time())
        jobid_datetimestamp = dt.datetime.fromtimestamp(jobid_timestamp).strftime('%Y%m%d%H%M%S')
        crucible_job_id = '%s.%s' % (str(jobid_datetimestamp), str(user_id))
    except:
        logger.error(traceback.format_exc())
        logger.error('error :: failed to determine a crucible_job_id')
        raise  # to webapp to return in the UI

    # Generate a job id based on the YMDHMS.user_id and a job directory
    try:
        crucible_path = os.path.dirname(settings.CRUCIBLE_DATA_FOLDER)
        crucible_job_dir = '%s/jobs/%s' % (crucible_path, crucible_job_id)
        if not path.exists(crucible_job_dir):
            logger.info(
                'creating crucible job directory - %s' % (
                    str(crucible_job_dir)))
            mkdir_p(crucible_job_dir)
    except:
        trace = traceback.format_exc()
        fail_msg = 'error :: failed to create the crucible job directory'
        logger.error(trace)
        logger.error(fail_msg)
        raise  # to webapp to return in the UI

    # TODO added checks of metric names
    metric_names = []
    if metrics_list:
        logger.info('submit_crucible_job :: %s metrics passed' % str(len(metrics_list)))
        for metric in metrics_list:
            metric_names.append(metric)

    # TODO added checks of metric namespaces, harder to do, but so that the UI
    # errors to the usr rather than sending a bad or non-existent metric to
    # Crucible
    if namespaces_list:
        logger.info('submit_crucible_job :: %s namespaces passed' % str(len(namespaces_list)))
        logger.info(
            'submit_crucible_job :: determine metrics for submit_crucible_job between %s and %s' % (
                str(from_timestamp), str(until_timestamp)))
        logger.info('getting MySQL engine')
        try:
            engine, fail_msg, trace = get_an_engine()
            logger.info(fail_msg)
        except:
            trace = traceback.format_exc()
            logger.error(trace)
            logger.error('%s' % fail_msg)
            logger.error('error :: could not get a MySQL engine to get metric names')
            raise  # to webapp to return in the UI

        if not engine:
            trace = 'none'
            fail_msg = 'error :: engine not obtained'
            logger.error(fail_msg)
            raise

        try:
            metrics_table, log_msg, trace = metrics_table_meta(skyline_app, engine)
            logger.info(log_msg)
            logger.info('metrics_table OK')
        except:
            logger.error(traceback.format_exc())
            logger.error('error :: failed to get metrics_table meta')
            if engine:
                engine_disposal(engine)
            raise  # to webapp to return in the UI

        metrics_like_query = text("""SELECT metric FROM metrics WHERE metric LIKE :like_string""")
        for namespace in namespaces_list:
            try:
                connection = engine.connect()
                results = connection.execute(metrics_like_query, like_string=str(namespace))
                connection.close()
                for row in results:
                    metric_name = str(row[0])
                    metric_names.append(metric_name)
            except:
                trace = traceback.format_exc()
                logger.error(trace)
                logger.error('error :: could not determine metrics from metrics table')
                if engine:
                    engine_disposal(engine)
                raise
        logger.info('submit_crucible_job :: %s metrics determined from passed namespaces' % str(len(metric_names)))

    logger.info('submit_crucible_job :: %s metrics to process' % str(len(metric_names)))
    metrics_submitted_to_process = []
    datapoint = 0
    triggered_algorithms = ['histogram_bins', 'first_hour_average', 'stddev_from_average', 'grubbs', 'ks_test', 'mean_subtraction_cumulation', 'median_absolute_deviation', 'stddev_from_moving_average', 'least_squares']
    added_at = int(time())
    for base_name in metric_names:
        sane_metricname = filesafe_metricname(str(base_name))
        derivative_metric = is_derivative_metric(skyline_app, base_name)
        if derivative_metric:
            target = 'nonNegativeDerivative(%s)' % base_name
        else:
            target = base_name
        # Generate a metric job directory
        crucible_anomaly_dir = '%s/%s' % (crucible_job_dir, sane_metricname)
        try:
            if not path.exists(crucible_anomaly_dir):
                logger.info(
                    'creating crucible metric job directory - %s' % (
                        str(crucible_anomaly_dir)))
                mkdir_p(crucible_anomaly_dir)
        except:
            trace = traceback.format_exc()
            fail_msg = 'error :: failed to create the crucible metric job directory'
            logger.error(trace)
            logger.error(fail_msg)
            raise  # to webapp to return in the UI
        if source == 'graphite':
            graphite_metric = True
        else:
            graphite_metric = False

        # @added 20200422 - Feature #3500: webapp - crucible_process_metrics
        #                   Feature #1448: Crucible web UI
        # In order for metrics to be analysed in Crucible like the Analyzer or
        # Mirage analysis, the time series data needs to be padded
        # Added pad_timeseries
        graphite_override_uri_parameters = 'from=%s&until=%s&target=%s' % (
            str(from_timestamp), str(until_timestamp), target)
        timeseries_full_duration = int(until_timestamp) - int(from_timestamp)
        pad_timeseries_with = 0
        if pad_timeseries == 'auto':
            if timeseries_full_duration > 3600:
                pad_timeseries_with = 3600
            if timeseries_full_duration > 86400:
                pad_timeseries_with = 86400
        if pad_timeseries == '86400':
            pad_timeseries_with = 86400
        if pad_timeseries == '604800':
            pad_timeseries_with = 604800
        if pad_timeseries == '0':
            pad_timeseries_with = 0
        if pad_timeseries_with:
            try:
                padded_from_timestamp = int(from_timestamp) - pad_timeseries_with
                graphite_override_uri_parameters = 'from=%s&until=%s&target=%s' % (
                    str(padded_from_timestamp), str(until_timestamp), target)
                logger.info('padding time series with %s seconds - %s' % (
                    str(pad_timeseries_with), str(graphite_override_uri_parameters)))
            except:
                logger.error(traceback.format_exc())
                logger.error('error :: failed to construct graphite_override_uri_parameters with pad_timeseries_with %s' % str(pad_timeseries_with))

        # @added 20200817 - Feature #3682: SNAB - webapp - crucible_process - run_algorithms
        # Allow the user to pass algorithms to run
        algorithms = settings.ALGORITHMS
        if run_algorithms:
            algorithms = run_algorithms

        # @modified 20200421 - Feature #3500: webapp - crucible_process_metrics
        #                      Feature #1448: Crucible web UI
        # Added add_to_panorama
        # @added 20200607 - Feature #3630: webapp - crucible_process_training_data
        # Added training_data_json
        crucible_anomaly_data = 'metric = \'%s\'\n' \
                                'value = \'%s\'\n' \
                                'from_timestamp = \'%s\'\n' \
                                'metric_timestamp = \'%s\'\n' \
                                'algorithms = %s\n' \
                                'triggered_algorithms = %s\n' \
                                'anomaly_dir = \'%s\'\n' \
                                'graphite_metric = %s\n' \
                                'run_crucible_tests = True\n' \
                                'added_by = \'%s\'\n' \
                                'added_at = \'%s\'\n' \
                                'graphite_override_uri_parameters = \'%s\'\n' \
                                'alert_interval = \'%s\'\n' \
                                'add_to_panorama = %s\n' \
                                'training_data_json = %s\n' \
            % (base_name, str(datapoint), str(from_timestamp),
               # @modified 20200817 - Feature #3682: SNAB - webapp - crucible_process - run_algorithms
               # str(until_timestamp), str(settings.ALGORITHMS),
               str(until_timestamp), str(algorithms),
               triggered_algorithms, crucible_anomaly_dir, str(graphite_metric),
               skyline_app, str(added_at), str(graphite_override_uri_parameters),
               str(alert_interval), str(add_to_panorama), str(training_data_json))

        # Create an anomaly file with details about the anomaly
        crucible_anomaly_file = '%s/%s.txt' % (crucible_anomaly_dir, sane_metricname)
        try:
            write_data_to_file(
                skyline_app, crucible_anomaly_file, 'w',
                crucible_anomaly_data)
            logger.info('added crucible anomaly file :: %s' % (crucible_anomaly_file))
        except:
            logger.error(traceback.format_exc())
            logger.error('error :: failed to add crucible anomaly file :: %s' % (crucible_anomaly_file))
        # Create a crucible check file
        crucible_check_file = '%s/%s.%s.txt' % (settings.CRUCIBLE_CHECK_PATH, str(added_at), sane_metricname)
        try:
            write_data_to_file(
                skyline_app, crucible_check_file, 'w',
                crucible_anomaly_data)
            logger.info('added crucible check :: %s,%s' % (base_name, str(added_at)))
            metrics_submitted_to_process.append(base_name)
        except:
            logger.error(traceback.format_exc())
            logger.error('error :: failed to add crucible check file :: %s' % (crucible_check_file))

    return (crucible_job_id, metrics_submitted_to_process, fail_msg, trace)


def get_crucible_jobs():
    """
    Get a list of all the metrics passed and generate Crucible check files for
    each

    :param requested_timestamp: the training data timestamp
    :param context: the request context, training_data or features_profiles
    :type requested_timestamp: str
    :type context: str
    :return: tuple of lists
    :rtype:  (list, list, list, list)

    """

    fail_msg = None
    trace = None
    crucible_jobs = []
    # metrics = []

    """
[root@skyline-1 ~] ls -al /opt/skyline/crucible/jobs/vista.test-prometheus.prometheus.node_load1
total 784
drwxr-xr-x.   2 skyline skyline   4096 Mar 28 14:05 .
drwxr-xr-x. 439 skyline skyline  53248 Mar 30 08:56 ..
-rw-r--r--.   1 skyline skyline  17612 Mar 28 14:05 detect_drop_off_cliff.png
-rw-r--r--.   1 skyline skyline  18504 Mar 28 13:55 first_hour_average.DETECTED.png
-rw-r--r--.   1 skyline skyline  17612 Mar 28 13:56 grubbs.png
-rw-r--r--.   1 skyline skyline  18682 Mar 28 13:54 histogram_bins.DETECTED.png
-rw-r--r--.   1 skyline skyline  17612 Mar 28 13:57 ks_test.png
-rw-r--r--.   1 skyline skyline  17612 Mar 28 14:04 least_squares.png
-rw-r--r--.   1 skyline skyline  18431 Mar 28 13:58 mean_subtraction_cumulation.DETECTED.png
-rw-r--r--.   1 skyline skyline  18605 Mar 28 13:59 median_absolute_deviation.DETECTED.png
-rw-r--r--.   1 skyline skyline 437652 Mar 28 13:53 vista.test-prometheus.prometheus.node_load1.json
-rw-rw-rw-.   1 skyline skyline     83 Mar 28 14:05 vista.test-prometheus.prometheus.node_load1.json.gz
-rw-r--r--.   1 skyline skyline  21857 Mar 28 13:53 vista.test-prometheus.prometheus.node_load1.png
-rw-r--r--.   1 skyline skyline    821 Mar 28 14:05 vista.test-prometheus.prometheus.node_load1.txt
-rw-r--r--.   1 skyline skyline  22022 Mar 28 14:05 skyline.anomalies.csv
-rw-r--r--.   1 skyline skyline  23824 Mar 28 14:05 skyline.anomalies_score.txt
-rw-r--r--.   1 skyline skyline  18056 Mar 28 14:05 skyline.consensus.anomalies.png
-rw-r--r--.   1 skyline skyline  18504 Mar 28 13:56 stddev_from_average.DETECTED.png
-rw-r--r--.   1 skyline skyline  18836 Mar 28 14:01 stddev_from_moving_average.DETECTED.png
[root@skyline-1 ~]
    """

    try:
        crucible_path = os.path.dirname(settings.CRUCIBLE_DATA_FOLDER)
        data_dir = '%s/jobs' % crucible_path
        for root, dirs, files in walk(data_dir):
            has_anomalies = None
            completed_job = False
            panorama_done = False
            add_metric = False
            skyline_anomalies = None
            skyline_consensus_anomalies = []
            skyline_consensus_anomalies_present = 0
            if len(files) > 0:
                for file in files:
                    if file.endswith('.DETECTED.png'):
                        has_anomalies = True
                for file in files:
                    if 'skyline.anomalies_score.txt' == file:
                        completed_job = True
                        skyline_anomalies_score_file = '%s/%s' % (root, file)
                        try:
                            with open(skyline_anomalies_score_file) as f:
                                output = f.read()
                            skyline_anomalies = literal_eval(output)
                        except:
                            trace = traceback.format_exc()
                            fail_msg = 'error :: failed to get skyline_anomalies from %s' % skyline_anomalies_score_file
                            logger.error(trace)
                            logger.error(fail_msg)
                            skyline_anomalies = None
                        if skyline_anomalies:
                            # skyline_anomalies format
                            # [timestamp, value, anomaly_score, triggered_algorithms]
                            # [skyline_anomaly[0], skyline_anomaly[1], skyline_anomaly[2], skyline_anomaly[3]]
                            # [1583234400.0, 44.39999999990687, 2, ['histogram_bins', 'median_absolute_deviation']],
                            # Convert float timestamp from Graphite to int
                            for timestamp, value, anomaly_score, triggered_algorithms in skyline_anomalies:
                                if anomaly_score >= settings.CONSENSUS:
                                    skyline_consensus_anomalies.append([int(timestamp), value, anomaly_score, triggered_algorithms])
                            del skyline_anomalies
                    if file.endswith('.json.gz'):
                        completed_job = True
                    if file.endswith('.sent_to_panorama.txt'):
                        completed_job = True
                        panorama_done = True
                for file in files:
                    if file.endswith('.txt'):
                        if 'skyline.anomalies_score.txt' in file:
                            completed_job = True
                        elif file.endswith('.sent_to_panorama.txt'):
                            add_metric = True
                            has_anomalies = True
                            completed_job = True
                        else:
                            metric_name = file.replace('.txt', '')
                            add_metric = True
                    if file.endswith('.json'):
                        metric_name = file.replace('.json', '')
                        add_metric = True
                    if file.endswith('.json.gz'):
                        completed_job = True
            if add_metric:
                crucible_job_id = os.path.basename(root)
                crucible_job_id_check = crucible_job_id.split('.', 1)[0]
                if re.search('\\d{14}', crucible_job_id_check):
                    add_metric = True
                else:
                    crucible_job_dir = os.path.dirname(root)
                    crucible_job_id = os.path.basename(crucible_job_dir)
                crucible_jobid_datetimestamp = crucible_job_id.split('.', 1)[0]
                py_date = dt.datetime.strptime(crucible_jobid_datetimestamp, '%Y%m%d%H%M%S')
                human_date = py_date.strftime('%Y-%m-%d %H:%M:%S')
                if skyline_consensus_anomalies:
                    skyline_consensus_anomalies_present = len(skyline_consensus_anomalies)
                    del skyline_consensus_anomalies
                crucible_jobs.append([metric_name, root, crucible_job_id, human_date, completed_job, has_anomalies, panorama_done, skyline_consensus_anomalies_present])
    except:
        trace = traceback.format_exc()
        fail_msg = 'error :: failed to create the crucible job directory'
        logger.error(trace)
        logger.error(fail_msg)
        raise  # to webapp to return in the UI

    return (crucible_jobs, fail_msg, trace)


def get_crucible_job(crucible_job_id, metric):
    """
    Get the crucible data for a Crucible analysis

    :param crucible_job_id: the crucible_job_id
    :param metric: the metric name
    :type crucible_job_id: str
    :type metric: str
    :return: tuple of lists
    :rtype:  (list, boolean, boolean, list, list, list, list, str, str, str)

    Returns (crucible_job_details, completed_job, has_anomalies, skyline_anomalies, skyline_consensus_anomalies, panorama_done, panorama_done_timestamp, panorama_done_user_id, image_files, image_file_names, graph_image_file, fail_msg, trace)

    """

    fail_msg = None
    trace = None
    has_anomalies = None
    completed_job = False
    crucible_job_details = []
    skyline_anomalies = []
    skyline_consensus_anomalies = []
    image_files = []
    image_file_names = []
    crucible_path = os.path.dirname(settings.CRUCIBLE_DATA_FOLDER)
    jobs_data_dir = '%s/jobs' % crucible_path

    # @modified 20210325 - Feature #3500: webapp - crucible_process_metrics
    #                      Feature #1448: Crucible web UI
    # filesafe metric name
    sane_metricname = filesafe_metricname(str(metric))
    # data_dir = '%s/%s/%s' % (jobs_data_dir, crucible_job_id, metric)
    data_dir = '%s/%s/%s' % (jobs_data_dir, crucible_job_id, sane_metricname)

    crucible_job_details_filename = '%s.txt' % metric
    crucible_job_details_file = '%s/%s' % (data_dir, crucible_job_details_filename)
    skyline_anomalies_score_file = '%s/skyline.anomalies_score.txt' % data_dir
    graph_image_file = '%s/%s.png' % (data_dir, metric)
    crucible_job_sent_to_panorama_file_pattern = '%s.%s.sent_to_panorama.txt' % (
        crucible_job_id, metric)
    panorama_done = False
    panorama_done_timestamp = None
    panorama_done_user_id = None
    logger.info('get_crucible_job :: %s for %s' % (crucible_job_id, metric))

    """
[root@skyline-1 ~] ls -al /opt/skyline/crucible/jobs/vista.test-prometheus.prometheus.node_load1
total 784
drwxr-xr-x.   2 skyline skyline   4096 Mar 28 14:05 .
drwxr-xr-x. 439 skyline skyline  53248 Mar 30 08:56 ..
-rw-r--r--.   1 skyline skyline  17612 Mar 28 14:05 detect_drop_off_cliff.png
-rw-r--r--.   1 skyline skyline  18504 Mar 28 13:55 first_hour_average.DETECTED.png
-rw-r--r--.   1 skyline skyline  17612 Mar 28 13:56 grubbs.png
-rw-r--r--.   1 skyline skyline  18682 Mar 28 13:54 histogram_bins.DETECTED.png
-rw-r--r--.   1 skyline skyline  17612 Mar 28 13:57 ks_test.png
-rw-r--r--.   1 skyline skyline  17612 Mar 28 14:04 least_squares.png
-rw-r--r--.   1 skyline skyline  18431 Mar 28 13:58 mean_subtraction_cumulation.DETECTED.png
-rw-r--r--.   1 skyline skyline  18605 Mar 28 13:59 median_absolute_deviation.DETECTED.png
-rw-r--r--.   1 skyline skyline 437652 Mar 28 13:53 vista.test-prometheus.prometheus.node_load1.json
-rw-rw-rw-.   1 skyline skyline     83 Mar 28 14:05 vista.test-prometheus.prometheus.node_load1.json.gz
-rw-r--r--.   1 skyline skyline  21857 Mar 28 13:53 vista.test-prometheus.prometheus.node_load1.png
-rw-r--r--.   1 skyline skyline    821 Mar 28 14:05 vista.test-prometheus.prometheus.node_load1.txt
-rw-r--r--.   1 skyline skyline  22022 Mar 28 14:05 skyline.anomalies.csv
-rw-r--r--.   1 skyline skyline  23824 Mar 28 14:05 skyline.anomalies_score.txt
-rw-r--r--.   1 skyline skyline  18056 Mar 28 14:05 skyline.consensus.anomalies.png
-rw-r--r--.   1 skyline skyline  18504 Mar 28 13:56 stddev_from_average.DETECTED.png
-rw-r--r--.   1 skyline skyline  18836 Mar 28 14:01 stddev_from_moving_average.DETECTED.png
[root@skyline-1 ~]
    """

    try:
        logger.info('get_crucible_job :: walking %s' % (data_dir))
        for root, dirs, files in walk(data_dir):
            for file in files:
                logger.info('get_crucible_job :: checking file - %s' % (file))
                append_file = '%s/%s' % (data_dir, file)
                if file == crucible_job_details_filename:
                    try:
                        logger.info('get_crucible_job :: getting crucible_job_details from file - %s' % (file))
                        with open(crucible_job_details_file) as f:
                            for line in f:
                                no_new_line = line.replace('\n', '')
                                no_equal_line = no_new_line.replace(' = ', ',')
                                array = str(no_equal_line.split(',', 1))
                                add_line = literal_eval(array)
                                crucible_job_details.append(add_line)
                        # with open(crucible_job_details_file) as f:
                        #     output = f.read()
                        # crucible_job_details = literal_eval(output)
                    except:
                        trace = traceback.format_exc()
                        fail_msg = 'error :: failed to get crucible_job_details from %s' % file
                        logger.error(trace)
                        logger.error(fail_msg)
                        crucible_job_details = None
                if file.endswith('.png'):
                    image_files.append(append_file)
                    image_file_names.append(file)
                if file.endswith('.DETECTED.png'):
                    has_anomalies = True
                if file.endswith(crucible_job_sent_to_panorama_file_pattern):
                    panorama_done = True
                    try:
                        with open(append_file) as f:
                            output = f.read()
                        panorama_done = literal_eval(output)
                        panorama_done_timestamp = panorama_done[0]
                        panorama_done_user_id = panorama_done[1]
                    except:
                        trace = traceback.format_exc()
                        fail_msg = 'error :: failed to get panorama_done from %s' % append_file
                        logger.error(trace)
                        logger.error(fail_msg)

                if file == 'skyline.anomalies_score.txt':
                    try:
                        with open(skyline_anomalies_score_file) as f:
                            output = f.read()
                        skyline_anomalies = literal_eval(output)
                    except:
                        trace = traceback.format_exc()
                        fail_msg = 'error :: failed to get crucible_job_details from %s' % file
                        logger.error(trace)
                        logger.error(fail_msg)
                        skyline_anomalies = None
                    skyline_anomalies_int_ts = []
                    skyline_consensus_anomalies = []
                    if skyline_anomalies:
                        # skyline_anomalies format
                        # [timestamp, value, anomaly_score, triggered_algorithms]
                        # [skyline_anomaly[0], skyline_anomaly[1], skyline_anomaly[2], skyline_anomaly[3]]
                        # [1583234400.0, 44.39999999990687, 2, ['histogram_bins', 'median_absolute_deviation']],
                        # Convert float timestamp from Graphite to int
                        for timestamp, value, anomaly_score, triggered_algorithms in skyline_anomalies:
                            skyline_anomalies_int_ts.append([int(timestamp), value, anomaly_score, triggered_algorithms])
                            if anomaly_score >= settings.CONSENSUS:
                                skyline_consensus_anomalies.append([int(timestamp), value, anomaly_score, triggered_algorithms])
                            # @added 20200817 - Feature #3682: SNAB - webapp - crucible_process - run_algorithms
                            if 'detect_drop_off_cliff' in triggered_algorithms:
                                skyline_consensus_anomalies.append([int(timestamp), value, anomaly_score, triggered_algorithms])
                    if skyline_anomalies_int_ts:
                        skyline_anomalies = skyline_anomalies_int_ts
                        try:
                            del skyline_anomalies_int_ts
                        except:
                            pass
                if file.endswith('.json.gz'):
                    completed_job = True
    except:
        trace = traceback.format_exc()
        fail_msg = 'error :: failed to create the crucible job directory'
        logger.error(trace)
        logger.error(fail_msg)
        raise  # to webapp to return in the UI

    return (crucible_job_details, completed_job, has_anomalies, skyline_anomalies, skyline_consensus_anomalies, panorama_done, panorama_done_timestamp, panorama_done_user_id, image_files, image_file_names, graph_image_file, fail_msg, trace)


def send_crucible_job_metric_to_panorama(crucible_job_id, base_name, user_id, user, skyline_consensus_anomalies):
    """
    Send the Crucible Skyline CONSENSUS anomalies for a crucible_job and metric
    to Panorama to insert into the anomalies database.

    :param crucible_job_id: the crucible_job_id
    :param base_name: the metric name
    :param user_id: the user_id
    :param user: the username
    :param skyline_consensus_anomalies: the Crucible Skyline CONSENSUS anomalies
    :type crucible_job_id: str
    :type base_name: str
    :type user_id: int
    :type user: str
    :type skyline_consensus_anomalies: list

    :return: tuple of lists
    :rtype:  (int, str, str)

    Returns (len(skyline_consensus_anomalies), fail_msg, trace)

    """

    fail_msg = None
    trace = None
    added_at = int(time())
    crucible_path = os.path.dirname(settings.CRUCIBLE_DATA_FOLDER)
    jobs_data_dir = '%s/jobs' % crucible_path
    data_dir = '%s/%s/%s' % (jobs_data_dir, crucible_job_id, base_name)
    crucible_job_details_filename = '%s.txt' % base_name
    crucible_job_details_file = '%s/%s' % (data_dir, crucible_job_details_filename)
    crucible_job_details = []
    try:
        logger.info('send_crucible_job_metric_to_panorama :: getting crucible_job_details from file - %s' % (crucible_job_details_file))
        with open(crucible_job_details_file) as f:
            for line in f:
                no_new_line = line.replace('\n', '')
                no_equal_line = no_new_line.replace(' = ', ',')
                array = str(no_equal_line.split(',', 1))
                add_line = literal_eval(array)
                crucible_job_details.append(add_line)
    except:
        trace = traceback.format_exc()
        fail_msg = 'error :: send_crucible_job_metric_to_panorama - failed to get crucible_job_details from file - %s' % crucible_job_details_file
        logger.error(trace)
        logger.error(fail_msg)
        raise  # to webapp to return in the UI
    try:
        timestamp_str = str(crucible_job_details[2][1])
        new_timestamp_str = timestamp_str.replace("'", "")
        from_timestamp = int(new_timestamp_str)
    except:
        trace = traceback.format_exc()
        fail_msg = 'error :: send_crucible_job_metric_to_panorama - failed to determine from_timestamp from get crucible_job_details'
        logger.error(trace)
        logger.error(fail_msg)
        raise  # to webapp to return in the UI

    label = 'Crucible job %s' % str(crucible_job_id)
    sane_metricname = filesafe_metricname(str(base_name))

    # skyline_consensus_anomalies format
    # [timestamp, value, anomaly_score, triggered_algorithms]
    # [skyline_anomaly[0], skyline_anomaly[1], skyline_anomaly[2], skyline_anomaly[3]]
    # [1583234400, 44.39999999990687, 2, ['histogram_bins', 'median_absolute_deviation']],
    for timestamp, datapoint, anomaly_score, triggered_algorithms in skyline_consensus_anomalies:
        # To allow multiple Panorama anomaly files to added quickly just
        # increment the added_at by 1 seconds so that all the files have a
        # unique name
        added_at += 1
        # Note:
        # The values are enclosed is single quoted intentionally
        # as the imp.load_source used results in a shift in the
        # decimal position when double quoted, e.g.
        # value = "5622.0" gets imported as
        # 2016-03-02 12:53:26 :: 28569 :: metric variable - value - 562.2
        # single quoting results in the desired,
        # 2016-03-02 13:16:17 :: 1515 :: metric variable - value - 5622.0
        source = 'graphite'
        panaroma_anomaly_data = 'metric = \'%s\'\n' \
                                'value = \'%s\'\n' \
                                'from_timestamp = \'%s\'\n' \
                                'metric_timestamp = \'%s\'\n' \
                                'algorithms = %s\n' \
                                'triggered_algorithms = %s\n' \
                                'app = \'%s\'\n' \
                                'source = \'%s\'\n' \
                                'added_by = \'%s\'\n' \
                                'added_at = \'%s\'\n' \
                                'label = \'%s\'\n' \
                                'user_id = \'%s\'\n' \
            % (base_name, str(datapoint), from_timestamp,
               str(timestamp), str(settings.ALGORITHMS),
               triggered_algorithms, skyline_app, source,
               this_host, str(added_at), label, str(user_id))
        # Create an anomaly file with details about the anomaly
        panaroma_anomaly_file = '%s/%s.%s.txt' % (
            settings.PANORAMA_CHECK_PATH, added_at, sane_metricname)
        try:
            write_data_to_file(
                skyline_app, panaroma_anomaly_file, 'w',
                panaroma_anomaly_data)
            logger.info('send_crucible_job_metric_to_panorama - added panorama anomaly file :: %s' % (panaroma_anomaly_file))
        except:
            logger.error(traceback.format_exc())
            logger.error('error :: send_crucible_job_metric_to_panorama - failed to add panorama anomaly file :: %s' % (panaroma_anomaly_file))

    crucible_job_sent_to_panorama_file = '%s/%s.%s.%s.sent_to_panorama.txt' % (
        data_dir, str(added_at), crucible_job_id, base_name)
    panorama_done_data = [added_at, int(user_id), skyline_consensus_anomalies]
    try:
        write_data_to_file(
            skyline_app, crucible_job_sent_to_panorama_file, 'w',
            str(panorama_done_data))
        logger.info('send_crucible_job_metric_to_panorama - added set to panorama crucible job file :: %s' % (crucible_job_sent_to_panorama_file))
        logger.info('send_crucible_job_metric_to_panorama - with contents :: %s' % (panorama_done_data))
    except:
        logger.error(traceback.format_exc())
        logger.error('error :: send_crucible_job_metric_to_panorama - failed to add panorama crucible job file :: %s' % (crucible_job_sent_to_panorama_file))

    return (len(skyline_consensus_anomalies), fail_msg, trace)
