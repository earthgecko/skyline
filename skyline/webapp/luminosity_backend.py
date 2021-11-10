from __future__ import division
import logging
import os
from ast import literal_eval
import traceback
from time import time

import settings
import skyline_version

skyline_version = skyline_version.__absolute_version__
skyline_app = 'webapp'
skyline_app_logger = '%sLog' % skyline_app
logger = logging.getLogger(skyline_app_logger)
skyline_app_logfile = '%s/%s.log' % (settings.LOG_PATH, skyline_app)
logfile = '%s/%s.log' % (settings.LOG_PATH, skyline_app)
try:
    ENABLE_WEBAPP_DEBUG = settings.ENABLE_WEBAPP_DEBUG
except EnvironmentError as err:
    logger.error('error :: cannot determine ENABLE_WEBAPP_DEBUG from settings - %s' % err)
    ENABLE_WEBAPP_DEBUG = False

try:
    luminosity_data_folder = settings.LUMINOSITY_DATA_FOLDER
except:
    luminosity_data_folder = '/opt/skyline/luminosity'
# classify_metrics_dir = '%s/classify_metrics' % luminosity_data_folder

this_host = str(os.uname()[1])


def get_classify_metrics(base_name, timestamp, algorithm, significant):
    """
    Get a list of all the files for metrics in classify_metrics

    :param base_name: the name of the metric
    :param timestamp: the timestamp
    :param algorithm: the algorithm
    :param significant: whether to return only significant results
    :type base_name: str
    :type timestamp: str
    :type algorithm: str
    :return: list of dicts
    :rtype:  {}

    Returns a dict of algorithm, metrics, timestamps and significance
    {
        "classify_metrics": {
            "level_shift": {
                'metric1': {
                    1604596200: {'siginifcant': True},
                    1606410600: {'siginifcant': True},
                    1607015400: {'siginifcant': True},
                    1602177000: {'siginifcant': False}
                },
                'metric2': {
                    1604596200: {'siginifcant': True},
                    1606410600: {'siginifcant': True},
                    1607015400: {'siginifcant': True},
                    1602177000: {'siginifcant': False}
                },
            },
            "volatility_shift": {
                'metric1': {
                    1604596200: {'siginifcant': True},
                    1606410600: {'siginifcant': True},
                    1607015400: {'siginifcant': True},
                    1602177000: {'siginifcant': False}
                },
                'metric2': {
                    1604596200: {'siginifcant': True},
                    1606410600: {'siginifcant': True},
                    1607015400: {'siginifcant': True},
                    1602177000: {'siginifcant': False}
                },
            }
        }
    }

    """

    fail_msg = None
    trace = None

    logger.info(
        'get_classify_metrics - base_name: %s, timestamp: %s, algorithm: %s, significant: %s' % (
            str(base_name), str(timestamp), str(algorithm), str(significant)))

    start = int(time())

    # @added 20211001 - reduce os.walk
    classify_metrics_dir = '%s/classify_metrics' % luminosity_data_folder
    if algorithm != 'all':
        classify_metrics_dir = '%s/classify_metrics/%s' % (luminosity_data_folder, algorithm)
        if base_name != 'all':
            metric_dir_str = base_name.replace('.', '/')
            classify_metrics_dir = '%s/%s' % (classify_metrics_dir, metric_dir_str)

    classified_metrics = []
    metrics = []
    classify_metrics_dict = {}
    for root, dirs, files in os.walk(classify_metrics_dir):

        # @added 20211001 - limit default results
        if base_name != 'all':
            root_str = root.replace('/', '.')
            if base_name not in root_str:
                continue
        if base_name == 'all':
            if len(classify_metrics_dict) == 10:
                break
#        if int(time()) > (start + 24):
#            logger.info(
#                'get_classify_metrics - breaking from os.walk due to run time')
#            break

        data_dict = None
        if files:
            for file in files:
                # @added 20211001 - limit default results
                if base_name == 'all':
                    if len(classify_metrics_dict) == 10:
                        break
#                if int(time()) > (start + 24):
#                    logger.info(
#                        'get_classify_metrics - breaking from files in os.walk due to run time')
#                    break

                try:
                    if file == 'data.txt':
                        data_dict = None
                        data_file = '%s/%s' % (root, file)
                        try:
                            with open(data_file) as f:
                                for line in f:
                                    data_dict = literal_eval(line)
                                    if data_dict:
                                        break
                        except Exception as e:
                            trace = traceback.format_exc()
                            logger.error(trace)
                            fail_msg = 'error :: get_classify_metrics :: failed to open data_file: %s - %s' % (data_file, e)
                            logger.error('%s' % fail_msg)

                        if data_dict:
                            metric = data_dict['metric']
                            if base_name != 'all':
                                if metric != base_name:
                                    continue

                            if timestamp != 'all':
                                if str(timestamp) != str(data_dict['timestamp']):
                                    continue

                            if algorithm != 'all':
                                if algorithm != data_dict['algorithm']:
                                    continue

                            if significant:
                                if 'significant.txt' not in files:
                                    continue

                            if 'significant.txt' not in files:
                                labelled_significant = False
                            else:
                                labelled_significant = True

                            if metric not in metrics:
                                metrics.append(metric)
                                classify_metrics_dict[metric] = {}
                            algorithm = data_dict['algorithm']
                            algorithm_dict = None
                            try:
                                algorithm_dict = classify_metrics_dict[metric][algorithm]
                            except:
                                classify_metrics_dict[metric][algorithm] = {}
                                algorithm_dict = True
                            timestamps_dict = None
                            if algorithm_dict:
                                try:
                                    timestamps_dict = classify_metrics_dict[metric][algorithm]['timestamps']
                                except:
                                    classify_metrics_dict[metric][algorithm]['timestamps'] = {}
                                    timestamps_dict = True
                            if timestamps_dict:
                                ts = data_dict['timestamp']
                                classify_metrics_dict[metric][algorithm]['timestamps'][ts] = {}
                                classify_metrics_dict[metric][algorithm]['timestamps'][ts]['files'] = files
                                classify_metrics_dict[metric][algorithm]['timestamps'][ts]['files_count'] = len(files)
                                classify_metrics_dict[metric][algorithm]['timestamps'][ts]['significant'] = labelled_significant
                                classify_metrics_dict[metric][algorithm]['timestamps'][ts]['anomalies'] = data_dict['anomalies']
                                classify_metrics_dict[metric][algorithm]['timestamps'][ts]['anomalies_count'] = len(data_dict['anomalies'])
                                classify_metrics_dict[metric][algorithm]['timestamps'][ts]['file_path'] = data_dict['file_path']
                            break
                except Exception as e:
                    trace = traceback.format_exc()
                    logger.error(trace)
                    fail_msg = 'error :: get_classify_metrics :: failed to add data to classify_metrics_dict - %s' % (e)
                    logger.error('%s' % fail_msg)

    classified_metrics = list(metrics)
    if classify_metrics_dict:
        for current_metric in classify_metrics_dict:
            for current_algorithm in classify_metrics_dict[current_metric]:
                classify_metrics_dict[current_metric][current_algorithm]['classifications'] = len(classify_metrics_dict[current_metric][current_algorithm]['timestamps'])
                significant_classifications = 0
                for ts in classify_metrics_dict[current_metric][current_algorithm]['timestamps']:
                    if classify_metrics_dict[current_metric][current_algorithm]['timestamps'][ts]['significant']:
                        significant_classifications += 1
                classify_metrics_dict[current_metric][current_algorithm]['significant'] = significant_classifications
    logger.info('get_classify_metrics - classify_metrics_dict has %s items' % (
        str(len(classify_metrics_dict))))

    return (classify_metrics_dict, classified_metrics, fail_msg, trace)
