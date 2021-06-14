from __future__ import division
import logging
import os
from sys import version_info
import traceback
import csv
# from ast import literal_eval

from sqlalchemy.sql import select
# @added 20170809 - Task #2132: Optimise Ionosphere DB usage
from pymemcache.client.base import Client as pymemcache_Client
import numpy as np
import pandas as pd
from tsfresh.feature_extraction import (
    # @modified 20210101 - Task #3928: Update Skyline to use new tsfresh feature extraction method
    # extract_features, ReasonableFeatureExtractionSettings)
    extract_features, EfficientFCParameters)

import settings
from skyline_functions import get_memcache_metric_object
from database import (
    get_engine, metrics_table_meta)
# @added 20210425 - Task #4030: refactoring
#                   Feature #4014: Ionosphere - inference
from functions.numpy.percent_different import get_percent_different

skyline_app = 'ionosphere'
skyline_app_logger = '%sLog' % skyline_app
logger = logging.getLogger(skyline_app_logger)
skyline_app_logfile = '%s/%s.log' % (settings.LOG_PATH, skyline_app)

python_version = int(version_info[0])

this_host = str(os.uname()[1])

# Converting one settings variable into a local variable, just because it is a
# long string otherwise.
try:
    ENABLE_IONOSPHERE_DEBUG = settings.ENABLE_IONOSPHERE_DEBUG
except:
    logger.error('error :: layers :: cannot determine ENABLE_IONOSPHERE_DEBUG from settings' % skyline_app)
    ENABLE_IONOSPHERE_DEBUG = False

if settings.MEMCACHE_ENABLED:
    memcache_client = pymemcache_Client((settings.MEMCACHED_SERVER_IP, settings.MEMCACHED_SERVER_PORT), connect_timeout=0.1, timeout=0.2)
else:
    memcache_client = None

LOCAL_DEBUG = False


def get_metrics_db_object(base_name):
    """
    Returns the data of a metric from metrics table as an object and populates
    memcached with a dict of the object

    :param base_name: the metric base_name
    :type base_name: str
    :return: metrics_db_object
    :rtype: object

    """

    def get_an_engine():
        try:
            engine, log_msg, trace = get_engine(skyline_app)
            return engine, log_msg, trace
        except:
            logger.error(traceback.format_exc())
            log_msg = 'error :: failed to get MySQL engine in spin_process'
            logger.error('error :: failed to get MySQL engine in spin_process')
            return None, log_msg, trace

    def engine_disposal(engine):
        if engine:
            try:
                engine.dispose()
            except:
                logger.error(traceback.format_exc())
                logger.error('error :: calling engine.dispose()')
        return

    metrics_db_object = None
    memcache_metrics_db_object = None
    metrics_db_object_key = 'metrics_db_object.%s' % str(base_name)
    memcache_metric_dict = None
    if settings.MEMCACHE_ENABLED:
        memcache_metric_dict = get_memcache_metric_object(skyline_app, base_name)

    query_metric_table = True
    if memcache_metric_dict:
        query_metric_table = False
        metrics_db_object = memcache_metric_dict
        logger.info('using %s key data from memcache' % metrics_db_object_key)

    # @modified 20170825 - Task #2132: Optimise Ionosphere DB usage
    # If no memcache data then MySQL query_metric_table
    if query_metric_table:
        try:
            engine, log_msg, trace = get_an_engine()
            logger.info(log_msg)
        except:
            logger.error(traceback.format_exc())
            logger.error('error :: could not get a MySQL engine to determine ionosphere_enabled')

        if not engine:
            logger.error('error :: engine not obtained to determine ionosphere_enabled')

        # Get the metrics_table metadata
        metrics_table = None
        try:
            metrics_table, log_msg, trace = metrics_table_meta(skyline_app, engine)
            logger.info('metrics_table OK for %s' % base_name)
        except:
            logger.error(traceback.format_exc())
            logger.error('error :: failed to get metrics_table meta for %s' % base_name)

        try:
            connection = engine.connect()
            stmt = select([metrics_table]).where(metrics_table.c.metric == base_name)
            result = connection.execute(stmt)
            try:
                result
            except:
                logger.error(traceback.format_exc())
                logger.error('error :: got no result from MySQL from metrics table for - %s' % base_name)
            row = result.fetchone()
            # @added 20170825 - Task #2132: Optimise Ionosphere DB usage
            # @modified - 20180524 - Task #2132: Optimise Ionosphere DB usage
            # Feature #2378: Add redis auth to Skyline and rebrow
            # Wrapped memcache_metrics_db_object, metrics_id,
            # metric_ionosphere_enabled and metrics_db_object in if row
            # as if row is None it can fail with:
            # TypeError: 'NoneType' object is not iterable
            # memcache_metrics_db_object = dict(row)
            if row:
                memcache_metrics_db_object = dict(row)
                # @added 20170115 - Feature #1854: Ionosphere learn - generations
                # Create the metrics_db_object so it is available throughout
                # Here we go! Learn!
                metrics_db_object = row
            else:
                logger.info('could not determine metric id for %s' % base_name)

            connection.close()

        except:
            logger.error(traceback.format_exc())
            logger.error('error :: could not determine ionosphere_enabled from metrics table for - %s' % base_name)

    # @added 20170825 - Task #2132: Optimise Ionosphere DB usage
    # Add the metric db object data to memcache
    # @modified 20191031 - Branch #3262: py3
    #                      Task #3304: py3 - handle pymemcache bytes not str
    # if settings.MEMCACHE_ENABLED and query_metric_table:
    if settings.MEMCACHE_ENABLED and query_metric_table and memcache_metrics_db_object:
        try:
            memcache_metric_dict = {}
            # @modified 20191030 - Branch #3262: py3
            #                      Task #3304: py3 - handle pymemcache bytes not str
            # for k, v in memcache_metrics_db_object.iteritems():
            if python_version == 2:
                for k, v in memcache_metrics_db_object.iteritems():
                    key_name = str(k)
                    key_value = str(v)
                    memcache_metric_dict[key_name] = key_value
            else:
                for k, v in memcache_metrics_db_object.items():
                    key_name = str(k)
                    key_value = str(v)
                    memcache_metric_dict[key_name] = key_value
            memcache_client.set(metrics_db_object_key, memcache_metric_dict, expire=3600)
            logger.info('set the memcache key - %s' % metrics_db_object_key)
        except:
            logger.error(traceback.format_exc())
            logger.error('error :: failed to set %s in memcache' % metrics_db_object_key)
        try:
            memcache_client.close()
        except:
            # @modified 20170913 - Task #2160: Test skyline with bandit
            # pass
            logger.error('error :: failed to close memcache_client')

    return metrics_db_object


def get_calculated_features(calculated_feature_file):
    """
    Called by :class:`~skyline.skyline.Ionosphere.spin_process` to
    create a list of feature_names and their calc_values.

    :param calculated_feature_file: the full path and file name of the
        features.transposed.csv file
    :type calculated_feature_file: str
    :return: calculated_features
    :rtype: list

    """
    if not os.path.isfile(calculated_feature_file):
        # @added 20191029 - Task #3302: Handle csv.reader in py3
        #                   Branch #3262: py3
        # Added an error to log
        logger.error('error :: get_calculated_features :: no calculated_feature_file found - %s' % (
            str(calculated_feature_file)))
        return []
    count_id = 0

    # @added 20191029 - Task #3302: Handle csv.reader in py3
    #                      Branch #3262: py3
    if python_version == 3:
        try:
            codecs
        except:
            import codecs

    calculated_features = []

    with open(calculated_feature_file, 'rb') as fr:
        # @modified 20191029 - Task #3302: Handle csv.reader in py3
        #                      Branch #3262: py3
        # reader = csv.reader(fr, delimiter=',')
        if python_version == 2:
            reader = csv.reader(fr, delimiter=',')
        else:
            reader = csv.reader(codecs.iterdecode(fr, 'utf-8'), delimiter=',')

        for i, line in enumerate(reader):
            if str(line[0]) != '':
                if ',' in line[0]:
                    feature_name = '"%s"' % str(line[0])
                else:
                    feature_name = str(line[0])
                count_id += 1
                # @modified 20190412 - Feature #2484: FULL_DURATION feature profiles
                # This sometimes generates an error so wrapped in try with
                # logging.
                # calc_value = float(line[1])
                # calculated_features.append([feature_name, calc_value])
                try:
                    calc_value = float(line[1])
                    calculated_features.append([feature_name, calc_value])
                except:
                    # @modified 20190412 - Feature #2484: FULL_DURATION feature profiles
                    # This logging determineed that at times no value as in empty,
                    # is some times encountered with autocorrelation_lag values
                    # so disabled traceback and log as info not error
                    # logger.error(traceback.format_exc())
                    # logger.error('failed to determine calc_value from value of - %s - for feature_name - %s' % (
                    #     str(line[1]), str(line[0])))
                    logger.info('get_calculated_features :: empty calc_value for feature_name - %s, not added to calculated_features' % (
                        str(line[0])))
    return calculated_features


def minmax_scale_check(
    fp_id_metric_ts, anomalous_timeseries, range_tolerance,
        range_tolerance_percentage, fp_id, base_name, metric_timestamp,
        features_percentage_diff):
    """
    Called by nothing yet.  Used to run a minmax scaling check and determine if
    the features_sum of the 2 minmax scaled timeseries match.

    :param fp_id_metric_ts:
    :param anomalous_timeseries:
    :param range_tolerance:
    :param range_tolerance_percentage
    :param fp_id:
    :param base_name:
    :param metric_timestamp:
    :param features_percentage_diff:
    :type fp_id_metric_ts: int
    :return: (minmax_not_anomalous, minmax_fp_features_sum, minmax_fp_features_count, minmax_anomalous_features_sum, minmax_anomalous_features_count)
    :rtype: tuple

    """

    # @modified 20191115 - Branch #3262: py3
    # not_anomalous = False

    try:
        minmax_fp_values = [x[1] for x in fp_id_metric_ts]
        min_fp_value = min(minmax_fp_values)
        max_fp_value = max(minmax_fp_values)
    except:
        min_fp_value = False
        max_fp_value = False
    try:
        minmax_anomalous_values = [x2[1] for x2 in anomalous_timeseries]
        min_anomalous_value = min(minmax_anomalous_values)
        max_anomalous_value = max(minmax_anomalous_values)
    except:
        min_anomalous_value = False
        max_anomalous_value = False
    lower_range_not_same = True
    try:
        if int(min_fp_value) == int(min_anomalous_value):
            lower_range_not_same = False
            lower_range_similar = True
            logger.info('min value of fp_id_metric_ts (%s) and anomalous_timeseries (%s) are the same' % (
                str(min_fp_value), str(min_anomalous_value)))
    except:
        lower_range_not_same = True
    if min_fp_value and min_anomalous_value and lower_range_not_same:
        if int(min_fp_value) == int(min_anomalous_value):
            lower_range_similar = True
            logger.info('min value of fp_id_metric_ts (%s) and anomalous_timeseries (%s) are the same' % (
                str(min_fp_value), str(min_anomalous_value)))
        else:
            lower_min_fp_value = int(min_fp_value - (min_fp_value * range_tolerance))
            upper_min_fp_value = int(min_fp_value + (min_fp_value * range_tolerance))
            if int(min_anomalous_value) in range(lower_min_fp_value, upper_min_fp_value):
                lower_range_similar = True
                logger.info('min value of fp_id_metric_ts (%s) and anomalous_timeseries (%s) are similar within %s percent of each other' % (
                    str(min_fp_value),
                    str(min_anomalous_value),
                    str(range_tolerance_percentage)))
    if not lower_range_similar:
        logger.info('lower range of fp_id_metric_ts (%s) and anomalous_timeseries (%s) are not similar' % (
            str(min_fp_value), str(min_anomalous_value)))
    upper_range_not_same = True
    try:
        if int(max_fp_value) == int(max_anomalous_value):
            upper_range_not_same = False
            upper_range_similar = True
            logger.info('max value of fp_id_metric_ts (%s) and anomalous_timeseries (%s) are the same' % (
                str(max_fp_value), str(max_anomalous_value)))
    except:
        upper_range_not_same = True
    if max_fp_value and max_anomalous_value and lower_range_similar and upper_range_not_same:
        # @added 20180717 - Task #2446: Optimize Ionosphere
        #                   Feature #2404: Ionosphere - fluid approximation
        # On low values such as 1 and 2, the range_tolerance
        # should be adjusted to account for the very small
        # range. TODO
        lower_max_fp_value = int(max_fp_value - (max_fp_value * range_tolerance))
        upper_max_fp_value = int(max_fp_value + (max_fp_value * range_tolerance))
        if int(max_anomalous_value) in range(lower_max_fp_value, upper_max_fp_value):
            upper_range_similar = True
            logger.info('max value of fp_id_metric_ts (%s) and anomalous_timeseries (%s) are similar within %s percent of each other' % (
                str(max_fp_value), str(max_anomalous_value),
                str(range_tolerance_percentage)))
        else:
            logger.info('max value of fp_id_metric_ts (%s) and anomalous_timeseries (%s) are not similar' % (
                str(max_fp_value), str(max_anomalous_value)))

    if lower_range_similar and upper_range_similar:
        range_similar = True
    else:
        logger.info('the ranges of fp_id_metric_ts and anomalous_timeseries differ significantly Min-Max scaling will be skipped')

    minmax_fp_ts = []
    # if fp_id_metric_ts:
    if range_similar:
        if LOCAL_DEBUG:
            logger.debug('debug :: creating minmax_fp_ts from minmax scaled fp_id_metric_ts')
        try:
            minmax_fp_values = [x[1] for x in fp_id_metric_ts]
            x_np = np.asarray(minmax_fp_values)
            # Min-Max scaling
            np_minmax = (x_np - x_np.min()) / (x_np.max() - x_np.min())
            for (ts, v) in zip(fp_id_metric_ts, np_minmax):
                minmax_fp_ts.append([ts[0], v])
            logger.info('minmax_fp_ts list populated with the minmax scaled time series with %s data points' % str(len(minmax_fp_ts)))
        except:
            logger.error(traceback.format_exc())
            logger.error('error :: could not minmax scale fp id %s time series for %s' % (str(fp_id), str(base_name)))
        if not minmax_fp_ts:
            logger.error('error :: minmax_fp_ts list not populated')

    minmax_anomalous_ts = []
    anomalous_ts_values_count = len(anomalous_timeseries)
    if minmax_fp_ts:
        # Only process if they are approximately the same length
        minmax_fp_ts_values_count = len(minmax_fp_ts)
        if minmax_fp_ts_values_count - anomalous_ts_values_count in range(-14, 14):
            try:
                minmax_anomalous_values = [x2[1] for x2 in anomalous_timeseries]
                x_np = np.asarray(minmax_anomalous_values)
                # Min-Max scaling
                np_minmax = (x_np - x_np.min()) / (x_np.max() - x_np.min())
                for (ts, v) in zip(fp_id_metric_ts, np_minmax):
                    minmax_anomalous_ts.append([ts[0], v])
            except:
                logger.error(traceback.format_exc())
                logger.error('error :: could not minmax scale current time series anomalous_timeseries for %s' % (str(fp_id), str(base_name)))
            if len(minmax_anomalous_ts) > 0:
                logger.info('minmax_anomalous_ts is populated with %s data points' % str(len(minmax_anomalous_ts)))
            else:
                logger.error('error :: minmax_anomalous_ts is not populated')
        else:
            logger.info('minmax scaled check will be skipped - anomalous_ts_values_count is %s and minmax_fp_ts is %s' % (str(anomalous_ts_values_count), str(minmax_fp_ts_values_count)))

    minmax_fp_ts_csv = '%s/fpid.%s.%s.minmax_fp_ts.tsfresh.input.std.csv' % (
        settings.SKYLINE_TMP_DIR, str(fp_id), base_name)
    minmax_fp_fname_out = minmax_fp_ts_csv + '.transposed.csv'
    anomalous_ts_csv = '%s/%s.%s.minmax_anomalous_ts.tsfresh.std.csv' % (
        settings.SKYLINE_TMP_DIR, metric_timestamp, base_name)
    anomalous_fp_fname_out = anomalous_ts_csv + '.transposed.csv'

    # @modified 20210101 - Task #3928: Update Skyline to use new tsfresh feature extraction method
    # tsf_settings = ReasonableFeatureExtractionSettings()
    # tsf_settings.disable_progressbar = True

    minmax_fp_features_sum = None
    minmax_anomalous_features_sum = None
    if minmax_anomalous_ts and minmax_fp_ts:
        if LOCAL_DEBUG:
            logger.debug('debug :: analyzing minmax_fp_ts and minmax_anomalous_ts')
        if not os.path.isfile(minmax_fp_ts_csv):
            if LOCAL_DEBUG:
                logger.debug('debug :: creating %s from minmax_fp_ts' % minmax_fp_ts_csv)
            datapoints = minmax_fp_ts
            converted = []
            for datapoint in datapoints:
                try:
                    new_datapoint = [float(datapoint[0]), float(datapoint[1])]
                    converted.append(new_datapoint)
                except:  # nosec
                    continue
            if LOCAL_DEBUG:
                if len(converted) > 0:
                    logger.debug('debug :: converted is populated')
                else:
                    logger.debug('debug :: error :: converted is not populated')
            for ts, value in converted:
                try:
                    utc_ts_line = '%s,%s,%s\n' % (base_name, str(int(ts)), str(value))
                    with open(minmax_fp_ts_csv, 'a') as fh:
                        fh.write(utc_ts_line)
                except:
                    logger.error(traceback.format_exc())
                    logger.error('error :: could not write to file %s' % (str(minmax_fp_ts_csv)))
        else:
            logger.info('file found %s, using for data' % minmax_fp_ts_csv)

        if not os.path.isfile(minmax_fp_ts_csv):
            logger.error('error :: file not found %s' % minmax_fp_ts_csv)
        else:
            logger.info('file exists to create the minmax_fp_ts data frame from - %s' % minmax_fp_ts_csv)

        try:
            df = pd.read_csv(minmax_fp_ts_csv, delimiter=',', header=None, names=['metric', 'timestamp', 'value'])
            df.columns = ['metric', 'timestamp', 'value']
        except:
            logger.error(traceback.format_exc())
            logger.error('error :: failed to created data frame from %s' % (str(minmax_fp_ts_csv)))
        try:
            df_features = extract_features(
                # @modified 20210101 - Task #3928: Update Skyline to use new tsfresh feature extraction method
                # df, column_id='metric', column_sort='timestamp', column_kind=None,
                # column_value=None, feature_extraction_settings=tsf_settings)
                df, default_fc_parameters=EfficientFCParameters(),
                column_id='metric', column_sort='timestamp', column_kind=None,
                column_value=None, disable_progressbar=True)
        except:
            logger.error(traceback.format_exc())
            logger.error('error :: failed to created df_features from %s' % (str(minmax_fp_ts_csv)))
        # Create transposed features csv
        if not os.path.isfile(minmax_fp_fname_out):
            # Transpose
            df_t = df_features.transpose()
            df_t.to_csv(minmax_fp_fname_out)
        else:
            if LOCAL_DEBUG:
                logger.debug('debug :: file exists - %s' % minmax_fp_fname_out)
        try:
            # Calculate the count and sum of the features values
            df_sum = pd.read_csv(
                minmax_fp_fname_out, delimiter=',', header=0,
                names=['feature_name', 'value'])
            df_sum.columns = ['feature_name', 'value']
            df_sum['feature_name'] = df_sum['feature_name'].astype(str)
            df_sum['value'] = df_sum['value'].astype(float)
            minmax_fp_features_count = len(df_sum['value'])
            minmax_fp_features_sum = df_sum['value'].sum()
            logger.info('minmax_fp_ts - features_count: %s, features_sum: %s' % (str(minmax_fp_features_count), str(minmax_fp_features_sum)))
        except:
            logger.error(traceback.format_exc())
            logger.error('error :: failed to created df_sum from %s' % (str(minmax_fp_fname_out)))

        if minmax_fp_features_count > 0:
            if LOCAL_DEBUG:
                logger.debug('debug :: minmax_fp_features_count of the minmax_fp_ts is %s' % str(minmax_fp_features_count))
        else:
            logger.error('error :: minmax_fp_features_count is %s' % str(minmax_fp_features_count))

        if not os.path.isfile(anomalous_ts_csv):
            datapoints = minmax_anomalous_ts
            converted = []
            for datapoint in datapoints:
                try:
                    new_datapoint = [float(datapoint[0]), float(datapoint[1])]
                    converted.append(new_datapoint)
                except:  # nosec
                    continue
            for ts, value in converted:
                utc_ts_line = '%s,%s,%s\n' % (base_name, str(int(ts)), str(value))
                with open(anomalous_ts_csv, 'a') as fh:
                    fh.write(utc_ts_line)

        df = pd.read_csv(anomalous_ts_csv, delimiter=',', header=None, names=['metric', 'timestamp', 'value'])
        df.columns = ['metric', 'timestamp', 'value']
        df_features_current = extract_features(
            # @modified 20210101 - Task #3928: Update Skyline to use new tsfresh feature extraction method
            # df, column_id='metric', column_sort='timestamp', column_kind=None,
            # column_value=None, feature_extraction_settings=tsf_settings)
            df, default_fc_parameters=EfficientFCParameters(),
            column_id='metric', column_sort='timestamp', column_kind=None,
            column_value=None, disable_progressbar=True)

        # Create transposed features csv
        if not os.path.isfile(anomalous_fp_fname_out):
            # Transpose
            df_t = df_features_current.transpose()
            df_t.to_csv(anomalous_fp_fname_out)
        # Calculate the count and sum of the features values
        df_sum_2 = pd.read_csv(
            anomalous_fp_fname_out, delimiter=',', header=0,
            names=['feature_name', 'value'])
        df_sum_2.columns = ['feature_name', 'value']
        df_sum_2['feature_name'] = df_sum_2['feature_name'].astype(str)
        df_sum_2['value'] = df_sum_2['value'].astype(float)
        minmax_anomalous_features_count = len(df_sum_2['value'])
        minmax_anomalous_features_sum = df_sum_2['value'].sum()
        logger.info('minmax_anomalous_ts - minmax_anomalous_features_count: %s, minmax_anomalous_features_sum: %s' % (
            str(minmax_anomalous_features_count),
            str(minmax_anomalous_features_sum)))

    if minmax_fp_features_sum and minmax_anomalous_features_sum:
        percent_different = None
        # @modified 20210425 - Task #4030: refactoring
        #                      Feature #4014: Ionosphere - inference
        # Use the common function added
        # try:
        #     fp_sum_array = [minmax_fp_features_sum]
        #     calc_sum_array = [minmax_anomalous_features_sum]
        #     percent_different = 100
        #     sums_array = np.array([minmax_fp_features_sum, minmax_anomalous_features_sum], dtype=float)
        #     calc_percent_different = np.diff(sums_array) / sums_array[:-1] * 100.
        #     percent_different = calc_percent_different[0]
        #     logger.info('percent_different between minmax scaled features sums - %s' % str(percent_different))
        # except:
        #     logger.error(traceback.format_exc())
        #     logger.error('error :: failed to calculate percent_different from minmax scaled features sums')
        try:
            percent_different = get_percent_different(minmax_fp_features_sum, minmax_anomalous_features_sum, True)
            logger.info('percent_different between minmax scaled features sums - %s' % str(percent_different))
        except Exception as e:
            logger.error('error :: failed to calculate percent_different between minmax scaled features sums - %s' % e)

        if percent_different:
            almost_equal = None
            try:
                # np.testing.assert_array_almost_equal(fp_sum_array, calc_sum_array)
                np.testing.assert_array_almost_equal(minmax_fp_features_sum, minmax_anomalous_features_sum)
                almost_equal = True
            except:
                almost_equal = False

            if almost_equal:
                minmax_not_anomalous = True
                logger.info('minmax scaled common features sums are almost equal, not anomalous')

            # if diff_in_sums <= 1%:
            if percent_different < 0:
                new_pdiff = percent_different * -1
                percent_different = new_pdiff

            if percent_different < float(features_percentage_diff):
                minmax_not_anomalous = True
                # log
                logger.info('not anomalous - minmax scaled features profile match - %s - %s' % (base_name, str(minmax_not_anomalous)))
                logger.info(
                    'minmax scaled calculated features sum are within %s percent of fp_id %s with %s, not anomalous' %
                    (str(features_percentage_diff),
                        str(fp_id), str(percent_different)))

            # @modified 20191115 - Branch #3262: py3
            # if minmax_not_anomalous:
            #     not_anomalous = True
            #     minmax = 1

                # Created time series resources for graphing in
                # the matched page

    return (minmax_not_anomalous, minmax_fp_features_sum, minmax_fp_features_count, minmax_anomalous_features_sum, minmax_anomalous_features_count)
