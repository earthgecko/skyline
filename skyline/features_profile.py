import logging
import os
# import sys
# import datetime
# import csv
import time
from ast import literal_eval

import traceback
# import json
from timeit import default_timer as timer
# import numpy as np
import pandas as pd

from tsfresh.feature_extraction import (

    # @modified 20210101 - Task #3928: Update Skyline to use new tsfresh feature extraction method
    # extract_features, ReasonableFeatureExtractionSettings)
    extract_features, EfficientFCParameters)
from tsfresh import __version__ as tsfresh_version

import settings
import skyline_version
from skyline_functions import write_data_to_file
# @added 20200813 - Feature #3670: IONOSPHERE_CUSTOM_KEEP_TRAINING_TIMESERIES_FOR
from skyline_functions import historical_data_dir_exists

# TSFRESH_VERSION below was only added to a single log output as it was unused
# however it may be used in one of the tests in some way, I shall have to search
from tsfresh_feature_names import TSFRESH_FEATURES, TSFRESH_VERSION

skyline_version = skyline_version.__absolute_version__

# @added 20200813 - Feature #3670: IONOSPHERE_CUSTOM_KEEP_TRAINING_TIMESERIES_FOR
try:
    IONOSPHERE_HISTORICAL_DATA_FOLDER = settings.IONOSPHERE_HISTORICAL_DATA_FOLDER
except:
    IONOSPHERE_HISTORICAL_DATA_FOLDER = '/opt/skyline/ionosphere/historical_data'
try:
    IONOSPHERE_CUSTOM_KEEP_TRAINING_TIMESERIES_FOR = settings.IONOSPHERE_CUSTOM_KEEP_TRAINING_TIMESERIES_FOR
except:
    IONOSPHERE_CUSTOM_KEEP_TRAINING_TIMESERIES_FOR = []


def feature_name_id(current_skyline_app, feature_name):
    """
    Determine the Skyline id of a tsfresh feature name

    :param feature_name: the tsfresh feature name
    :type feature_name: str
    :return: id
    :rtype: int

    """

    # f_name = 'value__spkt_welch_density__coeff_8'
    f_name = str(feature_name)
    for feature in TSFRESH_FEATURES:
        if str(feature[1]) == f_name:
            fn_id = str(feature[0])
            del f_name
            del feature
            return fn_id

    del feature
    del f_name
    return 0


def calculate_features_profile(current_skyline_app, timestamp, metric, context):
    """
    Calculates a tsfresh features profile from a training data set

    :param timestamp: the timestamp of metric anomaly with training data
    :type timestamp: str
    :param metric: the base_name of the metric
    :type metric: str
    :param context: the context
    :type metric: str

    :return: (features_profile_csv_file_path, successful, fail_msg, traceback_format_exc, calc_time)
    :rtype: int
    :rtype: (str, boolean, str, str, str)
    """

    current_skyline_app_logger = current_skyline_app + 'Log'
    current_logger = logging.getLogger(current_skyline_app_logger)

    base_name = str(metric)

    # @added 20190413 - Bug #2934: Ionosphere - no mirage.redis.24h.json file
    # Set a default log_context, just in case it is not set if something is
    # added in the future
    log_context = 'unknown'

    if context == 'training_data':
        log_context = 'training data'
    if context == 'features_profiles':
        log_context = 'features profile data'
    if context == 'ionosphere':
        log_context = 'ionosphere'
    # @added 20170114 - Feature #1854: Ionosphere learn
    if context == 'ionosphere_learn':
        log_context = 'ionosphere :: learn'

    # TODO
    # @added 20190314 - Feature #2484: FULL_DURATION feature profiles
    # Here we add the bifurcation to also create a features
    # profile at FULL_DURATION for all Mirage metrics.  With a
    # view to increase the number of matches trained metric
    # achieve by also allowing for the creation and comparing of
    # the FULL_DURATION features profiles as well.
    # How I am not certain but needs to tie up with this Feature in:
    # skyline/ionosphere/ionosphere.py
    # skyline/webapp/webapp.py
    if context == 'ionosphere_echo':
        log_context = 'ionosphere :: echo'
    if context == 'ionosphere_echo_check':
        log_context = 'ionosphere :: echo check'

    current_logger.info('%s feature profile creation requested for %s at %s' % (
        log_context, base_name, timestamp))

    timeseries_dir = base_name.replace('.', '/')
    if context == 'training_data' or context == 'ionosphere':
        metric_data_dir = '%s/%s/%s' % (
            settings.IONOSPHERE_DATA_FOLDER, timestamp, timeseries_dir)

        # @added 20200813 - Feature #3670: IONOSPHERE_CUSTOM_KEEP_TRAINING_TIMESERIES_FOR
        if context == 'training_data':
            metric_data_dir_does_not_exist = False
            if not os.path.exists(metric_data_dir):
                metric_data_dir_does_not_exist = True
            if IONOSPHERE_CUSTOM_KEEP_TRAINING_TIMESERIES_FOR and metric_data_dir_does_not_exist:
                try:
                    historical_data, metric_data_dir = historical_data_dir_exists(current_skyline_app, metric_data_dir)
                    if historical_data:
                        current_logger.info('create_features_profile :: using historical training data - %s' % metric_data_dir)
                except:
                    trace = traceback.format_exc()
                    current_logger.error(trace)
                    fail_msg = 'error :: create_features_profile :: failed to determine whether this is historical training data'
                    current_logger.error('%s' % fail_msg)
                    if context == 'training_data':
                        # Raise to webbapp I believe to provide traceback to user in UI
                        raise
                    else:
                        return False, False, False, fail_msg, trace

    if context == 'features_profiles':
        metric_data_dir = '%s/%s/%s' % (
            settings.IONOSPHERE_PROFILES_FOLDER, timeseries_dir, timestamp)

    # @added 20170113 - Feature #1854: Ionosphere learn
    if context == 'ionosphere_learn':
        metric_data_dir = '%s/%s/%s' % (
            settings.IONOSPHERE_LEARN_FOLDER, timestamp, timeseries_dir)

    # @added 20190327 - Feature #2484: FULL_DURATION feature profiles
    # Added ionosphere_echo and ionosphere_echo_check
    if context == 'ionosphere_echo' or context == 'ionosphere_echo_check':
        metric_data_dir = '%s/%s/%s' % (
            settings.IONOSPHERE_DATA_FOLDER, timestamp, timeseries_dir)

    features_profile_created_file = '%s/%s.%s.fp.created.txt' % (
        metric_data_dir, str(timestamp), base_name)

    features_profile_details_file = '%s/%s.%s.fp.details.txt' % (
        metric_data_dir, str(timestamp), base_name)

    # @added 20190327 - Feature #2484: FULL_DURATION feature profiles
    if context == 'ionosphere_echo_check':
        features_profile_created_file = '%s/%s.%s.echo.fp.created.txt' % (
            metric_data_dir, str(timestamp), base_name)
        features_profile_details_file = '%s/%s.%s.echo.fp.details.txt' % (
            metric_data_dir, str(timestamp), base_name)

    # @added 20170108 - Feature #1842: Ionosphere - Graphite now graphs
    # Added metric_check_file and ts_full_duration is needed to be determined
    # and added the to features_profile_details_file as it was not added here on
    # the 20170104 when it was added the webapp and ionosphere
    metric_var_filename = '%s.txt' % str(base_name)
    anomaly_check_file = '%s/%s' % (metric_data_dir, metric_var_filename)
    ts_full_duration = int(settings.FULL_DURATION)
    if os.path.isfile(anomaly_check_file):
        # Read the details file
        with open(anomaly_check_file, 'r') as f:
            anomaly_details = f.readlines()
            for i, line in enumerate(anomaly_details):
                if 'full_duration' in line:
                    _ts_full_duration = '%s' % str(line).split("'", 2)
                    full_duration_array = literal_eval(_ts_full_duration)
                    ts_full_duration = str(int(full_duration_array[1]))

    anomaly_json = '%s/%s.json' % (metric_data_dir, base_name)

    # @added 20190327 - Feature #2484: FULL_DURATION feature profiles
    if context == 'ionosphere_echo' or context == 'ionosphere_echo_check':
        ts_full_duration = str(settings.FULL_DURATION)
        full_duration_in_hours = int(settings.FULL_DURATION / 60 / 60)
        anomaly_json = '%s/%s.mirage.redis.%sh.json' % (metric_data_dir, base_name, str(full_duration_in_hours))

    ts_csv = '%s/%s.tsfresh.input.csv' % (metric_data_dir, base_name)
    # @added 20190327 - Feature #2484: FULL_DURATION feature profiles
    if context == 'ionosphere_echo_check':
        ts_csv = '%s/%s.echo.tsfresh.input.csv' % (metric_data_dir, base_name)

#    anomaly_json = '/opt/skyline/ionosphere/data/1480104000/stats/statsd/graphiteStats/calculationtime/stats.statsd.graphiteStats.calculationtime.json'
#    ts_csv = '/opt/skyline/ionosphere/data/1480104000/stats/statsd/graphiteStats/calculationtime/stats.statsd.graphiteStats.calculationtime.tsfresh.input.csv'
    # This is simply to stay in line with tsfresh naming conventions in their
    # docs and examples
    fname_in = ts_csv
    t_fname_out = fname_in + '.features.transposed.csv'

    fp_id = None
    f_calc = 'unknown'
    if os.path.isfile(features_profile_details_file):
        # @modified 20190413 - Bug #2934: Ionosphere - no mirage.redis.24h.json file
        # Added log_context to report the context
        current_logger.info('%s :: features profile details file exist - %s' % (
            log_context, features_profile_details_file))
        try:
            with open(features_profile_details_file, 'r') as f:
                fp_details_str = f.read()
            fp_details_array = literal_eval(fp_details_str)
            f_calc = ' (previously calculated by Ionosphere) - %s' % str(fp_details_array[2])
        except:
            trace = traceback.format_exc()
            current_logger.error(trace)
            # @modified 20190413 - Bug #2934: Ionosphere - no mirage.redis.24h.json file
            # Added log_context to report the context
            current_logger.error(
                'error: %s :: failed to read from %s' % (log_context, features_profile_details_file))
    else:
        # @modified 20190413 - Bug #2934: Ionosphere - no mirage.redis.24h.json file
        # Added log_context to report the context
        current_logger.info('%s - OK no features profile details file exists - %s' % (
            log_context, features_profile_details_file))

    fp_created = None
    if os.path.isfile(features_profile_created_file):
        # @modified 20190413 - Bug #2934: Ionosphere - no mirage.redis.24h.json file
        # Added log_context to report the context
        current_logger.info('%s :: features profile created file exist - %s' % (
            log_context, features_profile_created_file))
        try:
            with open(features_profile_created_file, 'r') as f:
                fp_created_str = f.read()
            fp_created_array = literal_eval(fp_created_str)
            fp_id = fp_created_array[0]
            fp_created = True
        except:
            trace = traceback.format_exc()
            current_logger.error(trace)
            # @modified 20190413 - Bug #2934: Ionosphere - no mirage.redis.24h.json file
            # Added log_context to report the context
            current_logger.error(
                'error: %s :: failed to read fp_id from %s' % (log_context, features_profile_created_file))
    else:
        # @modified 20190413 - Bug #2934: Ionosphere - no mirage.redis.24h.json file
        # Added log_context to report the context
        current_logger.info('%s :: OK no features profile created file exists - %s' % (
            log_context, features_profile_created_file))

    if os.path.isfile(t_fname_out):
        # @modified 20190413 - Bug #2934: Ionosphere - no mirage.redis.24h.json file
        # Added log_context to report the context
        current_logger.info('%s :: transposed features already exist - %s' % (
            log_context, t_fname_out))
        return str(t_fname_out), True, fp_created, fp_id, 'none', 'none', f_calc

    start = timer()
    raw_timeseries = []
    if os.path.isfile(anomaly_json):
        try:
            # Read the timeseries json file
            with open(anomaly_json, 'r') as f:
                raw_timeseries = f.read()
        except:
            trace = traceback.format_exc()
            current_logger.error(trace)
            # @modified 20190413 - Bug #2934: Ionosphere - no mirage.redis.24h.json file
            # Added log_context to report the context
            current_logger.error(
                'error: %s :: failed to read timeseries data from %s' % (log_context, anomaly_json))
            fail_msg = 'error: %s :: failed to read timeseries data from %s' % (log_context, anomaly_json)
            end = timer()
            return 'error', False, fp_created, fp_id, fail_msg, trace, f_calc
    else:
        trace = 'none'
        # @modified 20190413 - Bug #2934: Ionosphere - no mirage.redis.24h.json file
        # Added log_context to report the context
        fail_msg = 'error :: %s :: file not found - %s' % (log_context, anomaly_json)
        current_logger.error(fail_msg)
        end = timer()
        return 'error', False, fp_created, fp_id, fail_msg, trace, f_calc

    # Convert the timeseries to csv
    timeseries_array_str = str(raw_timeseries).replace('(', '[').replace(')', ']')
    del raw_timeseries
    timeseries = literal_eval(timeseries_array_str)

    datapoints = timeseries
    del timeseries
    converted = []
    for datapoint in datapoints:
        try:
            new_datapoint = [float(datapoint[0]), float(datapoint[1])]
            converted.append(new_datapoint)
        # @modified 20170913 - Task #2160: Test skyline with bandit
        # Added nosec to exclude from bandit tests
        except:  # nosec
            continue

    del datapoints

    if os.path.isfile(ts_csv):
        os.remove(ts_csv)

    for ts, value in converted:
        # print('%s,%s' % (str(int(ts)), str(value)))
        utc_ts_line = '%s,%s,%s\n' % (metric, str(int(ts)), str(value))
        with open(ts_csv, 'a') as fh:
            fh.write(utc_ts_line)

    del converted

    try:
        df = pd.read_csv(ts_csv, delimiter=',', header=None, names=['metric', 'timestamp', 'value'])
        # @modified 20190413 - Bug #2934: Ionosphere - no mirage.redis.24h.json file
        # Added log_context to report the context
        current_logger.info('%s :: DataFrame created with %s' % (
            log_context, ts_csv))
    except:
        trace = traceback.format_exc()
        current_logger.error(trace)
        # @modified 20190413 - Bug #2934: Ionosphere - no mirage.redis.24h.json file
        # Added log_context to report the context
        fail_msg = 'error: %s :: failed to create a pandas DataFrame with %s' % (log_context, ts_csv)
        current_logger.error('%s' % fail_msg)
        if os.path.isfile(ts_csv):
            os.remove(ts_csv)
            # @modified 20190413 - Bug #2934: Ionosphere - no mirage.redis.24h.json file
            # Added log_context to report the context
            current_logger.info('%s :: removed %s' % (log_context, ts_csv))
        end = timer()
        return 'error', False, fp_created, fp_id, fail_msg, trace, f_calc

# @added 20161207 - Task #1658: Patterning Skyline Ionosphere
# Coverting the Dataframe types to suit MySQL data types
# For anyone in here if you have done a code review of Skyline there are
# a number of questions that arise from the decision to deviate from json or
# storing msgppack as BLOB etc.  tsfresh used csv and we can csv from Graphite
# etc.  Skyline should be able to handle csv.  As for how data is stored in
# MySQL, this was given considerable review and thought.  Given that Ionosphere
# and Skyline in general should not be limited to the domain of analyzing
# Graphite machine metrics but other timeseries data sources too.
#    df['feature_name'] = df['feature_name'].astype(string)
#    df['value'] = df['value'].astype(float)

    # Test the DataFrame
    try:
        df_created = df.head()
        del df_created
    except:
        trace = traceback.format_exc()
        current_logger.debug(trace)
        # @modified 20190413 - Bug #2934: Ionosphere - no mirage.redis.24h.json file
        # Added log_context to report the context
        fail_msg = 'error: %s :: failed to read the pandas DataFrame created with %s' % (log_context, ts_csv)
        current_logger.error('%s' % fail_msg)
        if os.path.isfile(ts_csv):
            os.remove(ts_csv)
            # @modified 20190413 - Bug #2934: Ionosphere - no mirage.redis.24h.json file
            # Added log_context to report the context
            current_logger.info('%s :: removed %s' % (log_context, ts_csv))
        end = timer()
        return 'error', False, fp_created, fp_id, fail_msg, trace, f_calc

    df.columns = ['metric', 'timestamp', 'value']

    start_feature_extraction = timer()
    # @modified 20190413 - Bug #2934: Ionosphere - no mirage.redis.24h.json file
    # Added log_context to report the context
    current_logger.info('%s :: starting extract_features with %s' % (
        log_context, str(TSFRESH_VERSION)))
    df_features = False
    try:
        # @modified 20161226 - Bug #1822: tsfresh extract_features process stalling
        # Changed to use the new ReasonableFeatureExtractionSettings that was
        # introduced in tsfresh-0.4.0 to exclude the computationally high cost
        # of extracting features from very static timeseries that has little to
        # no variation is the values, which results in features taking up to
        # almost 600 seconds to calculate on a timeseries of length 10075
        # (168h - 1 datapoint per 60s)
        # In terms of inline feature calculatation, always exclude
        # high_comp_cost features.
        # df_features = extract_features(df, column_id='metric', column_sort='timestamp', column_kind=None, column_value=None)
        # @modified 20210101 - Task #3928: Update Skyline to use new tsfresh feature extraction method
        # tsf_settings = ReasonableFeatureExtractionSettings()
        # >>> from tsfresh.feature_extraction import extract_features, EfficientFCParameters
        # >>> extract_features(df, default_fc_parameters=EfficientFCParameters())

        # Disable tqdm progress bar
        # @modified 20210101 - Task #3928: Update Skyline to use new tsfresh feature extraction method
        # tsf_settings.disable_progressbar = True

        df_features = extract_features(
            # @modified 20210101 - Task #3928: Update Skyline to use new tsfresh feature extraction method
            # df, column_id='metric', column_sort='timestamp', column_kind=None,
            # column_value=None, feature_extraction_settings=tsf_settings)
            df, default_fc_parameters=EfficientFCParameters(),
            column_id='metric', column_sort='timestamp', column_kind=None,
            column_value=None, disable_progressbar=True)
        # @modified 20190413 - Bug #2934: Ionosphere - no mirage.redis.24h.json file
        # Added log_context to report the context
        current_logger.info('%s :: features extracted from %s data' % (
            log_context, ts_csv))
    except:
        trace = traceback.print_exc()
        current_logger.debug(trace)
        # @modified 20190413 - Bug #2934: Ionosphere - no mirage.redis.24h.json file
        # Added log_context to report the context
        fail_msg = 'error: %s :: extracting features with tsfresh from - %s' % (log_context, ts_csv)
        current_logger.error('%s' % fail_msg)
        end_feature_extraction = timer()
        # @modified 20190413 - Bug #2934: Ionosphere - no mirage.redis.24h.json file
        # Added log_context to report the context
        current_logger.info(
            '%s :: feature extraction failed in %.6f seconds' % (
                log_context, (end_feature_extraction - start_feature_extraction)))
        if os.path.isfile(ts_csv):
            os.remove(ts_csv)
            # @modified 20190413 - Bug #2934: Ionosphere - no mirage.redis.24h.json file
            # Added log_context to report the context
            current_logger.info('%s :: removed %s' % (log_context, ts_csv))
        end = timer()
        return 'error', False, fp_created, fp_id, fail_msg, trace, f_calc

    end_feature_extraction = timer()
    feature_extraction_time = end_feature_extraction - start_feature_extraction
    # @modified 20190413 - Bug #2934: Ionosphere - no mirage.redis.24h.json file
    # Added log_context to report the context
    current_logger.info(
        '%s :: feature extraction took %.6f seconds' % (log_context, feature_extraction_time))

    del df

    # write to disk
    fname_out = fname_in + '.features.csv'
    # df_features.to_csv(fname_out)

    # Transpose
    df_t = False
    try:
        df_t = df_features.transpose()
        # @modified 20190413 - Bug #2934: Ionosphere - no mirage.redis.24h.json file
        # Added log_context to report the context
        current_logger.info('%s :: features transposed' % log_context)
    except:
        trace = traceback.print_exc()
        current_logger.debug(trace)
        # @modified 20190413 - Bug #2934: Ionosphere - no mirage.redis.24h.json file
        # Added log_context to report the context
        fail_msg = 'error :: %s :: transposing tsfresh features from - %s' % (log_context, ts_csv)
        current_logger.error('%s' % fail_msg)
        if os.path.isfile(ts_csv):
            os.remove(ts_csv)
            # @modified 20190413 - Bug #2934: Ionosphere - no mirage.redis.24h.json file
            # Added log_context to report the context
            current_logger.info('%s :: removed %s' % (log_context, ts_csv))
        end = timer()
        return 'error', False, fp_created, fp_id, fail_msg, trace, f_calc

    del df_features

    # Create transposed features csv
    t_fname_out = fname_in + '.features.transposed.csv'
    try:
        df_t.to_csv(t_fname_out)
    except:
        trace = traceback.print_exc()
        current_logger.debug(trace)
        # @modified 20190413 - Bug #2934: Ionosphere - no mirage.redis.24h.json file
        # Added log_context to report the context
        fail_msg = 'error :: %s :: saving transposed tsfresh features from - %s' % (log_context, ts_csv)
        current_logger.error('%s' % fail_msg)
        if os.path.isfile(ts_csv):
            os.remove(ts_csv)
            # @modified 20190413 - Bug #2934: Ionosphere - no mirage.redis.24h.json file
            # Added log_context to report the context
            current_logger.info('%s :: removed %s' % (log_context, ts_csv))
        end = timer()
        return 'error', False, fp_created, fp_id, fail_msg, trace, f_calc

    del df_t

    # Calculate the count and sum of the features values
    df_sum = False
    try:
        df_sum = pd.read_csv(
            t_fname_out, delimiter=',', header=0,
            names=['feature_name', 'value'])
        df_sum.columns = ['feature_name', 'value']
        df_sum['feature_name'] = df_sum['feature_name'].astype(str)
        df_sum['value'] = df_sum['value'].astype(float)
    except:
        trace = traceback.print_exc()
        current_logger.error(trace)
        # @modified 20190413 - Bug #2934: Ionosphere - no mirage.redis.24h.json file
        # Added log_context to report the context
        current_logger.error('error :: %s :: failed to create Dataframe to sum' % log_context)
    try:
        features_count = len(df_sum['value'])
    except:
        trace = traceback.print_exc()
        current_logger.debug(trace)
        # @modified 20190413 - Bug #2934: Ionosphere - no mirage.redis.24h.json file
        # Added log_context to report the context
        current_logger.error('error :: %s :: failed to count number of features, set to 0' % log_context)
        features_count = 0
    try:
        features_sum = df_sum['value'].sum()
    except:
        trace = traceback.print_exc()
        current_logger.debug(trace)
        # @modified 20190413 - Bug #2934: Ionosphere - no mirage.redis.24h.json file
        # Added log_context to report the context
        current_logger.error('error :: %s :: failed to sum feature values, set to 0' % log_context)
        features_sum = 0

    end = timer()

    # @modified 20190413 - Bug #2934: Ionosphere - no mirage.redis.24h.json file
    # Added log_context to report the context
    current_logger.info('%s :: features saved to %s' % (log_context, fname_out))
    current_logger.info('%s :: transposed features saved to %s' % (
        log_context, t_fname_out))
    total_calc_time = '%.6f' % (end - start)
    calc_time = '%.6f' % (feature_extraction_time)
    current_logger.info('%s :: total feature profile completed in %s seconds' % (
        log_context, str(total_calc_time)))

    # Create a features profile details file
    try:
        # @modified 20170108 - Feature #1842: Ionosphere - Graphite now graphs
        # Added the ts_full_duration here as it was not added here on the 20170104
        # when it was added the webapp and ionosphere
        data = '[%s, \'%s\', %s, %s, %s, %s]' % (
            str(int(time.time())), str(tsfresh_version), str(calc_time),
            str(features_count), str(features_sum), str(ts_full_duration))
        write_data_to_file(current_skyline_app, features_profile_details_file, 'w', data)
    except:
        trace = traceback.format_exc()
        current_logger.error('%s' % trace)
        # @modified 20190413 - Bug #2934: Ionosphere - no mirage.redis.24h.json file
        # Added log_context to report the context
        fail_msg = 'error :: %s :: failed to write %s' % (log_context, features_profile_details_file)
        current_logger.error('%s' % fail_msg)

    del df_sum

    if os.path.isfile(ts_csv):
        os.remove(ts_csv)
        # @modified 20190413 - Bug #2934: Ionosphere - no mirage.redis.24h.json file
        # Added log_context to report the context
        current_logger.info('%s :: removed the created csv - %s' % (
            log_context, ts_csv))

    # @added 20170112 - Feature #1854: Ionosphere learn - Redis ionosphere.learn.work namespace
    # Ionosphere learn needs Redis works sets, but this was moved to
    # ionosphere_backend.py and learn.py not done here

    return str(t_fname_out), True, fp_created, fp_id, 'none', 'none', str(calc_time)
