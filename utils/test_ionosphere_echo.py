from time import sleep
import logging
import os
from os.path import dirname, join, realpath
from optparse import OptionParser
import sys
import time
from ast import literal_eval
import traceback
from timeit import default_timer as timer
import csv
import numpy as np
import pandas as pd
from tsfresh.feature_extraction import (
    # @modified 20210101 - Task #3928: Update Skyline to use new tsfresh feature extraction method
    # extract_features, ReasonableFeatureExtractionSettings)
    extract_features, EfficientFCParameters)
from tsfresh import __version__ as tsfresh_version

__location__ = os.path.realpath(os.path.join(os.getcwd(), dirname(__file__)))
# Add the shared settings file to namespace.
sys.path.insert(0, os.path.join(__location__, '..', 'skyline'))
import settings
import skyline_version
from skyline_functions import (
    write_data_to_file,
    # @added 20191029 - Task #3302: Handle csv.reader in py3
    #                   Branch #3262: py3
    read_csv)
# @added 20210425 - Task #4030: refactoring
#                   Feature #4014: Ionosphere - inference
from functions.numpy.percent_different import get_percent_different

from tsfresh_feature_names import TSFRESH_FEATURES, TSFRESH_VERSION

settings.LOG_PATH = '/tmp'
logging.basicConfig()

parser = OptionParser()
parser.add_option("-p", "--percentage_similar", dest="echo_percentage_similar", default=3.5,
                  help="Pass the percentage_similar to use for testing Ionosphere echo")

parser.add_option("-t", "--timestamp", dest="echo_timestamp", default='all',
                  help="Pass the timestamp of the metric training_data to test (default is all)")

parser.add_option("-m", "--metric", dest="echo_metric", default='all',
                  help="Pass the metric to test (default is all)")

parser.add_option("-o", "--only_match_once", dest="echo_only_match_once", default=True,
                  help="Only match once, if a match is found do not check any remaining fps - set to True/False (default is True)")

(options, args) = parser.parse_args()

IONOSPHERE_ECHO_MINMAX_SCALING_FEATURES_PERCENT_SIMILAR = float(options.echo_percentage_similar)
if str(options.echo_only_match_once) == 'True':
    ONLY_MATCH_ONCE = True
else:
    ONLY_MATCH_ONCE = False
python_version = int(sys.version_info[0])


def calculate_features(calculated_feature_file):
    calculated_features = []
    count_id = 0

    # @added 20191029 - Task #3302: Handle csv.reader in py3
    #                   Branch #3262: py3
    if python_version == 3:
        try:
            codecs
        except:
            import codecs

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
                try:
                    calc_value = float(line[1])
                    calculated_features.append([feature_name, calc_value])
                except:
                    print('error :: failed to determine calc_value for %s from %s' % (feature_name, calculated_feature_file))

    all_calc_features_sum_list = []
    for feature_name, calc_value in calculated_features:
        all_calc_features_sum_list.append(float(calc_value))
    del calculated_features
    all_calc_features_sum = sum(all_calc_features_sum_list)
    return all_calc_features_sum


def create_test_features_profile(json_file):
    filename = os.path.basename(json_file)
    metric = filename.replace('.mirage.redis.24h.json', '')
    metric_data_dir = os.path.dirname(json_file)
    anomaly_json = json_file
    ts_csv = '%s.test.echo.tsfresh.input.csv' % (json_file)
    fname_in = ts_csv
    t_fname_out = fname_in + '.features.transposed.csv'
    if os.path.isfile(t_fname_out):
        return t_fname_out
    start = timer()
    with open(anomaly_json, 'r') as f:
        raw_timeseries = f.read()
    # Convert the timeseries to csv
    try:
        timeseries_array_str = str(raw_timeseries).replace('(', '[').replace(')', ']')
        del raw_timeseries
        timeseries = literal_eval(timeseries_array_str)
        del timeseries_array_str
    except:
        print('error :: could not literal_eval %s' % anomaly_json)
        print(traceback.format_exc())
        return False
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

    if os.path.isfile(ts_csv):
        os.remove(ts_csv)

    for ts, value in converted:
        # print('%s,%s' % (str(int(ts)), str(value)))
        utc_ts_line = '%s,%s,%s\n' % (metric, str(int(ts)), str(value))
        with open(ts_csv, 'a') as fh:
            fh.write(utc_ts_line)
    del converted

    df = pd.read_csv(ts_csv, delimiter=',', header=None, names=['metric', 'timestamp', 'value'])
#    print('DataFrame created with %s' % ts_csv)
    df.columns = ['metric', 'timestamp', 'value']

    # @modified 20210101 - Task #3928: Update Skyline to use new tsfresh feature extraction method
    # tsf_settings = ReasonableFeatureExtractionSettings()
    # Disable tqdm progress bar
    # tsf_settings.disable_progressbar = True

    df_features = extract_features(
        # @modified 20210101 - Task #3928: Update Skyline to use new tsfresh feature extraction method
        # df, column_id='metric', column_sort='timestamp', column_kind=None,
        # column_value=None, feature_extraction_settings=tsf_settings)
        df, default_fc_parameters=EfficientFCParameters(),
        column_id='metric', column_sort='timestamp', column_kind=None,
        column_value=None, disable_progressbar=True)
    del df
#    print('features extracted from %s data' % ts_csv)
    # write to disk
    fname_out = fname_in + '.features.csv'
    # Transpose
    df_t = df_features.transpose()
#    print('features transposed')
    # Create transposed features csv
    t_fname_out = fname_in + '.features.transposed.csv'
    df_t.to_csv(t_fname_out)
    del df_t
    # Calculate the count and sum of the features values
    df_sum = pd.read_csv(
        t_fname_out, delimiter=',', header=0,
        names=['feature_name', 'value'])
    df_sum.columns = ['feature_name', 'value']
    df_sum['feature_name'] = df_sum['feature_name'].astype(str)
    df_sum['value'] = df_sum['value'].astype(float)

    features_count = len(df_sum['value'])
    features_sum = df_sum['value'].sum()
    del df_sum
#    print('features saved to %s' % (fname_out))
#    print('transposed features saved to %s' % (t_fname_out))
    return t_fname_out


def calculate_features_other_minmax(use_file, i_json_file, metric):

    fp_id = 'testing.feature2484'
    base_name = metric
    metric_timestamp = 'none'

    not_anomalous = False
    minmax_not_anomalous = False
    minmax = 0
    minmax_check = True

    with open(use_file, 'r') as f:
        raw_timeseries = f.read()
    # Convert the timeseries to csv
    timeseries_array_str = str(raw_timeseries).replace('(', '[').replace(')', ']')
    del raw_timeseries
    anomalous_timeseries = literal_eval(timeseries_array_str)
    anomalous_ts_values_count = len(anomalous_timeseries)

    with open(i_json_file, 'r') as f:
        fp_raw_timeseries = f.read()
    # Convert the timeseries to csv
    fp_timeseries_array_str = str(fp_raw_timeseries).replace('(', '[').replace(')', ']')
    del fp_raw_timeseries
    fp_id_metric_ts = literal_eval(fp_timeseries_array_str)
    fp_id_metric_ts_values_count = len(fp_id_metric_ts)

    try:
        range_tolerance = settings.IONOSPHERE_MINMAX_SCALING_RANGE_TOLERANCE
    except:
        range_tolerance = 0.15
    range_tolerance_percentage = range_tolerance * 100
    check_range = False
    range_similar = False
    if fp_id_metric_ts:
        if anomalous_ts_values_count > 0:
            check_range = True
    lower_range_similar = False
    upper_range_similar = False

    min_fp_value = None
    min_anomalous_value = None
    max_fp_value = None
    max_anomalous_value = None

    if check_range:
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
                print('min value of fp_id_metric_ts (%s) and anomalous_timeseries (%s) are the same' % (
                    str(min_fp_value), str(min_anomalous_value)))
        except:
            lower_range_not_same = True
        if min_fp_value and min_anomalous_value and lower_range_not_same:
            if int(min_fp_value) == int(min_anomalous_value):
                lower_range_similar = True
                print('min value of fp_id_metric_ts (%s) and anomalous_timeseries (%s) are the same' % (
                    str(min_fp_value), str(min_anomalous_value)))
            else:
                lower_min_fp_value = int(min_fp_value - (min_fp_value * range_tolerance))
                upper_min_fp_value = int(min_fp_value + (min_fp_value * range_tolerance))
                if int(min_anomalous_value) in range(lower_min_fp_value, upper_min_fp_value):
                    lower_range_similar = True
                    print('min value of fp_id_metric_ts (%s) and anomalous_timeseries (%s) are similar within %s percent of each other' % (
                        str(min_fp_value),
                        str(min_anomalous_value),
                        str(range_tolerance_percentage)))
        if not lower_range_similar:
            print('lower range of fp_id_metric_ts (%s) and anomalous_timeseries (%s) are not similar' % (
                str(min_fp_value), str(min_anomalous_value)))
        upper_range_not_same = True
        try:
            if int(max_fp_value) == int(max_anomalous_value):
                upper_range_not_same = False
                upper_range_similar = True
                print('max value of fp_id_metric_ts (%s) and anomalous_timeseries (%s) are the same' % (
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
                print('max value of fp_id_metric_ts (%s) and anomalous_timeseries (%s) are similar within %s percent of each other' % (
                    str(max_fp_value), str(max_anomalous_value),
                    str(range_tolerance_percentage)))
            else:
                print('max value of fp_id_metric_ts (%s) and anomalous_timeseries (%s) are not similar' % (
                    str(max_fp_value), str(max_anomalous_value)))
    if lower_range_similar and upper_range_similar:
        range_similar = True
    else:
        print('the ranges of fp_id_metric_ts and anomalous_timeseries differ significantly Min-Max scaling will be skipped')

    minmax_fp_ts = []
    # if fp_id_metric_ts:
    if range_similar:
        try:
            minmax_fp_values = [x[1] for x in fp_id_metric_ts]
            x_np = np.asarray(minmax_fp_values)
            # Min-Max scaling
            np_minmax = (x_np - x_np.min()) / (x_np.max() - x_np.min())
            for (ts, v) in zip(fp_id_metric_ts, np_minmax):
                minmax_fp_ts.append([ts[0], v])
            print('minmax_fp_ts list populated with the minmax scaled time series with %s data points' % str(len(minmax_fp_ts)))
            del minmax_fp_values
        except:
            print('error :: could not minmax scale fp id %s time series for %s' % (str(fp_id), str(base_name)))
        if not minmax_fp_ts:
            print('error :: minmax_fp_ts list not populated')

    minmax_anomalous_ts = []
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
                del anomalous_timeseries
                del minmax_anomalous_values
            except:
                print('error :: could not minmax scale current time series anomalous_timeseries for %s' % (str(fp_id), str(base_name)))
            if len(minmax_anomalous_ts) > 0:
                print('minmax_anomalous_ts is populated with %s data points' % str(len(minmax_anomalous_ts)))
            else:
                print('error :: minmax_anomalous_ts is not populated')
        else:
            print('minmax scaled check will be skipped - anomalous_ts_values_count is %s and minmax_fp_ts is %s' % (str(anomalous_ts_values_count), str(minmax_fp_ts_values_count)))

    minmax_fp_ts_csv = '%s/fpid.%s.%s.minmax_fp_ts.tsfresh.input.std.csv' % (
        settings.SKYLINE_TMP_DIR, str(fp_id), base_name)
    if os.path.isfile(minmax_fp_ts_csv):
        os.remove(minmax_fp_ts_csv)
    minmax_fp_fname_out = minmax_fp_ts_csv + '.transposed.csv'
    if os.path.isfile(minmax_fp_fname_out):
        os.remove(minmax_fp_fname_out)
    anomalous_ts_csv = '%s/%s.%s.minmax_anomalous_ts.tsfresh.std.csv' % (
        settings.SKYLINE_TMP_DIR, metric_timestamp, base_name)
    if os.path.isfile(anomalous_ts_csv):
        os.remove(anomalous_ts_csv)
    anomalous_fp_fname_out = anomalous_ts_csv + '.transposed.csv'
    if os.path.isfile(anomalous_fp_fname_out):
        os.remove(anomalous_fp_fname_out)

    tsf_settings = ReasonableFeatureExtractionSettings()
    tsf_settings.disable_progressbar = True
    minmax_fp_features_sum = None
    minmax_anomalous_features_sum = None
    if minmax_anomalous_ts and minmax_fp_ts:
        if not os.path.isfile(minmax_fp_ts_csv):
            datapoints = minmax_fp_ts
            converted = []
            for datapoint in datapoints:
                try:
                    new_datapoint = [float(datapoint[0]), float(datapoint[1])]
                    converted.append(new_datapoint)
                except:  # nosec
                    continue
            for ts, value in converted:
                try:
                    utc_ts_line = '%s,%s,%s\n' % (base_name, str(int(ts)), str(value))
                    with open(minmax_fp_ts_csv, 'a') as fh:
                        fh.write(utc_ts_line)
                except:
                    print('error :: could not write to file %s' % (str(minmax_fp_ts_csv)))
            del converted
        else:
            print('file found %s, using for data' % minmax_fp_ts_csv)

        if not os.path.isfile(minmax_fp_ts_csv):
            print('error :: file not found %s' % minmax_fp_ts_csv)
        else:
            print('file exists to create the minmax_fp_ts data frame from - %s' % minmax_fp_ts_csv)

        try:
            df = pd.read_csv(minmax_fp_ts_csv, delimiter=',', header=None, names=['metric', 'timestamp', 'value'])
            df.columns = ['metric', 'timestamp', 'value']
        except:
            print('error :: failed to created data frame from %s' % (str(minmax_fp_ts_csv)))
        try:
            df_features = extract_features(
                # @modified 20210101 - Task #3928: Update Skyline to use new tsfresh feature extraction method
                # df, column_id='metric', column_sort='timestamp', column_kind=None,
                # column_value=None, feature_extraction_settings=tsf_settings)
                df, default_fc_parameters=EfficientFCParameters(),
                column_id='metric', column_sort='timestamp', column_kind=None,
                column_value=None, disable_progressbar=True)
        except:
            print('error :: failed to created df_features from %s' % (str(minmax_fp_ts_csv)))
        # Create transposed features csv
        if not os.path.isfile(minmax_fp_fname_out):
            # Transpose
            df_t = df_features.transpose()
            df_t.to_csv(minmax_fp_fname_out)

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
            print('minmax_fp_ts - features_count: %s, features_sum: %s' % (str(minmax_fp_features_count), str(minmax_fp_features_sum)))
            del df_sum
        except:
            print('error :: failed to created df_sum from %s' % (str(minmax_fp_fname_out)))

        if minmax_fp_features_count > 0:
            print('debug :: minmax_fp_features_count of the minmax_fp_ts is %s' % str(minmax_fp_features_count))
        else:
            print('error :: minmax_fp_features_count is %s' % str(minmax_fp_features_count))

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
            del converted

        df = pd.read_csv(anomalous_ts_csv, delimiter=',', header=None, names=['metric', 'timestamp', 'value'])
        df.columns = ['metric', 'timestamp', 'value']
        df_features_current = extract_features(
            # @modified 20210101 - Task #3928: Update Skyline to use new tsfresh feature extraction method
            # df, column_id='metric', column_sort='timestamp', column_kind=None,
            # column_value=None, feature_extraction_settings=tsf_settings)
            df, default_fc_parameters=EfficientFCParameters(),
            column_id='metric', column_sort='timestamp', column_kind=None,
            column_value=None, disable_progressbar=True)
        del df

        # Create transposed features csv
        if not os.path.isfile(anomalous_fp_fname_out):
            # Transpose
            df_t = df_features_current.transpose()
            df_t.to_csv(anomalous_fp_fname_out)
            del df_t
            del df_features_current
        # Calculate the count and sum of the features values
        df_sum_2 = pd.read_csv(
            anomalous_fp_fname_out, delimiter=',', header=0,
            names=['feature_name', 'value'])
        df_sum_2.columns = ['feature_name', 'value']
        df_sum_2['feature_name'] = df_sum_2['feature_name'].astype(str)
        df_sum_2['value'] = df_sum_2['value'].astype(float)
        minmax_anomalous_features_count = len(df_sum_2['value'])
        minmax_anomalous_features_sum = df_sum_2['value'].sum()
        print('minmax_anomalous_ts - minmax_anomalous_features_count: %s, minmax_anomalous_features_sum: %s' % (
            str(minmax_anomalous_features_count),
            str(minmax_anomalous_features_sum)))
        del df_sum_2
        del minmax_anomalous_ts

    percent_different = 100
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
        #     print('percent_different between minmax scaled features sums - %s' % str(percent_different))
        # except:
        #     print('error :: failed to calculate percent_different from minmax scaled features sums')
        try:
            percent_different = get_percent_different(minmax_fp_features_sum, minmax_anomalous_features_sum, True)
            print('percent_different between minmax_fp_features_sum and minmax_anomalous_features_sum - %s' % str(percent_different))
        except Exception as e:
            print('error :: failed to calculate percent_different - %s' % e)

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
                print('minmax scaled common features sums are almost equal, not anomalous')

            # if diff_in_sums <= 1%:
            if percent_different < 0:
                new_pdiff = percent_different * -1
                percent_different = new_pdiff

            # @modified 20190321
            # if percent_different < (settings.IONOSPHERE_FEATURES_PERCENT_SIMILAR + 1):
            if percent_different < IONOSPHERE_ECHO_MINMAX_SCALING_FEATURES_PERCENT_SIMILAR:
                minmax_not_anomalous = True
                # log
                print('not anomalous - minmax scaled features profile match - %s - %s' % (base_name, str(minmax_not_anomalous)))
                print(
                    'minmax scaled calculated features sum are within %s percent of fp_id %s with %s, not anomalous' %
                    (str(IONOSPHERE_ECHO_MINMAX_SCALING_FEATURES_PERCENT_SIMILAR),
                        str(fp_id), str(percent_different)))
            if minmax_not_anomalous:
                not_anomalous = True
                minmax = 1
                # Created time series resources for graphing in
                # the matched page

    try:
        clean_file = anomalous_ts_csv
        if os.path.isfile(anomalous_ts_csv):
            os.remove(anomalous_ts_csv)
        # print('cleaned up - %s' % clean_file)
    except:
        print('no anomalous_ts_csv file to clean up')
    try:
        clean_file = anomalous_fp_fname_out
        if os.path.isfile(anomalous_fp_fname_out):
            os.remove(anomalous_fp_fname_out)
        # print('cleaned up - %s' % clean_file)
    except:
        print('no anomalous_fp_fname_out file to clean up')
    return not_anomalous

def calculate_features_other_file(check_file, current_features_sum, metric):
    previous_features_sum = calculate_features(check_file)
    print('file :: %s' % str(check_file))
    print('current_features_sum :: %s' % str(current_features_sum))
    print('previous_features_sum :: %s' % str(previous_features_sum))

    fp_sum_array = [previous_features_sum]
    calc_sum_array = [current_features_sum]
    sum_fp_values = previous_features_sum
    sum_calc_values = current_features_sum

    not_anomalous = False
    percent_different = 100
    sums_array = np.array([sum_fp_values, sum_calc_values], dtype=float)
    try:
        calc_percent_different = np.diff(sums_array) / sums_array[:-1] * 100.
        percent_different = calc_percent_different[0]
        print('percent_different between common features sums - %s' % str(percent_different))
    except:
        print('error :: failed to calculate percent_different')

    almost_equal = None
    try:
        np.testing.assert_array_almost_equal(fp_sum_array, calc_sum_array)
        almost_equal = True
    except:
        almost_equal = False

    if almost_equal:
        not_anomalous = True
        # @modified 20170118 - Bug #1860: Debug learn not matched in ionosphere
        # This broke it, no variable was interpolated
        # print('common features sums are almost equal, not anomalous' % str(relevant_fp_feature_values_count))
        print('common features sums are almost equal, not anomalous')

    # if diff_in_sums <= 1%:
    if percent_different < 0:
        new_pdiff = percent_different * -1
        percent_different = new_pdiff

    if percent_different < (settings.IONOSPHERE_FEATURES_PERCENT_SIMILAR + 1):
        not_anomalous = True
        # log
        print('not anomalous - features profile match - %s' % metric)
        print(
            'calculated features sum are within %s percent with %s, not anomalous' %
            (str(settings.IONOSPHERE_FEATURES_PERCENT_SIMILAR),
                str(percent_different)))
    return not_anomalous


def compare_features(metric, timestamp, use_file):
    start = timer()
    results = []
    metricname_dir = metric.replace('.', '/')
    current_metric_dir = '%s/%s' % (settings.IONOSPHERE_PROFILES_FOLDER, metricname_dir)
    metric_json_file = '%s.mirage.redis.24h.json' % metric
    not_matched = 0
    fp_matched = 0
    fp_mm_matched = 0
    def find_all(name, path):
        result = []
        for root, dirs, files in os.walk(path):
            if name in files:
                if not 'csv' in files:
                    result.append(os.path.join(root, name))
        return result
    files = find_all(metric_json_file, current_metric_dir)

    current_calculated_features_file = None
    if use_file:
        current_calculated_features_file = create_test_features_profile(use_file)
    if current_calculated_features_file:
        current_features_sum = calculate_features(current_calculated_features_file)
        # print('current_calculated_features_file - %s' % current_calculated_features_file)
        # print('current_calculated_features_file sum - %s' % str(current_features_sum))

        for i_json_file in files:
            matched_with = 'none'
            not_anomalous = False
            if i_json_file != use_file:
                check_calculated_features_file = create_test_features_profile(i_json_file)
                if check_calculated_features_file:
                    check_file = check_calculated_features_file
                    not_anomalous = calculate_features_other_file(check_file, current_features_sum, metric)
                    if not not_anomalous:
                        not_anomalous = calculate_features_other_minmax(use_file, i_json_file, metric)
                        if not_anomalous:
                            matched_with = 'features_profile - minmax'
                            fp_mm_matched += 1
                            if ONLY_MATCH_ONCE == True:
                                break
                        else:
                            not_matched += 1
                    else:
                        matched_with = 'features_profile'
                        fp_matched += 1
                        if ONLY_MATCH_ONCE == True:
                            break
            else:
                create_test_features_profile(i_json_file)
            results.append([not_anomalous, matched_with, i_json_file])
    for not_anomalous, matched_with, i_json_file in results:
        print(not_anomalous, matched_with, i_json_file)
    end = timer()
    total_calc_time = '%.6f' % (end - start)
    print('total features profiles calculations and comparisons completed in %s seconds' % str(total_calc_time))
    print('Not matched :: %s, fp matched :: %s, fp minmax matched :: %s' % (
        str(not_matched), str(fp_matched), str(fp_mm_matched)))
    del files

def main():
    if options.echo_timestamp != 'all':
        use_path = '%s/%s' % (settings.IONOSPHERE_DATA_FOLDER, str(options.echo_timestamp))
    else:
        use_path = settings.IONOSPHERE_DATA_FOLDER

    if options.echo_metric != 'all':
        metricname_dir = options.echo_metric.replace('.', '/')
        current_metric_dir = '%s/%s' % (settings.IONOSPHERE_PROFILES_FOLDER, metricname_dir)
        metric_json_file = '%s.mirage.redis.24h.json' % options.echo_metric
    else:
        current_metric_dir = settings.IONOSPHERE_PROFILES_FOLDER
        metric_json_file = 'mirage.redis.24h.json'

    def find_all(name, path):
        result = []
        for root, dirs, files in os.walk(use_path):
            for i_file in files:
                check = 0
                if metric_json_file in i_file:
                    if 'csv' in i_file:
                        continue
                    matched = 0
                    check_path = os.path.dirname(os.path.join(root, i_file))
                    for c_root, c_dirs, c_files in os.walk(check_path):
                        for m_file in c_files:
                            if 'matched' in m_file:
                                matched = 1
                            if 'fp.details.txt' in m_file:
                                check = 1
                    if matched == 0 and check == 1:
                        result.append(os.path.join(root, i_file))
        return result
    files = find_all(metric_json_file, current_metric_dir)

    print('Testing Ionosphere echo on %s training_date files' % str(len(files)))
    for i_file in files:
        use_file = i_file
        done_file = '%s.echo.csv.checks.done' % i_file
        if os.path.isfile(done_file):
            print('already done :: %s' % i_file)
            continue
        filename = os.path.basename(use_file)
        metric = filename.replace('.mirage.redis.24h.json', '')
        timestamp = 'none'
        compare_features(metric, timestamp, use_file)
        with open(done_file, 'w') as fh:
            fh.write('done')

if __name__== "__main__":
  main()
