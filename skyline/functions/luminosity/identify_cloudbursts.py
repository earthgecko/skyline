from __future__ import division
import logging
import os
import sys
from timeit import default_timer as timer
import traceback
import operator
import warnings
from os import getpid

import requests
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import ruptures as rpt
import datetime as dt
from adtk.visualization import plot
from adtk.data import validate_series
from adtk.detector import PersistAD

sys.path.insert(0, '/opt/skyline/github/skyline/skyline')
if True:
    import settings
    from skyline_functions import get_redis_conn_decoded, get_graphite_metric
    from functions.pandas.timeseries_to_datetime_indexed_df import timeseries_to_datetime_indexed_df
    from functions.graphite.get_metrics_timeseries import get_metrics_timeseries
    from functions.database.queries.get_all_db_metric_names import get_all_db_metric_names
    from functions.redis.get_metric_timeseries import get_metric_timeseries

    from functions.luminosity.find_cloudburst_motifs import find_cloudburst_motifs
    from custom_algorithms import run_custom_algorithm_on_timeseries
    from matched_or_regexed_in_list import matched_or_regexed_in_list

warnings.filterwarnings('ignore')
try:
    from settings import MAX_GRAPHITE_TARGETS
except:
    MAX_GRAPHITE_TARGETS = 200

this_host = str(os.uname()[1])

current_path = os.path.dirname(__file__)
parent_path = os.path.dirname(current_path)
root_path = os.path.dirname(parent_path)
m66_algorithm_source = '%%s/custom_algorithms/m66.py' % root_path


def identify_cloudbursts(current_skyline_app, plot_graphs=False, log=False):
    """
    Find significant changes (cloudbursts) in metrics.
    """

    current_skyline_app_logger = current_skyline_app + 'Log'
    current_logger = logging.getLogger(current_skyline_app_logger)

    child_process_pid = os.getpid()
    function_str = '%s :: functions.luminosity.identify_cloudbursts' % current_skyline_app
    if log:
        current_logger.info('%s :: running for process_pid - %s for %s' % (
            function_str, str(child_process_pid), metric))

    start = timer()

    full_uniques = '%sunique_metrics' % settings.FULL_NAMESPACE
    unique_metrics = list(redis_conn_decoded.smembers(full_uniques))

    timer_start_all = timer()
    custom_algorithm = 'm66'
    m66_algorithm_source = '%%s/custom_algorithms/m66.py' % root_path
    custom_algorithms = {}
    custom_algorithms[custom_algorithm] = {
        'algorithm_source': m66_algorithm_source,
        'algorithm_parameters': {
            'nth_median': 6, 'sigma': 6, 'window': 5, 'return_anomalies': True,
            'save_plots_to': False, 'save_plots_to_absolute_dir': False,
            'filename_prefix': False
        },
        'max_execution_time': 1.0,
        'consensus': 1,
        'algorithms_allowed_in_consensus': ['m66'],
        'run_3sigma_algorithms': False,
        'run_before_3sigma': False,
        'run_only_if_consensus': False,
        'use_with': ['crucible', 'luminosity'],
        'debug_logging': False,

    }

    m66_candidate_metrics = {}

    align = True
    truncate_last_datapoint = True
    window = 4
    summarize_intervalString = '15min'
    summarize_func = 'median'
    nth_median = 6
    n_sigma = 6
    custom_algorithm = 'median_6_6sigma'
    m66_candidate_metrics = {}
    found = 0
    now_timestamp = int(time())
    check_last = 3600

    candidate_metrics = {}

    for metric in unique_metrics:
        metric_name = metric
        if metric_name.startswith(settings.FULL_NAMESPACE):
            base_name = metric_name.replace(settings.FULL_NAMESPACE, '', 1)
        else:
            base_name = metric_name
        timeseries = []
        timeseries = get_metric_timeseries(skyline_app, metric, False)
        if not timeseries:
            continue
        if truncate_last_datapoint:
            timeseries_length = len(timeseries)
            timeseries = timeseries[1:(timeseries_length - 2)]

                for custom_algorithm in list(custom_algorithms.keys()):
                    custom_algorithms_dict = custom_algorithms[custom_algorithm]
                    custom_algorithm_dict = {}
                    custom_algorithm_dict['debug_logging'] = False
                    debug_algorithm_logging = False
                    if debug_algorithms:
                        custom_algorithm_dict['debug_logging'] = True
                        debug_algorithm_logging = True
                    algorithm_source = '/opt/skyline/github/skyline/skyline/custom_algorithms/%s.py' % algorithm
                    custom_algorithm_dict['algorithm_source'] = algorithm_source
                    if LUMINOSITY_CLASSIFY_ANOMALIES_SAVE_PLOTS:
                        custom_algorithm_dict['algorithm_parameters'] = {
                            'window': window, 'c': 6.0, 'return_anomalies': True,
                            'realtime_analysis': False,
                            'save_plots_to': metric_training_data_dir,
                            'save_plots_to_absolute_dir': True,
                            'filename_prefix': 'luminosity.classify_anomaly',
                            'debug_logging': debug_algorithm_logging,
                        }
                        custom_algorithm_dict['max_execution_time'] = 10.0
                    else:
                        custom_algorithm_dict['algorithm_parameters'] = {
                            'window': window, 'c': 6.0, 'return_anomalies': True,
                            'realtime_analysis': False,
                            'debug_logging': debug_algorithm_logging,
                        }
                        custom_algorithm_dict['max_execution_time'] = 5.0

                    if algorithm == base_algorithm:

                    if current_skyline_app == 'webapp':

                        anomalous, anomalyScore, anomalies, anomalies_dict = run_custom_algorithm_on_timeseries(current_skyline_app, current_pid, base_name, timeseries, custom_algorithm, custom_algorithm_dict, debug_algorithms)


                        result, anomalyScore, anomalies = run_custom_algorithm_on_timeseries(skyline_app, current_pid, base_name, timeseries, custom_algorithm, custom_algorithm_dict, debug_algorithms)

    if return_anomalies:
        return (anomalous, anomalyScore, anomalies)
    else:
        return (anomalous, anomalyScore)

                    else:


                rolling_df = pd.DataFrame(timeseries, columns=['date', 'value'])
                rolling_df['date'] = pd.to_datetime(rolling_df['date'], unit='s')
                datetime_index = pd.DatetimeIndex(rolling_df['date'].values)
                rolling_df = rolling_df.set_index(datetime_index)
                rolling_df.drop('date', axis=1, inplace=True)
                original_rolling_df = rolling_df.copy()
                # MinMax scale
                rolling_df = (rolling_df - rolling_df.min()) / (rolling_df.max() - rolling_df.min())
                window = 6
                data = rolling_df['value'].tolist()
                s = pd.Series(data)
                rolling_median_s = s.rolling(window).median()
                median = rolling_median_s.tolist()
                data = median
                s = pd.Series(data)
                rolling_median_s = s.rolling(window).median()
                median_2 = rolling_median_s.tolist()
                data = median_2
                s = pd.Series(data)
                rolling_median_s = s.rolling(window).median()
                median_3 = rolling_median_s.tolist()
                data = median_3
                s = pd.Series(data)
                rolling_median_s = s.rolling(window).median()
                median_4 = rolling_median_s.tolist()
                data = median_4
                s = pd.Series(data)
                rolling_median_s = s.rolling(window).median()
                median_5 = rolling_median_s.tolist()
                data = median_5
                s = pd.Series(data)
                rolling_median_s = s.rolling(window).median()
                median_6 = rolling_median_s.tolist()
                data = median_6
                s = pd.Series(data)
                rolling_std_s = s.rolling(window).std()
                rolling_df['std_median_6'] = rolling_std_s.tolist()
                std_median_6 = rolling_df['std_median_6'].fillna(0).tolist()
                metric_stddev = np.std(std_median_6)
                std_median_6_6sigma = []
                anomalies = False
                for value in std_median_6:
                    if value > (metric_stddev * 6):
                        std_median_6_6sigma.append(1)
                        anomalies = True
                    else:
                        std_median_6_6sigma.append(0)
                rolling_df['std_median_6_6sigma'] = std_median_6_6sigma

                if anomalies:
                    last_trigger = None
                    current_triggers = []
                    anomalies = []
                    # Only tag anomalous if the 6sigma triggers for window
                    for index, item in enumerate(timeseries):
                        if std_median_6_6sigma[index] == 1:
                            current_triggers.append(index)
                        else:
                            if len(current_triggers) > (window / 2):
                                for trigger_index in current_triggers:
                                    anomalies.append(timeseries[(trigger_index - (window * 3))])
                            current_triggers = []
                    if anomalies:
                        anomalies_data = []
                        anomalies_timestamps = [int(item[0]) for item in anomalies]
                        for item in timeseries:
                            if int(item[0]) in anomalies_timestamps:
                                anomalies_data.append(1)
                            else:
                                anomalies_data.append(0)
                        rolling_df['anomalies'] = anomalies_data
                        m66_candidate_metrics[base_name] = {}
                        m66_candidate_metrics[base_name][custom_algorithm] = {}
                        m66_candidate_metrics[base_name][custom_algorithm]['anomalies'] = anomalies
                        # rolling_df['value'].plot(figsize=(18, 6), title=base_name)
                        title = '%s - median 6 6-sigma persisted' % base_name
                        # rolling_df['std_median_6_6sigma'].plot(figsize=(18, 6), title=title)
                        plot(original_rolling_df['value'], anomaly=rolling_df['anomalies'], anomaly_color='red', title=title)
            except Exception as e:
                print(e)
                break
            try:
                del rolling_df
            except:
                pass
        timer_end = timer()
        print('median_6_6sigma analysis of %s metrics took %.6f seconds, significant changes now detected on %s metrics' % (
            str(len(current_base_names)), (timer_end - timer_start), str(len(m66_candidate_metrics))))
    timer_end_all = timer()
    print('%s metrics analysed with m66, took %.6f seconds - %s metrics found with significant changes' % (
        str(len(metrics)), (timer_end_all - timer_start_all), str(len(m66_candidate_metrics))))

    # hmmm OK m66 works better than others


    # Try m66 on the last week
    # gary@mc11:~$ date -d "2021-07-07 00:00:00" +%s
    # 1625612400
    from_timestamp = 1625612400 + 86400
    until_timestamp = from_timestamp + (86400 * 7)
    metrics_to_do = list(metrics)
    more_analysis_metrics_timeseries = {}
    timer_start_all = timer()
    custom_algorithm = 'median_6_6sigma'
    m66_candidate_metrics = {}
    while len(metrics_to_do) > 0:
        current_base_names = []
        found = 0
        while len(current_base_names) < 50 and len(metrics_to_do) > 0:
            current_base_names.append(metrics_to_do.pop(0))

        metrics_functions = {}
        for base_name in current_base_names:
            metrics_functions[base_name] = {}
    #        metrics_functions[base_name]['functions'] = None
            metrics_functions[base_name]['functions'] = {'summarize': {'intervalString': '15min', 'func': 'sum'}}

        timer_start = timer()
        metrics_timeseries = get_metrics_timeseries(skyline_app, metrics_functions, from_timestamp, until_timestamp, log=False)
        timer_end = timer()
        # print('get_metrics_timeseries took %.6f seconds to fetch %s' % (
        #     (timer_end - timer_start), str(len(current_base_names))))

        timer_start = timer()
        for base_name in current_base_names:
            timeseries = []
            try:
                # timeseries = get_metric_timeseries(skyline_app, base_name, log=False)
                timeseries = metrics_timeseries[base_name]['timeseries']
                if not timeseries:
                    continue
                rolling_df = pd.DataFrame(timeseries, columns=['date', 'value'])
                rolling_df['date'] = pd.to_datetime(rolling_df['date'], unit='s')
                datetime_index = pd.DatetimeIndex(rolling_df['date'].values)
                rolling_df = rolling_df.set_index(datetime_index)
                rolling_df.drop('date', axis=1, inplace=True)
                original_rolling_df = rolling_df.copy()
                # MinMax scale
                rolling_df = (rolling_df - rolling_df.min()) / (rolling_df.max() - rolling_df.min())
                window = 6
                data = rolling_df['value'].tolist()
                s = pd.Series(data)
                rolling_median_s = s.rolling(window).median()
                median = rolling_median_s.tolist()
                data = median
                s = pd.Series(data)
                rolling_median_s = s.rolling(window).median()
                median_2 = rolling_median_s.tolist()
                data = median_2
                s = pd.Series(data)
                rolling_median_s = s.rolling(window).median()
                median_3 = rolling_median_s.tolist()
                data = median_3
                s = pd.Series(data)
                rolling_median_s = s.rolling(window).median()
                median_4 = rolling_median_s.tolist()
                data = median_4
                s = pd.Series(data)
                rolling_median_s = s.rolling(window).median()
                median_5 = rolling_median_s.tolist()
                data = median_5
                s = pd.Series(data)
                rolling_median_s = s.rolling(window).median()
                median_6 = rolling_median_s.tolist()
                data = median_6
                s = pd.Series(data)
                rolling_std_s = s.rolling(window).std()
                rolling_df['std_median_6'] = rolling_std_s.tolist()
                std_median_6 = rolling_df['std_median_6'].fillna(0).tolist()
                metric_stddev = np.std(std_median_6)
                std_median_6_6sigma = []
                anomalies = False
                for value in std_median_6:
                    if value > (metric_stddev * 6):
                        std_median_6_6sigma.append(1)
                        anomalies = True
                    else:
                        std_median_6_6sigma.append(0)
                rolling_df['std_median_6_6sigma'] = std_median_6_6sigma

                if anomalies:
                    last_trigger = None
                    current_triggers = []
                    anomalies = []
                    # Only tag anomalous if the 6sigma triggers for window
                    for index, item in enumerate(timeseries):
                        if std_median_6_6sigma[index] == 1:
                            current_triggers.append(index)
                        else:
                            if len(current_triggers) > (window / 2):
                                for trigger_index in current_triggers:
                                    anomalies.append(timeseries[(trigger_index - (window * 3))])
                            current_triggers = []
                    if anomalies:
                        anomalies_data = []
                        anomalies_timestamps = [int(item[0]) for item in anomalies]
                        for item in timeseries:
                            if int(item[0]) in anomalies_timestamps:
                                anomalies_data.append(1)
                            else:
                                anomalies_data.append(0)
                        rolling_df['anomalies'] = anomalies_data
                        m66_candidate_metrics[base_name] = {}
                        m66_candidate_metrics[base_name][custom_algorithm] = {}
                        m66_candidate_metrics[base_name][custom_algorithm]['anomalies'] = anomalies
                        # rolling_df['value'].plot(figsize=(18, 6), title=base_name)
                        title = '%s - median 6 6-sigma persisted' % base_name
                        # rolling_df['std_median_6_6sigma'].plot(figsize=(18, 6), title=title)
                        plot(original_rolling_df['value'], anomaly=rolling_df['anomalies'], anomaly_color='red', title=title)
            except Exception as e:
                print('error: %s' % e)
        timer_end = timer()
        print('median_6_6sigma analysis of %s metrics took %.6f seconds, significant changes now detected on %s metrics' % (
            str(len(current_base_names)), (timer_end - timer_start), str(len(m66_candidate_metrics))))
    timer_end_all = timer()
    print('%s metrics analysed with m66, took %.6f seconds - %s metrics found with significant changes' % (
        str(len(metrics)), (timer_end_all - timer_start_all), str(len(m66_candidate_metrics))))

    # Try m66 on the 3 months
    from_timestamp = 1618660800 - (86400 * 7)
    until_timestamp = from_timestamp + (((86400 * 7) * 4) * 3)
    metrics_to_do = list(metrics)
    more_analysis_metrics_timeseries = {}
    timer_start_all = timer()
    custom_algorithm = 'median_6_6sigma'
    m66_candidate_metrics = {}
    while len(metrics_to_do) > 0:
        current_base_names = []
        found = 0
        while len(current_base_names) < 50 and len(metrics_to_do) > 0:
            current_base_names.append(metrics_to_do.pop(0))

        metrics_functions = {}
        for base_name in current_base_names:
            metrics_functions[base_name] = {}
    #        metrics_functions[base_name]['functions'] = None
            metrics_functions[base_name]['functions'] = {'summarize': {'intervalString': '240min', 'func': 'sum'}}

        timer_start = timer()
        metrics_timeseries = get_metrics_timeseries(skyline_app, metrics_functions, from_timestamp, until_timestamp, log=False)
        timer_end = timer()
        # print('get_metrics_timeseries took %.6f seconds to fetch %s' % (
        #     (timer_end - timer_start), str(len(current_base_names))))

        timer_start = timer()
        for base_name in current_base_names:
            timeseries = []
            try:
                # timeseries = get_metric_timeseries(skyline_app, base_name, log=False)
                timeseries = metrics_timeseries[base_name]['timeseries']
                if not timeseries:
                    continue
                rolling_df = pd.DataFrame(timeseries, columns=['date', 'value'])
                rolling_df['date'] = pd.to_datetime(rolling_df['date'], unit='s')
                datetime_index = pd.DatetimeIndex(rolling_df['date'].values)
                rolling_df = rolling_df.set_index(datetime_index)
                rolling_df.drop('date', axis=1, inplace=True)
                original_rolling_df = rolling_df.copy()
                # MinMax scale
                rolling_df = (rolling_df - rolling_df.min()) / (rolling_df.max() - rolling_df.min())
                window = 6
                data = rolling_df['value'].tolist()
                s = pd.Series(data)
                rolling_median_s = s.rolling(window).median()
                median = rolling_median_s.tolist()
                data = median
                s = pd.Series(data)
                rolling_median_s = s.rolling(window).median()
                median_2 = rolling_median_s.tolist()
                data = median_2
                s = pd.Series(data)
                rolling_median_s = s.rolling(window).median()
                median_3 = rolling_median_s.tolist()
                data = median_3
                s = pd.Series(data)
                rolling_median_s = s.rolling(window).median()
                median_4 = rolling_median_s.tolist()
                data = median_4
                s = pd.Series(data)
                rolling_median_s = s.rolling(window).median()
                median_5 = rolling_median_s.tolist()
                data = median_5
                s = pd.Series(data)
                rolling_median_s = s.rolling(window).median()
                median_6 = rolling_median_s.tolist()
                data = median_6
                s = pd.Series(data)
                rolling_std_s = s.rolling(window).std()
                rolling_df['std_median_6'] = rolling_std_s.tolist()
                std_median_6 = rolling_df['std_median_6'].fillna(0).tolist()
                metric_stddev = np.std(std_median_6)
                std_median_6_6sigma = []
                anomalies = False
                for value in std_median_6:
                    if value > (metric_stddev * 6):
                        std_median_6_6sigma.append(1)
                        anomalies = True
                    else:
                        std_median_6_6sigma.append(0)
                rolling_df['std_median_6_6sigma'] = std_median_6_6sigma

                if anomalies:
                    last_trigger = None
                    current_triggers = []
                    anomalies = []
                    # Only tag anomalous if the 6sigma triggers for window
                    for index, item in enumerate(timeseries):
                        if std_median_6_6sigma[index] == 1:
                            current_triggers.append(index)
                        else:
                            if len(current_triggers) > (window / 2):
                                for trigger_index in current_triggers:
                                    anomalies.append(timeseries[(trigger_index - (window * 3))])
                            current_triggers = []
                    if anomalies:
                        anomalies_data = []
                        anomalies_timestamps = [int(item[0]) for item in anomalies]
                        for item in timeseries:
                            if int(item[0]) in anomalies_timestamps:
                                anomalies_data.append(1)
                            else:
                                anomalies_data.append(0)
                        rolling_df['anomalies'] = anomalies_data
                        m66_candidate_metrics[base_name] = {}
                        m66_candidate_metrics[base_name][custom_algorithm] = {}
                        m66_candidate_metrics[base_name][custom_algorithm]['anomalies'] = anomalies
                        # rolling_df['value'].plot(figsize=(18, 6), title=base_name)
                        title = '%s - median 6 6-sigma persisted' % base_name
                        # rolling_df['std_median_6_6sigma'].plot(figsize=(18, 6), title=title)
                        plot(original_rolling_df['value'], anomaly=rolling_df['anomalies'], anomaly_color='red', title=title)
            except Exception as e:
                print('error: %s' % e)
        timer_end = timer()
        print('median_6_6sigma analysis of %s metrics took %.6f seconds, significant changes now detected on %s metrics' % (
            str(len(current_base_names)), (timer_end - timer_start), str(len(m66_candidate_metrics))))
    timer_end_all = timer()
    print('%s metrics analysed with m66, took %.6f seconds - %s metrics found with significant changes' % (
        str(len(metrics)), (timer_end_all - timer_start_all), str(len(m66_candidate_metrics))))

    # Not fucking bad...

    # Compare to adtk_level shift and persistAD consensus
    if True:
        from adtk.data import validate_series
        from adtk.visualization import plot
        from adtk.detector import PersistAD

    current_pid = getpid()
    found_level_shifts = {}
    metrics_to_do = list(metrics)
    more_analysis_metrics_timeseries = {}
    timer_start_all = timer()
    while len(metrics_to_do) > 0:

        # JUST A SAMPLE
        if len(metrics_to_do) <= (len(metrics) - 50):
            break

        current_base_names = []
        found = 0
        while len(current_base_names) < 50 and len(metrics_to_do) > 0:
            current_base_names.append(metrics_to_do.pop(0))

        metrics_functions = {}
        for base_name in current_base_names:
            metrics_functions[base_name] = {}
    #        metrics_functions[base_name]['functions'] = None
            metrics_functions[base_name]['functions'] = {'summarize': {'intervalString': '240min', 'func': 'sum'}}

        timer_start = timer()
        metrics_timeseries = get_metrics_timeseries(skyline_app, metrics_functions, from_timestamp, until_timestamp, log=False)
        timer_end = timer()
        print('get_metrics_timeseries took %.6f seconds to fetch %s' % (
            (timer_end - timer_start), str(len(current_base_names))))

        algorithm = 'adtk_level_shift'
        custom_algorithm = algorithm
        custom_algorithm_dict = {}
        custom_algorithm_dict['debug_logging'] = True
        debug_algorithm_logging = True
        algorithm_source = '/opt/skyline/github/skyline/skyline/custom_algorithms/%s.py' % algorithm
        custom_algorithm_dict['algorithm_source'] = algorithm_source
        custom_algorithm_dict['algorithm_parameters'] = {
            'window': 12, 'c': 6.0, 'min_periods': 12, 'return_anomalies': True,
            'realtime_analysis': False,
            'debug_logging': debug_algorithm_logging,
        }
        custom_algorithm_dict['max_execution_time'] = 10.0
        debug_algorithms = False

        timer_start = timer()
        for base_name in current_base_names:
            try:
                timeseries = metrics_timeseries[base_name]['timeseries']
            except KeyError:
                continue
            if not timeseries:
                continue
            result = None
            anomalyScore = None
            anomalies = []
            try:
                result, anomalyScore, anomalies = run_custom_algorithm_on_timeseries(skyline_app, current_pid, base_name, timeseries, custom_algorithm, custom_algorithm_dict, debug_algorithms)
                if result:
                    print('%s -  result: %s, anomalyScore: %s, anomalies: %s' % (
                        base_name, str(result), str(anomalyScore), str(len(anomalies))))
            except Exception as e:
                print('error :: failed to run custom_algorithm %s on %s - %s' % (
                    custom_algorithm, base_name, e))
            triggered = False
            if anomalies:
                found += 1
                found_level_shifts[base_name] = {}
                found_level_shifts[base_name]['anomalyScore'] = anomalyScore
                found_level_shifts[base_name]['anomalies'] = anomalies
                more_analysis_metrics_timeseries[base_name] = {}
                more_analysis_metrics_timeseries[base_name]['timeseries'] = timeseries
        timer_end = timer()
        print('adtk_level_shift analysis of %s metrics took %.6f seconds, level shift was detected on %s metrics' % (
            str(len(current_base_names)), (timer_end - timer_start), str(found)))

    timer_end_all = timer()
    print('%s metrics analysed with adtk_level_shift, took %.6f seconds - %s metrics found with level shifts' % (
        str(len(metrics)), (timer_end_all - timer_start_all), str(len(found_level_shifts))))


    found_persists = {}
    found_level_shift_and_persists = []
    all_found_level_shift_metrics = list(found_level_shifts.keys())
    for base_name in all_found_level_shift_metrics:
        timeseries = more_analysis_metrics_timeseries[base_name]['timeseries']
        if not timeseries:
            print('failed to find timeseries for %s' % base_name)
            continue
        df = pd.DataFrame(timeseries, columns=['date', 'value'])
        df['date'] = pd.to_datetime(df['date'], unit='s')
        datetime_index = pd.DatetimeIndex(df['date'].values)
        df = df.set_index(datetime_index)
        df.drop('date', axis=1, inplace=True)
        s = validate_series(df)
    #    persist_ad = PersistAD(c=3.0, side='both', window=12, min_periods=12)
        persist_ad = PersistAD(c=6.0, side='both', window=12, min_periods=12)
        anomaly_df = persist_ad.fit_detect(s)
        anomalies = anomaly_df.loc[anomaly_df['value'] > 0]
        if len(anomalies) == 0:
            continue

        anomaly_timestamps = list(anomalies.index.astype(np.int64) // 10**9)
        anomalies_list = []
        for ts, value in timeseries:
            if int(ts) in anomaly_timestamps:
                anomalies_list.append([int(ts), value])
        anomalies = list(anomalies_list)

        found_persists[base_name] = {}
        found_persists[base_name]['anomalies'] = anomalies
        level_shift_anomalies = found_level_shifts[base_name]['anomalies']
        level_shift_anomaly_timestamps = [item[0] for item in level_shift_anomalies]
        consensus = False
        for ts, value in anomalies:
            if int(ts) in level_shift_anomaly_timestamps:
                consensus = True
        if consensus:
            found_level_shift_and_persists.append(base_name)
            try:
                plot(s, anomaly=anomaly_df, anomaly_color='red', title=base_name)
            except Exception as e:
                print('failed to plot persist anomalies for %s - %s' % (base_name, e))

    timer_start_ruptures = timer()
    for base_name in found_level_shift_and_persists:
        timeseries = more_analysis_metrics_timeseries[base_name]['timeseries']
        if not timeseries:
            print('failed to find timeseries for %s' % base_name)
            continue
        working_timeseries_timestamps = [int(ts) for ts, value in timeseries]
        working_timeseries_values = [v for ts, v in timeseries]
        working_values = np.array(working_timeseries_values)
        algo_c = rpt.KernelCPD(kernel='linear', min_size=12).fit(working_values)  # written in C
        results = algo_c.predict(pen=2)
        values = working_values
        rpt.display(values, results, figsize=(18, 6))
        title = '%s' % base_name
        plt.title(title)
        plt.show()
    timer_end_ruptures = timer()
    print('%s metrics analysed with ruptures, took %.6f seconds' % (
        str(len(found_level_shift_and_persists)),
        (timer_end_ruptures - timer_start_ruptures)))


# @added 20210726 - Info #4198: ppscore
# ppscore is the best cloudburst candidate algorithm found to data

####
    return cloudbursts_found, plot_images
