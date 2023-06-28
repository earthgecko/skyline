"""
get_test_metrics.py
"""
from ast import literal_eval
from time import time

from skyline_functions import get_redis_conn_decoded


# @added 20230202 - Feature #4842: webapp - api - get_flux_test_metrics
#                   Feature #4840: flux - prometheus - x-test-only header
def get_test_metrics(current_skyline_app, namespace):
    """
    Get the test metric data from Redis and return a dict

    :param current_skyline_app: the app calling the function
    :param namespace: the namespace
    :type current_skyline_app: str
    :type namespace: str
    :return: test_metric_data
    :rtype: dict

    """
    test_metric_data = {
        'errors': [],
    }
    try:
        redis_conn_decoded = get_redis_conn_decoded(current_skyline_app)
    except Exception as err:
        test_metric_data['error'].append(err)

    all_metrics = []
    all_dropped = []
    all_metrics_with_no_values = []
    current_aligned_ts = (int(time()) // 60 * 60)
    last_aligned_ts = current_aligned_ts - 60
    timestamps = [current_aligned_ts, last_aligned_ts, (last_aligned_ts - 60), (last_aligned_ts - 120)]
    evalute_keys = ['metrics', 'dropped', 'ingest_count', 'drop_count', 'metrics_with_no_values']
    for timestamp in timestamps:
        test_only_redis_key = 'flux.prometheus.test_only.%s.%s' % (str(namespace), str(timestamp))
        try:
            test_metric_data_str = redis_conn_decoded.hgetall(test_only_redis_key)
            if test_metric_data_str:
                test_metric_data[timestamp] = {}
                for key in list(test_metric_data_str.keys()):
                    if key in evalute_keys:
                        continue
                    test_metric_data[timestamp][key] = test_metric_data_str[key]
                for key in list(test_metric_data_str.keys()):
                    if key in evalute_keys:
                        key_value = literal_eval(test_metric_data_str[key])
                        test_metric_data[timestamp][key] = key_value
                        if key == 'metrics':
                            all_metrics = list(set(all_metrics + key_value))
                            test_metric_data['ingest_count'] = len(all_metrics)
                            test_metric_data['metrics'] = list(all_metrics)
                        if key == 'dropped':
                            all_dropped = list(set(all_dropped + key_value))
                            test_metric_data['drop_count'] = len(all_dropped)
                            test_metric_data['dropped'] = list(all_dropped)
                        if key == 'metrics_with_no_values':
                            all_metrics_with_no_values = list(set(all_metrics_with_no_values + key_value))
                            test_metric_data['no_values_count'] = len(all_metrics_with_no_values)
                            test_metric_data['metrics_with_no_values'] = list(all_metrics_with_no_values)
        except Exception as err:
            test_metric_data['error'].append(err)

    if 'metrics' in test_metric_data:
        if 'errors' in test_metric_data:
            del test_metric_data['errors']

    return test_metric_data
