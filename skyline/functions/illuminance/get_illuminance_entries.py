"""
get_illuminance_entries.py
"""
import logging
from time import strftime, gmtime
from ast import literal_eval
import copy

from skyline_functions import get_redis_conn_decoded
# @added 20220729 - Task #2732: Prometheus to Skyline
#                   Branch #4300: prometheus
from functions.metrics.get_base_name_from_metric_id import get_base_name_from_metric_id
from functions.metrics.get_metric_id_from_base_name import get_metric_id_from_base_name
# @added 20221104 - Task #2732: Prometheus to Skyline
#                   Branch #4300: prometheus
from functions.metrics.get_base_names_and_metric_ids import get_base_names_and_metric_ids


# @added 20220516 - Feature #2580: illuminance
# @modified 20230126 - Feature #4830: webapp - panorama_plot_anomalies - all_events
# Added for_metric_id
def get_illuminance_entries(current_skyline_app, from_timestamp, until_timestamp, for_metric_id=0):
    """
    Get entires from the Redis app illuminance hashed for the period defined and
    returns the illuminance_dict.

    :param current_skyline_app: the app calling the function
    :param from_timestamp: the from timestamp
    :param until_timestamp: the until timestamp
    :param for_metric_id: the metric id to filter on
    :type current_skyline_app: str
    :type from_timestamp: int
    :type until_timestamp: int
    :type for_metric_id: int
    :return: illuminance_dict
    :rtype: dict

    """
    function_str = 'functions.illuminance.get_illuminance_entries'
    illuminance_dict = {}

    skyline_app_logger = '%sLog' % current_skyline_app
    logger = logging.getLogger(skyline_app_logger)

    from_date_string = str(strftime('%Y-%m-%d', gmtime(from_timestamp)))
    until_date_string = str(strftime('%Y-%m-%d', gmtime(until_timestamp)))

    try:
        redis_conn_decoded = get_redis_conn_decoded(current_skyline_app)
    except Exception as err:
        logger.error('error :: %s :: %s :: get_redis_conn_decoded failed - %s' % (
            current_skyline_app, function_str, err))
        return illuminance_dict

    # @added 20221104 - Task #2732: Prometheus to Skyline
    #                   Branch #4300: prometheus
    # Improve performance.  Instead of using get_base_name_from_metric_id for
    # every labelled metric rather surface all metrics once and then check the
    # object.
    base_names_with_ids = {}
    ids_with_base_names = {}
    try:
        base_names_with_ids = get_base_names_and_metric_ids(current_skyline_app)
    except Exception as err:
        logger.error('error :: %s :: %s :: get_base_names_and_metric_ids failed - %s' % (
            current_skyline_app, function_str, err))
    if base_names_with_ids:
        for base_name in list(base_names_with_ids.keys()):
            metric_id = int(str(base_names_with_ids[base_name]))
            ids_with_base_names[metric_id] = base_name

    last_timestamp_hash = int(int(until_timestamp) // 60 * 60)
    current_timestamp_hash = int(int(from_timestamp) // 60 * 60)
    timestamp_hashes = []
    while current_timestamp_hash < (last_timestamp_hash + 60):
        timestamp_hashes.append(current_timestamp_hash)
        current_timestamp_hash = current_timestamp_hash + 60

    for date_str in list(set([from_date_string, until_date_string])):
        illuminance_dict[date_str] = {}
        for timestamp_hash in timestamp_hashes:
            timestamp_hash_date_string = str(strftime('%Y-%m-%d %H:%M:%S', gmtime(timestamp_hash)))
            for app in ['analyzer', 'analyzer_batch', 'mirage', 'boundary']:
                illuminance_redis_hash = '%s.illuminance.%s' % (app, date_str)
                use_illuminance_dict = {}
                current_illuminance_dict = {}
                try:
                    current_illuminance_dict = redis_conn_decoded.hget(illuminance_redis_hash, timestamp_hash)
                except Exception as err:
                    logger.error('error :: %s :: failed to hget %s from %s - %s' % (
                        function_str, str(timestamp_hash), illuminance_redis_hash, err))
                if current_illuminance_dict:
                    try:
                        # app_key_set = illuminance_dict[date_str][app]
                        app_key_set = copy.deepcopy(illuminance_dict[date_str][app])
                        del app_key_set
                    except KeyError:
                        illuminance_dict[date_str][app] = {}

                    # @modified 20220729 - Task #2732: Prometheus to Skyline
                    #                      Branch #4300: prometheus
                    # Determine labelled_metric base_name
                    # illuminance_dict[date_str][app][timestamp_hash] = literal_eval(current_illuminance_dict)
                    # use_illuminance_dict = {}
                    c_illuminance_dict = literal_eval(current_illuminance_dict)
                    for metric_key in list(c_illuminance_dict.keys()):
                        use_metric_key = str(metric_key)
                        metric_id = 0
                        if metric_key.startswith('labelled_metrics.'):
                            try:
                                metric_id_str = metric_key.replace('labelled_metrics.', '', 1)
                                metric_id = int(float(metric_id_str))
                            except:
                                pass
                        if not metric_id:
                            try:
                                metric_id = int(float(metric_key))
                            except:
                                pass

                        # @added 20221104 - Task #2732: Prometheus to Skyline
                        #                   Branch #4300: prometheus
                        # Improve performance
                        metric = None
                        if metric_id and ids_with_base_names:
                            try:
                                metric = ids_with_base_names[metric_id]
                                if metric:
                                    use_metric_key = str(metric)
                            except KeyError:
                                metric = None

                        # @modified 20221104 - Task #2732: Prometheus to Skyline
                        #                   Branch #4300: prometheus
                        # Improve performance
                        # if metric_id:
                        if metric_id and not metric:
                            try:
                                metric = get_base_name_from_metric_id(current_skyline_app, metric_id)
                                if metric:
                                    use_metric_key = str(metric)
                            except Exception as err:
                                logger.error('error :: %s :: %s :: base_name_from_metric_id failed to determine metric from metric_id: %s - %s' % (
                                    current_skyline_app, function_str, str(metric_id), str(err)))
                        use_illuminance_dict[use_metric_key] = c_illuminance_dict[metric_key]

                        # @added 20221104 - Task #2732: Prometheus to Skyline
                        #                   Branch #4300: prometheus
                        # Improve performance
                        if not metric_id and base_names_with_ids:
                            try:
                                metric_id = base_names_with_ids[use_metric_key]
                            except KeyError:
                                metric_id = 0

                        # @added 20220729 - Task #2732: Prometheus to Skyline
                        #                   Branch #4300: prometheus
                        # Add the metric_id because it is hard to determine
                        # and compare a list of long Prometheus metric names
                        # having the metric ids as well is much better at
                        # showing the same metrics
                        if not metric_id:
                            try:
                                metric_id = get_metric_id_from_base_name(current_skyline_app, use_metric_key)
                            except Exception as err:
                                logger.error('error :: %s :: %s :: get_metric_id_from_base_name failed to determine metric_id for %s - %s' % (
                                    current_skyline_app, function_str, str(use_metric_key), str(err)))

                        # @modified 20230126 - Feature #4830: webapp - panorama_plot_anomalies - all_events
                        # Added for_metric_id to allow to only return illuminance
                        # events for a single metric
                        if for_metric_id:
                            if int(for_metric_id) == int(metric_id):
                                use_illuminance_dict[use_metric_key]['metric_id'] = metric_id
                        else:
                            use_illuminance_dict[use_metric_key]['metric_id'] = metric_id
                if use_illuminance_dict:
                    illuminance_dict[date_str][app][timestamp_hash_date_string] = use_illuminance_dict

    return illuminance_dict
