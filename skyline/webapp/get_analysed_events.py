"""
get_analysed_events.py
"""
import logging
import datetime
from time import strftime, gmtime, mktime
from ast import literal_eval

from skyline_functions import get_redis_conn_decoded


# @added 202200504 - Feature #4530: namespace.analysed_events
def get_analysed_events(
        current_skyline_app, from_timestamp, until_timestamp, namespace=None):
    """
    Return a dict with the number of analysed events, estimate the first day
    count based on dividing the total of the first day by the elapsed number of
    minutes that have past in the until_timestamp day.

    :param current_skyline_app: the app calling the function
    :param from_timestamp: get analsed events from
    :param until_timestamp: get analsed events until
    :param namespace: get analsed events for namespace
    :type current_skyline_app: str
    :type from_timestamp: int
    :type until_timestamp: int
    :type namespace: str
    :return: analysed_events
    :rtype: dict

    """
    function_str = 'get_analysed_events'
    analysed_events = {}

    current_skyline_app_logger = current_skyline_app + 'Log'
    current_logger = logging.getLogger(current_skyline_app_logger)

    from_date_str = str(strftime('%Y-%m-%d', gmtime(from_timestamp)))
    until_date_str = str(strftime('%Y-%m-%d', gmtime(until_timestamp)))

    from_date_day_start_timestamp = int(mktime(datetime.datetime.strptime(from_date_str, '%Y-%m-%d').timetuple()))
    until_date_day_start_timestamp = int(mktime(datetime.datetime.strptime(until_date_str, '%Y-%m-%d').timetuple()))

    date_keys_to_get = {}
    current_key_date_str = str(from_date_str)
    current_date_timestamp = from_date_day_start_timestamp
    while current_key_date_str != until_date_str:
        date_keys_to_get[current_key_date_str] = current_date_timestamp
        current_date_timestamp = current_date_timestamp + 86400
        current_key_date_str = str(strftime('%Y-%m-%d', gmtime(current_date_timestamp)))
    date_keys_to_get[current_key_date_str] = current_date_timestamp

    last_day_minutes_since_midnight = int(int(until_timestamp - until_date_day_start_timestamp) / 60)
    sample_minutes = 1440 - last_day_minutes_since_midnight
    estimated_from_timestamp = int(from_timestamp + (sample_minutes * 60))

    try:
        redis_conn_decoded = get_redis_conn_decoded(current_skyline_app)
    except Exception as err:
        current_logger.error('error :: %s :: get_redis_conn_decoded failed - %s' % (
            function_str, err))
        raise

    for date_str in list(date_keys_to_get.keys()):
        date_data_dict = {}
        estimate = False
        if date_str == from_date_str and last_day_minutes_since_midnight > 0:
            estimate = True
        try:
            date_str_timestamp = str(date_keys_to_get[date_str])
            date_data_dict_str = redis_conn_decoded.hget('metrics_manager.namespace.analysed_events', date_str_timestamp)
            if date_data_dict_str:
                date_data_dict = literal_eval(date_data_dict_str)
        except Exception as err:
            current_logger.error('error :: %s :: failed to hget %s from metrics_manager.namespace.analysed_events - %s' % (
                function_str, date_str, err))
        if not date_data_dict:
            continue
        if estimate:
            estimated_date_data_dict = {}
            for key in list(date_data_dict.keys()):
                if isinstance(date_data_dict[key], dict):
                    estimated_date_data_dict[key] = {}
                    for i_key in list(date_data_dict[key].keys()):
                        if isinstance(date_data_dict[key][i_key], int):
                            value = int((date_data_dict[key][i_key] / 1440) * sample_minutes)
                        else:
                            value = date_data_dict[key][i_key]
                        estimated_date_data_dict[key][i_key] = value
                if isinstance(date_data_dict[key], int):
                    value = int((date_data_dict[key] / 1440) * sample_minutes)
                else:
                    value = date_data_dict[key]
                estimated_date_data_dict[key] = value
            date_data_dict = estimated_date_data_dict
        analysed_events[date_keys_to_get[date_str]] = date_data_dict

    analysed_events_dict = {
        'from': from_timestamp, 'until': until_timestamp,
        'estimated_from_timestamp': estimated_from_timestamp}
    for timestamp in list(analysed_events.keys()):
        for key in list(analysed_events[timestamp].keys()):
            if not namespace:
                if isinstance(analysed_events[timestamp][key], int):
                    value = analysed_events[timestamp][key]
                    try:
                        has_value = analysed_events_dict[key]
                        del has_value
                    except:
                        analysed_events_dict[key] = 0
                    analysed_events_dict[key] += value
            if isinstance(analysed_events[timestamp][key], dict):
                for i_key in list(analysed_events[timestamp][key].keys()):
                    if isinstance(analysed_events[timestamp][key][i_key], int):
                        if namespace and i_key != namespace:
                            continue
                        value = analysed_events[timestamp][key][i_key]
                        try:
                            has_value = analysed_events_dict[key][i_key]
                        except:
                            try:
                                has_parent = analysed_events_dict[key]
                                del has_parent
                            except:
                                analysed_events_dict[key] = {}
                            analysed_events_dict[key][i_key] = 0
                    analysed_events_dict[key][i_key] += value
    return analysed_events_dict
