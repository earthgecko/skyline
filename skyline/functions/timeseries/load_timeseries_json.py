"""
load_timeseries_json.py
"""
import logging
from ast import literal_eval


# @added 20230119
def load_timeseries_json(current_skyline_app, anomaly_json):
    """
    Load the timeseries data from a json file and return the timeseries as a
    list.

    :param current_skyline_app: the app calling the function so the function
        knows which log to write too.
    :param anomaly_json: the anomaly_json path and file name
    :type current_skyline_app: str
    :type anomaly_json: str
    :return: timeseries
    :rtype: list
    """
    function_str = 'functions.timeseries.load_anomaly_json'

    timeseries = []
    anomalous_timeseries = []
    try:
        with open((anomaly_json), 'r') as f:
            raw_timeseries = f.read()
        timeseries_array_str = str(raw_timeseries).replace('(', '[').replace(')', ']')
        del raw_timeseries
        anomalous_timeseries = literal_eval(timeseries_array_str)
        del timeseries_array_str
    except Exception as err:
        skyline_app_logger = '%sLog' % current_skyline_app
        logger = logging.getLogger(skyline_app_logger)
        logger.error('error :: %s :: failed to created timeseries from %s - %s' % (
            function_str, anomaly_json, err))
        anomalous_timeseries = []

    for datapoint in anomalous_timeseries:
        try:
            new_datapoint = [int(datapoint[0]), float(datapoint[1])]
            timeseries.append(new_datapoint)
        except:  # nosec
            continue

    return timeseries
