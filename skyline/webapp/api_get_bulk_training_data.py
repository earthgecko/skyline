"""
api_get_bulk_training_data.py
"""
import logging
from time import time
from flask import request

from functions.ionosphere.get_training_data import get_training_data


# @added 20221016 - Feature #4650: ionosphere.bulk.training
def api_get_bulk_training_data(current_skyline_app):
    """
    Return a dict with the available bulk training data.

    :param current_skyline_app: the app calling the function
    :type current_skyline_app: str
    :return: training_data
    :rtype: dict

    """
    function_str = 'api_get_training_data'
    training_data = {}

    current_skyline_app_logger = current_skyline_app + 'Log'
    current_logger = logging.getLogger(current_skyline_app_logger)

    from_timestamp = 0
    if 'from_timestamp' in request.args:
        try:
            from_timestamp = int(request.args.get('from_timestamp'))
            current_logger.info('%s :: with from_timestamp: %s' % (function_str, (from_timestamp)))
        except Exception as err:
            current_logger.error('error :: %s :: bad from_timestamp argument - %s' % (function_str, err))
            return 400
    until_timestamp = 0
    if 'until_timestamp' in request.args:
        try:
            until_timestamp = int(request.args.get('until_timestamp'))
            current_logger.info('%s :: with until_timestamp: %s' % (function_str, (until_timestamp)))
        except Exception as err:
            current_logger.error('error :: %s :: bad until_timestamp argument - %s' % (function_str, err))
            return 400

    namespaces = None
    if 'namespaces' in request.args:
        try:
            namespaces = request.args.get('namespaces')
            current_logger.info('api_get_training_data :: with namespaces: %s' % str(namespaces))
        except:
            namespaces = None
    if namespaces:
        namespaces = namespaces.split(',')

    try:
        training_data = get_training_data(current_skyline_app, metrics=None, namespaces=namespaces, key_by='timestamp')
    except Exception as err:
        current_logger.error('error :: %s :: get_training_data failed - %s' % (function_str, err))

    # Remove any training not in the defined period or already trained
    training_count = 0
    for timestamp in list(training_data.keys()):
        if from_timestamp and timestamp < from_timestamp:
            del training_data[timestamp]
            continue
        if until_timestamp and timestamp > until_timestamp:
            del training_data[timestamp]
            continue
        for metric_name in list(training_data[timestamp].keys()):
            if training_data[timestamp][metric_name]['trained']:
                del training_data[timestamp][metric_name]
                continue
            training_count += 1

    current_logger.info('%s :: returning %s training data entries' % (
        function_str, str(training_count)))
    return training_data
