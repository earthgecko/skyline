"""
callback.py
"""
import json
import logging
import traceback

import requests

# @added 20240913 - Feature #5468: callback
def callback(current_skyline_app, url, data):
    """

    Request the provided URL with the provided data

    :param current_skyline_app: the current_skyline_app
    :param url: the URL request
    :param data: the data to POST 
    :type current_skyline_app: str
    :type url: str
    :type data: dict
    :return: response_code
    :rtype: int

    """
    function_str = 'callback'
    current_skyline_app_logger = current_skyline_app + 'Log'
    current_logger = logging.getLogger(current_skyline_app_logger)

    response_code = 0
    headers = {
        'Accept': 'application/json',
        'Content-Type': 'application/json',
    }

    current_logger.info('%s :: making request to %s with data: %s' % (
        function_str, str(url), str(data)))
    data_dict = {
        'app': current_skyline_app,
        'data': data,
    }
    response = None
    try:
        # @modified 20241106 - Task #5526: Build v5.0.0 and upgrade deps
        # Add timeout for bandit B113
        #r = requests.post(url, data=json.dumps(data_dict), headers=headers)
        connect_timeout = 5
        read_timeout = 20
        use_timeout = (int(connect_timeout), int(read_timeout))
        r = requests.post(url, data=json.dumps(data_dict), headers=headers, timeout=use_timeout)
        try:
            response = r.json()
        except:
            pass
    except Exception as err:
        current_logger.error(traceback.format_exc())
        current_logger.error('error :: %s :: failed to get response from %s with data: %s, err: %s' % (
            function_str, str(url), str(data), err))
        if response:
            current_logger.info('%s :: %s, response: %s' % (
                function_str, str(url), str(response)))
    try:
        response_code = r.status_code
    except Exception as err:
        current_logger.error('error :: %s :: failed to determine r.status_code from %s with data: %s, err: %s' % (
            function_str, str(url), str(data), err))
        if response:
            current_logger.info('%s :: %s, response: %s' % (
                function_str, str(url), str(response)))

    return response_code
