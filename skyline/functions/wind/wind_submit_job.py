"""
wind_submit_job.py
"""
import gzip
import json
import logging

import requests


# @added 20240404 - Feature #5320: wind
def wind_submit_job(current_skyline_app, work_dict):
    """
    Submit a job to wind.

    :param current_skyline_app: the app calling the function
    :param work_dict: the work_dict
    :type current_skyline_app: str
    :type work_dict: dict
    :return: results_dict
    :rtype: dict

    """

    function_str = 'functions.wind.wind_return_results'
    current_skyline_app_logger = current_skyline_app + 'Log'
    current_logger = logging.getLogger(current_skyline_app_logger)

    results_dict = {'status_code': None}

    work_url = None
    try:
        work_url = str(work_dict['work_url'])
    except Exception as err:
        current_logger.error('error :: %s :: failed to determine work_url from work_dict, err: %s' % (
            function_str, err))
        results_dict = {'status_code': 400, 'reason': 'no work_url in work_dict'}
        return results_dict

    auth_user = None
    auth_password = None
    if 'auth_user' in work_dict.keys():
        auth_user = work_dict['auth_user']
    if 'auth_password' in work_dict.keys():
        auth_password = work_dict['auth_password']
    job = None
    try:
        job = work_dict['job']
    except KeyError:
        job = None
    current_logger.info('%s :: submitting %s job to %s' % (
        function_str, str(job), work_url))

    headers = {'content-encoding': 'gzip', 'content-type': 'application/json'}

    response = {}
    r = None
    try:
        payload = gzip.compress(json.dumps(work_dict).encode('utf-8'))
        if auth_user and auth_password:
            # r = requests.post(work_url, auth=(auth_user, auth_password), json=work_dict, headers=headers, timeout=10)
            r = requests.post(work_url, auth=(auth_user, auth_password), data=payload, headers=headers, timeout=30)
        else:
            # r = requests.post(work_url, json=work_dict, headers=headers, timeout=10)
            r = requests.post(work_url, data=payload, headers=headers, timeout=30)
    except Exception as err:
        current_logger.error('error :: %s :: requests post failed to %s, err: %s' % (
            function_str, str(work_url), err))
    status_code = None
    if r:
        try:
            status_code = r.status_code
        except Exception as err:
            current_logger.error('error :: %s :: failed to determine response status_code, err: %s' % (
                function_str, err))
        try:
            response = r.json()
        except Exception as err:
            current_logger.error('error :: %s :: failed to read response - %s' % (
                function_str, err))
    results_dict['status_code'] = status_code
    results_dict['response'] = response

    return results_dict
