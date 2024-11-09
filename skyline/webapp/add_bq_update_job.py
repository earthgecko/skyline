"""
add_bq_update_job.py
"""
import copy
import logging
import os

from datetime import datetime
from time import time
from flask import request

import settings
from skyline_functions import get_redis_conn_decoded, mkdir_p
from functions.settings.get_bq_accounts_settings import get_bq_accounts_settings
from functions.vista.create_bq_update_job import create_bq_update_job

# @added 20240621 - Feature #5372: vista - bq_update
#                   Feature #5352: vista - bigquery
def add_bq_update_job(current_skyline_app):
    """
    Return a dict with the backfill job details.

    :param current_skyline_app: the app calling the function
    :type current_skyline_app: str
    :return: bq_update_job_dict
    :rtype: dict

    """
    function_str = 'add_bq_update_job'
    bq_update_job_dict = {}

    current_skyline_app_logger = current_skyline_app + 'Log'
    current_logger = logging.getLogger(current_skyline_app_logger)

    vista_bq_account_key = None
    # Test whether form or json POST
    form_data = False
    try:
        vista_bq_account_key = request.form['vista_bq_account_key']
        if vista_bq_account_key:
            form_data = True
            current_logger.info('add_bq_update_job, form POST')
            current_logger.info('add_bq_update_job - vista_bq_account_key passed: %s' % str(vista_bq_account_key))
    except:
        current_logger.info('add_bq_update_job no form data trying json')

    post_data = {}
    if not form_data:
        try:
            post_data = request.get_json()
        except Exception as err:
            current_logger.error('error :: add_bq_update_job - no POST data - %s' % (
                err))
            current_logger.info('add_bq_update_job, return 400 no POST data')
            bq_update_job_dict['status_code'] = 400
            bq_update_job_dict['error'] = 'no post data'
            return bq_update_job_dict
        try:
            vista_bq_account_key = post_data['data']['vista_bq_account_key']
            if vista_bq_account_key:
                current_logger.info('add_bq_update_job - vista_bq_account_key passed: %s' % str(vista_bq_account_key))
        except KeyError:
            vista_bq_account_key = None
        except Exception as err:
            vista_bq_account_key = None
            current_logger.error('error :: add_bq_update_job - evaluation of  post_data[\'data\'][\'vista_bq_account_key\'] failed - %s' % (
                err))
    if not vista_bq_account_key:
        current_logger.info('add_bq_update_job, return 400 no vista_bq_account_key determined')
        bq_update_job_dict['status_code'] = 400
        bq_update_job_dict['error'] = 'no vista_bq_account_key argument'
        return bq_update_job_dict

    from_timestamp = None
    try:
        if form_data:
            from_timestamp = int(request.form['from_timestamp'])
        else:
            from_timestamp = int(post_data['data']['from_timestamp'])
        current_logger.info('add_bq_update_job :: with from_timestamp: %s' % str(from_timestamp))
    except:
        from_timestamp = None
    bq_update_job_dict['from_timestamp'] = from_timestamp

    until_timestamp = None
    try:
        if form_data:
            until_timestamp = int(request.form['until_timestamp'])
        else:
            until_timestamp = int(post_data['data']['until_timestamp'])
        current_logger.info('add_bq_update_job :: with until_timestamp: %s' % str(until_timestamp))
    except:
        until_timestamp = None
    bq_update_job_dict['until_timestamp'] = until_timestamp

    verify_ssl = True
    try:
        if form_data:
            verify_ssl_string = request.form['verify_ssl']
        else:
            verify_ssl_string = post_data['data']['verify_ssl']
        if verify_ssl_string == 'false':
            verify_ssl = False
        current_logger.info('add_bq_update_job :: with verify_ssl: %s' % str(verify_ssl))
    except:
        verify_ssl = True
    bq_update_job_dict['verify_ssl'] = verify_ssl

    try:
        if form_data:
            added_by = request.form['added_by']
        else:
            added_by = post_data['data']['added_by']
        current_logger.info('add_bq_update_job :: with added_by: %s' % str(added_by))
        bq_update_job_dict['added_by'] = added_by
    except Exception as err:
        current_logger.warning('warning :: add_bq_update_job :: added_by was not passed, err: %s' % (
            err))
        bq_update_job_dict['status_code'] = 400
        bq_update_job_dict['error'] = 'no valid added_by argument'
        return bq_update_job_dict

    vista_bq_accounts = {}
    try:
        vista_bq_accounts = get_bq_accounts_settings(current_skyline_app)
    except Exception as err:
        current_logger.error('error :: add_bq_update_job :: get_bq_accounts_settings failed, err: %s' % err)

    # Validate the vista_bq_account_key
    vista_bq_account = {}
    try:
        vista_bq_account = copy.deepcopy(vista_bq_accounts[vista_bq_account_key])
        if vista_bq_account:
            bq_update_job_dict['vista_bq_account_key'] = vista_bq_account_key
    except Exception as err:
        current_logger.error('error :: add_bq_update_job :: invalid vista_bq_account_key parameter, does not exist, err: %s' % err)
        bq_update_job_dict['status_code'] = 400
        bq_update_job_dict['error'] = 'invalid vista_bq_account_key passed, does not exist'
        return bq_update_job_dict
    # Validate that BigQuery account is enabled for updating
    if 'update_data_period' not in vista_bq_account.keys():
        current_logger.error('error :: add_bq_update_job :: update_data_period is not defined in the bq_account settings for %s' % vista_bq_account_key)
        bq_update_job_dict['status_code'] = 400
        bq_update_job_dict['error'] = 'update_data_period is not defined in the bq_account settings'
        return bq_update_job_dict
    if 'update_replace_query' not in vista_bq_account.keys():
        current_logger.error('error :: add_bq_update_job :: update_replace_query is not defined in the bq_account settings for %s' % vista_bq_account_key)
        bq_update_job_dict['status_code'] = 400
        bq_update_job_dict['error'] = 'update_replace_query is not defined in the bq_account settings'
        return bq_update_job_dict

    try:
        bq_update_job_dict = create_bq_update_job(current_skyline_app, bq_update_job_dict)
    except Exception as err:
        current_logger.error('error :: add_bq_update_job :: create_bq_update_job failed, err: %s' % err)
        bq_update_job_dict['status_code'] = 500
        bq_update_job_dict['error'] = 'create_bq_update_job failed'
        return bq_update_job_dict

    return bq_update_job_dict
