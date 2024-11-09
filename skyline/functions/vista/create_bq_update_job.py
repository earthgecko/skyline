"""
create_bq_update_job.py
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


# @added 20240621 - Feature #5372: vista - bq_update
#                   Feature #5352: vista - bigquery
def create_bq_update_job(current_skyline_app, bq_update_params={}):
    """
    Add a bq_update job to the vista.bq_update.work Redis hash

    :param current_skyline_app: the app calling the function
    :param bq_update_params: this is a dictionary of the update job to add it is
        the same as the values that would be in :mod:`settings.VISTA_BQ_ACCOUNTS`
        which are used here but can be overwritten the same parameters passed in
        the bq_update_params dict.  The only required key in the
        bq_update_params dict is the vista_bq_account_key.  IMPORTANT - the
        update_data_schedule parameter in both the :mod:`settings.VISTA_BQ_ACCOUNTS`
        and if passed in the bq_update_params is IGNORED because a job is either
        submitted by vista at the scheduled time or the job is submitted by the
        webapp, either way, update_data_schedule is not checked as the job has
        been scheduled by vista or the user.  bq_update_params can also be
        passed with additional from_timestamp and until_timestamp keys to limit
        the update_data_period to certain periods if desired.  If the
        from_timestamp and until_timestamp keys are passed then rather than
        using to update_data_period, the job will only update the periods
        between the two timestamp at each schedule_interval.  Meaning if the
        update job is normally run for the last 30 days, this parameters can be
        used to make the update job just run for a single day, so now - 30d.
        It should be noted that this is mostly for testing purposes.
    :type current_skyline_app: str
    :return: bq_update_job_dict
    :rtype: dict

    """
    function_str = 'create_bq_update_job'
    bq_update_job_dict = {}

    current_skyline_app_logger = current_skyline_app + 'Log'
    current_logger = logging.getLogger(current_skyline_app_logger)

    vista_bq_account_key = None
    try:
        vista_bq_account_key = bq_update_params['vista_bq_account_key']
        current_logger.info('create_bq_update_job - vista_bq_account_key passed: %s' % str(vista_bq_account_key))
    except Exception as err:
        current_logger.error('no vista_bq_account_key passed, err: %s' % err)
        bq_update_job_dict = {'error': 'no vista_bq_account_key passed'}
        return bq_update_job_dict

    job_id = '%s.%s' % (str(int(time())), vista_bq_account_key)

    vista_bq_accounts = {}
    try:
        vista_bq_accounts = get_bq_accounts_settings(current_skyline_app)
    except Exception as err:
        current_logger.error('error :: create_bq_update_job :: get_bq_accounts_settings failed, err: %s' % err)
    bq_account_settings = {}
    try:
        bq_account_settings = vista_bq_accounts[vista_bq_account_key]
    except Exception as err:
        current_logger.error('error :: create_bq_update_job :: failed to determine settings for %s, err: %s' % (
            vista_bq_account_key, err))
    if not bq_account_settings:
        err_msg = 'failed to determine settings for %s' % vista_bq_account_key
        current_logger.error('error :: create_bq_update_job :: %s' % err_msg)
        bq_update_job_dict = {'error': err_msg}
        return bq_update_job_dict

    for key, item in bq_update_params.items():
        bq_account_settings[key] = item

    data = {'job_id': job_id}
    for key, item in bq_account_settings.items():
        data[key] = item

    try:
        redis_conn_decoded = get_redis_conn_decoded(current_skyline_app)
    except Exception as err:
        current_logger.error('error :: create_bq_update_job :: get_redis_conn_decoded failed, err: %s' % (
            err))
        bq_update_job_dict['status_code'] = 500
        bq_update_job_dict['error'] = 'failed to connect to Redis'
        return bq_update_job_dict
    try:
        redis_conn_decoded.hset('vista.bq_update.work', job_id, str(data))
    except Exception as err:
        current_logger.error('error :: create_bq_update_job :: failed to add item to vista.bq_update.work hash, data: %s, err: %s' % (
            str(data), err))
        bq_update_job_dict['status_code'] = 500
        bq_update_job_dict['error'] = 'failed to add backfill job to vista.bq_update.work'
        return bq_update_job_dict

    bq_update_job_dict = copy.deepcopy(data)
    try:
        if 'oauth_credentials' in bq_update_job_dict:
            if len(bq_update_job_dict['oauth_credentials']) > 0:
                bq_update_job_dict['oauth_credentials'] = 'REDACTED'
    except:
        pass
    try:
        if 'service_credentials' in bq_update_job_dict:
            if len(bq_update_job_dict['service_credentials']) > 0:
                bq_update_job_dict['service_credentials'] = 'REDACTED'
    except:
        pass

    current_logger.info('%s :: returning bq_update_job_dict: %s' % (
        function_str, str(bq_update_job_dict)))

    return bq_update_job_dict
