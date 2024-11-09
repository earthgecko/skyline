"""
get_bq_backfill_job.py
"""
import copy
import logging
from ast import literal_eval

from flask import request

import settings
from skyline_functions import get_redis_conn_decoded
from backend import get_cluster_data


# @added 20240516 - Feature #5352: vista - bigquery
def get_bq_backfill_job(current_skyline_app, job_id):
    """
    Return a dict with the backfill job details.

    :param current_skyline_app: the app calling the function
    :param cluster_data: the cluster_data parameter from the request
    :type current_skyline_app: str
    :type cluster_data: bool
    :return: bq_backfill_job_dict
    :rtype: dict

    """
    function_str = 'get_bq_backfill_job'
    bq_backfill_job_dict = {'job_id': job_id}

    current_skyline_app_logger = current_skyline_app + 'Log'
    current_logger = logging.getLogger(current_skyline_app_logger)

    cluster_data = False
    try:
        cluster_data_str = request.args.get('cluster_data', 'false')
        if cluster_data_str == 'true':
            cluster_data = True
    except:
        cluster_data = False
    cluster_call = False
    try:
        cluster_call_str = request.args.get('cluster_call', 'false')
        if cluster_call_str == 'true':
            cluster_call = True
    except:
        cluster_call = False

    try:
        redis_conn_decoded = get_redis_conn_decoded(current_skyline_app)
    except Exception as err:
        current_logger.error('error :: get_bq_backfill_job :: get_redis_conn_decoded failed, err: %s' % (
            err))
        bq_backfill_job_dict['status_code'] = 500
        bq_backfill_job_dict['error'] = 'failed to connect to Redis'
        return bq_backfill_job_dict

    try:
        bq_backfill_job_dict_str = redis_conn_decoded.hget('vista.bq_backfill.job_status', job_id)
        if bq_backfill_job_dict_str:
            bq_backfill_job_dict = literal_eval(bq_backfill_job_dict_str)
    except Exception as err:
        current_logger.error('error :: get_bq_backfill_job :: failed to hget on vista.bq_backfill.processed, err: %s' % (
            err))
        bq_backfill_job_dict['status_code'] = 500
        bq_backfill_job_dict['error'] = 'failed to query job'
        return bq_backfill_job_dict

    if not bq_backfill_job_dict and cluster_data and settings.REMOTE_SKYLINE_INSTANCES and not cluster_call:
        api_uri = 'bq_backfill_job_status=true&job_id=%s&cluster_data=false&cluster_call=true' % str(job_id)
        try:
            bq_backfill_job_dicts = get_cluster_data(api_uri, 'bq_backfill_job_status')
        except Exception as err:
            current_logger.error('error :: get_bq_backfill_job :: get_cluster_data failed, err: %s' % (
                err))
            bq_backfill_job_dict['status_code'] = 500
            bq_backfill_job_dict['error'] = 'failed to add backfill job to queue'
            return bq_backfill_job_dict
        for job_dict in bq_backfill_job_dicts:
            if len(job_dict) > 0:
                bq_backfill_job_dict = copy.deepcopy(job_dict)
                break
    if 'vista_bq_account_key' in list(bq_backfill_job_dict.keys()):
        bq_backfill_job_dict['status_code'] = 200
    else:
        bq_backfill_job_dict['status_code'] = 404
    log_bq_backfill_job_dict = copy.deepcopy(bq_backfill_job_dict)
    if 'results' in log_bq_backfill_job_dict.keys():
        log_bq_backfill_job_dict['results'] ='redacted for log'
    current_logger.info('%s :: returning bq_backfill_job_dict: %s' % (
        function_str, str(log_bq_backfill_job_dict)))
    return bq_backfill_job_dict
