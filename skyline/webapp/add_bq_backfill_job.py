"""
add_bq_backfill_job.py
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


# @added 20240515 - Feature #5352: vista - bigquery
def add_bq_backfill_job(current_skyline_app):
    """
    Return a dict with the backfill job details.

    :param current_skyline_app: the app calling the function
    :type current_skyline_app: str
    :return: bq_backfill_job_dict
    :rtype: dict

    """
    function_str = 'add_bq_backfill_job'
    bq_backfill_job_dict = {}

    current_skyline_app_logger = current_skyline_app + 'Log'
    current_logger = logging.getLogger(current_skyline_app_logger)

    vista_bq_account_key = None
    # Test whether form or json POST
    form_data = False
    try:
        vista_bq_account_key = request.form['vista_bq_account_key']
        if vista_bq_account_key:
            form_data = True
            current_logger.info('add_bq_backfill_job, form POST')
            current_logger.info('add_bq_backfill_job - vista_bq_account_key passed: %s' % str(vista_bq_account_key))
    except:
        current_logger.info('add_bq_backfill_job no form data trying json')

    post_data = {}
    if not form_data:
        try:
            post_data = request.get_json()
        except Exception as err:
            current_logger.error('error :: add_bq_backfill_job - no POST data - %s' % (
                err))
            current_logger.info('add_bq_backfill_job, return 400 no POST data')
            bq_backfill_job_dict['status_code'] = 400
            bq_backfill_job_dict['error'] = 'no post data'
            return bq_backfill_job_dict
        try:
            vista_bq_account_key = post_data['data']['vista_bq_account_key']
            if vista_bq_account_key:
                current_logger.info('add_bq_backfill_job - vista_bq_account_key passed: %s' % str(vista_bq_account_key))
        except KeyError:
            vista_bq_account_key = None
        except Exception as err:
            vista_bq_account_key = None
            current_logger.error('error :: add_bq_backfill_job - evaluation of  post_data[\'data\'][\'vista_bq_account_key\'] failed - %s' % (
                err))
    if not vista_bq_account_key:
        current_logger.info('add_bq_backfill_job, return 400 no vista_bq_account_key determined')
        bq_backfill_job_dict['status_code'] = 400
        bq_backfill_job_dict['error'] = 'no vista_bq_account_key argument'
        return bq_backfill_job_dict

    from_timestamp = None
    try:
        if form_data:
            from_timestamp = int(request.form['from_timestamp'])
        else:
            from_timestamp = int(post_data['data']['from_timestamp'])
        current_logger.info('add_bq_backfill_job :: with from_timestamp: %s' % str(from_timestamp))
        bq_backfill_job_dict['from_timestamp'] = from_timestamp
    except Exception as err:
        current_logger.error('error :: add_bq_backfill_job :: no valid from_timestamp argument, err: %s' % err)
        bq_backfill_job_dict['status_code'] = 400
        bq_backfill_job_dict['error'] = 'no valid from_timestamp argument'
        return bq_backfill_job_dict

    until_timestamp = None
    try:
        if form_data:
            until_timestamp = int(request.form['until_timestamp'])
        else:
            until_timestamp = int(post_data['data']['until_timestamp'])
        current_logger.info('add_bq_backfill_job :: with until_timestamp: %s' % str(until_timestamp))
        bq_backfill_job_dict['until_timestamp'] = until_timestamp
    except Exception as err:
        current_logger.error('error :: add_bq_backfill_job :: no valid until_timestamp argument, err: %s' % err)
        bq_backfill_job_dict['status_code'] = 400
        bq_backfill_job_dict['error'] = 'no valid until_timestamp argument'
        return bq_backfill_job_dict

    interval = None
    try:
        if form_data:
            interval = int(request.form['interval'])
        else:
            interval = int(post_data['data']['interval'])
        current_logger.info('add_bq_backfill_job :: with interval: %s' % str(interval))
        bq_backfill_job_dict['interval'] = interval
    except Exception as err:
        current_logger.error('error :: add_bq_backfill_job :: no valid interval argument, err: %s' % err)
        bq_backfill_job_dict['status_code'] = 400
        bq_backfill_job_dict['error'] = 'no valid interval argument'
        return bq_backfill_job_dict
    date_format = None
    try:
        if form_data:
            date_format = request.form['date_format']
        else:
            date_format = post_data['data']['date_format']
        current_logger.info('add_bq_backfill_job :: with date_format: %s' % str(date_format))
        bq_backfill_job_dict['date_format'] = date_format
    except Exception as err:
        current_logger.error('error :: add_bq_backfill_job :: no valid date_format argument, err: %s' % err)
        bq_backfill_job_dict['status_code'] = 400
        bq_backfill_job_dict['error'] = 'no valid date_format argument'
        return bq_backfill_job_dict
    # Validate date_format
    valid_date_format = False
    try:
        now = datetime.now()
        valid_date_format = now.strftime(date_format)
    except Exception as err:
        current_logger.error('error :: add_bq_backfill_job :: invalid date_format argument, err: %s' % err)
        valid_date_format = False
    if not valid_date_format:
        bq_backfill_job_dict['status_code'] = 400
        bq_backfill_job_dict['error'] = 'an invalid date_format argument was passed'
        return bq_backfill_job_dict
    replace_query_string = None
    try:
        if form_data:
            replace_query_string = request.form['replace_query_string']
        else:
            replace_query_string = post_data['data']['replace_query_string']
        current_logger.info('add_bq_backfill_job :: with replace_query_string: %s' % str(replace_query_string))
        bq_backfill_job_dict['replace_query_string'] = replace_query_string
    except Exception as err:
        current_logger.error('error :: add_bq_backfill_job :: no valid replace_query_string argument, err: %s' % err)
        bq_backfill_job_dict['status_code'] = 400
        bq_backfill_job_dict['error'] = 'no valid replace_query_string argument'
        return bq_backfill_job_dict

    zero_fill = True
    try:
        if form_data:
            zero_fill_string = request.form['zero_fill']
        else:
            zero_fill_string = post_data['data']['zero_fill']
        if zero_fill_string == 'false':
            zero_fill = False
        current_logger.info('add_bq_backfill_job :: with zero_fill: %s' % str(zero_fill))
        bq_backfill_job_dict['zero_fill'] = zero_fill
    except:
        zero_fill = True

    replace_redis_timeseries = False
    try:
        if form_data:
            replace_redis_timeseries_string = request.form['replace_redis_timeseries']
        else:
            replace_redis_timeseries_string = post_data['data']['replace_redis_timeseries']
        if replace_redis_timeseries_string == 'true':
            replace_redis_timeseries = False
        current_logger.info('add_bq_backfill_job :: with replace_redis_timeseries: %s' % str(replace_redis_timeseries))
        bq_backfill_job_dict['replace_redis_timeseries'] = replace_redis_timeseries
    except:
        dry_run = True

    max_creates_per_minute = 500
    try:
        if form_data:
            max_creates_per_minute = int(request.form['max_creates_per_minute'])
        else:
            max_creates_per_minute = int(post_data['data']['max_creates_per_minute'])
        current_logger.info('add_bq_backfill_job :: with max_creates_per_minute: %s' % str(max_creates_per_minute))
        bq_backfill_job_dict['max_creates_per_minute'] = max_creates_per_minute
    except:
        max_creates_per_minute = 500

    dry_run = True
    try:
        if form_data:
            dry_run_string = request.form['dry_run']
        else:
            dry_run_string = post_data['data']['dry_run']
        if dry_run_string == 'false':
            dry_run = False
        current_logger.info('add_bq_backfill_job :: with dry_run: %s' % str(dry_run))
        bq_backfill_job_dict['dry_run'] = dry_run
    except:
        dry_run = True

    flux_test = False
    try:
        if form_data:
            flux_test_string = request.form['flux_test']
        else:
            flux_test_string = post_data['data']['flux_test']
        if flux_test_string == 'true':
            flux_test = True
        current_logger.info('add_bq_backfill_job :: with flux_test: %s' % str(flux_test))
        bq_backfill_job_dict['flux_test'] = flux_test
    except:
        flux_test = False

    external_progress_url = None
    try:
        if form_data:
            external_progress_url = request.form['external_progress_url']
        else:
            external_progress_url = post_data['data']['external_progress_url']
        if external_progress_url:
            current_logger.info('add_bq_backfill_job :: with external_progress_url: %s' % str(external_progress_url))
            bq_backfill_job_dict['external_progress_url'] = external_progress_url
    except:
        external_progress_url = None

    verify_ssl = settings.VERIFY_SSL
    try:
        if form_data:
            verify_ssl_string = request.form['verify_ssl']
        else:
            verify_ssl_string = post_data['data']['verify_ssl']
        if verify_ssl_string == 'false':
            verify_ssl = False
        current_logger.info('add_bq_backfill_job :: with verify_ssl: %s' % str(verify_ssl))
        bq_backfill_job_dict['verify_ssl'] = verify_ssl
    except:
        verify_ssl = settings.VERIFY_SSL

    try:
        if form_data:
            added_by = request.form['added_by']
        else:
            added_by = post_data['data']['added_by']
        current_logger.info('add_bq_backfill_job :: with added_by: %s' % str(added_by))
        bq_backfill_job_dict['added_by'] = added_by
    except Exception as err:
        current_logger.info('warning :: add_bq_backfill_job :: added_by was not passed, err: %s' % (
            err))
        bq_backfill_job_dict['status_code'] = 400
        bq_backfill_job_dict['error'] = 'no valid added_by argument'
        return bq_backfill_job_dict

    use_archive_data = True
    try:
        if form_data:
            use_archive_data_string = request.form['use_archive_data']
        else:
            use_archive_data_string = post_data['data']['use_archive_data']
        if use_archive_data_string == 'false':
            use_archive_data = False
        current_logger.info('add_bq_backfill_job :: with use_archive_data: %s' % str(use_archive_data))
        bq_backfill_job_dict['use_archive_data'] = use_archive_data
    except:
        use_archive_data = True

    vista_bq_accounts = {}
    try:
        vista_bq_accounts = get_bq_accounts_settings(current_skyline_app)
    except Exception as err:
        current_logger.error('error :: add_bq_backfill_job :: get_bq_accounts_settings failed, err: %s' % err)

    # Validate the vista_bq_account_key
    vista_bq_account = {}
    try:
        vista_bq_account = copy.deepcopy(vista_bq_accounts[vista_bq_account_key])
        if vista_bq_account:
            bq_backfill_job_dict['vista_bq_account'] = vista_bq_account_key
    except Exception as err:
        current_logger.error('error :: add_bq_backfill_job :: invalid vista_bq_account_key parameter, does not exist, err: %s' % err)
        bq_backfill_job_dict['status_code'] = 400
        bq_backfill_job_dict['error'] = 'invalid vista_bq_account_key passed, does not exist'
        return bq_backfill_job_dict
    # Validate that BigQuery account is disabled in Vista
    if 'schedule' not in vista_bq_account.keys():
        current_logger.error('error :: add_bq_backfill_job :: schedule is not defined in the bq_account settings for %s' % vista_bq_account_key)
        bq_backfill_job_dict['status_code'] = 400
        bq_backfill_job_dict['error'] = 'schedule is not defined in the bq_account settings'
        return bq_backfill_job_dict
    schedule_disabled = True
    for key, value in vista_bq_account['schedule'].items():
        if value != 0:
            schedule_disabled = False
    if not schedule_disabled and added_by not in ['vista_bq_update','webapp']:
        current_logger.error('error :: add_bq_backfill_job :: schedule is not disabeld in the bq_account settings for %s' % vista_bq_account_key)
        bq_backfill_job_dict['status_code'] = 400
        bq_backfill_job_dict['error'] = 'the schedule is not disabled in the bq_account settings all values must be 0'
        return bq_backfill_job_dict

    if 'schedule_interval' not in vista_bq_account.keys():
        current_logger.error('error :: add_bq_backfill_job :: schedule_interval is not defined in the bq_account settings for %s' % vista_bq_account_key)
        bq_backfill_job_dict['status_code'] = 400
        bq_backfill_job_dict['error'] = 'schedule_interval is not defined in the bq_account settings'
        return bq_backfill_job_dict

    # Check that replace_query_string is in the query
    if replace_query_string not in vista_bq_account['query']:
        current_logger.error('error :: add_bq_backfill_job :: the replace_query_string string was not found in the account settings query')
        bq_backfill_job_dict['status_code'] = 400
        bq_backfill_job_dict['error'] = 'the replace_query_string string was not found in the account settings query'
        return bq_backfill_job_dict

    # Add job to bq_backfill Redis queue
    job_id = '%s.%s' % (str(int(time())), vista_bq_account_key)
    status_url = '%s/api?bq_backfill_job_status=true&job_id=%s' % (
        settings.SKYLINE_URL, job_id
    )
    if settings.REMOTE_SKYLINE_INSTANCES:
        status_url = status_url + '&cluster_data=true'

    data = {}
    try:
        data = {
            'job_id': job_id, 'vista_bq_account_key': vista_bq_account_key,
            'from_timestamp': from_timestamp, 'until_timestamp': until_timestamp,
            'interval': interval, 'date_format': date_format,
            'replace_query_string': replace_query_string, 'zero_fill': zero_fill,
            'dry_run': dry_run, 'replace_redis_timeseries': replace_redis_timeseries,
            'max_creates_per_minute': max_creates_per_minute,
            'use_archive_data': use_archive_data,
            'flux_test': flux_test, 'verify_ssl': verify_ssl, 'status': 'pending',
            'status_url': status_url, 'percent_complete': 0,
            'intervals_to_fetch': [], 'intervals_fetched': [],
            'external_progress_url': external_progress_url, 'added_by': added_by,
        }
    except Exception as err:
        current_logger.error('error :: add_bq_backfill_job :: failed to create data dict, err: %s' % (
            err))
        bq_backfill_job_dict['status_code'] = 500
        bq_backfill_job_dict['error'] = 'failed to create backfill job data dict'
        return bq_backfill_job_dict

    try:
        redis_conn_decoded = get_redis_conn_decoded(current_skyline_app)
    except Exception as err:
        current_logger.error('error :: add_bq_backfill_job :: get_redis_conn_decoded failed, err: %s' % (
            err))
        bq_backfill_job_dict['status_code'] = 500
        bq_backfill_job_dict['error'] = 'failed to connect to Redis'
        return bq_backfill_job_dict
    try:
        redis_conn_decoded.hset('vista.bq_backfill.work', job_id, str(data))
    except Exception as err:
        current_logger.error('error :: add_bq_backfill_job :: failed to add item to vista.bq_backfill.work hash, data: %s, err: %s' % (
            str(data), err))
        bq_backfill_job_dict['status_code'] = 500
        bq_backfill_job_dict['error'] = 'failed to add backfill job to vista.bq_backfill.work'
        return bq_backfill_job_dict

    try:
        redis_conn_decoded.hset('vista.bq_backfill.job_status', job_id, str(data))
    except Exception as err:
        current_logger.error('error :: add_bq_backfill_job :: failed to add item to vista.bq_backfill.job_status hash, data: %s, err: %s' % (
            str(data), err))
    if len(data) > 0:
        bq_backfill_job_dict = copy.deepcopy(data)
        bq_backfill_job_dict['status_code'] = 200
    current_logger.info('%s :: returning bq_backfill_job_dict: %s' % (
        function_str, str(bq_backfill_job_dict)))
    return bq_backfill_job_dict
