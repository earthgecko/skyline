"""
bq_backfill.py
"""
import copy
import gzip
import json
import logging
import os
import sys
import traceback

from ast import literal_eval
from contextlib import nullcontext
from datetime import datetime, timezone
from time import time, sleep

import requests

# For version pinning consider implementing a skyline-bq-py31013 environment
# and adding that to the path so that imports come from there rather than from
# the skyline-py31013 environment which has different versions of some libraries
# e.g. googleapis-common-protos

# VIA webapp and API rather than via config...

sys.path.insert(0, '/opt/skyline/github/skyline/skyline')
import settings
from skyline_functions import get_redis_conn_decoded, get_graphite_metric, mkdir_p
from functions.database.queries.get_all_inactive_db_metric_names import get_all_inactive_db_metric_names
from functions.database.queries.set_metric_ids_as_active import set_metric_ids_as_active
from functions.settings.get_bq_accounts_settings import get_bq_accounts_settings
from functions.settings.get_external_settings import get_external_settings
from functions.redis.populate_redis_metric import populate_redis_metric

try:
    VISTA_BQ_ACCOUNTS = copy.deepcopy(settings.VISTA_BQ_ACCOUNTS)
except:
    VISTA_BQ_ACCOUNTS = {}
try:
    VISTA_BQ_VIRTUALENV_PATH = settings.VISTA_BQ_VIRTUALENV_PATH
except:
    VISTA_BQ_VIRTUALENV_PATH = None
if VISTA_BQ_VIRTUALENV_PATH:
    sys.path.insert(0, VISTA_BQ_VIRTUALENV_PATH)
    from functions.bigquery.bq_df_to_metrics import bq_df_to_metrics
    from functions.bigquery.bq_credentials import get_bq_credentials
    import pandas as pd
    import pandas_gbq
else:
    bq_df_to_metrics = nullcontext()
    get_bq_credentials = nullcontext()
    get_bq_credentials = nullcontext()
    pandas_gbq = nullcontext()


parent_skyline_app = 'vista'
child_skyline_app = 'bq_backfill'
skyline_app_logger = '%sLog' % parent_skyline_app
logger = logging.getLogger(skyline_app_logger)
skyline_app = '%s.%s' % (parent_skyline_app, child_skyline_app)

# To ensure that the Graphite MAX_CREATES_PER_MINUTE is not breached.
# Send to flux but only send Graphite MAX_CREATES_PER_MINUTE per minute.
# This also limits the number of metrics that have to be created in the
# DB as well and mitigates any thundering herd operations.
MAX_CREATES_PER_MINUTE = 450

# In order to prevent throwing tons on data at Graphite, horizon, DB, Redis, etc
# the backfill is limited to how many submissions can be made per minute.
SECONDS_BETWEEN_SUBMISSION = 5

DEBUG_LOGGING = False

# @added 20240516 - Feature #5352: vista - bigquery
def bq_backfill(bq_backfill_job_id):
    """
    This function is used to get data from BigQuery, convert it to metrics and
    submit it to flux and Redis (if it would be dropped by Horizon due to
    :mod:`settings.MAX_RESOLUTION`)
    """

    start_backfill_job = time()
    try:
        redis_conn_decoded = get_redis_conn_decoded(parent_skyline_app)
    except Exception as err:
        logger.error('error :: bq_backfill - redis connection failed, err: %s' % err)

    def remove_key(key):
        logger.info('bq_backfill :: removing %s from vista.bq_backfill.work' % key)
        try:
            redis_conn_decoded.hdel('vista.bq_backfill.work', key)
        except Exception as err:
            logger.error('error :: bq_backfill :: failed to hdel %s from vista.bq_backfill.work, err: %s' % (
                key, err))

    def update_job_status(key, data):
        logger.info('bq_backfill :: updating job status for %s in vista.bq_backfill.job_status' % key)
        try:
            redis_conn_decoded.hset('vista.bq_backfill.job_status', key, str(data))
        except Exception as err:
            # @modified 20241106 - Task #5526: Build v5.0.0 and upgrade deps
            # bandit - B608:hardcoded_sql_expressions
            logger.error('error :: bq_backfill :: failed to update job status with hset on %s in vista.bq_backfill.job_status, err: %s' % (
                key, err))  # nosec B608

    def fail_job(key, data, err_msg):
        remove_key(key)
        data['error'] = err_msg
        data['status'] = 'failed'
        data['status_code'] = '500'
        update_job_status(key, data)

    vista_bq_backfill_work = {}
    try:
        vista_bq_backfill_work = redis_conn_decoded.hgetall('vista.bq_backfill.work')
    except Exception as err:
        logger.error('error :: bq_backfill :: failed to hgetall on vista.bq_backfill.work, err: %s' % err)
        vista_bq_backfill_work = {}

    vista_bq_account_key = None
    from_timestamp = None
    until_timestamp = None
    interval = None
    replace_query_string = None
    date_format = None
    job_id = None
    external_progress_url = None
    work_dict = {}
    # Get a job from the work queue
    for job_id, work_dict_str in vista_bq_backfill_work.items():
        if job_id != bq_backfill_job_id:
            continue
        try:
            work_dict = literal_eval(work_dict_str)
        except Exception as err:
            err_msg = 'bq_backfill :: failed to literal_eval work_dict'
            logger.error('error :: bq_backfill :: %s: %s, for job_id: %s , err: %s' % (
                err_msg, str(work_dict_str), str(job_id), err))
            fail_job(job_id, work_dict_str, err_msg)
            continue
        try:
            vista_bq_account_key = work_dict['vista_bq_account_key']
        except Exception as err:
            err_msg = 'failed to determine vista_bq_account_key from work_dict'
            logger.error('error :: bq_backfill :: %s: %s, for job_id: %s , err: %s' % (
                err_msg, str(work_dict_str), str(job_id), err))
            fail_job(job_id, work_dict, err_msg)
            continue
        try:
            from_timestamp = work_dict['from_timestamp']
        except Exception as err:
            err_msg = 'failed to determine from_timestamp from work_dict'
            logger.error('error :: bq_backfill :: %s: %s, for job_id: %s , err: %s' % (
                err_msg, str(work_dict_str), str(job_id), err))
            fail_job(job_id, work_dict, err_msg)
            continue
        try:
            until_timestamp = work_dict['until_timestamp']
        except Exception as err:
            err_msg = 'failed to determine until_timestamp from work_dict'
            logger.error('error :: bq_backfill :: %s: %s, for job_id: %s , err: %s' % (
                err_msg, str(work_dict_str), str(job_id), err))
            fail_job(job_id, work_dict. err_msg)
            continue
        try:
            interval = work_dict['interval']
        except Exception as err:
            err_msg = 'failed to determine interval from work_dict'
            logger.error('error :: bq_backfill :: %s:%s, err: %s' % (
                err_msg, str(work_dict_str), err))
            fail_job(job_id, work_dict, err_msg)
            continue
        try:
            replace_query_string = work_dict['replace_query_string']
        except Exception as err:
            err_msg = 'failed to determine replace_query_string from work_dict'
            logger.error('error :: bq_backfill :: %s: %s, for job_id: %s , err: %s' % (
                err_msg, str(work_dict_str), str(job_id), err))
            fail_job(job_id, work_dict, err_msg)
            continue
        try:
            date_format = work_dict['date_format']
        except Exception as err:
            err_msg = 'failed to determine date_format from work_dict'
            logger.error('error :: bq_backfill :: %s: %s, for job_id: %s , err: %s' % (
                err_msg, str(work_dict_str), str(job_id), err))
            fail_job(job_id, work_dict, err_msg)
            continue
        try:
            added_by = work_dict['added_by']
        except Exception as err:
            err_msg = 'failed to determine added_by from work_dict'
            logger.error('error :: bq_backfill :: %s: %s, for job_id: %s , err: %s' % (
                err_msg, str(work_dict_str), str(job_id), err))
            fail_job(job_id, work_dict, err_msg)
            continue

        # The default is to zero_fill
        # CAN MAKES SO MANY MORE MERTICS!!!!
        zero_fill = False
        try:
            zero_fill = work_dict['zero_fill']
        except:
            zero_fill = False

        # The default is to NOT replace_redis_timeseries
        replace_redis_timeseries = True
        try:
            replace_redis_timeseries = work_dict['replace_redis_timeseries']
        except:
            replace_redis_timeseries = False

        max_creates_per_minute = 500
        try:
            max_creates_per_minute = work_dict['max_creates_per_minute']
        except:
            max_creates_per_minute = 500

        # The default is to dry_run
        dry_run = True
        try:
            dry_run = work_dict['dry_run']
        except:
            dry_run = True

        # The default is to not test
        flux_test = False
        try:
            flux_test = work_dict['flux_test']
        except:
            flux_test = False

        verify_ssl = settings.VERIFY_SSL
        try:
            verify_ssl = work_dict['verify_ssl']
        except:
            verify_ssl = settings.VERIFY_SSL

        use_archive_data = True
        try:
            use_archive_data = work_dict['use_archive_data']
        except:
            use_archive_data = True

        try:
            external_progress_url = work_dict['external_progress_url']
        except:
            external_progress_url = None

        break

    if added_by in ['webapp', 'api'] and not replace_query_string:
        err_msg = 'failed to replace_query_string'
        logger.error('error :: bq_backfill :: %s' % err_msg)
        fail_job(job_id, work_dict, err_msg)
        return

    if added_by == 'vista_bq_fetcher':
        replace_query_string = None

    started = int(time())
    work_dict['started'] = started
    job_key_dict = {'started': started, 'job_id': job_id, 'work_dict': work_dict}
    try:
        redis_conn_decoded.set('vista.bq_backfill.running', str(job_key_dict))
        logger.info('bq_backfill set vista.bq_backfill.running Redis key with %s' % str(job_key_dict))
    except Exception as err:
        logger.error('error :: bq_backfill :: failed to set on vista.bq_backfill.running from Redis, err: %s' % err)

    """
    # Example work_dict that is submitted via /bq_backfill by the webapp, direct
    # or by vista_bq_fetcher
    work_data = {
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
    """

    # Remove the work
    remove_key(job_id)

    # Update the job status
    work_dict['status'] = 'processing'
    intervals_to_fetch_timestamps = {}
    intervals_to_fetch = []
    if len(work_dict['intervals_to_fetch']) == 0:
        end_timestamp = int(until_timestamp) + 1
        ctime = int(from_timestamp)
        while ctime < end_timestamp:
            try:
                interval_date_fromat = datetime.fromtimestamp(int(ctime)).strftime(date_format)
                intervals_to_fetch.append(interval_date_fromat)
                intervals_to_fetch_timestamps[interval_date_fromat] = int(ctime)
            except Exception as err:
                logger.error('error :: bq_backfill :: failed to convert %s to date_format, err: %s' % (
                    str(ctime), err))
            ctime = ctime + interval
            if ctime > end_timestamp:
                break
        work_dict['intervals_to_fetch'] = intervals_to_fetch
    try:
        update_job_status(job_id, str(work_dict))
    except Exception as err:
        logger.error('error :: bq_backfill :: update_job_status failed, job_id: %s, work_dict: %s, err: %s' % (
            job_id, str(work_dict), err))

    vista_bq_accounts = {}
    try:
        vista_bq_accounts = get_bq_accounts_settings(parent_skyline_app)
    except Exception as err:
        logger.error('error :: bq_fetcher :: get_bq_accounts_settings failed, err: %s' % err)

    bq_account_settings = {}
    try:
        bq_account_settings = vista_bq_accounts[vista_bq_account_key]
    except Exception as err:
        logger.error('error :: bq_fetcher :: failed to determine settings for %s, err: %s' % (
            vista_bq_account_key, err))

    if not bq_account_settings:
        err_msg = 'failed to determine settings for %s' % vista_bq_account_key
        logger.error('error :: bq_fetcher :: %s' % err_msg)
        fail_job(job_id, work_dict, err_msg)
        try:
            redis_conn_decoded.delete('vista.bq_backfill.running')
        except Exception as err:
            logger.error('error :: bq_backfill :: failed to delete on vista.bq_backfill.running from Redis, err: %s' % err)
        return

    query = None
    try:
        query = bq_account_settings['query']
    except Exception as err:
        err_msg = 'failed to determine query from settings for %s' % vista_bq_account_key
        logger.error('error :: bq_fetcher :: %s, err: %s' % (
            err_msg, err))
        fail_job(job_id, work_dict, err_msg)
        try:
            redis_conn_decoded.delete('vista.bq_backfill.running')
        except Exception as err:
            logger.error('error :: bq_backfill :: failed to delete on vista.bq_backfill.running from Redis, err: %s' % err)
        return

    metric_format = 'graphite'
    try:
        metric_format = bq_account_settings['metric_format']
    except Exception as err:
        logger.error('error :: bq_backfill :: failed to ddetermine metric_format from bq_account_settings defaulting to Graphite, err: %s' % err)
        metric_format = 'graphite'    

    flux_api_token = None
    try:
        flux_api_token = bq_account_settings['flux_api_token']
    except Exception as err:
        err_msg = 'failed to determine flux_api_token from settings for %s' % vista_bq_account_key
        logger.error('error :: bq_fetcher :: %s, err: %s' % (
            err_msg, err))
        fail_job(job_id, work_dict, err_msg)
        try:
            redis_conn_decoded.delete('vista.bq_backfill.running')
        except Exception as err:
            logger.error('error :: bq_backfill :: failed to delete on vista.bq_backfill.running from Redis, err: %s' % err)
        return

    try:
        full_duration = bq_account_settings['full_duration']
    except Exception as err:
        err_msg = 'failed to determine full_duration from settings for %s' % vista_bq_account_key
        logger.error('error :: bq_fetcher :: %s, err: %s' % (
            err_msg, err))
        fail_job(job_id, work_dict, err_msg)
        try:
            redis_conn_decoded.delete('vista.bq_backfill.running')
        except Exception as err:
            logger.error('error :: bq_backfill :: failed to delete on vista.bq_backfill.running from Redis, err: %s' % err)
        return

    try:
        schedule_interval = bq_account_settings['schedule_interval']
    except Exception as err:
        err_msg = 'failed to determine schedule_interval from settings for %s' % vista_bq_account_key
        logger.error('error :: bq_fetcher :: %s, err: %s' % (
            err_msg, err))
        fail_job(job_id, work_dict, err_msg)
        try:
            redis_conn_decoded.delete('vista.bq_backfill.running')
        except Exception as err:
            logger.error('error :: bq_backfill :: failed to delete on vista.bq_backfill.running from Redis, err: %s' % err)
        return

    flux_key_namespace_prefixes = {
        settings.FLUX_SELF_API_KEY: None
    }
    for flux_token in settings.FLUX_API_KEYS:
        flux_key_namespace_prefixes[flux_token] = settings.FLUX_API_KEYS[flux_token]
    skyline_external_settings = {}
    try:
        skyline_external_settings = get_external_settings(parent_skyline_app, None, True)
    except Exception as err:
        logger.error('error :: bq_backfill :: get_external_settings failed, err: %s' % (
            err))
    if skyline_external_settings:
        for settings_key in list(skyline_external_settings.keys()):
            external_setting_keys = list(skyline_external_settings[settings_key].keys())
            if 'flux_token' in external_setting_keys:
                flux_token = skyline_external_settings[settings_key]['flux_token']
                if flux_token and flux_token not in flux_key_namespace_prefixes.keys():
                    flux_key_namespace_prefixes[flux_token] = skyline_external_settings[settings_key]['namespace']

    namespace_prefix = None
    if flux_api_token:
        try:
            namespace_prefix = flux_key_namespace_prefixes[flux_api_token]
        except Exception as err:
            logger.error('error :: bq_backfill :: failed to determine flux namespace_prefix from flux_api_token, err: %s' % (
                err))
            namespace_prefix = None

    # Credentials can be passed in a json file or as a dict (as from
    # external_settings)
    credentials = None
    use_credentials = None
    oauth_credentials = {}
    service_credentials = {}
    oauth_credentials_file = None
    service_credentials_file = None
    project_id = None
    if 'project_id' in bq_account_settings.keys():
        project_id = bq_account_settings['project_id']
    if 'oauth_credentials' in bq_account_settings.keys():
        oauth_credentials = bq_account_settings['oauth_credentials']
    if 'oauth_credentials_file' in bq_account_settings.keys():
        oauth_credentials_file = bq_account_settings['oauth_credentials_file']
    if 'service_credentials' in bq_account_settings.keys():
        service_credentials = bq_account_settings['service_credentials']
    if 'service_credentials_file' in bq_account_settings.keys():
        service_credentials_file = bq_account_settings['service_credentials_file']
    try:
        credentials = get_bq_credentials(
                            parent_skyline_app, credentials=use_credentials,
                            project_id=project_id,
                            oauth_credentials=oauth_credentials,
                            oauth_credentials_file=oauth_credentials_file,
                            service_credentials=service_credentials,
                            service_credentials_file=service_credentials_file)
    except Exception as err:
        logger.error('error :: bq_backfill :: get_bq_credentials failed, err: %s' % (
            err))
    if not credentials:
        logger.error('error :: bq_backfill :: no credentials retrieved for %s' % vista_bq_account_key)
        err_msg = 'failed to get credentials for %s' % vista_bq_account_key
        logger.error('error :: bq_fetcher :: %s' % err_msg)
        fail_job(job_id, work_dict, err_msg)
        try:
            redis_conn_decoded.delete('vista.bq_backfill.running')
        except Exception as err:
            logger.error('error :: bq_backfill :: failed to delete on vista.bq_backfill.running from Redis, err: %s' % err)
        return
    else:
        use_credentials = credentials

    if dry_run:
        work_dict['results'] = {}

    # Resources to determine what metric ids need to be set to active again
    all_db_metric_ids_with_names = {}
    try:
        all_db_metric_ids_with_names = redis_conn_decoded.hgetall('metrics_manager.all_db_metric_ids_with_names')
    except Exception as err:
        logger.error('error :: bq_update :: failed hgetall metrics_manager.all_db_metric_ids_with_names, err: %s' % (
            err))
    all_base_names_with_ids = {}
    all_ids_with_base_names = {}
    for id_str, i_base_name in all_db_metric_ids_with_names.items():
        metric_id = int(float(id_str))
        all_base_names_with_ids[i_base_name] = metric_id
        all_ids_with_base_names[metric_id] = i_base_name
    # get inactive metric_ids
    inactive_metric_names_with_id = {}
    try:
        inactive_metric_names, inactive_metric_names_with_id = get_all_inactive_db_metric_names(parent_skyline_app, with_ids=True)
    except Exception as err:
        logger.error('error :: bq_update :: get_all_inactive_db_metric_names failed, err: %s' % (
            err))
    inactive_metric_ids = set(list(inactive_metric_names_with_id.values()))
    metric_ids_to_set_as_active_again = []

    # Track the number of unique metrics to control how many are sent to flux to
    # ensure that the Graphite MAX_CREATES_PER_MINUTE is not breached initially
    unique_metrics = set()

    flux_url = '%s/flux/metric_data_post' % settings.SKYLINE_URL
    headers = {'content-encoding': 'gzip', "content-type": "application/json"}

    estimated_time = 0

    failed_job = False
    failed_job_err = None
    total_intervals_to_fetch = len(intervals_to_fetch)
    logger.info('bq_backfill :: %s intervals_to_fetch for %s' % (
        str(total_intervals_to_fetch), vista_bq_account_key))

    # Track all the new metrics created to rate limit and not breach Graphite
    # MAX_CREATES_PER_MINUTE
    metrics_created = set()
    # Record all timestamps to determine whether Redis needs to be populated
    # for vista_bq_fetcher jobs
    timestamps_submitted = set()
    last_submission_timestamp = 0
    redis_populated_metrics = []
    if metric_format == 'graphite':
        work_dict['redis_metrics_populated'] = len(redis_populated_metrics)

    known_metrics = []
    try:
        known_metrics = redis_conn_decoded.hkeys('aet.metrics_manager.metric_names_with_ids')
    except Exception as err:
        logger.error('error :: bq_backfill :: hkeys failed on aet.metrics_manager.metric_names_with_ids, err: %s' % (
            err))

    start_bq_to_flux = int(time())
    intervals_fetched = []
    for fetch_interval in intervals_to_fetch:
        if failed_job:
            break
        start_fetch_interval = int(time())
        sql = str(query)
        if added_by != 'vista_bq_fetcher':
            try:
                unquoted_replace_string = str(fetch_interval)
                replace_string = "'%s'" % unquoted_replace_string
                sql = query.replace(replace_query_string, replace_string)
            except Exception as err:
                err_msg = 'failed to generate sql with replace_query_string and %s' % str(fetch_interval)
                logger.error('error :: bq_backfill :: %s, err: %s' % (
                    err_msg, err))
                fail_job(job_id, work_dict, err_msg)
                failed_job = True
                failed_job_err = str(err_msg)
                break

        logger.info('bq_backfill :: fetching data for %s with sql: %s' % (vista_bq_account_key, sql))

        # Just to ensure the credentials are refreshed if they have expired
        # during the loop
        if use_credentials:
            try:
                credentials = get_bq_credentials(
                                    parent_skyline_app, credentials=use_credentials,
                                    project_id=project_id,
                                    oauth_credentials=oauth_credentials,
                                    oauth_credentials_file=oauth_credentials_file,
                                    service_credentials=service_credentials,
                                    service_credentials_file=service_credentials_file)
            except Exception as err:
                logger.error('error :: bq_backfill :: get_bq_credentials failed, err: %s' % (
                    err))
            if credentials:
                use_credentials = credentials

        df = None

        do_not_send_to_flux = False

        # Load archive data if it exists and has been requested
        # the date_folder must be the date and time it was downloaded not the
        # fetch_interval
        # date_folder = datetime.strftime(datetime.strptime(fetch_interval, date_format), '%Y%m%d%H%M%S')
        date_folder_ts = int(start_backfill_job)
        date_folder = datetime.fromtimestamp(date_folder_ts, tz=timezone.utc).strftime('%Y%m%d%H%M%S')

        # Search the path for the latest archive for the 
        archive_data_search_path = '%s/vista/bq_archives/%s' % (
            settings.SKYLINE_DIR, vista_bq_account_key)
        bq_archives = []
        if os.path.isdir(archive_data_search_path) and use_archive_data:
            for dir_path, folders, files in os.walk(archive_data_search_path):
                try:
                    if files:
                        for i in files:
                            path_and_file = '%s/%s' % (dir_path, i)
                            if i.endswith('.csv'):
                                bq_archives.append(path_and_file)
                except Exception as err:
                    logger.error('error :: bq_backfill :: os.walk failed on %s, err: %s' % (
                        str(archive_data_search_path), err))
        # Sort latest to oldest
        bq_archives = sorted(bq_archives, reverse=True)
        logger.info('bq_backfill :: %s bq_archives found' % (str(len(bq_archives))))
        archive_csv_filename = '%s.%s.csv' % (vista_bq_account_key, str(fetch_interval))
        archive_csv = None
        # Find latest archive if one exists
        for i in bq_archives:
            if archive_csv_filename in i:
                archive_csv = str(i)
                break

        archive_data_used = False
        if archive_csv:
            if os.path.isfile(archive_csv):
                logger.info('bq_backfill :: loading data for %s from latest archive data file %s' % (str(fetch_interval), archive_csv))
                try:
                    df = pd.read_csv(archive_csv)
                except Exception as err:
                    err_msg = 'pd.read_csv failed with archive_csv: %s' % archive_csv
                    logger.error('error :: bq_backfill :: %s, err: %s' % (
                        err_msg, err))
                if df is not None:
                    logger.info('bq_backfill :: loaded %s data rows for %s from %s' % (
                        str(len(df)), str(fetch_interval), archive_csv))
                    archive_data_used = True

                    # do_not_send_to_flux = True

        if df is None:
            try:
                df = pandas_gbq.read_gbq(sql, project_id=bq_account_settings['project_id'], credentials=credentials, progress_bar_type=None)
            except Exception as err:
                err_msg = 'pandas_gbq.read_gbq failed with sql: %s' % sql
                logger.error(traceback.format_exc())
                logger.error('error :: bq_backfill :: %s, err: %s' % (
                    err_msg, err))
                fail_job(job_id, work_dict, err_msg)
                failed_job = True
                failed_job_err = str(err_msg)
                break

        # Save the archive data if it exists and has been requested
        # The date_folder must be the date and time it was downloaded not the
        # fetch_interval
        date_folder_ts = int(start_backfill_job)
        date_folder = datetime.fromtimestamp(date_folder_ts, tz=timezone.utc).strftime('%Y%m%d%H%M%S')
        archive_data_path = '%s/vista/bq_archives/%s/%s' % (
            settings.SKYLINE_DIR, vista_bq_account_key, date_folder)
        new_archive_csv = '%s/%s' % (str(archive_data_path), archive_csv_filename)

        if df is not None and not archive_data_used:
                if not os.path.isdir(archive_data_path):
                    try:
                        mkdir_p(archive_data_path)
                    except Exception as err:
                        err_msg = 'failed to create archive_data_path: %s' % archive_data_path
                        logger.error('error :: bq_backfill :: %s, err: %s' % (
                            err_msg, err))
                if not os.path.isfile(new_archive_csv):
                    logger.info('bq_backfill :: archiving data for %s to %s' % (str(fetch_interval), archive_csv))
                    try:
                        df.to_csv(new_archive_csv, index=False)
                    except Exception as err:
                        err_msg = 'df.to_csv failed to create archive_csv: %s' % new_archive_csv
                        logger.error('error :: bq_backfill :: %s, err: %s' % (
                            err_msg, err))
                    if os.path.isfile(new_archive_csv):
                        logger.info('bq_backfill :: archived data for %s to %s' % (str(fetch_interval), new_archive_csv))
                    else:
                        logger.error('error :: bq_backfill :: df.to_csv failed to create archive_csv: %s' % new_archive_csv)

        intervals_fetched.append(fetch_interval)
        logger.info('bq_backfill :: %s rows returned for interval: %s' % (
            str(len(df)), str(fetch_interval)))

        metrics = []
        if len(df) > 0:
            try:
                metrics = bq_df_to_metrics(parent_skyline_app, bq_account_settings, df, backfill=True, zero_fill=zero_fill, return_nan_metrics=False)
            except Exception as err:
                logger.error(traceback.format_exc())
                err_msg = 'bq_df_to_metrics failed for fetch_interval: %s' % str(fetch_interval)
                logger.error('error :: bq_backfill :: %s, err: %s' % (
                    err_msg, err))
                fail_job(job_id, work_dict, err_msg)
                failed_job = True
                failed_job_err = str(err_msg)
                break
        logger.info('bq_backfill :: %s metrics generated for interval: %s' % (
            str(len(metrics)), str(fetch_interval)))

        # Update the unique_metrics
        metrics_list = []
        if metrics:
            metrics_list = [metric_data['metric'] for metric_data in metrics]
            unique_metrics.update(metrics_list)
            timestamp_list = list(set([metric_data['timestamp'] for metric_data in metrics]))
            timestamps_submitted.update(timestamp_list)
        if dry_run:
            work_dict['results'][fetch_interval] = metrics

        # Set any inactive metrics to active again
        metric_ids_to_set_as_active_again = []
        for base_name in metrics_list:
            metric_id = None
            try:
                metric_id = all_base_names_with_ids[base_name]
            except:
                pass
            if metric_id:
                if metric_id in inactive_metric_ids:
                    metric_ids_to_set_as_active_again.append(metric_id)
        if len(metric_ids_to_set_as_active_again):
            logger.info('bq_backfill :: %s metrics to set as active' % str(len(metric_ids_to_set_as_active_again)))
            set_metrics_as_active_count = None
            try:
                set_metrics_as_active_count = set_metric_ids_as_active(parent_skyline_app, metric_ids_to_set_as_active_again)
            except Exception as err:
                logger.error('error :: bq_backfill :: set_metric_ids_as_active failed - %s' % (
                    str(err)))
            logger.info('bq_backfill :: %s metrics set as active' % str(set_metrics_as_active_count))

        logger.info('bq_backfill :: the job now has %s unique metrics, submitting to flux' % (
            str(len(unique_metrics))))

        # Send to flux but only send Graphite MAX_CREATES_PER_MINUTE per minute.
        # This also limits the number of metrics that have to be created in the
        # DB as well and mitigates any thundering herd operations.
        start_flux_metrics_submission = int(time())
        start_batch = 0
        metrics_sent_in_batch = []
        metrics_sent = []
        batch_created_metrics = []
        failed_count = 0
        if metric_format == 'graphite' and metrics:
            # Create batches
            metrics_payloads = []
            for i in range(0, len(metrics), max_creates_per_minute):
                metrics_payload = metrics[i:(i + max_creates_per_minute)]
                metrics_payloads.append(metrics_payload)
            # Send metrics in a controlled manner
            for index, metrics_payload in enumerate(metrics_payloads):
                if dry_run:
                    if not flux_test:
                        sleep(3)
                        continue
                if failed_job:
                    break

                if do_not_send_to_flux:
                    continue

                # Only make x submissions per minute
                if last_submission_timestamp:
                    next_submission = last_submission_timestamp + SECONDS_BETWEEN_SUBMISSION
                else:
                    next_submission = int(time()) + 1
                while next_submission < int(time()):
                    sleep(1)
                if not start_batch:
                    start_batch = int(time())
                if int(time()) >= (start_batch + 59):
                    # Start a new batch
                    start_batch = int(time())
                    metrics_sent_in_batch = []
                    batch_created_metrics = []
                # send
                payload = {
                    "key": flux_api_token,
                    "metrics": metrics_payload,
                }
                if flux_test:
                    payload['test'] = True
                    if index == 0 and DEBUG_LOGGING:
                        logger.debug('debug :: bq_backfill :: flux payload: %s' % str(payload))

                metrics_to_create = [metric_data['metric'] for metric_data in metrics_payload if metric_data['metric'] not in list(metrics_created)]
                unknown_metrics_to_create = []
                if len(known_metrics) > 0:
                    for metric in metrics_to_create:
                        known_metric = len([km for km in known_metrics if metric in km])
                        if not known_metric:
                            unknown_metrics_to_create.append(metric)
                    metrics_to_create = list(unknown_metrics_to_create)

                logger.info('bq_backfill :: %s new metrics to create in batch' % str(len(metrics_to_create)))
                now = int(time())
                if now < (start_batch + 59):
                    # Do not breach Graphite MAX_CREATES_PER_MINUTE
                    if len(batch_created_metrics) >= max_creates_per_minute:
                        sleep_for = (start_batch + 59) - now
                        logger.info('bq_backfill :: sleeping for %s seconds to not breach Graphite MAX_CREATES_PER_MINUTE' % str(sleep_for))
                        sleep(sleep_for)
                        # Start a new batch
                        start_batch = int(time())
                        metrics_sent_in_batch = []
                        batch_created_metrics = []
                # Send
                success = False
                while not success:
                    r = None
                    try:
                        request_body = gzip.compress(json.dumps(payload).encode('utf-8'))
                        r = requests.post(flux_url, data=request_body, headers=headers, timeout=50, verify=verify_ssl)
                    except Exception as err:
                        logger.error('error :: bq_backfill :: failed to POST %s data to flux, err: %s' % (
                            str(fetch_interval), err))
                        failed_count += 1
                    if failed_count == 3:
                        failed_job = True
                        failed_job_err = 'the submission of metrics for %s failed 3 times, backfill job for %s failed' % (
                            str(fetch_interval), vista_bq_account_key)
                        break
                    if r:
                        if r.status_code in [200, 204]:
                            success = True
                            logger.info('bq_backfill :: posted %s metrics to flux for %s' % (
                                str(len(metrics_payload)), fetch_interval))
                        elif r.status_code == 207:
                            logger.warning('warning :: bq_backfill :: flux responded with status_code of 207, some metrics for %s not accepted' % str(fetch_interval))
                            success = True
                            if '207_responses' not in work_dict.keys():
                                work_dict['207_responses'] = {}
                            work_dict['207_responses'][fetch_interval] = r.json()
                        else:
                            logger.warning('warning :: bq_backfill :: flux responded with status_code of %s for %s' % (
                                str(r.status_code), str(fetch_interval)))
                            ##### TEMP
                            failed_job = True
                            logger.error('error :: bq_backfill :: failed to POST data to flux, data: %s' % str(payload))
                            failed_job_err = 'the submission of metrics for %s, backfill job for %s failed' % (
                                str(fetch_interval), vista_bq_account_key)
                            break

                # After send update the lists and sets
                last_submission_timestamp = int(time())
                metrics_sent_in_batch = [metric_data['metric'] for metric_data in metrics_payload]
                for i_metric in metrics_sent_in_batch:
                    metrics_sent.append(i_metric)
                if len(metrics_to_create) > 0:
                    metrics_created.update(metrics_to_create)
                batch_created_metrics = batch_created_metrics + metrics_to_create
                if success:
                    logger.info('bq_backfill :: %s metrics submitted to flux, total new batch_created_metrics: %s, total job metrics_created: %s' % (
                        str(len(metrics_sent_in_batch)), str(len(batch_created_metrics)),
                        str(len(metrics_created))))
                    failed_count = 0

        work_dict['intervals_fetched'] = intervals_fetched
        work_dict['intervals_fetched_count'] = len(intervals_fetched)
        work_dict['total_intervals_to_fetch_count'] = total_intervals_to_fetch
        work_dict['unique_metrics_count'] = len(unique_metrics)
        percent_complete = float(100 * (len(intervals_fetched) / total_intervals_to_fetch))
        if metric_format == 'graphite':
            # Redis also needs to be populated so this is only half the work
            percent_complete = percent_complete / 2
        work_dict['percent_complete'] = percent_complete
        work_dict['elasped_seconds'] = time() - start_backfill_job
        try:
            update_job_status(job_id, str(work_dict))
        except Exception as err:
            logger.error('error :: bq_backfill :: update_job_status failed, job_id: %s, work_dict: %s, err: %s' % (
                job_id, str(work_dict), err))

    logger.info('bq_backfill :: %s BigQuery data metrics submitted to flux in %s seconds' % (
        str(len(unique_metrics)), str(int(time()) - start_bq_to_flux)))

    # Populate Redis - the populate_redis_metric functions handles any sharding 
    do_populate_redis = False
    submit_timestamps = []
    if not dry_run and metric_format == 'graphite':
        do_populate_redis = True
        # Only populate Redis if the data is older than MAX_RESOLUTION
        # and would have been dropped by Horizon when Graphite pickled it on
        # from flux
        max_resolution_timestamp = int(time()) - settings.MAX_RESOLUTION
        submit_timestamps = [int(ts) for ts in sorted(list(timestamps_submitted)) if int(ts) <= max_resolution_timestamp]
        if not submit_timestamps:
            do_populate_redis = False
    if do_populate_redis:
        logger.info('bq_backfill :: sleeping for 30 seconds to allow Graphite to process the data')
        sleep(30)
        start_populate_redis = int(time())
        use_from_timestamp = submit_timestamps[0]
        if replace_redis_timeseries:
            # Populate Redis with the full_duration from Graphite
            use_from_timestamp = until_timestamp - full_duration
        if until_timestamp == use_from_timestamp:
            use_from_timestamp = use_from_timestamp - 1
            until_timestamp = until_timestamp + 1
        logger.info('bq_backfill :: populating Redis with %s BigQuery data metrics with namespace_prefix: %s, from_timestamp: %s, until_timestamp: %s' % (
            str(len(unique_metrics)), str(namespace_prefix),
            str(use_from_timestamp), str(until_timestamp)))
        if added_by == 'vista_bq_fetcher':
            replace_redis_timeseries = False
        for base_name in unique_metrics:
            populated_timeseries = []
            use_base_name = str(base_name)
            if namespace_prefix:
                use_base_name = '%s.%s' % (namespace_prefix, base_name)
            # Surface the timeseries from the local Graphite because if this is
            # in a cluster it is possible that the remote graphite/webapp will
            # not have access to the metric until the remote carbon flushes it
            metric_timeseries = []
            try:
                metric_timeseries = get_graphite_metric(parent_skyline_app, use_base_name, from_timestamp, until_timestamp, 'list', 'object')
            except Exception as err:
                logger.error('error :: bq_backfill :: get_graphite_metric failed, err: %s' % err)
            try:
                populated_timeseries = populate_redis_metric(parent_skyline_app, use_base_name, from_timestamp=use_from_timestamp, until_timestamp=until_timestamp, timeseries=metric_timeseries, replace=replace_redis_timeseries, verify_ssl=verify_ssl)
            except Exception as err:
                logger.error('error :: bq_backfill :: populate_redis_metric failed for %s, job_id: %s, err: %s' % (
                    base_name, job_id, err))
            logger.info('bq_backfill :: populated Redis with %s data points for %s' % (
                str(len(populated_timeseries)), use_base_name))
            redis_populated_metrics.append(use_base_name)
            work_dict['percent_complete'] = (float(100 * (len(redis_populated_metrics) / len(unique_metrics))) / 2) + 50
            work_dict['redis_metrics_populated'] = len(redis_populated_metrics)
            work_dict['elasped_seconds'] = time() - start_backfill_job
            try:
                update_job_status(job_id, str(work_dict))
            except Exception as err:
                logger.error('error :: bq_backfill :: update_job_status failed, job_id: %s, work_dict: %s, err: %s' % (
                    job_id, str(work_dict), err))
        logger.info('bq_backfill :: %s BigQuery data metrics populated to Redis in %s seconds' % (
            str(len(unique_metrics)), str(int(time()) - start_populate_redis)))

    # @added 20240627 - Feature #5372: vista - bq_update
    reprocess_metrics = False
    if len(intervals_to_fetch) == 1 and added_by == 'vista_bq_update':
        today_date_fromat = datetime.fromtimestamp(int(time())).strftime(date_format)
        interval_to_fetch = intervals_to_fetch[0]
        if interval_to_fetch == today_date_fromat:
            reprocess_metrics = True
    if reprocess_metrics:
        # Add a reprocess job to ensure that the newly submitted metrics are
        # processed
        reprocess_url = '%s/reprocess_batch_metrics' % settings.SKYLINE_URL
        user = str(settings.WEBAPP_AUTH_USER)
        password = str(settings.WEBAPP_AUTH_USER_PASSWORD)
        fetch_interval_timestamp = intervals_to_fetch_timestamps[interval_to_fetch]
        update_namespace = bq_account_settings['batch_processing_namespace']
        # Reprocess current data
        post_data = {
            'data': {
                'namespaces': [],
                'metrics': metrics_sent,
                'from_timestamp': fetch_interval_timestamp,
                'until_timestamp': fetch_interval_timestamp,
                'dry_run': False,
                'oneshot': False,
            }
        }
        r = None
        try:
            headers = {"content-type": "application/json"}
            r = requests.post(reprocess_url, auth=(user, password), json=post_data, headers=headers, timeout=5, verify=verify_ssl)
            logger.info('bq_backfill :: reprocess API request for namespace %s for %s returned status_code: %s' % (
                update_namespace, str(fetch_interval_timestamp), str(r.status_code)))
            logger.info('bq_backfill :: reprocess API request returned status_code: %s' % (
                str(r.status_code)))
            if r.status_code != 200:
                logger.error('error :: bq_backfill :: reprocess request did not retrun 200 for %s , reason: %s' % (
                    update_namespace, str(r.reason)))
        except Exception as err:
            logger.error('error :: bq_backfill :: reprocess request failed for %s on %s, err: %s' % (
                update_namespace, str(reprocess_url), err))

    work_dict['status'] = 'complete'
    if failed_job:
        work_dict['status'] = 'failed'
        logger.error('error :: bq_backfill :: failed job, err: %s' % str(failed_job_err))
        fail_job(job_id, work_dict, str(failed_job_err))

    work_dict['percent_complete'] = 100.0
    try:
        update_job_status(job_id, str(work_dict))
    except Exception as err:
        logger.error('error :: bq_backfill :: update_job_status failed, job_id: %s, work_dict: %s, err: %s' % (
            job_id, str(work_dict), err))

    # @added 20240609 
    # sleep and then run updates?
    # updates = bq_update()

    try:
        redis_conn_decoded.delete('vista.bq_backfill.running')
    except Exception as err:
        logger.error('error :: bq_backfill :: failed to delete on vista.bq_backfill.running from Redis, err: %s' % err)
