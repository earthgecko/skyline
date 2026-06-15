"""
bq_update.py
"""
import copy
import logging
import os
import sys
import traceback

from ast import literal_eval
from contextlib import nullcontext
from datetime import datetime, timezone
from time import time, sleep

import pandas as pd
import numpy as np
import requests

# For version pinning consider implementing a skyline-bq-py31013 environment
# and adding that to the path so that imports come from there rather than from
# the skyline-py31013 environment which has different versions of some libraries
# e.g. googleapis-common-protos

sys.path.insert(0, '/opt/skyline/github/skyline/skyline')
import settings
from skyline_functions import get_redis_conn_decoded, get_graphite_metric, mkdir_p
from functions.cluster.is_shard_metric import is_shard_metric
from functions.database.queries.get_all_inactive_db_metric_names import get_all_inactive_db_metric_names
from functions.database.queries.query_anomalies import get_anomalies_for_period
from functions.database.queries.set_metric_ids_as_active import set_metric_ids_as_active
from functions.database.queries.update_anomaly import update_anomaly
from functions.graphite.update_graphite_metric import update_graphite_metric
from functions.metrics.get_metric_id_from_base_name import get_metric_id_from_base_name
from functions.numpy.percent_different import get_percent_different
from functions.redis.populate_redis_metric import populate_redis_metric
from functions.settings.get_bq_accounts_settings import get_bq_accounts_settings
from functions.settings.get_external_settings import get_external_settings

from functions.vista.create_bq_archive import create_bq_archive

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
    import pandas_gbq
else:
    bq_df_to_metrics = nullcontext()
    get_bq_credentials = nullcontext()
    get_bq_credentials = nullcontext()
    pandas_gbq = nullcontext()

try:
    HORIZON_SHARDS = copy.deepcopy(settings.HORIZON_SHARDS)
except:
    HORIZON_SHARDS = {}

parent_skyline_app = 'vista'
child_skyline_app = 'bq_update'
skyline_app_logger = '%sLog' % parent_skyline_app
logger = logging.getLogger(skyline_app_logger)
skyline_app = '%s' % parent_skyline_app
this_host = str(os.uname()[1])


# @added 20240617 - Feature #5372: vista - bq_update
#                   Feature #5352: vista - bigquery
def bq_update(bq_update_job_id):
    """
    This function is used to get data from BigQuery and compare it to the last
    data fetched from BigQuery for the period (if an archive is available, if
    not an archive is created using data from Redis and/or Graphite).  The
    archive/current values are compared to the values for the timestamp from the
    latest BigQuery data for the period and if the BigQuery value has changed,
    the values are updated Graphite and Redis.  The metrics that have changed
    are then reprocessed up to the timestamp of the changed data and if no
    anomaly is detected for the updated timestamp and previously an anomaly WAS
    detected for the timestamp the existing anomaly is updated with the new
    value and the previous value and new value for the anomaly is recorded in
    the database anomalies_updated table.
    """

    start_bq_update_job = time()
    try:
        redis_conn_decoded = get_redis_conn_decoded(skyline_app)
    except Exception as err:
        logger.error('error :: bq_update - redis connection failed, err: %s' % err)

    def remove_key(key):
        logger.info('bq_update :: removing %s from vista.bq_update.work' % key)
        try:
            redis_conn_decoded.hdel('vista.bq_update.work', key)
        except Exception as err:
            logger.error('error :: bq_update :: failed to hdel %s from vista.bq_update.work, err: %s' % (
                key, err))

    def fail_job(key, data, err_msg):
        remove_key(key)

    vista_bq_update_work = {}
    try:
        vista_bq_update_work = redis_conn_decoded.hgetall('vista.bq_update.work')
    except Exception as err:
        logger.error('error :: bq_update :: failed to hgetall on vista.bq_update.work, err: %s' % err)
        vista_bq_update_work = {}

    vista_bq_account_key = None
    from_timestamp = None
    until_timestamp = None
    interval = None
    replace_query_string = None
    date_format = None
    job_id = None
    work_dict = {}
    # Get the job from the work queue
    for job_id, work_dict_str in vista_bq_update_work.items():
        if job_id != bq_update_job_id:
            continue
        try:
            work_dict = literal_eval(work_dict_str)
        except Exception as err:
            err_msg = 'bq_update :: failed to literal_eval work_dict'
            logger.error('error :: bq_update :: %s: %s, for job_id: %s , err: %s' % (
                err_msg, str(work_dict_str), str(job_id), err))
            fail_job(job_id, work_dict_str, err_msg)
            work_dict = {}
            break
        try:
            vista_bq_account_key = work_dict['vista_bq_account_key']
        except Exception as err:
            err_msg = 'failed to determine vista_bq_account_key from work_dict'
            logger.error('error :: bq_update :: %s: %s, for job_id: %s , err: %s' % (
                err_msg, str(work_dict_str), str(job_id), err))
            fail_job(job_id, work_dict, err_msg)
            work_dict = {}
            break
        from_timestamp = None
        try:
            from_timestamp = work_dict['from_timestamp']
        except:
            from_timestamp = None
        until_timestamp = None
        try:
            until_timestamp = work_dict['until_timestamp']
        except:
            until_timestamp = None
        try:
            interval = work_dict['schedule_interval']
        except Exception as err:
            err_msg = 'failed to determine schedule_interval from work_dict'
            logger.error('error :: bq_update :: %s:%s, err: %s' % (
                err_msg, str(work_dict_str), err))
            fail_job(job_id, work_dict, err_msg)
            work_dict = {}
            break
        try:
            replace_query_string = work_dict['update_replace_query']
        except Exception as err:
            err_msg = 'failed to determine update_replace_query from work_dict'
            logger.error('error :: bq_update :: %s: %s, for job_id: %s , err: %s' % (
                err_msg, str(work_dict_str), str(job_id), err))
            fail_job(job_id, work_dict, err_msg)
            work_dict = {}
            break
        try:
            date_format = work_dict['date_field_format']
        except Exception as err:
            err_msg = 'failed to determine date_field_format from work_dict'
            logger.error('error :: bq_update :: %s: %s, for job_id: %s , err: %s' % (
                err_msg, str(work_dict_str), str(job_id), err))
            fail_job(job_id, work_dict, err_msg)
            work_dict = {}
            break
        verify_ssl = True
        try:
            verify_ssl = work_dict['verify_ssl']
        except:
            verify_ssl = True
        break

    if len(work_dict) == 0:
        err_msg = 'failed to construct work_dict'
        logger.error('error :: bq_update :: %s: %s, for job_id: %s , err: %s' % (
            err_msg, str(work_dict_str), str(job_id), err_msg))
        fail_job(job_id, work_dict, err_msg)
        return

    started = int(time())
    today_date_fromat = datetime.fromtimestamp(int(started)).strftime(date_format)

    work_dict['started'] = started
    job_key_dict = {'started': started, 'job_id': job_id, 'work_dict': work_dict}
    try:
        redis_conn_decoded.setex('vista.bq_update.running', 7200, str(job_key_dict))
        # @modified 20241106 - Task #5526: Build v5.0.0 and upgrade deps
        # bandit - B608:hardcoded_sql_expressions
        logger.info('bq_update set vista.bq_update.running Redis key with %s' % str(job_key_dict))  # nosec B608
    except Exception as err:
        logger.error('error :: bq_update :: failed to set on vista.bq_update.running from Redis, err: %s' % err)  # nosec B608

    """
    # Example work_dict that is submitted via /bq_update by the webapp, direct
    # or by vista_bq_update
    work_data = {
        'job_id': '1718967838.customer.account_activity',
        'flux_api_token': 'xxxx',
        'oauth_credentials': {
            'token': 'xxxx', 'refresh_token': 'xxxx',
            'token_uri': 'https://oauth2.googleapis.com/token',
            'client_id': 'xxx.apps.googleusercontent.com',
            'client_secret': 'xxx'
        },
        'oauth_credentials_file': '', 'service_credentials': {},
        'service_credentials_file': '', 'project_id': 'account-data-prod',
        'query': 'SELECT * FROM `account-data-prod.skyline.account_activity` WHERE date = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)',
        'metric_format': 'graphite', 'metric_prefix': 'customer.account_activity',
        'date_field': 'date', 'date_field_format': '%Y-%m-%d',
        'primary_dimension': 'acc_id', 'metric_dimensions': ['requests', 'clickthru', 'signups'],
        'schedule': {'minute': 0, 'hour': 1, 'day_of_month': '*', 'month': '*', 'day_of_week': '*'},
        'schedule_interval': 86400, 'full_duration': 31536000,  'stale_period': 604800,
        'resolution': 86400, 'batch_processing_namespace': 'customer.account_activity',
        'non_derivative_namespaces': ['customer.account_activity'],
        'zero_fill_analysis': True,
        'update_data_period': 2592000,
        'update_data_schedule': {'minute': 15, 'hour': [0,6,12,18], 'day_of_month': '*', 'month': '*', 'day_of_week': '*'},
        'update_replace_query': 'DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)',
        'from_timestamp': 1717027200, 'until_timestamp': 1717027200,
        'verify_ssl': False, 'added_by': 'webapp',
        'vista_bq_account_key': 'customer.account_activity'}
    }
    """

    # Remove the work
    remove_key(job_id)

    # Update the job status
    work_dict['status'] = 'processing'
    intervals_to_fetch_timestamps = {}
    resolution = work_dict['resolution']
    if not until_timestamp:
        until_timestamp = int(time()) // resolution * resolution
    until_timestamp = until_timestamp  // resolution * resolution
    if not from_timestamp:
        from_timestamp = until_timestamp - work_dict['update_data_period']
    from_timestamp = from_timestamp  // resolution * resolution
    intervals_to_fetch = []
    end_timestamp = int(until_timestamp) + 1
    ctime = int(from_timestamp)
    while ctime < end_timestamp:
        try:
            interval_date_fromat = datetime.fromtimestamp(int(ctime)).strftime(date_format)
            intervals_to_fetch.append(interval_date_fromat)
            intervals_to_fetch_timestamps[interval_date_fromat] = int(ctime)
        except Exception as err:
            logger.error('error :: bq_update :: failed to convert %s to date_format, err: %s' % (
                str(ctime), err))
        ctime = ctime + interval
        if ctime > end_timestamp:
            break
    work_dict['intervals_to_fetch'] = intervals_to_fetch
    current_period_timestamp = int(time()) // resolution * resolution

    vista_bq_accounts = {}
    try:
        vista_bq_accounts = get_bq_accounts_settings(skyline_app)
    except Exception as err:
        logger.error('error :: bq_update :: get_bq_accounts_settings failed, err: %s' % err)

    bq_account_settings = {}
    try:
        bq_account_settings = vista_bq_accounts[vista_bq_account_key]
    except Exception as err:
        logger.error('error :: bq_update :: failed to determine settings for %s, err: %s' % (
            vista_bq_account_key, err))

    if not bq_account_settings:
        err_msg = 'failed to determine settings for %s' % vista_bq_account_key
        logger.error('error :: bq_update :: %s' % err_msg)
        fail_job(job_id, work_dict, err_msg)
        try:
            redis_conn_decoded.delete('vista.bq_update.running')
        except Exception as err:
            logger.error('error :: bq_update :: failed to delete on vista.bq_update.running from Redis, err: %s' % err)
        return

    query = None
    try:
        query = bq_account_settings['query']
    except Exception as err:
        err_msg = 'failed to determine query from settings for %s' % vista_bq_account_key
        logger.error('error :: bq_update :: %s, err: %s' % (
            err_msg, err))
        fail_job(job_id, work_dict, err_msg)
        try:
            redis_conn_decoded.delete('vista.bq_update.running')
        except Exception as err:
            logger.error('error :: bq_update :: failed to delete on vista.bq_update.running from Redis, err: %s' % err)
        return

    metric_format = 'graphite'
    try:
        metric_format = bq_account_settings['metric_format']
    except Exception as err:
        logger.error('error :: bq_update :: failed to ddetermine metric_format from bq_account_settings defaulting to Graphite, err: %s' % err)
        metric_format = 'graphite'    

    flux_api_token = None
    try:
        flux_api_token = bq_account_settings['flux_api_token']
    except Exception as err:
        err_msg = 'failed to determine flux_api_token from settings for %s' % vista_bq_account_key
        logger.error('error :: bq_update :: %s, err: %s' % (
            err_msg, err))
        fail_job(job_id, work_dict, err_msg)
        try:
            redis_conn_decoded.delete('vista.bq_update.running')
        except Exception as err:
            logger.error('error :: bq_update :: failed to delete on vista.bq_update.running from Redis, err: %s' % err)
        return

    try:
        full_duration = bq_account_settings['full_duration']
    except Exception as err:
        err_msg = 'failed to determine full_duration from settings for %s' % vista_bq_account_key
        logger.error('error :: bq_update :: %s, err: %s' % (
            err_msg, err))
        fail_job(job_id, work_dict, err_msg)
        try:
            redis_conn_decoded.delete('vista.bq_update.running')
        except Exception as err:
            logger.error('error :: bq_update :: failed to delete on vista.bq_update.running from Redis, err: %s' % err)
        return

    try:
        metric_prefix = bq_account_settings['metric_prefix']
    except Exception as err:
        err_msg = 'failed to determine metric_prefix from settings for %s' % vista_bq_account_key
        logger.error('error :: bq_update :: %s, err: %s' % (
            err_msg, err))
        fail_job(job_id, work_dict, err_msg)
        try:
            redis_conn_decoded.delete('vista.bq_update.running')
        except Exception as err:
            logger.error('error :: bq_update :: failed to delete on vista.bq_update.running from Redis, err: %s' % err)
        return

    # @added 20240725 - Feature #5372: vista - bq_update
    # Handle BQ api fail and try use another node. BigQuery can deny by IP
    # it appears
    use_remote = False
    try:
        use_remote = bq_account_settings['use_remote']
    except Exception as err:
        use_remote = False

    flux_key_namespace_prefixes = {
        settings.FLUX_SELF_API_KEY: None
    }
    for flux_token in settings.FLUX_API_KEYS:
        flux_key_namespace_prefixes[flux_token] = settings.FLUX_API_KEYS[flux_token]
    skyline_external_settings = {}
    try:
        skyline_external_settings = get_external_settings(parent_skyline_app, None, True)
    except Exception as err:
        logger.error('error :: bq_update :: get_external_settings failed, err: %s' % (
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
            logger.error('error :: bq_update :: failed to determine flux namespace_prefix from flux_api_token, err: %s' % (
                err))
            namespace_prefix = None

    # Resources to determine what metric ids need to be set to active again
    all_db_metric_ids_with_names = {}
    try:
        all_db_metric_ids_with_names = redis_conn_decoded.hgetall('metrics_manager.all_db_metric_ids_with_names')
    except Exception as err:
        logger.error('error :: bq_update :: failed to determine flux namespace_prefix from flux_api_token, err: %s' % (
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
        inactive_metric_names, inactive_metric_names_with_id = get_all_inactive_db_metric_names(skyline_app, with_ids=True)
    except Exception as err:
        logger.error('error :: bq_update :: get_all_inactive_db_metric_names failed, err: %s' % (
            err))
    inactive_metric_ids = set(list(inactive_metric_names_with_id.values()))
    metric_ids_to_set_as_active_again = []

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
                            skyline_app, credentials=use_credentials,
                            project_id=project_id,
                            oauth_credentials=oauth_credentials,
                            oauth_credentials_file=oauth_credentials_file,
                            service_credentials=service_credentials,
                            service_credentials_file=service_credentials_file)
    except Exception as err:
        logger.error('error :: bq_update :: get_bq_credentials failed, err: %s' % (
            err))
    if not credentials:
        logger.error('error :: bq_update :: no credentials retrieved for %s' % vista_bq_account_key)
        err_msg = 'failed to get credentials for %s' % vista_bq_account_key
        logger.error('error :: bq_update :: %s' % err_msg)
        fail_job(job_id, work_dict, err_msg)
        try:
            redis_conn_decoded.delete('vista.bq_update.running')
        except Exception as err:
            logger.error('error :: bq_update :: failed to delete on vista.bq_update.running from Redis, err: %s' % err)
        return
    else:
        use_credentials = credentials

    failed_job = False
    failed_job_err = None
    total_intervals_to_fetch = len(intervals_to_fetch)
    logger.info('bq_update :: %s intervals_to_fetch for %s' % (
        str(total_intervals_to_fetch), vista_bq_account_key))

    until_graphite_timestamp = int(started // interval * interval)
    from_graphite_timestamp = until_graphite_timestamp - full_duration

    reprocess_url = '%s/reprocess_batch_metrics' % settings.SKYLINE_URL
    user = str(settings.WEBAPP_AUTH_USER)
    password = str(settings.WEBAPP_AUTH_USER_PASSWORD)

    # if no archive and fetch_interval is today a backfill job will be added at
    # the end of the run
    add_backfill_job = None

    intervals_fetched = []
    for fetch_interval in intervals_to_fetch:
        if failed_job:
            break
        start_fetch_interval = int(time())

        metric_ids_to_set_as_active_again = []

        fetch_interval_timestamp = intervals_to_fetch_timestamps[fetch_interval]

        # Search the path for the latest archive for the 
        archive_data_search_path = '%s/vista/bq_archives/%s' % (
            settings.SKYLINE_DIR, vista_bq_account_key)
        bq_archives = []
        if os.path.isdir(archive_data_search_path):
            for dir_path, folders, files in os.walk(archive_data_search_path):
                try:
                    if files:
                        for i in files:
                            path_and_file = '%s/%s' % (dir_path, i)
                            if i.endswith('.csv'):
                                bq_archives.append(path_and_file)
                except Exception as err:
                    logger.error('error :: bq_update :: os.walk failed on %s, err: %s' % (
                        str(archive_data_search_path), err))
        # Sort latest to oldest
        bq_archives = sorted(bq_archives, reverse=True)
        logger.info('bq_update :: %s bq_archives found' % (str(len(bq_archives))))
        archive_csv_filename = '%s.%s.csv' % (vista_bq_account_key, str(fetch_interval))
        archive_csv = None
        # Find latest archive if one exists
        for i in bq_archives:
            if archive_csv_filename in i:
                archive_csv = str(i)
                break

        # Only create the archive if the fetch_interval is not today's date
        create_archive = True
        if fetch_interval == today_date_fromat:
            if not archive_csv:
                create_archive = False
                add_backfill_job = str(fetch_interval)
                logger.info('bq_update :: no archive csv found for %s, which is the current period so not creating archive with Redis and Graphite data' % fetch_interval)
                logger.info('bq_update :: no archive csv found for current period %s so a backfill job will be added' % fetch_interval)

        if not archive_csv and create_archive:
            # GET THE DATA FROM REDIS (LOCAL) AND GRAPHITE (REMOTE) IF AN ARCHIVE DOES NOT EXIST ... AND MAKE THE ARCHIVE?
            # Use create_bq_archive function
            logger.info('bq_update :: no archive csv found, creating with Redis and Graphite data')
            archive_file = None
            try:
                metrics_data, archive_file = create_bq_archive(skyline_app, vista_bq_account_key, fetch_interval_timestamp)
                try:
                    del metrics_data
                except:
                    pass
                if os.path.isfile(archive_file):
                    logger.info('bq_update :: created archive csv - %s' % archive_file)
                    archive_csv = str(archive_file)
            except Exception as err:
                logger.error('error :: bq_update :: create_bq_archive failed for %s, err: %s' % (
                    str(fetch_interval_timestamp), err))

        if not archive_csv and add_backfill_job:
            logger.info('bq_update :: no archive_csv was found or created for %s but backfill job will be added' % (
                str(fetch_interval)))
            continue

        if not archive_csv:
            logger.error('error :: bq_update :: no archive_csv was found or created for %s, cannot proceed' % (
                str(fetch_interval)))
            continue

        previous_df = None
        if archive_csv:
            if os.path.isfile(archive_csv):
                logger.info('bq_update :: loading data for %s from latest archive data file %s' % (str(fetch_interval), archive_csv))
                try:
                    previous_df = pd.read_csv(archive_csv)
                except Exception as err:
                    err_msg = 'pd.read_csv failed with archive_csv: %s' % archive_csv
                    logger.error('error :: bq_update :: %s, err: %s' % (
                        err_msg, err))
                if previous_df is not None:
                    logger.info('bq_update :: loaded %s data rows for %s from %s' % (
                        str(len(previous_df)), str(fetch_interval), archive_csv))

        if previous_df is None:
            logger.error('error :: bq_update :: failed to load any data rows for %s from archive' % (
                str(fetch_interval)))
            continue

        sql = str(query)
        try:
            unquoted_replace_string = str(fetch_interval)
            replace_string = "'%s'" % unquoted_replace_string
            sql = query.replace(replace_query_string, replace_string)
        except Exception as err:
            err_msg = 'failed to generate sql with replace_query_string and %s' % str(fetch_interval)
            logger.error('error :: bq_update :: %s, err: %s' % (
                err_msg, err))
            fail_job(job_id, work_dict, err_msg)
            failed_job = True
            failed_job_err = str(err_msg)
            break

        logger.info('bq_update :: fetching current data for %s with sql: %s' % (vista_bq_account_key, sql))

        primary_dimension = bq_account_settings['primary_dimension']
        metric_dimensions = bq_account_settings['metric_dimensions']

        # Just to ensure the credentials are refreshed if they have expired
        # during the loop
        if use_credentials:
            try:
                credentials = get_bq_credentials(
                                    skyline_app, credentials=use_credentials,
                                    project_id=project_id,
                                    oauth_credentials=oauth_credentials,
                                    oauth_credentials_file=oauth_credentials_file,
                                    service_credentials=service_credentials,
                                    service_credentials_file=service_credentials_file)
            except Exception as err:
                logger.error('error :: bq_update :: get_bq_credentials failed, err: %s' % (
                    err))
            if credentials:
                use_credentials = credentials

        # @added 20240725 - Feature #5372: vista - bq_update
        # Handle BQ api fail and try use another node. BigQuery can deny by IP
        # it appears
        pandas_gbq_error = False
        configuration = {'jobTimeoutMs': 120000, 'timeoutMs': 120000}

        df = None
        if df is None:
            try:
                # @modified 20240725 - Feature #5372: vista - bq_update
                # Added configuration
                # @modified 20240725 - Feature #5372: vista - bq_update
                # Only use pandas_gbq if use_remote has not be set
                if not use_remote:
                    df = pandas_gbq.read_gbq(sql, project_id=bq_account_settings['project_id'], credentials=credentials, progress_bar_type=None, configuration=configuration)
                else:
                    df = None
            except Exception as err:
                err_msg = 'pandas_gbq.read_gbq failed with sql: %s' % sql
                logger.error(traceback.format_exc())
                logger.error('error :: bq_update :: %s, err: %s' % (
                    err_msg, err))
                # @modified 20240725 - Feature #5372: vista - bq_update
                # Handle BQ api fail and try use another node. BigQuery can deny by IP
                # it appears
                #fail_job(job_id, work_dict, err_msg)
                #failed_job = True                
                #break
                pandas_gbq_error = True
                if HORIZON_SHARDS:
                    use_remote = True

        tmp_archive_csv = None
        try_remote = False
        if pandas_gbq_error and HORIZON_SHARDS:
            try_remote = True
        if use_remote:
            try_remote = True
        if try_remote:
            archive_csv_url = None
            logger.info('bq_update :: requesting bq_query from other cluster nodes')
            for remote_url, remote_user, remote_password, shostname in settings.REMOTE_SKYLINE_INSTANCES:
                if shostname == this_host:
                    continue
                if archive_csv_url:
                    break
                try:
                    # Although a POST request query parameters are added for tracking
                    # and debugging in the log
                    shard_url = '%s/bq_query' % remote_url
                except Exception as err:
                    logger.error('error :: bq_update :: failed to construct the shard_url for %s, err: %s' % (
                        shostname, err))
                data_dict = {
                    'data': {
                    'vista_bq_account_key': vista_bq_account_key,
                    'sql': sql,
                    'host': this_host,
                    'cluster_data': False,
                    'cluster_call': False,
                    }
                }
                logger.info('bq_update :: requesting %s with data_dict: %s' % (shard_url, str(data_dict)))
                r = None
                try:
                    headers = {"content-type": "application/json"}
                    r = requests.post(shard_url, auth=(remote_user, remote_password), json=data_dict, headers=headers, timeout=125, verify=settings.VERIFY_SSL)
                    logger.info('bq_update :: %s returned status_code: %s' % (
                        shard_url, str(r.status_code)))
                except Exception as err:
                    logger.error('error :: bq_update :: request failed on %s, err: %s' % (
                        str(shard_url), err))
                if r:
                    try:
                        response_json = r.json()
                        archive_csv_url = response_json['data']['archive_csv_url']
                        tmp_archive_csv = response_json['data']['archive_csv']
                        logger.info('bq_update :: %s response: %s' % (
                            shostname, str(response_json)))
                    except Exception as err:
                        logger.error('error :: bq_update :: failed to determine archive_csv_url from %s response, err: %s' % (
                            shostname, err))
                if archive_csv_url:
                    break
            if archive_csv_url:
                r = None
                try:
                    r = requests.get(archive_csv_url, auth=(remote_user, remote_password), timeout=125, verify=settings.VERIFY_SSL)
                    logger.info('bq_update :: %s returned status_code: %s' % (
                        shostname, str(r.status_code)))
                except Exception as err:
                    logger.error('error :: bq_update :: request failed on %s, err: %s' % (
                        str(archive_csv_url), err))
                if r:
                    # Check if the request was successful (status code 200)
                    if r.status_code == 200:
                        # Save the content of the response to a local CSV file
                        with open(tmp_archive_csv, 'wb') as fh:
                            fh.write(r.content)
                        logger.info('bq_update :: saved %s' % (
                            tmp_archive_csv))
                    else:
                        logger.error('error :: bq_update :: failed to download and save %s' % (
                            str(tmp_archive_csv)))
        if tmp_archive_csv:
            try:
                df = pd.read_csv(tmp_archive_csv)
                logger.info('bq_update :: read %s rows from %s' % (
                    str(len(df)), tmp_archive_csv))
            except Exception as err:
                logger.error('error :: bq_update :: failed to load %s, err: %s' % (
                    str(tmp_archive_csv), err))

        if df is None:
            err_msg = 'no data fetched'
            fail_job(job_id, work_dict, err_msg)
            failed_job = True                
            break

        intervals_fetched.append(fetch_interval)
        logger.info('bq_update :: %s rows returned for interval: %s' % (
            str(len(df)), str(fetch_interval)))

        previous_data = {}
        for index, data in previous_df.to_dict('index').items():
            try:
                acc_id = data[primary_dimension]
            except KeyError:
                continue
            previous_data[acc_id] = {}
            for metric_dimension in metric_dimensions:
                try:
                    previous_data[acc_id][metric_dimension] = data[metric_dimension]
                except KeyError:
                    continue
        current_data = {}
        for index, data in df.to_dict('index').items():
            try:
                acc_id = data[primary_dimension]
            except KeyError:
                continue
            current_data[acc_id] = {}
            for metric_dimension in metric_dimensions:
                try:
                    current_data[acc_id][metric_dimension] = data[metric_dimension]
                except KeyError:
                    continue

        updates = {}
        big_changes = {}
        d = fetch_interval
        different = {}
        new_data = {}
        if d not in big_changes.keys():
            big_changes[fetch_interval] = {}
        for acc_id, cdata in current_data.items():
            try:
                odata = {}
                try:
                    odata = previous_data[acc_id]
                except:
                    odata = {}
                if len(odata) == 0:
                    new_data[acc_id] = {fetch_interval_timestamp: {}}
                    for key in cdata.keys():
                        if str(cdata[key]) == 'nan':
                            continue
                        metric = '%s.%s.%s' % (metric_prefix, acc_id, str(key))
                        base_name = '%s.%s' % (namespace_prefix, metric)
                        new_data[acc_id][fetch_interval_timestamp][metric] = cdata[key]
                        metric_id = None
                        try:
                            metric_id = all_base_names_with_ids[base_name]
                        except:
                            pass
                        if metric_id:
                            if metric_id in inactive_metric_ids:
                                metric_ids_to_set_as_active_again.append(metric_id)
                    continue
                if str(cdata) != str(odata):
                    different[acc_id] = {fetch_interval_timestamp: {}}
                    for key, value in cdata.items():
                        if str(value) != str(odata[key]):
                            if isinstance(value, float):
                                if isinstance(odata[key], float):
                                    close = np.allclose([value], [odata[key]])
                                    if close:
                                        continue
                            if str(value) == 'nan':
                                continue

                            metric = '%s.%s.%s' % (metric_prefix, acc_id, str(key))
                            base_name = '%s.%s' % (namespace_prefix, metric)
                            metric_id = None
                            try:
                                metric_id = all_base_names_with_ids[base_name]
                            except:
                                pass
                            if metric_id:
                                if metric_id in inactive_metric_ids:
                                    metric_ids_to_set_as_active_again.append(metric_id)

                            different[acc_id][fetch_interval_timestamp][metric] = {}
                            different[acc_id][fetch_interval_timestamp][metric]['current'] = value
                            different[acc_id][fetch_interval_timestamp][metric]['previous'] = odata[key]

                            if isinstance(value, float) and isinstance(odata[key], float):
                                percent_different = None
                                if odata[key] == 0 and str(value) != 'nan':
                                    percent_different = 100
                                if str(value) == 'nan' and odata[key] != 0:
                                    percent_different = 100
                                if str(value) == 'nan' and odata[key] == 0:
                                    percent_different = 0
                                if value == 0 and str(odata[key]) == 'nan':
                                    percent_different = 0
                                if value == 0 and str(odata[key]) != 'nan':
                                    percent_different = 100
                                if percent_different is None:
                                    percent_different = get_percent_different(value, odata[key], True)
                                    # print('get_percent_different', value, odata[key])
                                different[acc_id][fetch_interval_timestamp][metric]['percent_different'] = percent_different
                                if percent_different:
                                    if percent_different > 15:
                                        if acc_id not in big_changes[d].keys():
                                            big_changes[d][acc_id] = {fetch_interval_timestamp: {}}
                                        big_changes[d][acc_id][fetch_interval_timestamp][metric] = {'current': value, 'previous':  odata[key], '%diff': percent_different}
                    if len(different[acc_id][fetch_interval_timestamp]) == 0 and len(different[acc_id][fetch_interval_timestamp]) == 0:
                        del different[acc_id][fetch_interval_timestamp]
            except Exception as err:
                logger.error('error :: bq_update :: failed to determine differences between archive and current data for %s, err: %s' % (
                    str(acc_id), err))

        for acc_id in list(different.keys()):
            if len(different[acc_id]) == 0:
                del different[acc_id]

        significant_changes = len(big_changes[d])
        if not significant_changes:
            del big_changes[d]
        logger.info('bq_update :: %s - new: %s, updated: %s, significant_changes: %s' % (
            fetch_interval, str(len(new_data)), str(len(different)),
            str(significant_changes)))
        if len(new_data) > 0:
            updates[fetch_interval] = {'new': new_data}
        if len(different) > 0:
            try:
                updates[fetch_interval]['updates'] = different
            except:
                updates[fetch_interval] = {'updates': different}

        if len(metric_ids_to_set_as_active_again):
            logger.info('bq_update :: %s metrics to set as active' % str(len(metric_ids_to_set_as_active_again)))  # nosec B608
            set_metrics_as_active_count = None
            try:
                set_metrics_as_active_count = set_metric_ids_as_active(skyline_app, metric_ids_to_set_as_active_again)
            except Exception as err:
                logger.error('error :: bq_update :: set_metric_ids_as_active failed - %s' % (
                    str(err)))  # nosec B608
            logger.info('bq_update :: %s metrics set as active' % str(set_metrics_as_active_count))  # nosec B608

        # Do all Graphite updates first and then sleep once to allow Graphite
        # write and relay to happen, rather than sleeping per metric
        for acc_id in new_data.keys():
            for fetch_interval_timestamp in new_data[acc_id].keys():
                for metric in new_data[acc_id][fetch_interval_timestamp]:
                    base_name = '%s.%s' % (namespace_prefix, metric)
                    value = new_data[acc_id][fetch_interval_timestamp][metric]
                    # Update Graphite via the relay line protocol port
                    graphite_updated = False
                    try:
                        graphite_updated = update_graphite_metric(skyline_app, base_name, fetch_interval_timestamp, value)
                    except Exception as err:
                        logger.error('error :: bq_update :: update_graphite_metric failed for %s, timestamp: %s, value: %s, err: %s' % (
                            base_name, str(fetch_interval_timestamp),
                            str(value), err))
                    logger.info('bq_update :: new timestamp: %s and value: %s for %s,  graphite_updated: %s' % (
                        str(fetch_interval_timestamp), str(value), base_name,
                        str(graphite_updated)))
                    if graphite_updated:
                        # As to not swamp Graphite
                        sleep(0.05)

        for acc_id in list(different.keys()):
            for fetch_interval_timestamp in different[acc_id].keys():
                for metric in different[acc_id][fetch_interval_timestamp]:
                    base_name = '%s.%s' % (namespace_prefix, metric)
                    value = different[acc_id][fetch_interval_timestamp][metric]['current']
                    previous_value = different[acc_id][fetch_interval_timestamp][metric]['previous']
                    # Update Graphite via the relay line protocol port
                    graphite_updated = False
                    try:
                        graphite_updated = update_graphite_metric(skyline_app, base_name, fetch_interval_timestamp, value)
                    except Exception as err:
                        logger.error('error :: bq_update :: update_graphite_metric failed for %s, timestamp: %s, value: %s, err: %s' % (
                            base_name, str(fetch_interval_timestamp),
                            str(value), err))
                    logger.info('bq_update :: timestamp: %s, value: %s (previous value: %s) for %s, graphite_updated: %s' % (
                        str(fetch_interval_timestamp), str(value), str(previous_value),
                        base_name, str(graphite_updated)))
                    if graphite_updated:
                        # As to not swamp Graphite
                        sleep(0.05)

        # sleep for 5 to 60 seconds to allow Graphite write and the relay to
        # happen if there are many updates
        sleep_for = 0
        if len(new_data) > 0 or len(different) > 0:
            sleep_for = 5
        if len(new_data) > 10 or len(different) > 10:
            sleep_for = 10
        if len(new_data) > 30 or len(different) > 30:
            sleep_for = 20
        if len(new_data) > 100 or len(different) > 100:
            sleep_for = 60
        if sleep_for > 1:
            # @added 20240703 - Allow for more update checks to be scheduled
            # but only run one per hour
            last_bq_update_key = 'vista.bq_update.%s.last_update' % vista_bq_account_key
            try:
                redis_conn_decoded.setex(last_bq_update_key, 3600, int(time()))
                logger.info('bq_update :: set %s Redis key' % str(last_bq_update_key))  # nosec B608
            except Exception as err:
                logger.error('error :: bq_fetcher :: failed to set %s, err: %s' % (
                    str(last_bq_update_key), err))

            logger.info('bq_update :: sleeping for %ss to allow Graphite to write and replicate' % str(sleep_for))
            sleep(sleep_for)

        for acc_id in new_data.keys():
            for fetch_interval_timestamp in new_data[acc_id].keys():
                for metric in new_data[acc_id][fetch_interval_timestamp]:
                    base_name = '%s.%s' % (namespace_prefix, metric)
                    value = new_data[acc_id][fetch_interval_timestamp][metric]

                    # Do not reprocess if the new data point has a timestamp
                    # for the current period and will be analysed normally via
                    # analyzer_batch because it is a new data point
                    if int(fetch_interval_timestamp) == current_period_timestamp:
                        continue

                    # Repopulate Redis
                    until_graphite_timestamp = int(started // interval * interval)
                    from_graphite_timestamp = until_graphite_timestamp - full_duration
                    # Surface the timeseries from the local Graphite because if this is
                    # in a cluster it is possible that the remote graphite/webapp will
                    # not have access to the metric until the remote carbon flushes it
                    metric_timeseries = []
                    try:
                        metric_timeseries = get_graphite_metric(skyline_app, base_name, from_graphite_timestamp, until_graphite_timestamp, 'list', 'object')
                    except Exception as err:
                        logger.error('error :: bq_update :: get_graphite_metric failed, err: %s' % err)
                    replace_redis_timeseries = True
                    try:
                        populated_timeseries = populate_redis_metric(skyline_app, base_name, from_timestamp=from_graphite_timestamp, until_timestamp=until_graphite_timestamp, timeseries=metric_timeseries, replace=replace_redis_timeseries, verify_ssl=verify_ssl)
                        if len(populated_timeseries) > 0:
                            logger.info('bq_update :: repopulated Redis time series key for %s with %s data points, populated_timeseries[-1]: %s' % (
                                base_name, str(len(populated_timeseries)),
                                str(populated_timeseries[-1])))
                    except Exception as err:
                        logger.error('error :: bq_update :: populate_redis_metric failed for %s, job_id: %s, err: %s' % (
                            base_name, job_id, err))
                    if HORIZON_SHARDS:
                        shard_metric = is_shard_metric(base_name)
                        if shard_metric:
                            # sleep to allow the shard host to finalise new
                            # Redis timeseries
                            sleep(1.0)
                    # Reprocess new data
                    post_data = {
                        'data': {
                            'namespaces': base_name,
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
                        logger.info('bq_update :: reprocess API request for %s returned status_code: %s' % (
                            base_name, str(r.status_code)))
                        logger.info('bq_update :: reprocess API request for %s at %s, for new value: %s, returned status_code: %s' % (
                            base_name, str(fetch_interval_timestamp),
                            str(value), str(r.status_code)))
                    except Exception as err:
                        logger.error('error :: bq_update :: reprocess request failed for %s on %s, err: %s' % (
                            base_name, str(reprocess_url), err))

        anomalies_updated = {}
        for acc_id in list(different.keys()):
            for fetch_interval_timestamp in different[acc_id].keys():
                for metric in different[acc_id][fetch_interval_timestamp]:
                    base_name = '%s.%s' % (namespace_prefix, metric)
                    value = different[acc_id][fetch_interval_timestamp][metric]['current']
                    previous_value = different[acc_id][fetch_interval_timestamp][metric]['previous']
                    # Repopulate Redis with all the data
                    until_graphite_timestamp = int(started // interval * interval)
                    from_graphite_timestamp = until_graphite_timestamp - full_duration
                    # Surface the timeseries from the local Graphite because if this is
                    # in a cluster it is possible that the remote graphite/webapp will
                    # not have access to the metric until the remote carbon flushes it
                    metric_timeseries = []
                    try:
                        metric_timeseries = get_graphite_metric(skyline_app, base_name, from_graphite_timestamp, until_graphite_timestamp, 'list', 'object')
                    except Exception as err:
                        logger.error('error :: bq_update :: get_graphite_metric failed, err: %s' % err)
                    if len(metric_timeseries) > 0:
                        logger.info('bq_update :: get_graphite_metric for %s at %s with previous_value: %s, new value: %s, metric_timeseries[-3:]: %s' % (
                            base_name, str(fetch_interval_timestamp),
                            str(previous_value), str(value), str(metric_timeseries[-3:])))
                    else:
                        logger.info('warning :: bq_update :: get_graphite_metric for %s at %s with previous_value: %s, new value: %s, no time series from Graphite, len(metric_timeseries): %s' % (
                            base_name, str(fetch_interval_timestamp),
                            str(previous_value), str(value),
                            str(len(metric_timeseries))))

                    replace_redis_timeseries = True
                    populated_timeseries = None
                    if len(metric_timeseries) > 0:
                        try:
                            populated_timeseries = populate_redis_metric(skyline_app, base_name, from_timestamp=from_graphite_timestamp, until_timestamp=until_graphite_timestamp, timeseries=metric_timeseries, replace=replace_redis_timeseries, verify_ssl=verify_ssl)
                            if len(populated_timeseries) > 0:
                                logger.info('bq_update :: repopulated Redis time series key for %s with %s data points, populated_timeseries[-1]: %s' % (
                                    base_name, str(len(populated_timeseries)),
                                    str(populated_timeseries[-1])))
                        except Exception as err:
                            logger.error('error :: bq_update :: populate_redis_metric failed for %s, job_id: %s, err: %s' % (
                                base_name, job_id, err))
                    # Determine if there is an anomaly to update
                    try:
                        metric_id = get_metric_id_from_base_name(skyline_app, base_name)
                    except Exception as err:
                        logger.error('error :: bq_update :: get_metric_id_from_base_name failed with metric: %s, err: %s' % (
                            str(base_name), err))
                    anomalies_dict = {}
                    try:
                        anomalies_dict = get_anomalies_for_period(skyline_app, [metric_id], (fetch_interval_timestamp - 5), (fetch_interval_timestamp + 5))
                    except Exception as err:
                        logger.error('error :: bq_update :: get_anomalies_for_period failed for metric_id %s, err: %s' % (
                            str(metric_id), err))
                    if len(anomalies_dict) > 0:
                        for anomaly_id in anomalies_dict.keys():
                            logger.error('bq_update :: updating anomaly id: %s for %s with new value' % (
                                str(anomaly_id), base_name))
                            anomaly_updated = False
                            try:
                                field = 'anomalous_datapoint'
                                anomaly_updated = update_anomaly(skyline_app, anomaly_id, field, value)
                                if anomaly_updated:
                                    logger.info('bq_update :: anomaly_updated: %s' % (
                                        str(anomaly_updated)))
                            except Exception as err:
                                logger.error('error :: bq_update :: update_anomaly failed for anomaly id: %s, err: %s' % (
                                    str(anomaly_id), err))
                            if anomaly_updated:
                                anomalies_updated[anomaly_id] = {'previous_value': anomalies_dict[anomaly_id]['anomalous_datapoint'], 'new_value': value}
                    else:
                        logger.info('bq_update :: no anomaly to update for %s at %s' % (
                            base_name, str(fetch_interval_timestamp)))

                    # Reprocess if significantly different.  What is a
                    # significant difference?
                    percent_different = different[acc_id][fetch_interval_timestamp][metric]['percent_different']
                    if percent_different > 15 and len(metric_timeseries) > 0:
                        if HORIZON_SHARDS:
                            shard_metric = is_shard_metric(base_name)
                            if shard_metric:
                                # sleep to allow the shard host to finalise new
                                # Redis timeseries
                                sleep(1.0)
                        post_data = {
                            'data': {
                                'namespaces': base_name,
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
                            logger.info('bq_update :: reprocess API request for %s at %s because percent_different: %s, previous_value: %s, new_value: %s, returned status_code: %s' % (
                                base_name, str(fetch_interval_timestamp),
                                str(percent_different), str(previous_value),
                                str(value), str(r.status_code)))
                        except Exception as err:
                            logger.error('error :: bq_update :: reprocess request failed for %s on %s, err: %s' % (
                                base_name, str(reprocess_url), err))

        if len(anomalies_updated) > 0:
            try:
                updates[fetch_interval]['anomalies_updated'] = copy.deepcopy(anomalies_updated)
            except:
                updates[fetch_interval] = {'anomalies_updated': copy.deepcopy(anomalies_updated)}

        # Save the latest archive data
        # The date_folder must be the date and time it was downloaded not the
        # fetch_interval
        date_folder_ts = int(start_bq_update_job)
        date_folder = datetime.fromtimestamp(date_folder_ts, tz=timezone.utc).strftime('%Y%m%d%H%M%S')
        archive_data_path = '%s/vista/bq_archives/%s/%s' % (
            settings.SKYLINE_DIR, vista_bq_account_key, date_folder)
        new_archive_csv = '%s/%s' % (str(archive_data_path), archive_csv_filename)
        # Only save a new archive if it was different from the last archive
        if df is not None and len(updates) > 0:
            if not os.path.isdir(archive_data_path):
                try:
                    mkdir_p(archive_data_path)
                except Exception as err:
                    err_msg = 'failed to create archive_data_path: %s' % archive_data_path
                    logger.error('error :: bq_update :: %s, err: %s' % (
                        err_msg, err))
            if not os.path.isfile(new_archive_csv):
                logger.info('bq_update :: archiving data for %s to %s' % (str(fetch_interval), new_archive_csv))
                try:
                    df.to_csv(new_archive_csv, index=False)
                except Exception as err:
                    err_msg = 'df.to_csv failed to create archive_csv: %s' % new_archive_csv
                    logger.error('error :: bq_update :: %s, err: %s' % (
                        err_msg, err))
                if os.path.isfile(new_archive_csv):
                    logger.info('bq_update :: archived data for %s to %s' % (str(fetch_interval), new_archive_csv))
                else:
                    logger.error('error :: bq_update :: df.to_csv failed to create archive_csv: %s' % new_archive_csv)

        if len(updates) > 0:
            hash_key = 'vista.bq_update.%s.%s.%s' % (vista_bq_account_key, str(fetch_interval), date_folder)
            changed_count = 0
            try:
                changed_count = len(different)
            except:
                changed_count = 0
            new_count = 0
            try:
                new_count = len(new_data)
            except:
                new_count = 0
            anomaly_updated_count = 0
            try:
                anomaly_updated_count = len(anomalies_updated)
            except:
                anomaly_updated_count = 0
            try:
                redis_conn_decoded.hset(hash_key, 'anomaly_updated_count', anomaly_updated_count)
                redis_conn_decoded.hset(hash_key, 'changed_count', changed_count)
                redis_conn_decoded.hset(hash_key, 'new_count', new_count)
                redis_conn_decoded.hset(hash_key, 'new', str(new_data))
                redis_conn_decoded.hset(hash_key, 'changed', str(different))
                redis_conn_decoded.hset(hash_key, 'anomalies_updated', str(anomalies_updated))
                redis_conn_decoded.expire(hash_key, 2592000)
                logger.info('bq_update :: created %s' % hash_key)
            except Exception as err:
                logger.error('error :: bq_update :: failed to create Redis hash %s, err: %s' % (
                    hash_key, err))

    if add_backfill_job:
        logger.info('bq_update :: no archive found for current date adding bq_backfill job for %s %s data' % (
            vista_bq_account_key, add_backfill_job))

        bq_backfill_url = '%s/bq_backfill' % (settings.SKYLINE_URL)
        bq_update_url = '%s/bq_update' % (settings.SKYLINE_URL)
        user = str(settings.WEBAPP_AUTH_USER)
        password = str(settings.WEBAPP_AUTH_USER_PASSWORD)

        # Submit a bq_backfill job
        try:
            interval = int(bq_account_settings['schedule_interval'])
        except Exception as err:
            logger.error('error :: bq_update :: could not determine schedule_interval from %s, err: %s' % (
                vista_bq_account_key, err))
        backfill_from_timestamp = int(time()) // interval * interval
        post_data = {}
        try:
            post_data = {
                'data': {
                    'bq_backfill': True,
                    'vista_bq_account_key': vista_bq_account_key,
                    'from_timestamp': backfill_from_timestamp,
                    # Also use the from_timestamp for the until_timestamp in a backfill job
                    'until_timestamp': backfill_from_timestamp,
                    'interval': interval,
                    'date_format': bq_account_settings['date_field_format'],
                    'replace_query_string': bq_account_settings['update_replace_query'],
                    'zero_fill': False,
                    'replace_redis_timeseries': False,
                    'max_creates_per_minute': 1500,
                    'dry_run': False,
                    'flux_test': False,
                    'added_by': 'vista_bq_update',
                }
            }
        except Exception as err:
            logger.error('error :: bq_update :: failed to build post_data for bq_backfill job, err: %s' % err)
        r = None
        try:
            headers = {"content-type": "application/json"}
            r = requests.post(bq_backfill_url, auth=(user, password), json=post_data, headers=headers, timeout=5, verify=settings.VERIFY_SSL)
            logger.info('bq_update :: bq_backfill API request made for %s, returned status_code: %s' % (
                vista_bq_account_key, str(r.status_code)))
        except Exception as err:
            logger.error('error :: bq_update :: bq_backfill request failed for %s on %s, err: %s' % (
                vista_bq_account_key, str(bq_backfill_url), err))
        response = None
        if r:
            try:
                response = r.json()
            except Exception as err:
                logger.error(traceback.format_exc())
                logger.error('error :: bq_update :: failed to parse json response from %s, err: %s' % (
                    str(bq_backfill_url), err))
        if response:
            logger.info('bq_update :: bq_backfill_job submitted for %s, response: %s' % (
                vista_bq_account_key, str(response)))
        else:
            logger.error('error :: bq_update :: bq_backfill job submitted for %s but no response' % (
                vista_bq_account_key))

    try:
        redis_conn_decoded.delete('vista.bq_update.running')
    except Exception as err:
        logger.error('error :: bq_update :: failed to delete on vista.bq_update.running from Redis, err: %s' % err)
