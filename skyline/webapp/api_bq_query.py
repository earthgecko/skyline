"""
api_bq_query.py
"""
import copy
import logging
import os
import sys
import traceback

from contextlib import nullcontext
from datetime import datetime
from time import time
from flask import request

# For version pinning consider implementing a skyline-bq-py31013 environment
# and adding that to the path so that imports come from there rather than from
# the skyline-py31013 environment which has different versions of some libraries
# e.g. googleapis-common-protos
import settings
from functions.settings.get_bq_accounts_settings import get_bq_accounts_settings

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


# @added 20240725 - Feature #5372: vista - bq_update
#                   Feature #5352: vista - bigquery
def api_bq_query(current_skyline_app):
    """
    Return a dict with the backfill job details.

    :param current_skyline_app: the app calling the function
    :type current_skyline_app: str
    :return: bq_query_results
    :rtype: dict

    """
    bq_query_results = {'status_code': 500, 'archive_csv': None, 'archive_csv_url': None}

    start = time()
    current_skyline_app_logger = current_skyline_app + 'Log'
    current_logger = logging.getLogger(current_skyline_app_logger)

    vista_bq_account_key = None
    # Test whether form or json POST
    form_data = False
    try:
        vista_bq_account_key = request.form['vista_bq_account_key']
        if vista_bq_account_key:
            form_data = True
            current_logger.info('api_bq_query :: form POST')
            current_logger.info('api_bq_query - vista_bq_account_key passed: %s' % str(vista_bq_account_key))
    except:
        current_logger.info('api_bq_query no form data trying json')

    post_data = {}
    if not form_data:
        try:
            post_data = request.get_json()
        except Exception as err:
            current_logger.error('error :: api_bq_query - no POST data - %s' % (
                err))
            current_logger.info('api_bq_query :: return 400 no POST data')
            bq_query_results['status_code'] = 400
            bq_query_results['error'] = 'no post data'
            return bq_query_results
        if post_data:
            current_logger.info('api_bq_query :: post_data: %s' % str(post_data))
        try:
            vista_bq_account_key = post_data['data']['vista_bq_account_key']
            if vista_bq_account_key:
                current_logger.info('api_bq_query - vista_bq_account_key passed: %s' % str(vista_bq_account_key))
        except KeyError:
            vista_bq_account_key = None
        except Exception as err:
            vista_bq_account_key = None
            current_logger.error('error :: api_bq_query - evaluation of  post_data[\'data\'][\'vista_bq_account_key\'] failed - %s' % (
                err))
    if not vista_bq_account_key:
        current_logger.info('api_bq_query :: return 400 no vista_bq_account_key determined')
        bq_query_results['status_code'] = 400
        bq_query_results['error'] = 'no vista_bq_account_key argument'
        return bq_query_results

    sql = None
    try:
        if form_data:
            sql = str(request.form['sql'])
        else:
            sql = str(post_data['data']['sql'])
        current_logger.info('api_bq_query :: with sql: %s' % sql)
    except Exception as err:
        current_logger.error('api_bq_query :: failed to determine sql, err: %s' % err)
        current_logger.info('api_bq_query :: return 400 no sql passed')
        bq_query_results['status_code'] = 400
        bq_query_results['error'] = 'no sql argument'
        return bq_query_results

    vista_bq_accounts = {}
    try:
        vista_bq_accounts = get_bq_accounts_settings(current_skyline_app)
    except Exception as err:
        current_logger.error('error :: api_bq_query :: get_bq_accounts_settings failed, err: %s' % err)
        current_logger.info('api_bq_query :: returning 500')
        bq_query_results['status_code'] = 500
        bq_query_results['error'] = 'get_bq_accounts_settings failed'
        return bq_query_results

    bq_account_settings = {}
    try:
        bq_account_settings = vista_bq_accounts[vista_bq_account_key]
    except Exception as err:
        current_logger.error('error :: api_bq_query :: failed to determine settings for %s, err: %s' % (
            vista_bq_account_key, err))

    if not bq_account_settings:
        err_msg = 'failed to determine settings for %s' % vista_bq_account_key
        current_logger.error('error :: api_bq_query :: %s' % err_msg)
        current_logger.info('api_bq_query :: returning 500')
        bq_query_results['status_code'] = 500
        bq_query_results['error'] = err_msg
        return bq_query_results

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
                            current_skyline_app, credentials=use_credentials,
                            project_id=project_id,
                            oauth_credentials=oauth_credentials,
                            oauth_credentials_file=oauth_credentials_file,
                            service_credentials=service_credentials,
                            service_credentials_file=service_credentials_file)
    except Exception as err:
        current_logger.error('error :: api_bq_query :: get_bq_credentials failed, err: %s' % (
            err))
    if not credentials:
        current_logger.error('error :: api_bq_query :: no credentials retrieved for %s' % vista_bq_account_key)
        err_msg = 'failed to get credentials for %s' % vista_bq_account_key
        current_logger.error('error :: api_bq_query :: %s' % err_msg)
        current_logger.info('api_bq_query :: returning 500')
        bq_query_results['status_code'] = 500
        bq_query_results['error'] = err_msg
        return bq_query_results
    else:
        use_credentials = credentials

    configuration = {'jobTimeoutMs': 120000, 'timeoutMs': 120000}
    df = None
    try:
        df = pandas_gbq.read_gbq(sql, project_id=bq_account_settings['project_id'], credentials=credentials, progress_bar_type=None, configuration=configuration)
    except Exception as err:
        err_msg = 'pandas_gbq.read_gbq failed with sql: %s' % sql
        current_logger.error(traceback.format_exc())
        current_logger.error('error :: api_bq_query :: %s, err: %s' % (
            err_msg, err))
        current_logger.info('api_bq_query :: returning 500')
        bq_query_results['status_code'] = 500
        bq_query_results['error'] = err_msg
        return bq_query_results

    if df is None:
        err_msg = 'pandas_gbq.read_gbq returned no data df is None'
        current_logger.error('error :: api_bq_query :: %s' % (
            err_msg))
        current_logger.info('api_bq_query :: returning 500')
        bq_query_results['status_code'] = 500
        bq_query_results['error'] = err_msg
        return bq_query_results

    current_logger.info('api_bq_query :: fetched %s rows' % str(len(df)))

    vista_bq_archive_path = '%s/vista/bq_archives' % settings.SKYLINE_DIR
    new_archive_csv = '%s/%s.%s.csv' % (vista_bq_archive_path, str(int(start)), bq_account_settings['project_id'])
    try:
        df.to_csv(new_archive_csv, index=False)
        current_logger.info('api_bq_query :: created %s' % new_archive_csv)
    except Exception as err:
        err_msg = 'df.to_csv failed to create archive_csv: %s' % new_archive_csv
        current_logger.error('error :: bq_update :: %s, err: %s' % (
            err_msg, err))
    if os.path.isfile(new_archive_csv):
        archive_csv_url = '%s/vista_bq_archive_file?file=%s' % (settings.SKYLINE_URL, new_archive_csv)
        bq_query_results = {'status_code': 200, 'archive_csv': new_archive_csv, 'archive_csv_url': archive_csv_url}
    current_logger.info('api_bq_query :: returning bq_query_results: %s' % str(bq_query_results))

    return bq_query_results
