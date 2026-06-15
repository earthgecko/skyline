"""
get_external_bq_accounts.py
"""
import copy
import logging
import traceback
from ast import literal_eval

import settings
from skyline_functions import get_redis_conn_decoded

# @added 20240515 - Feature #5352: vista - bigquery
def get_external_bq_accounts(current_skyline_app, log=False):
    """
    Determine accounts from external_settings that have bq settings.

    :param current_skyline_app: the app calling the function
    :type current_skyline_app: str
    :return: external_bq_accounts
    :rtype: dict

    """

    external_bq_accounts = {}
    function_str = '%s :: functions.settings.get_external_bq_accounts' % current_skyline_app
    current_skyline_app_logger = current_skyline_app + 'Log'
    if log:
        current_logger = logging.getLogger(current_skyline_app_logger)
    else:
        current_logger = None

    skyline_external_settings = {}

    try:
        redis_conn_decoded = get_redis_conn_decoded(current_skyline_app)
        if log:
            current_logger.info('%s :: got redis_conn_decoded' % function_str)
    except Exception as err:
        if not log:
            current_logger = logging.getLogger(current_skyline_app_logger)
        current_logger.error(traceback.format_exc())
        current_logger.error('error :: %s :: get_redis_conn_decoded failed, err: %s' % (
            function_str, err))
        return external_bq_accounts

    try:
        skyline_external_settings_raw = redis_conn_decoded.get('skyline.external_settings')
        if skyline_external_settings_raw:
            skyline_external_settings = literal_eval(skyline_external_settings_raw)
    except Exception as err:
        current_logger.error('error :: %s :: could not get Redis set skyline.external_settings - %s' % (
            function_str, err))
        return external_bq_accounts

    if skyline_external_settings:
        for settings_key in list(skyline_external_settings.keys()):
            try:
                external_setting_keys = list(skyline_external_settings[settings_key].keys())
                if 'bq_settings' not in external_setting_keys:
                    continue
                try:
                    flux_token = skyline_external_settings[settings_key]['flux_token']
                except KeyError:
                    flux_token = None

                """
                # Example
                skyline_external_settings[settings_key]['bq_settings'] = {
                    '<UNIQUE_IDENTIFIER>': {
                        'flux_api_token': '<FLUX_API_TOKEN>',
                        'oauth_credentials': {},
                        'service_credentials': {},
                        'project_id': '<BQ_PROJECT_ID>',
                        'query': 'SELECT * FROM `<TABLE>` WHERE <COLUMNN> = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)',
                        'min_expected_results': 900,
                        'metric_format': 'graphite',
                        'metric_prefix': '<METRIC_PREFIX_TO_USE>',
                        'primary_dimension': '<DESIRED_PRIMARY_COLUMN>',
                        'metric_dimensions': ['<COLUMNN_D>', '<COLUMNN_E>', '<COLUMNN_F>','<COLUMNN_G>'],
                        'schedule': {'minute': 0, 'hour': 15, 'day_of_month': '*', 'month': '*', 'day_of_week': '*'},
                        'full_duration': <DESIRED_REDIS_FULLDURATION>,
                        'batch_processing_namespace': '<BATCH_PROCESSING_NAMESPACE>',
                    }
                }
                """
                for bq_account, bq_account_dict in skyline_external_settings[settings_key]['bq_settings'].items():
                    try:
                        flux_api_token = None
                        oauth_credentials = {}
                        service_credentials = {}
                        project_id = None
                        bq_account_dict['project_id']
                        query = None
                        min_expected_results = 0
                        metric_format = None
                        metric_prefix = None
                        primary_dimension = None
                        metric_dimensions = None
                        schedule = None
                        credentials = None
                        credentials_type = None
                        try:
                            flux_api_token = bq_account_dict['flux_api_token']
                        except:
                            flux_api_token = flux_token
                        try:
                            oauth_credentials = bq_account_dict['oauth_credentials']
                        except:
                            oauth_credentials = {}
                        try:
                            service_credentials = bq_account_dict['service_credentials']
                        except:
                            service_credentials = {}
                        project_id = bq_account_dict['project_id']
                        query = bq_account_dict['query']
                        try:
                            min_expected_results = bq_account_dict['min_expected_results']
                        except:
                            min_expected_results = 0
                        metric_format = bq_account_dict['metric_format']
                        metric_prefix = bq_account_dict['metric_prefix']
                        primary_dimension = bq_account_dict['primary_dimension']
                        metric_dimensions = bq_account_dict['metric_dimensions']
                        schedule = bq_account_dict['schedule']
                    except Exception as err:
                        current_logger.error('error :: %s :: failed to interpolate bq_settings for %s, err: %s' % (
                            function_str, settings_key, err))
                        continue
                    if not isinstance(flux_api_token, str):
                        current_logger.error('error :: %s :: flux_api_token for %s is invalid' % (
                            function_str, settings_key))
                        continue
                    if not isinstance(project_id, str):
                        current_logger.error('error :: %s :: project_id for %s is invalid, project_id: %s' % (
                            function_str, settings_key, str(project_id)))
                        continue
                    if not isinstance(query, str):
                        current_logger.error('error :: %s :: query for %s is invalid, query: %s' % (
                            function_str, settings_key, str(query)))
                        continue
                    if metric_format not in ['graphite', 'labelled_metrics', 'prometheus', 'victoriametrics']:
                        current_logger.error('error :: %s :: metric_format for %s is invalid, metric_format: %s' % (
                            function_str, settings_key, str(metric_format)))
                        continue
                    if not isinstance(metric_prefix, str):
                        current_logger.error('error :: %s :: metric_prefix for %s is invalid, metric_prefix: %s' % (
                            function_str, settings_key, str(metric_prefix)))
                        continue
                    if not isinstance(primary_dimension, str):
                        current_logger.error('error :: %s :: primary_dimension for %s is invalid, primary_dimension: %s' % (
                            function_str, settings_key, str(primary_dimension)))
                        continue
                    if not isinstance(metric_dimensions, list):
                        current_logger.error('error :: %s :: metric_dimensions for %s is invalid, metric_dimensions: %s' % (
                            function_str, settings_key, str(metric_dimensions)))
                        continue
                    if not isinstance(schedule, dict):
                        current_logger.error('error :: %s :: schedule for %s is invalid, schedule: %s' % (
                            function_str, settings_key, str(schedule)))
                        continue
                    # Validate credentials
                    if isinstance(oauth_credentials, dict):
                        if len(oauth_credentials) > 0:
                            try:
                                oauth_token = oauth_credentials['token']
                                oauth_refresh_token = oauth_credentials['refresh_token']
                                oauth_token_uri = oauth_credentials['token_uri']
                                oauth_client_id = oauth_credentials['client_id']
                                oauth_client_secret = oauth_credentials['client_secret']
                                credentials = copy.deepcopy(oauth_credentials)
                                credentials_type = 'oauth'
                            except Exception as err:
                                current_logger.error('error :: %s :: oauth_credentials for %s are invalid, err: %s' % (
                                    function_str, settings_key, err))
                                continue
                    if isinstance(service_credentials, dict):
                        if len(service_credentials) > 0:
                            try:
                                services_private_key_id = service_credentials['private_key_id']
                                services_private_key = service_credentials['private_key']
                                services_client_email = service_credentials['client_email']
                                services_client_id = service_credentials['client_id']
                                services_client_x509_cert_url = service_credentials['client_x509_cert_url']
                                credentials = copy.deepcopy(service_credentials)
                                credentials_type = 'service'
                            except Exception as err:
                                current_logger.error('error :: %s :: service_credentials for %s are invalid, err: %s' % (
                                    function_str, settings_key, err))
                                continue
                    if str(credentials_type) not in ['oauth', 'service']:
                        current_logger.error('error :: %s :: failed to determine credentials_type for %s, credentials_type: %s' % (
                            function_str, settings_key, str(credentials_type)))
                        continue
                    if not credentials:
                        current_logger.error('error :: %s :: failed to determine credentials for %s' % (
                            function_str, settings_key))
                        continue
                    external_bq_accounts[bq_account] = copy.deepcopy(bq_account_dict)
                    external_bq_accounts[bq_account]['flux_api_token'] = flux_api_token
                    external_bq_accounts[bq_account]['min_expected_results'] = min_expected_results
            except Exception as err:
                current_logger.error('error :: %s :: failed to interpolate bq_settings for %s, err: %s' % (
                    function_str, settings_key, err))

    return external_bq_accounts
