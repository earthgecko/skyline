"""
get_bq_accounts_settings.py
"""
import copy
import logging
import sys

sys.path.insert(0, '/opt/skyline/github/skyline/skyline')
import settings
from functions.settings.get_external_bq_accounts import get_external_bq_accounts

try:
    VISTA_BQ_ACCOUNTS = copy.deepcopy(settings.VISTA_BQ_ACCOUNTS)
except:
    VISTA_BQ_ACCOUNTS = {}


# @added 20240515 - Feature #5352: vista - bigquery
def get_bq_accounts_settings(current_skyline_app):
    """
    Returns internal and external bq_settings
    """
    current_skyline_app_logger = current_skyline_app + 'Log'
    current_logger = logging.getLogger(current_skyline_app_logger)
    
    vista_bq_accounts = copy.deepcopy(VISTA_BQ_ACCOUNTS)

    internal_bq_accounts = list(vista_bq_accounts.keys())
    external_bq_accounts = {}
    try:
        external_bq_accounts = get_external_bq_accounts(current_skyline_app)
    except Exception as err:
        current_logger.error('error :: get_bq_account_settings :: get_external_bq_accounts failed, err: %s' % err)
    if len(external_bq_accounts) > 0:
        for bq_account, bq_account_dict in external_bq_accounts.items():
            if bq_account in internal_bq_accounts:
                current_logger.warning('warning :: get_bq_account_settings :: settings for %s from settings.VISTA_BQ_ACCOUNTS are being overwritten from external_settings' % str(bq_account))
            vista_bq_accounts[bq_account] = copy.deepcopy(bq_account_dict)

    vista_bq_accounts_log = copy.deepcopy(vista_bq_accounts)
    vista_bq_account_keys = list(vista_bq_accounts_log.keys())
    for vista_bq_account in vista_bq_account_keys:
        try:
            if 'oauth_credentials' in vista_bq_accounts_log[vista_bq_account]:
                if len(vista_bq_accounts_log[vista_bq_account]['oauth_credentials']) > 0:
                    # @modified 20241106 - Task #5526: Build v5.0.0 and upgrade deps
                    # B105:hardcoded_password_string] Possible hardcoded password
                    vista_bq_accounts_log[vista_bq_account]['oauth_credentials'] = 'REDACTED'  # nosec B105
        except:
            pass
        try:
            if 'service_credentials' in vista_bq_accounts_log[vista_bq_account]:
                if len(vista_bq_accounts_log[vista_bq_account]['service_credentials']) > 0:
                    vista_bq_accounts_log[vista_bq_account]['service_credentials'] = 'REDACTED'  # nosec B105
        except:
            pass
        try:
            if 'flux_api_token' in vista_bq_accounts_log[vista_bq_account]:
                if len(vista_bq_accounts_log[vista_bq_account]['flux_api_token']) > 0:
                    vista_bq_accounts_log[vista_bq_account]['flux_api_token'] = 'REDACTED'  # nosec B105
        except:
            pass
    current_logger.debug('debug :: get_bq_account_settings :: vista_bq_accounts: %s' % str(vista_bq_accounts_log))

    if not vista_bq_accounts:
        current_logger.warning('warning :: get_bq_account_settings :: no vista_bq_accounts settings found')
    return vista_bq_accounts
