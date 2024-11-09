"""
bq_credentials.py
"""
import json
import os
import logging
import sys
from contextlib import nullcontext

# For version pinning consider implementing a skyline-bq-py31013 environment
# and adding that to the path so that imports come from there rather than from
# the skyline-py31013 environment which has different versions of some libraries
# e.g. googleapis-common-protos

sys.path.insert(0, '/opt/skyline/github/skyline/skyline')
import settings

try:
    VISTA_BQ_VIRTUALENV_PATH = settings.VISTA_BQ_VIRTUALENV_PATH
except:
    VISTA_BQ_VIRTUALENV_PATH = None
if VISTA_BQ_VIRTUALENV_PATH:
    sys.path.insert(0, VISTA_BQ_VIRTUALENV_PATH)
    from google.oauth2.credentials import Credentials
    from google.auth.transport.requests import Request
else:
    Credentials = nullcontext()
    Request = nullcontext()

# @added 20240514 - Feature #5352: vista - bigquery
def get_bq_credentials(
        current_skyline_app, credentials=None, project_id=None,
        oauth_credentials={}, oauth_credentials_file=None,
        service_credentials={}, service_credentials_file=None):
    """
    Returns credentials
    """
    current_skyline_app_logger = current_skyline_app + 'Log'
    current_logger = logging.getLogger(current_skyline_app_logger)

    oauth_token = None
    oauth_refresh_token = None
    oauth_token_uri = None
    oauth_client_id = None
    oauth_client_secret = None
    if isinstance(oauth_credentials_file, str):
        if os.path.isfile(str(oauth_credentials_file)):
            try:
                with open(oauth_credentials_file, 'r') as fh:
                    oauth_credentials = json.load(fh)
            except Exception as err:
                current_logger.error('error :: get_bq_credentials :: json.load failed with oauth_credentials_file: %s, err: %s' % (
                    str(oauth_credentials_file), err))
    if oauth_credentials:
        try:
            oauth_token = oauth_credentials['token']
            oauth_refresh_token = oauth_credentials['refresh_token']
            oauth_token_uri = oauth_credentials['token_uri']
            oauth_client_id = oauth_credentials['client_id']
            oauth_client_secret = oauth_credentials['client_secret']
        except Exception as err:
            current_logger.error('error :: get_bq_credentials :: failed parse details from oauth_credentials, err: %s' % (
                err))

    if oauth_token and oauth_refresh_token:
        try:
            # credentials = Credentials(
            #     token='xxx',  # This is the access token you received
            #     refresh_token='xxx',  # The refresh token
            #     token_uri='https://oauth2.googleapis.com/token',  # Token URI for refreshing the token
            #     client_id='123-xxx.apps.googleusercontent.com',  # Your OAuth 2.0 Client ID
            #     client_secret='xxx'  # Your OAuth 2.0 Client Secret
            # )
            if not credentials:
                credentials = Credentials(
                    token=oauth_token,
                    refresh_token=oauth_refresh_token,
                    token_uri=oauth_token_uri,
                    client_id=oauth_client_id,
                    client_secret=oauth_client_secret,
                )
        except Exception as err:
            current_logger.error('error :: get_bq_credentials :: Credentials failed with oauth variables, err: %s' % (
                err))
        if credentials:
            try:
                if credentials.expired:
                    credentials.refresh(Request())
            except Exception as err:
                current_logger.error('error :: get_bq_credentials :: credentials.refresh failed, err: %s' % (
                    err))
                credentials = None

    service_private_key_id = None
    service_private_key = None
    service_client_email = None
    service_client_id = None
    service_client_x509_cert_url = None
    if isinstance(service_credentials_file, str):
        if os.path.isfile(str(service_credentials_file)):
            with open(service_credentials_file, 'r') as fh:
                service_credentials = json.load(fh)
    if service_credentials:
        try:
            service_private_key_id = service_credentials['private_key_id']
        except Exception as err:
            current_logger.error('error :: get_bq_credentials :: could not determine private_key_id from service_credentials, err: %s' % (
                err))

    # TODO - add service account auth when available

    if not credentials:
        current_logger.error('error :: get_bq_credentials :: failed to get credentials')
    return credentials
