"""
validate_key.py
"""
import traceback
import sys
import os.path
# @added 20220722 - Task #4624: Change all dict copy to deepcopy
import copy

from logger import set_up_logging

sys.path.append(os.path.join(os.path.dirname(os.path.realpath(__file__)), os.pardir))
sys.path.insert(0, os.path.dirname(__file__))

# @modified 20191115 - Branch #3262: py3
# This prevents flake8 E402 - module level import not at top of file
if True:
    import settings
    from skyline_functions import get_redis_conn_decoded

logger = set_up_logging(None)

# Validation of FLUX_API_KEYS
valid_keys = []
try:
    FLUX_SELF_API_KEY = settings.FLUX_SELF_API_KEY
    valid_keys.append(FLUX_SELF_API_KEY)
except:
    pass
FLUX_API_KEYS = {}
try:
    # @modified 20220722 - Task #4624: Change all dict copy to deepcopy
    # FLUX_API_KEYS = settings.FLUX_API_KEYS.copy()
    FLUX_API_KEYS = copy.deepcopy(settings.FLUX_API_KEYS)
except:
    pass
if FLUX_API_KEYS:
    for flux_api_key in FLUX_API_KEYS:
        try:
            valid_keys.append(str(flux_api_key))
        except:
            pass


def validate_key(caller, apikey):

    # @added 20200818 - Feature #3694: flux - POST multiple metrics
    # Added metric_namespace_prefix which is declared via the FLUX_API_KEYS
    metric_namespace_prefix = None

    keyValid = False

    try:
        isAlNum = False
        isAlNum = apikey.isalnum()
        if isAlNum:
            keyLength = len(apikey)
            if keyLength == 32:
                # Check to determine if it is a valid API key
                keyValid = True
            else:
                # @modified 20210421 - Task #4030: refactoring
                # semgrep - python-logger-credential-disclosure
                # logger.error('error :: %s :: invalid api key length of %s - %s' % (
                #     caller, str(keyLength), str(apikey)))
                logger.error('error :: %s :: invalid api key length' % (
                    caller))
        # @added 20200818 - Feature #3694: flux - POST multiple metrics
        # Added validation of FLUX_API_KEYS
        if valid_keys and keyValid:
            if apikey not in valid_keys:
                # @modified 20210421 - Task #4030: refactoring
                # semgrep - python-logger-credential-disclosure
                logger.error('error :: %s :: invalid api key - not known' % (
                    # caller, str(keyLength), str(apikey)))
                    caller))
                keyValid = False
            if keyValid:
                try:
                    metric_namespace_prefix = settings.FLUX_API_KEYS[apikey]
                except:
                    metric_namespace_prefix = None
            if apikey in list(external_flux_keys.keys()):
                try:
                    metric_namespace_prefix = external_flux_keys[apikey]
                except:
                    metric_namespace_prefix = None

        # @added 20220117 - Feature #4324: flux - reload external_settings
        #                   Feature #4376: webapp - update_external_settings
        update_keys = False
        if not keyValid:
            last_valid_keys_update = None
            update_external_settings_key = None
            try:
                last_valid_keys_update = redis_conn_decoded.get('flux.last_valid_keys_update')
            except Exception as err:
                if LOCAL_DEBUG:
                    logger.error('error :: listen :: could get flux.last_valid_keys_update from Redis - %s' % str(err))
                last_valid_keys_update = None
            if last_valid_keys_update:
                if int(last_valid_keys_update) < (int(time()) - 30):
                    logger.info('%s :: last valid key update check was more than 30 seconds ago, checking update' % (
                        caller))
                    update_keys = True
            if not update_keys:
                try:
                    update_external_settings_key = redis_conn_decoded.get('skyline.external_settings.update.flux')
                except Exception as err:
                    if LOCAL_DEBUG:
                        logger.error('error :: listen :: could get skyline.external_settings.update.flux from Redis - %s' % str(err))
                    last_valid_keys_update = None
                if update_external_settings_key:
                    logger.info('%s :: skyline.external_settings.update.flux, updating keys' % (
                        caller))
                    update_keys = True
        updated_valid_keys = []
        updated_external_flux_keys = []
        if update_keys:
            try:
                updated_valid_keys, updated_external_flux_keys = check_validate_key_update(caller)
            except Exception as err:
                if LOCAL_DEBUG:
                    logger.error('error :: listen :: check_validate_key_update failed - %s' % str(err))
                updated_valid_keys = []
                updated_external_flux_keys = []
        if updated_valid_keys:
            for uv_key in updated_valid_keys:
                valid_keys.append(uv_key)
        if updated_external_flux_keys:
            for uv_key in updated_external_flux_keys:
                valid_keys.append(uv_key)
        if update_keys and valid_keys:
            if apikey not in valid_keys:
                logger.error('error :: %s :: invalid api key - not known' % (
                    caller))
                keyValid = False
            if keyValid:
                try:
                    metric_namespace_prefix = settings.FLUX_API_KEYS[apikey]
                except:
                    metric_namespace_prefix = None
            if apikey in list(external_flux_keys.keys()):
                try:
                    metric_namespace_prefix = external_flux_keys[apikey]
                except:
                    metric_namespace_prefix = None

        if not keyValid:
            # @modified 20210421 - Task #4030: refactoring
            # semgrep - python-logger-credential-disclosure
            # logger.error('error :: %s :: invalid api key - %s' % (
            #    caller, str(apikey)))
            logger.error('error :: %s :: invalid api key' % (
                caller))
            # resp.status = falcon.HTTP_400
            # @modified 20200818 - Feature #3694: flux - POST multiple metrics
            # Added metric_namespace_prefix
            return False, metric_namespace_prefix
    except:
        logger.error(traceback.format_exc())
        # @modified 20210421 - Task #4030: refactoring
        # semgrep - python-logger-credential-disclosure
        # logger.error('error :: %s :: failed to validate api key - %s' % (
        #     caller, str(apikey)))
        logger.error('error :: %s :: failed to validate api key' % (
            caller))
        # resp.status = falcon.HTTP_400
        # @modified 20200818 - Feature #3694: flux - POST multiple metrics
        # Added metric_namespace_prefix
        return False, metric_namespace_prefix

    # @modified 20200818 - Feature #3694: flux - POST multiple metrics
    # Added metric_namespace_prefix
    return True, metric_namespace_prefix
