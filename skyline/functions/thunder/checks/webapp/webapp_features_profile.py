"""
webapp_features_profile.py
"""
import logging
import traceback
from os import path, kill, uname
from time import time

import requests
from signal import SIGHUP

import settings

from functions.thunder.send_event import thunder_send_event
from functions.thunder.check_thunder_failover_key import check_thunder_failover_key

skyline_app = 'thunder'
skyline_app_logger = '%sLog' % skyline_app
logger = logging.getLogger(skyline_app_logger)

# The required THUNDER directories which are failed over to and
# used in the event that Redis is down
THUNDER_EVENTS_DIR = '%s/thunder/events' % settings.SKYLINE_TMP_DIR
THUNDER_KEYS_DIR = '%s/thunder/keys' % settings.SKYLINE_TMP_DIR

this_host = str(uname()[1])


# @added 20230622 - Feature #4958: webapp_features_profile - status
def thunder_check_webapp_features_profile(self):
    """
    Determine the state of the gunicorn webapp_features_profile gunicorn
    workers.  If they are not responsive the main webapp_features_profile
    gunicorn process is issued -HUP to reload the workers

    :param self: the self object
    :type self: object
    :return: success
    :rtype: boolean

    """

    function_str = 'functions.thunder.thunder_check_webapp_features_profile'

    hup_issued = False

    webapp_features_profile_pid_file = '%s/webapp_features_profile.pid' % settings.PID_PATH
    if not path.isfile(webapp_features_profile_pid_file):
            logger.warning('warning :: thunder/rolling :: %s does not exist, nothing to check' % (
                  webapp_features_profile_pid_file))
            return True

    webapp_features_profile_status = True
    r_json = None
    try:
        fp_url = 'http://127.0.0.1:%s/ionosphere_features_profile' % str((settings.WEBAPP_PORT + 1))
        post_data = {'status': True}
        r = requests.post(fp_url, data=post_data, timeout=3)
        r_json = r.json()
    except Exception as err:
            logger.error('error :: thunder/rolling :: webapp_features_profile did not respond - %s' % err)
            webapp_features_profile_status = False
    if r_json:
        try:
            if r_json['status'] != 'OK':
                logger.warning('warning :: thunder/rolling :: webapp_features_profile did not report status OK - %s' % str(r_json))
                webapp_features_profile_status = False
        except Exception as err:
                logger.warning('warning :: thunder/rolling :: webapp_features_profile did not report status OK - %s' % err)
                webapp_features_profile_status = False
    if not webapp_features_profile_status:
        logger.info('thunder/rolling :: getting main pid of webapp_features_profile gunicorn process')
        pid = None
        try:
            with open(webapp_features_profile_pid_file, 'r') as fh:
                for line in fh:
                    pid = int(line.strip())
                    break
        except Exception as err:
            logger.error('error :: thunder/rolling :: failed to determine pid of webapp_features_profile - %s' % err)
        if pid:
            logger.info('thunder/rolling :: issueing -HUP to main pid %s of webapp_features_profile gunicorn process' % str(pid))
            try:
                kill(pid, SIGHUP)
                logger.info('thunder/rolling :: os.kill SIGHUP issued to main pid %s of webapp_features_profile gunicorn process' % str(pid))
                hup_issued = True
            except Exception as err:
                logger.error('error :: thunder/rolling :: os.kill SIGHUP failed - %s' % err)
        else:
            logger.error('error :: thunder/rolling :: could not determine pid, nothing to HUP')
            return False

    check_app = 'webapp'
    event_type = 'webapp_features_profile'
    metric = 'skyline.%s.%s.%s' % (check_app, this_host, event_type)

    success = True
    now = int(time())

    # Determine if a webapp_features_profile thunder alert has been sent
    thunder_alert = None
    cache_key = 'thunder.alert.%s.%s.alert' % (check_app, event_type)
    try:
        thunder_alert = self.redis_conn_decoded.get(cache_key)
    except Exception as err:
        logger.error(traceback.format_exc())
        logger.error('error :: thunder/rolling :: %s :: failed to get %s Redis key - %s' % (
            function_str, cache_key, err))
    if not thunder_alert:
        thunder_alert = check_thunder_failover_key(self, cache_key)

    # If normal send a recovered alert
    if webapp_features_profile_status and thunder_alert:
        # Remove the alert Redis key
        try:
            self.redis_conn_decoded.delete(cache_key)
        except Exception as err:
            logger.error(traceback.format_exc())
            logger.error('error :: thunder/rolling :: %s :: failed to delete %s Redis key - %s' % (
                function_str, cache_key, err))
        # Send thunder recovered event
        level = 'notice'
        message = '%s - webapp_features_profile has recovered' % level
        status = 'webapp_features_profile has recovered'
        thunder_event = {
            'level': level,
            'event_type': event_type,
            'message': message,
            'app': check_app,
            'metric': metric,
            'source': 'thunder',
            'timestamp': now,
            'expiry': 59,
            'data': {'status': status}
        }
        submitted = None
        try:
            submitted = thunder_send_event(skyline_app, thunder_event, log=True)
        except Exception as e:
            logger.error('error :: thunder/rolling :: %s :: thunder_send_event failed - %s' % (
                function_str, e))
        if submitted:
            logger.info('thunder/rolling :: %s :: Analyzer run_time recovered thunder_send_event submitted' % (
                function_str))
        else:
            logger.error('error :: thunder/rolling :: %s :: Analyzer run_time recovered thunder_send_event failed' % (
                function_str))

    # If hup issued send an alert
    if not webapp_features_profile_status and hup_issued and not thunder_alert:
        level = 'alert'
        message = '%s - webapp_features_profile was issued HUP because no status returned' % level
        status = 'webapp_features_profile was issued HUP because no status returned'
        try:
            expiry = int(settings.THUNDER_CHECKS[check_app][event_type]['expiry'])
        except Exception as err:
            logger.error('error :: %s :: failed to determine the expiry for %s %s check - %s' % (
                function_str, check_app, event_type, err))
            expiry = 300
        thunder_event = {
            'level': 'alert',
            'event_type': event_type,
            'message': message,
            'app': check_app,
            'metric': metric,
            'source': 'thunder',
            'timestamp': now,
            'expiry': expiry,
            'data': {'status': status}
        }
        try:
            submitted = thunder_send_event(skyline_app, thunder_event, log=True)
        except Exception as e:
            logger.error('error :: %s :: thunder_send_event failed - %s' % (
                function_str, e))
        if submitted:
            logger.info('%s :: webapp webapp_features_profile status thunder_send_event submitted' % (
                function_str))
        else:
            logger.error('error :: %s :: webapp webapp_features_profile status thunder_send_event failed' % (
                function_str))
    return success
