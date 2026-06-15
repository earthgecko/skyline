"""
api_thunder_sms.py
"""
import logging
from time import time

from flask import request

import settings
from functions.aws.send_sms import send_sms
from skyline_functions import get_redis_conn_decoded

try:
    thunder_sms_keys = settings.SMS_ALERT_OPTS['thunder_sms_keys']
except:
    thunder_sms_keys = {}

# @added 20250404 - Feature #5619: thunder_sms
def api_thunder_sms(current_skyline_app):
    """
    Return send an SMS via AWS and return the thunder_sms_response_dict.

    :param current_skyline_app: the app calling the function
    :type current_skyline_app: str
    :return: thunder_sms_response_dict
    :rtype: dict

    """
    function_str = 'api_thunder_sms'
    success = False
    response = {}

    thunder_sms_response_dict = {
        'status': success,
        'response': response
    }

    current_skyline_app_logger = current_skyline_app + 'Log'
    current_logger = logging.getLogger(current_skyline_app_logger)

    if not thunder_sms_keys:
        thunder_sms_response_dict['status_code'] = 401
        thunder_sms_response_dict['message'] = 'Unauthorized'
        return thunder_sms_response_dict

    try:
        redis_conn_decoded = get_redis_conn_decoded(current_skyline_app)
    except Exception as err:
        current_logger.error('error :: api_thunder_sms - get_redis_conn_decoded failed, err: %s' % (
            err))

    # Allow for muting
    mute = False
    mute_expiry = 1200
    if 'mute' in request.args:
        mute_key = request.args.get('mute')
        for authorised_key in thunder_sms_keys.values():
            if mute_key == authorised_key:
                mute = True
                break
    if mute:
        if 'expiry' in request.args:
            mute_expiry_str = request.args.get('expiry')
            try:
                mute_expiry = int(mute_expiry_str)
            except:
                mute_expiry = 1200
        redis_mute_key = 'thunder_sms.%s' % mute_key
        try:
            redis_conn_decoded.setex(redis_mute_key, mute_expiry, int(time()))
        except Exception as err:
            current_logger.error('error :: api_thunder_sms - setex failed to set %s, err: %s' % (
                redis_mute_key, err))
        message = 'SMS muted for %s seconds' % str(mute_expiry)
        thunder_sms_response_dict = {
            'status': True,
            'response': {'message': message}
        }
        thunder_sms_response_dict['status_code'] = 200
        current_logger.info('%s :: thunder_sms muted for %s seconds' % (
            function_str, str(mute_expiry)))
        return thunder_sms_response_dict

    # Test whether form or json POST
    form_data = False
    try:
        key = request.form['key']
        if key:
            form_data = True
            thunder_sms_response_dict['form_data'] = True
            current_logger.info('api_thunder_sms, form POST')
            current_logger.info('api_thunder_sms - key passed')
    except:
        current_logger.info('api_thunder_sms no form data trying json')

    post_data = {}
    if not form_data:
        try:
            post_data = request.get_json()
        except Exception as err:
            current_logger.error('error :: api_thunder_sms - no POST data, err: %s' % (
                err))
            current_logger.info('api_thunder_sms, return 400 no POST data')
            thunder_sms_response_dict['status_code'] = 400
            thunder_sms_response_dict['error'] = 'no post data'
            return thunder_sms_response_dict
        try:
            key = post_data['key']
            if key:
                current_logger.info('api_thunder_sms - key passed')
        except KeyError:
            key = None
        except Exception as err:
            key = None
            current_logger.error('error :: api_thunder_sms - evaluation of  post_data[\'key\'] failed, err: %s' % (
                err))
    if not key:
        current_logger.info('api_thunder_sms, return 400 no key determined')
        thunder_sms_response_dict['status_code'] = 400
        thunder_sms_response_dict['error'] = 'Bad Request'
        return thunder_sms_response_dict

    thunder_sms_response_dict['key'] = key
    authorised = False
    for authorised_key in thunder_sms_keys.values():
        if key == authorised_key:
            authorised = True
    if not authorised:
        thunder_sms_response_dict['status_code'] = 403
        thunder_sms_response_dict['message'] = 'Forbidden'
        return thunder_sms_response_dict

    # Check if muted
    muted = False
    redis_mute_key = 'thunder_sms.%s' % key
    try:
        muted = redis_conn_decoded.ttl(redis_mute_key)
    except Exception as err:
        current_logger.error('error :: api_thunder_sms - redis get failed on %s, err: %s' % (
            redis_mute_key, err))
    if muted:
        # Redis can return -2 if there is no key ttl
        if muted > 0:
            message = 'SMS muted for %s seconds' % str(muted)
            thunder_sms_response_dict = {
                'status': True,
                'response': {'message': message}
            }
            thunder_sms_response_dict['status_code'] = 200
            current_logger.info('%s :: thunder_sms muted for %s seconds' % (
                function_str, str(muted)))
            return thunder_sms_response_dict

    number = None
    message = None

    if form_data:
        all_form_data = request.form
        form_data_dict = {}
        for key, value in all_form_data.items():
            form_data_dict[key] = value
        current_logger.info('api_thunder_sms, form_data_dict: %s' % str(form_data_dict))
        try:
            number = request.form['number']
        except KeyError:
            number = None
        except Exception as err:
            current_logger.error('api_thunder_sms no step, err: %s' % err)
        try:
            message = request.form['message']
        except KeyError:
            message = None
        except Exception as err:
            current_logger.error('api_thunder_sms no message, err: %s' % err)

    if not form_data:
        current_logger.info('api_thunder_sms, post_data_dict: %s' % str(post_data))
        try:
            number = post_data['number']
        except KeyError:
            number = None
        except Exception as err:
            current_logger.error('error :: api_thunder_sms - evaluation of  post_data[\'number\'] failed, err: %s' % (
                err))
        try:
            message = post_data['message']
        except KeyError:
            message = None
        except Exception as err:
            current_logger.error('error :: api_thunder_sms - evaluation of  post_data[\'message\'] failed, err: %s' % (
                err))
    return_400 = False
    if not number:
        return_400 = True
    if not message:
        return_400 = True
    if return_400:
        thunder_sms_response_dict['status_code'] = 400
        thunder_sms_response_dict['message'] = 'Bad Request'
        return thunder_sms_response_dict

    number_authorised = False
    try:
        if key == thunder_sms_keys[number]:
            number_authorised = True
    except:
        number_authorised = False
    if not number_authorised:
        thunder_sms_response_dict['status_code'] = 403
        thunder_sms_response_dict['message'] = 'Forbidden'
        return thunder_sms_response_dict

    # Rate limit to 200 SMS messages in a 20 min period
    sent_count = 0
    sent_count_ttl = 0
    redis_rate_key = 'thunder_sms.sent_count.%s' % key
    try:
        sent_count = redis_conn_decoded.get(redis_rate_key)
        if sent_count:
            sent_count_ttl = redis_conn_decoded.ttl(redis_rate_key)
    except Exception as err:
        current_logger.error('error :: api_thunder_sms - redis ttl failed on %s, err: %s' % (
            redis_rate_key, err))
    if sent_count:
        try:
            sent_count = int(float(sent_count))
        except:
            sent_count = 1
        if sent_count > 199:
            message = 'SMS rate limited for %s seconds' % str(sent_count_ttl)
            thunder_sms_response_dict = {
                'status': True,
                'response': {'message': message}
            }
            thunder_sms_response_dict['status_code'] = 200
            current_logger.info('%s :: thunder_sms rate limited for %s seconds' % (
                function_str, str(sent_count_ttl)))
            return thunder_sms_response_dict

    try:
        success, response = send_sms(current_skyline_app, number, message)
    except Exception as err:
        current_logger.error('error :: api_thunder_sms - send_sms failed, err: %s' % (
            err))
    if success:
        try:
            redis_conn_decoded.incr(redis_rate_key, 1)
            redis_conn_decoded.expire(redis_rate_key, 1200)
        except Exception as err:
            current_logger.error('error :: api_thunder_sms - failed to incr and expire on %s, err: %s' % (
                redis_rate_key, err))
        thunder_sms_response_dict['status_code'] = 200
    else:
        thunder_sms_response_dict['status_code'] = 500
        thunder_sms_response_dict['message'] = 'SMS failed'
    thunder_sms_response_dict['status'] = success
    thunder_sms_response_dict['response'] = response
    current_logger.info('%s :: success: %s' % (
        function_str, str(success)))
    return thunder_sms_response_dict
