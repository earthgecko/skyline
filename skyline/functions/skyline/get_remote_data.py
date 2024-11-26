"""
get_remote_data.py
"""
import logging
import os
import traceback

import requests

import settings

try:
    REMOTE_SKYLINE_INSTANCES = list(settings.REMOTE_SKYLINE_INSTANCES)
except:
    REMOTE_SKYLINE_INSTANCES = []

LOCAL_DEBUG = False


# @added 20240625 - Feature #5378: functions.skyline.get_remote_data
def get_remote_data(current_skyline_app, remote_skyline_instance, data_required, endpoint, save_file=False):
    """
    A global function to get remote data for other cluster nodes based
    metrics_manager get_remote_data.

    """

    current_skyline_app_logger = current_skyline_app + 'Log'
    current_logger = logging.getLogger(current_skyline_app_logger)
    remote_data = []
    try:
        connect_timeout = int(settings.GRAPHITE_CONNECT_TIMEOUT)
        read_timeout = int(settings.GRAPHITE_READ_TIMEOUT)
    except:
        connect_timeout = 5
        read_timeout = 10
    use_timeout = (int(connect_timeout), int(read_timeout))

    for remote_skyline_instance in REMOTE_SKYLINE_INSTANCES:
        data = []
        r = None
        user = None
        password = None
        use_auth = False
        try:
            user = str(remote_skyline_instance[1])
            password = str(remote_skyline_instance[2])
            use_auth = True
        except:
            user = None
            password = None
        try:
            url = '%s/%s' % (str(remote_skyline_instance[0]), endpoint)
            if use_auth:
                r = requests.get(url, timeout=use_timeout, auth=(user, password))
            else:
                r = requests.get(url, timeout=use_timeout)
        except Exception as err:
            current_logger.error(traceback.format_exc())
            current_logger.error('error :: get_remote_data :: failed to get %s from %s, err: %s' % (
                str(endpoint), str(remote_skyline_instance[0]), err))
        if not r:
            current_logger.info('warning :: get_remote_data :: no r from %s on %s' % (
                endpoint, str(remote_skyline_instance[0])))
            continue
        if r:
            if r.status_code != 200:
                current_logger.error('error :: get_remote_data :: %s from %s responded with status code %s and reason %s' % (
                    endpoint, str(remote_skyline_instance[0]), str(r.status_code), str(r.reason)))
            if save_file and r.status_code == 200:
                file_saved = False
                try:
                    open(save_file, 'wb').write(r.content)
                    if not os.path.isfile(save_file):
                        current_logger.error('error :: get_remote_data :: failed to save_file %s from %s' % (
                            str(save_file), str(remote_skyline_instance[0])))
                    else:
                        file_saved = True
                except Exception as err:
                    current_logger.error(traceback.format_exc())
                    current_logger.error('error :: get_remote_data :: failed to get %s from %s, err: %s' % (
                        endpoint, str(remote_skyline_instance[0]), err))
                return file_saved
            js = None
            try:
                js = r.json()
            except Exception as err:
                current_logger.error(traceback.format_exc())
                current_logger.error('error :: get_remote_data :: failed to get json from the response from %s on %s, err: %s' % (
                    endpoint, str(remote_skyline_instance), err))
                current_logger.debug('debug :: get_remote_data :: data_required: %s, endpoint: %s, save_file: %s' % (
                    str(data_required), str(endpoint), str(save_file)))
            if js:
                if LOCAL_DEBUG:
                    current_logger.debug('debug :: get_remote_data :: got response for %s from %s' % (
                        str(data_required), str(remote_skyline_instance[0])))
                try:
                    data = js['data'][data_required]
                    if LOCAL_DEBUG:
                        current_logger.debug('debug :: get_remote_data :: response for %s has %s items' % (
                            str(data_required), str(len(data))))
                    remote_data.append(data)
                except Exception as err:
                    current_logger.error(traceback.format_exc())
                    current_logger.error('error :: get_remote_data :: failed to build data from %s on %s, err: %s' % (
                        str(data_required), str(remote_skyline_instance), err))
    return remote_data