"""
send_long_full_duration_metrics_to_victoriametrics.py
"""
import logging
import socket
import traceback
from time import time

import settings
from skyline_functions import get_redis_conn_decoded

try:
    VICTORIAMETRICS_ENABLED = settings.VICTORIAMETRICS_ENABLED
except:
    VICTORIAMETRICS_ENABLED = False
vm_host = None
if VICTORIAMETRICS_ENABLED:
    try:
        vm_host = settings.VICTORIAMETRICS_OPTS['public_ipaddress']
    except:
        vm_host = None
    if not vm_host:
        try:
            vm_host = settings.VICTORIAMETRICS_OPTS['host']
        except:
            vm_host = None
vm_graphite_port = 8403
if VICTORIAMETRICS_ENABLED:
    try:
        vm_graphite_port = int(float(settings.VICTORIAMETRICS_OPTS['graphite_port']))
    except:
        vm_graphite_port = 8403


# @added 20250331 - Feature #5614: VictoriaMetrics - LONGTERM_FULL_DURATION_NAMESPACES
def send_long_full_duration_metrics_to_victoriametrics(current_skyline_app):
    """
    Fetch data from victoriametrics and return it as object, a graph image or
    save it as file

    :param current_skyline_app: the skyline app using this function
    :type current_skyline_app: str
    :return: sent_count
    :rtype: int

    """

    function_str = 'send_long_full_duration_metrics_to_victoriametrics'
    sent_count = 0

    current_skyline_app_logger = str(current_skyline_app) + 'Log'
    current_logger = logging.getLogger(current_skyline_app_logger)

    if not VICTORIAMETRICS_ENABLED:
        current_logger.error('error :: %s :: send_long_full_duration_metrics_to_victoriametrics was called but VICTORIAMETRICS_ENABLED is not set to True' % (
            function_str))
        return sent_count
    if not vm_host:
        current_logger.error('error :: %s :: send_long_full_duration_metrics_to_victoriametrics was called but vm_host is set to None' % (
            function_str))
        return sent_count

    redis_conn_decoded = None
    try:
        redis_conn_decoded = get_redis_conn_decoded(current_skyline_app)
    except Exception as err:
        current_logger.error('error :: %s :: get_redis_conn_decoded failed, err: %s' % (
            function_str, err))

    long_full_duration_metrics_data_csv_strs = []
    last_now = int(time()) - 60
    last_aligned_ts = int(last_now // 60 * 60)
    long_full_duration_metrics_data_redis_key = 'skyline.long_full_duration_metrics.%s' % str(last_aligned_ts)
    current_logger.info('%s :: checking for long_full_duration_metrics in %s' % (
        function_str, long_full_duration_metrics_data_redis_key))
    try:
        long_full_duration_metrics_data_csv_strs = list(redis_conn_decoded.smembers(long_full_duration_metrics_data_redis_key))
    except Exception as err:
        current_logger.error('error :: %s :: smembers failed on %s, err: %s' % (
            function_str, long_full_duration_metrics_data_redis_key, err))
    current_logger.info('%s :: %s items from %s' % (
        function_str, str(len(long_full_duration_metrics_data_csv_strs)),
        long_full_duration_metrics_data_redis_key))

    errors = []
    victoriametrics_data_list = []
    for metric_data_csv_string in long_full_duration_metrics_data_csv_strs:
        try:
            metric_data = metric_data_csv_string.split(',')
            metric_data_str = '%s %s %s' % (
                metric_data[0], str(float(metric_data[1])),
                str(int(metric_data[2])))
            victoriametrics_data_list.append(metric_data_str)
        except Exception as err:
            errors.append(['failed to add', str(metric_data_csv_string), err])

    current_logger.info('%s :: %s long_full_duration metrics to send to %s' % (
        function_str, str(len(long_full_duration_metrics_data_csv_strs)),
        str(vm_host)))

    if victoriametrics_data_list:
        # To send victoriametrics limit to 500 metrics per socket submission
        victoriametrics_submissions = []
        for i in range(0, len(victoriametrics_data_list), 500):
            victoriametrics_submissions.append(victoriametrics_data_list[i:(i + 500)])
        for victoriametrics_data in victoriametrics_submissions:
            batch_size = len(victoriametrics_data)
            data = None
            if batch_size > 0:
                try:
                    data = '\n'.join(f'{item}' for item in victoriametrics_data) + '\n'
                except Exception as err:
                    current_logger.error('error :: %s :: failed to format data to send %s, err: %s' % (
                        function_str, str(vm_host), err))
                    data = None
            if data:
                try:
                    sock = socket.socket()
                    sock.settimeout(5)
                    try:
                        sock.connect((vm_host, vm_graphite_port))
                        sock.settimeout(None)
                    except socket.error:
                        sock.settimeout(None)
                    try:
                        sock.sendall(data.encode())
                        sock.close()
                        sent_count += batch_size
                        current_logger.info('%s :: sent batch of %s Graphite metrics to %s' % (
                            function_str, str(batch_size), vm_host))
                    except Exception as err:
                        current_logger.error('error :: %s :: could not send data to %s, err: %s' % (
                            function_str, str(vm_host), err))
                except Exception as err:
                    current_logger.error(traceback.format_exc())
                    current_logger.error('error :: %s :: socket failed to %s, err: %s' % (
                        function_str, str(vm_host), err))
        del victoriametrics_submissions

    return sent_count
