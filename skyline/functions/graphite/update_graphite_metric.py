"""
update_graphite_metric.py
"""
import logging
import socket
import traceback

try:
    from settings import CARBON_HOST
except:
    CARBON_HOST = ''
try:
    from settings import CARBON_PORT
except:
    CARBON_PORT = ''


# @added 20240618 - Feature #5372: vista - bq_update
def update_graphite_metric(current_skyline_app, base_name, timestamp, value):
    """
    Update a metric value at a timestamp in Graphite via the relay line protocol
    port so that it gets replicated if a Graphite cluster configuration is in
    use.

    :param current_skyline_app: the skyline_app
    :param base_name: the base_name of the metric to update
    :param timestamp: the timestamp to update
    :param value: the value
    :type current_skyline_app: str
    :type base_name: str
    :type timestamp: int
    :type value: float
    :return: changed
    :rtype: dict

    """

    endpoint = '%s:%d' % (CARBON_HOST, CARBON_PORT)
    try:
        sock = socket.socket()
        sock.settimeout(3)
        try:
            sock.connect((CARBON_HOST, CARBON_PORT))
            sock.settimeout(None)
        except socket.error:
            sock.settimeout(None)
        try:
            message = '%s %s %i\n' % (base_name, str(value), int(timestamp))
            sock.sendall(message.encode())
            sock.close()
            return True
        except Exception as err:
            current_skyline_app_logger = str(current_skyline_app) + 'Log'
            current_logger = logging.getLogger(current_skyline_app_logger)
            current_logger.error('error :: update_graphite_metric :: could not send data to Graphite at %s, err: %s' % (
                endpoint, err))
            return False
    except Exception as err:
        current_skyline_app_logger = str(current_skyline_app) + 'Log'
        current_logger = logging.getLogger(current_skyline_app_logger)
        current_logger.error(traceback.format_exc())
        current_logger.error('error :: update_graphite_metric :: socket failed to Graphite at %s, err: %s' % (
            endpoint, err))
    return False
