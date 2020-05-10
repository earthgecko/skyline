"""
Skyline functions

These are shared functions that are required in multiple modules.
"""
import logging
from os import path
from time import time
import socket
import datetime
import errno
import os

import traceback
import json
import requests
# @modified 20191025 - Task #3290: Handle urllib2 in py3
#                      Branch #3262: py3
# try:
#     import urllib2
# except ImportError:
#     import urllib.request
#     import urllib.error

# @added 20191114 - Bug #3266: py3 Redis binary objects not strings
#                   Branch #3262: py3
# Added a single functions to deal with Redis connection and the
# charset='utf-8', decode_responses=True arguments required in py3
from redis import StrictRedis

import settings

try:
    # @modified 20190518 - Branch #3002: docker
    # from settings import GRAPHITE_HOST
    from settings import CARBON_HOST
except:
    # @modified 20190518 - Branch #3002: docker
    # GRAPHITE_HOST = ''
    CARBON_HOST = ''
try:
    from settings import CARBON_PORT
except:
    CARBON_PORT = ''

# @added 20191007 - Feature #3250: Allow Skyline to send metrics to another Carbon host
try:
    from settings import SKYLINE_METRICS_CARBON_HOST
    skyline_metrics_carbon_host = SKYLINE_METRICS_CARBON_HOST
except:
    skyline_metrics_carbon_host = CARBON_HOST
try:
    from settings import SKYLINE_METRICS_CARBON_PORT
    skyline_metrics_carbon_port = SKYLINE_METRICS_CARBON_PORT
except:
    skyline_metrics_carbon_port = CARBON_PORT

config = {'user': settings.PANORAMA_DBUSER,
          'password': settings.PANORAMA_DBUSERPASS,
          'host': settings.PANORAMA_DBHOST,
          'port': settings.PANORAMA_DBPORT,
          'database': settings.PANORAMA_DATABASE,
          'raise_on_warnings': True}


# @added 20191025 - Bug #3266: py3 Redis binary objects not strings
#                   Branch #3262: py3
# Added a single functions to deal with Redis connection and the
# charset='utf-8', decode_responses=True arguments required in py3
def get_redis_conn(current_skyline_app):
    """
    Get a Redis connection

    :param current_skyline_app: the skyline app using this function
    :type current_skyline_app: str
    :return: REDIS_CONN
    :rtype: object

    """
    current_skyline_app_logger = str(current_skyline_app) + 'Log'
    current_logger = logging.getLogger(current_skyline_app_logger)

    REDIS_CONN = None
    try:
        if settings.REDIS_PASSWORD:
            REDIS_CONN = StrictRedis(
                password=settings.REDIS_PASSWORD,
                unix_socket_path=settings.REDIS_SOCKET_PATH)
        else:
            REDIS_CONN = StrictRedis(unix_socket_path=settings.REDIS_SOCKET_PATH)
    except:
        from redis import StrictRedis
    if not REDIS_CONN:
        try:
            if settings.REDIS_PASSWORD:
                REDIS_CONN = StrictRedis(
                    password=settings.REDIS_PASSWORD,
                    unix_socket_path=settings.REDIS_SOCKET_PATH)
            else:
                REDIS_CONN = StrictRedis(unix_socket_path=settings.REDIS_SOCKET_PATH)
        except:
            current_logger.info(traceback.format_exc())
            current_logger.error('error :: %s - redis connection failed' % current_skyline_app)
            return None
    return REDIS_CONN


def get_redis_conn_decoded(current_skyline_app):
    """
    Get a Redis connection with decoded responses, to read sets

    :param current_skyline_app: the skyline app using this function
    :type current_skyline_app: str
    :return: REDIS_CONN_DECODED
    :rtype: object

    """
    try:
        python_version
    except:
        from sys import version_info
        python_version = int(version_info[0])

    current_skyline_app_logger = str(current_skyline_app) + 'Log'
    current_logger = logging.getLogger(current_skyline_app_logger)

    REDIS_CONN_DECODED = None
    try:
        if settings.REDIS_PASSWORD:
            if python_version == 2:
                REDIS_CONN_DECODED = StrictRedis(
                    password=settings.REDIS_PASSWORD,
                    unix_socket_path=settings.REDIS_SOCKET_PATH)
            else:
                REDIS_CONN_DECODED = StrictRedis(
                    password=settings.REDIS_PASSWORD,
                    unix_socket_path=settings.REDIS_SOCKET_PATH,
                    charset='utf-8', decode_responses=True)
        else:
            if python_version == 2:
                REDIS_CONN_DECODED = StrictRedis(unix_socket_path=settings.REDIS_SOCKET_PATH)
            else:
                REDIS_CONN_DECODED = StrictRedis(
                    unix_socket_path=settings.REDIS_SOCKET_PATH,
                    charset='utf-8', decode_responses=True)
    except:
        from redis import StrictRedis
    if not REDIS_CONN_DECODED:
        try:
            if settings.REDIS_PASSWORD:
                if python_version == 2:
                    REDIS_CONN_DECODED = StrictRedis(
                        password=settings.REDIS_PASSWORD,
                        unix_socket_path=settings.REDIS_SOCKET_PATH)
                else:
                    REDIS_CONN_DECODED = StrictRedis(
                        password=settings.REDIS_PASSWORD,
                        unix_socket_path=settings.REDIS_SOCKET_PATH,
                        charset='utf-8', decode_responses=True)
            else:
                if python_version == 2:
                    REDIS_CONN_DECODED = StrictRedis(unix_socket_path=settings.REDIS_SOCKET_PATH)
                else:
                    REDIS_CONN_DECODED = StrictRedis(
                        unix_socket_path=settings.REDIS_SOCKET_PATH,
                        charset='utf-8', decode_responses=True)
        except:
            current_logger.info(traceback.format_exc())
            current_logger.error('error :: %s - redis decoded connection failed' % current_skyline_app)
            return None
    return REDIS_CONN_DECODED


def send_graphite_metric(current_skyline_app, metric, value):
    """
    Sends the skyline_app metrics to the `GRAPHITE_HOST` if a graphite
    host is defined.

    :param current_skyline_app: the skyline app using this function
    :param metric: the metric namespace
    :param value: the metric value (as a str not an int)
    :type current_skyline_app: str
    :type metric: str
    :type value: str
    :return: ``True`` or ``False``
    :rtype: boolean

    """

    # @added 20190805 - Task #2828: Skyline - Python 3.7
    try:
        python_version
    except:
        from sys import version_info
        python_version = int(version_info[0])

    # @added 20190518 - Branch #3002: docker
    # If the CARBON_HOST is set to the default do not send_graphite_metric
    # @modified 20191007 - Feature #3250: Allow Skyline to send metrics to another Carbon host
    # if CARBON_HOST == 'YOUR_GRAPHITE_HOST.example.com':
    if skyline_metrics_carbon_host == 'YOUR_GRAPHITE_HOST.example.com':
        current_skyline_app_logger = str(current_skyline_app) + 'Log'
        current_logger = logging.getLogger(current_skyline_app_logger)
        current_logger.info('CARBON_HOST is not configured in settings.py no CARBON_HOST to send metrics to')
        return False

    # @modified 20190518 - Branch #3002: docker
    # Use the CARBON_HOST rather than GRAPHITE_HOST to allow for the 2 to be
    # on different hosts
    # if GRAPHITE_HOST != '':
    # @modified 20191007 - Feature #3250: Allow Skyline to send metrics to another Carbon host
    # if CARBON_HOST != '':
    if skyline_metrics_carbon_host != '':
        sock = socket.socket()
        sock.settimeout(10)

        # Handle connection error to Graphite #116 @etsy
        # Fixed as per https://github.com/etsy/skyline/pull/116 and
        # mlowicki:etsy_handle_connection_error_to_graphite
        # Handle connection error to Graphite #7 @ earthgecko
        # merged 1 commit into earthgecko:master from
        # mlowicki:handle_connection_error_to_graphite on 16 Mar 2015
        try:
            # @modified 20190518 - Branch #3002: docker
            # sock.connect((GRAPHITE_HOST, CARBON_PORT))
            # @modified 20191007 - Feature #3250: Allow Skyline to send metrics to another Carbon host
            # sock.connect((CARBON_HOST, CARBON_PORT))
            sock.connect((skyline_metrics_carbon_host, skyline_metrics_carbon_port))
            sock.settimeout(None)
        except socket.error:
            sock.settimeout(None)
            # @modified 20190518 - Branch #3002: docker
            # endpoint = '%s:%d' % (GRAPHITE_HOST, CARBON_PORT)
            # @modified 20191007 - Feature #3250: Allow Skyline to send metrics to another Carbon host
            # endpoint = '%s:%d' % (CARBON_HOST, CARBON_PORT)
            endpoint = '%s:%d' % (skyline_metrics_carbon_host, skyline_metrics_carbon_port)
            current_skyline_app_logger = str(current_skyline_app) + 'Log'
            current_logger = logging.getLogger(current_skyline_app_logger)
            current_logger.error(
                'error :: cannot connect to Graphite at %s' % endpoint)
            return False

        # For the same reason as above
        # sock.sendall('%s %s %i\n' % (name, value, time()))
        try:
            # @modified 20190805 - Task #2828: Skyline - Python 3.7
            if python_version == 2:
                sock.sendall('%s %s %i\n' % (metric, value, time()))
            if python_version == 3:
                message = '%s %s %i\n' % (metric, value, time())
                sock.sendall(message.encode())
            sock.close()
            return True
        except:
            # @modified 20190518 - Branch #3002: docker
            # endpoint = '%s:%d' % (GRAPHITE_HOST, CARBON_PORT)
            # @modified 20191007 - Feature #3250: Allow Skyline to send metrics to another Carbon host
            # endpoint = '%s:%d' % (CARBON_HOST, CARBON_PORT)
            endpoint = '%s:%d' % (skyline_metrics_carbon_host, skyline_metrics_carbon_port)
            current_skyline_app_logger = str(current_skyline_app) + 'Log'
            current_logger = logging.getLogger(current_skyline_app_logger)
            current_logger.error(
                'error :: could not send data to Graphite at %s' % endpoint)
            return False

    return False


def mkdir_p(path):
    """
    Create nested directories.

    :param path: directory path to create
    :type path: str
    :return: returns True

    """
    try:
        os.getpid()
    except:
        import os
    try:
        os.makedirs(path, mode=0o755)
        return True
    # Python >2.5
    except OSError as exc:
        if exc.errno == errno.EEXIST and os.path.isdir(path):
            pass
        else:
            raise


# @added 20191023 - Task #3290: Handle urllib2 in py3
#                   Branch #3262: py3
# Use urlretrieve
def get_graphite_graph_image(current_skyline_app, url=None, image_file=None):
    """
    Fetches a Graphite graph image of a metric and saves the image to the
    specified file.

    :param current_skyline_app: the app calling the function so the function
        knows which log to write too.
    :param url: the graph URL
    :param image_file: the absolute path and file name to save the graph png image
        as.
    :type current_skyline_app: str
    :type url: str
    :type image_file: str
    :return: True
    :rtype:  boolean

    """
    try:
        python_version
    except:
        from sys import version_info
        python_version = int(version_info[0])
    try:
        urllib.urlretrieve
    except:
        try:
            import urllib
        except:
            # For backwards compatibility with py2 load urlib.request as urllib so
            # that urllib.urlretrieve is available to both as the same module.
            # from urllib import request as urllib
            import urllib.request
            import urllib.error

    current_skyline_app_logger = str(current_skyline_app) + 'Log'
    current_logger = logging.getLogger(current_skyline_app_logger)

    if not url:
        fail_msg = 'error :: %s :: get_graphite_graph_image :: URL - %s' % (
            str(current_skyline_app), str(url), str(image_file))
        current_logger.error(fail_msg)
        return False
    if not image_file:
        fail_msg = 'error :: %s :: get_graphite_graph_image :: image_file - %s' % (
            str(current_skyline_app), str(image_file))
        current_logger.error(fail_msg)
        return False

    file_dir = os.path.dirname(image_file)
    if not os.path.exists(file_dir):
        try:
            os.makedirs(file_dir, mode=0o755)
        # Python >2.5
        except OSError as exc:
            if exc.errno == errno.EEXIST and os.path.isdir(path):
                pass
            else:
                raise

    if not os.path.exists(file_dir):
        current_logger.error(
            'error :: %s :: get_graphite_graph_image - could not create directory - %s' % (str(file_dir)))

    try:
        current_logger.info('%s :: get_graphite_graph_image :: saving %s to %s' % (
            str(current_skyline_app), str(url), str(image_file)))
        if python_version == 2:
            urllib.urlretrieve(url, image_file)
            os.chmod(image_file, 0o644)
        if python_version == 3:
            urllib.request.urlretrieve(url, image_file)
            os.chmod(image_file, mode=0o644)
        current_logger.info('%s :: get_graphite_graph_image :: saved %s to %s' % (
            str(current_skyline_app), str(url), str(image_file)))
    except:
        current_logger.error(traceback.format_exc())
        fail_msg = 'error :: %s :: get_graphite_graph_image :: failed to save %s to %s' % (
            str(current_skyline_app), str(url), str(image_file))
        current_logger.error(fail_msg)
        if current_skyline_app == 'webapp':
            # Raise to webbapp
            raise
        else:
            return False
    return True


def load_metric_vars(current_skyline_app, metric_vars_file):
    """
    Import the metric variables for a check from a metric check variables file

    :param current_skyline_app: the skyline app using this function
    :param metric_vars_file: the path and filename to the metric variables files
    :type current_skyline_app: str
    :type metric_vars_file: str
    :return: the metric_vars module object or ``False``
    :rtype: object or boolean

    """
    try:
        os.getpid()
    except:
        import os

    try:
        imp
    except:
        import imp

    metric_vars = False
    metric_vars_got = False
    if os.path.isfile(metric_vars_file):
        current_skyline_app_logger = str(current_skyline_app) + 'Log'
        current_logger = logging.getLogger(current_skyline_app_logger)
        current_logger.info(
            'loading metric variables from import - metric_check_file - %s' % (
                str(metric_vars_file)))
        # Bug #1460: panorama check file fails
        # global metric_vars
        # current_logger.info('set global metric_vars')
        with open(metric_vars_file) as f:
            try:
                metric_vars = imp.load_source('metric_vars', '', f)
                metric_vars_got = True
            except:
                current_logger.info(traceback.format_exc())
                msg = 'failed to import metric variables - metric_check_file'
                current_logger.error(
                    'error :: %s - %s' % (msg, str(metric_vars_file)))
                metric_vars = False

        if settings.ENABLE_DEBUG and metric_vars_got:
            current_logger.info(
                'metric_vars determined - metric variable - metric - %s' % str(metric_vars.metric))
    else:
        current_logger.error('error :: metric_vars_file not found - %s' % (str(metric_vars_file)))

    return metric_vars


def write_data_to_file(current_skyline_app, write_to_file, mode, data):
    """
    Write date to a file

    :param current_skyline_app: the skyline app using this function
    :param file: the path and filename to write the data into
    :param mode: ``w`` to overwrite, ``a`` to append
    :param data: the data to write to the file
    :type current_skyline_app: str
    :type file: str
    :type mode: str
    :type data: str
    :return: ``True`` or ``False``
    :rtype: boolean

    """
    try:
        os.getpid()
    except:
        import os

    try:
        python_version
    except:
        from sys import version_info
        python_version = int(version_info[0])

    file_dir = os.path.dirname(write_to_file)
    if not os.path.exists(file_dir):
        try:
            os.makedirs(file_dir, mode=0o755)
        # Python >2.5
        except OSError as exc:
            if exc.errno == errno.EEXIST and os.path.isdir(path):
                pass
            else:
                raise

    if not os.path.exists(file_dir):
        current_skyline_app_logger = str(current_skyline_app) + 'Log'
        current_logger = logging.getLogger(current_skyline_app_logger)
        current_logger.error(
            'error :: could not create directory - %s' % (str(file_dir)))

    try:
        with open(write_to_file, mode) as fh:
            fh.write(data)
        if python_version == 2:
            os.chmod(write_to_file, 0o644)
        if python_version == 3:
            os.chmod(write_to_file, mode=0o644)

        return True
    except:
        return False

    return False


def fail_check(current_skyline_app, failed_check_dir, check_file_to_fail):
    """
    Move a failed check file.

    :param current_skyline_app: the skyline app using this function
    :param failed_check_dir: the directory where failed checks are moved to
    :param check_file_to_fail: failed check file to move
    :type current_skyline_app: str
    :type failed_check_dir: str
    :type check_file_to_fail: str
    :return: ``True``, ``False``
    :rtype: boolean

    """
    try:
        os.getpid()
    except:
        import os

    try:
        shutil
    except:
        import shutil

    try:
        python_version
    except:
        from sys import version_info
        python_version = int(version_info[0])

    current_skyline_app_logger = str(current_skyline_app) + 'Log'
    current_logger = logging.getLogger(current_skyline_app_logger)

    if not os.path.exists(failed_check_dir):
        try:
            mkdir_p(failed_check_dir)
            current_logger.info(
                'created failed_check_dir - %s' % str(failed_check_dir))
        except:
            current_logger.info(traceback.format_exc())
            current_logger.error(
                'error :: failed to create failed_check_dir - %s' %
                str(failed_check_dir))
            return False

    check_file_name = os.path.basename(str(check_file_to_fail))
    failed_check_file = '%s/%s' % (failed_check_dir, check_file_name)

    try:
        shutil.move(check_file_to_fail, failed_check_file)
        if python_version == 2:
            os.chmod(failed_check_file, 0o644)
        if python_version == 3:
            os.chmod(failed_check_file, mode=0o644)

        current_logger.info('moved check file to - %s' % failed_check_file)
        return True
    except OSError:
        current_logger.error(traceback.format_exc())
        msg = 'failed to move check file to -%s' % failed_check_file
        current_logger.error('error :: %s' % msg)
        pass

    return False


def get_graphite_metric(
    current_skyline_app, metric, from_timestamp, until_timestamp, data_type,
        output_object):
    """
    Fetch data from graphite and return it as object or save it as file

    :param current_skyline_app: the skyline app using this function
    :param metric: metric name
    :param from_timestamp: unix timestamp
    :param until_timestamp: unix timestamp
    :param data_type: image or json
    :param output_object: object or path and filename to save data as, if set to
                          object, the object is returned
    :type current_skyline_app: str
    :type metric: str
    :type from_timestamp: str
    :type until_timestamp: str
    :type data_type: str
    :type output_object: str
    :return: timeseries string, ``True``, ``False``
    :rtype: str or boolean

    """
    try:
        os.getpid()
    except:
        import os

    try:
        python_version
    except:
        from sys import version_info
        python_version = int(version_info[0])

    try:
        quote(current_skyline_app, safe='')
    except:
        from requests.utils import quote

    try:
        time.time()
    except:
        import time

    try:
        re
    except:
        import re

    # @added 20191025 - Task #3290: Handle urllib2 in py3
    #                   Branch #3262: py3
#    try:
#        urllib.urlretrieve
#    except:
#        try:
#            import urllib as urllib
#        except:
#            import urllib.request
#            import urllib.error

    current_skyline_app_logger = str(current_skyline_app) + 'Log'
    current_logger = logging.getLogger(current_skyline_app_logger)

#    if settings.ENABLE_DEBUG:
    current_logger.info('graphite_metric - %s' % (metric))

    # @added 20160803 - Unescaped Graphite target - https://github.com/earthgecko/skyline/issues/20
    #                   bug1546: Unescaped Graphite target
    # @modified 20191029 - Branch #3263: py3
    # Commented out colon
    # new_metric_namespace = metric.replace(':', '\:')
    # metric_namespace = new_metric_namespace.replace('(', '\(')
    metric_namespace = metric.replace('(', '\(')
    metric = metric_namespace.replace(')', '\)')

    # Graphite timeouts
    connect_timeout = int(settings.GRAPHITE_CONNECT_TIMEOUT)
    if settings.ENABLE_DEBUG:
        current_logger.info('connect_timeout - %s' % str(connect_timeout))

    current_logger.info('connect_timeout - %s' % str(connect_timeout))

    read_timeout = int(settings.GRAPHITE_READ_TIMEOUT)
    if settings.ENABLE_DEBUG:
        current_logger.info('read_timeout - %s' % str(read_timeout))

    current_logger.info('read_timeout - %s' % str(read_timeout))

    graphite_from = datetime.datetime.fromtimestamp(int(from_timestamp)).strftime('%H:%M_%Y%m%d')
    current_logger.info('graphite_from - %s' % str(graphite_from))

    graphite_until = datetime.datetime.fromtimestamp(int(until_timestamp)).strftime('%H:%M_%Y%m%d')
    current_logger.info('graphite_until - %s' % str(graphite_until))

    output_format = data_type

    # @modified 20170603 - Feature #2034: analyse_derivatives
    # Added deriative functions to convert the values of metrics strictly
    # increasing monotonically to their deriative products in Graphite now
    # graphs
    known_derivative_metric = False
    REDIS_CONN_DECODED = None
    try:
        REDIS_CONN_DECODED = get_redis_conn_decoded(current_skyline_app)
    except:
        current_logger.error(traceback.format_exc())
        current_logger.error('error :: %s :: get_graphite_metric - failed to get_redis_conn')

#    try:
#        # @modified 20180519 - Feature #2378: Add redis auth to Skyline and rebrow
#        if settings.REDIS_PASSWORD:
#            REDIS_CONN = StrictRedis(password=settings.REDIS_PASSWORD, unix_socket_path=settings.REDIS_SOCKET_PATH)
#        else:
#            REDIS_CONN = StrictRedis(unix_socket_path=settings.REDIS_SOCKET_PATH)
#    except:
#        from redis import StrictRedis
#    if not REDIS_CONN:
#        try:
#            # @modified 20180519 - Feature #2378: Add redis auth to Skyline and rebrow
#            if settings.REDIS_PASSWORD:
#                REDIS_CONN = StrictRedis(password=settings.REDIS_PASSWORD, unix_socket_path=settings.REDIS_SOCKET_PATH)
#            else:
#                REDIS_CONN = StrictRedis(unix_socket_path=settings.REDIS_SOCKET_PATH)
#        except:
#            current_logger.error('error :: %s - redis connection failed' % current_skyline_app)

    try:
        derivative_metrics = list(REDIS_CONN_DECODED.smembers('derivative_metrics'))
    except:
        derivative_metrics = []
    redis_metric_name = '%s%s' % (settings.FULL_NAMESPACE, str(metric))

    # @added 20180423 - Feature #2034: analyse_derivatives
    #                   Branch #2270: luminosity
    metric_found_in_redis = False

    if redis_metric_name in derivative_metrics:
        known_derivative_metric = True
        metric_found_in_redis = True
    if known_derivative_metric:
        try:
            non_derivative_metrics = list(REDIS_CONN_DECODED.smembers('non_derivative_metrics'))
        except:
            non_derivative_metrics = []
        if redis_metric_name in non_derivative_metrics:
            known_derivative_metric = False
            metric_found_in_redis = True

    # @added 20180423 - Feature #2034: analyse_derivatives
    #                   Branch #2270: luminosity
    if not metric_found_in_redis and settings.OTHER_SKYLINE_REDIS_INSTANCES:
        # @modified 20180519 - Feature #2378: Add redis auth to Skyline and rebrow
        # for redis_ip, redis_port in settings.OTHER_SKYLINE_REDIS_INSTANCES:
        for redis_ip, redis_port, redis_password in settings.OTHER_SKYLINE_REDIS_INSTANCES:
            if not metric_found_in_redis:
                try:
                    if redis_password:
                        other_redis_conn = StrictRedis(host=str(redis_ip), port=int(redis_port), password=str(redis_password))
                    else:
                        other_redis_conn = StrictRedis(host=str(redis_ip), port=int(redis_port))
                    other_derivative_metrics = list(other_redis_conn.smembers('derivative_metrics'))
                except:
                    current_logger.error(traceback.format_exc())
                    current_logger.error('error :: failed to connect to Redis at %s on port %s' % (str(redis_ip), str(redis_port)))
                if redis_metric_name in other_derivative_metrics:
                    known_derivative_metric = True
                    metric_found_in_redis = True
                    current_logger.info('%s found in derivative_metrics in Redis at %s on port %s' % (redis_metric_name, str(redis_ip), str(redis_port)))

    target_metric = metric
    if known_derivative_metric:
        target_metric = 'nonNegativeDerivative(%s)' % str(metric)

    # graphite URL
    graphite_port = '80'
    if settings.GRAPHITE_PORT != '':
        graphite_port = str(settings.GRAPHITE_PORT)
    # @added 20191002 - Branch #3002: docker
    # Unset the graphite_port if normal https
    if settings.GRAPHITE_PORT == '443' and settings.GRAPHITE_PROTOCOL == 'https':
        graphite_port = ''

    # @modified 20190520 - Branch #3002: docker
    # image_url = settings.GRAPHITE_PROTOCOL + '://' + settings.GRAPHITE_HOST + ':' + graphite_port + '/render?from=' + graphite_from + '&until=' + graphite_until + '&target=' + target_metric
    # image_url = settings.GRAPHITE_PROTOCOL + '://' + settings.GRAPHITE_HOST + ':' + graphite_port + '/api/datasources/proxy/1/render/?from=' + graphite_from + '&until=' + graphite_until + '&target=' + target_metric
    image_url = settings.GRAPHITE_PROTOCOL + '://' + settings.GRAPHITE_HOST + ':' + graphite_port + '/' + settings.GRAPHITE_RENDER_URI + '?from=' + graphite_from + '&until=' + graphite_until + '&target=' + target_metric
    url = image_url + '&format=' + output_format

    current_logger.info('graphite url - %s' % (url))

    get_image = False
    if data_type == 'image' and output_object != 'object':
        if not os.path.isfile(output_object):
            get_image = True
        else:
            if settings.ENABLE_DEBUG:
                current_logger.info('graph file exists - %s' % str(output_object))
            return True

    if get_image:
        current_logger.info(
            'retrieving png - surfacing %s graph from graphite from %s to %s' % (
                metric, str(graphite_from), str(graphite_until)))

        graphite_image_file = output_object
        if 'width' not in image_url:
            image_url += '&width=586'
        if 'height' not in image_url:
            image_url += '&height=308'
        # @added 20170106 - Feature #1842: Ionosphere - Graphite now graphs
        # settings, color and title
        if str(current_skyline_app) == 'webapp':
            get_ionosphere_graphs = False
            if 'graphite_now' in output_object:
                get_ionosphere_graphs = True
            # @added 20170107 - Feature #1852: Ionosphere - features_profile matched graphite graphs
            # Added graphite_matched_images matched.fp_id
            if 'matched.fp_id' in output_object:
                get_ionosphere_graphs = True
            # @added 20170308 - Feature #1960: ionosphere_layers
            # Added graphite_layers_matched_images matched.layer
            if 'matched.layers.fp_id' in output_object:
                get_ionosphere_graphs = True
            if get_ionosphere_graphs:
                int_hours = int((int(until_timestamp) - int(from_timestamp)) / 60 / 60)
                if 'graphite_now' in output_object:
                    no_extension = os.path.splitext(output_object)[0]
                    _hours = os.path.splitext(no_extension)[1]
                    hours = _hours.replace('.', '')
                    int_hours = hours.replace('h', '')
                str_value = str(int_hours)
                period = 'hours'
                if int(int_hours) > 24:
                    str_value = str(int(int_hours) / 24)
                    period = 'days'
                if 'graphite_now' in output_object:
                    unencoded_graph_title = 'Graphite NOW at %s %s' % (str_value, period)
                # @added 20170308 - Feature #1960: ionosphere_layers
                matched_graph = False
                if 'matched.fp_id' in output_object:
                    matched_graph = True
                if 'matched.layers.fp_id' in output_object:
                    matched_graph = True
                # @modified 20170308 - Feature #1960: ionosphere_layers
                # if 'matched.fp_id' in output_object:
                if matched_graph:
                    human_date = time.strftime('%Y-%m-%d %H:%M:%S %Z', time.localtime(int(until_timestamp)))
                    if 'matched.fp_id' in output_object:
                        tail_piece = re.sub('.*\.fp_id-', '', output_object)
                        matched_fp_id = re.sub('\..*', '', tail_piece)
                        unencoded_graph_title = 'fp_id %s matched - %s at %s hours' % (
                            str(matched_fp_id), str(human_date),
                            str(int_hours))
                    if 'matched.layers.fp_id' in output_object:
                        # layers_id
                        tail_piece = re.sub('.*\.layers_id-', '', output_object)
                        matched_layers_id = re.sub('\..*', '', tail_piece)
                        unencoded_graph_title = 'layers_id %s matched - %s at %s hours' % (
                            str(matched_layers_id), str(human_date),
                            str(int_hours))
                graph_title_string = quote(unencoded_graph_title, safe='')
                graph_title = '&title=%s' % graph_title_string
                add_parameters = '%s&colorList=blue%s' % (settings.GRAPHITE_GRAPH_SETTINGS, graph_title)
                if 'matched.layers.fp_id' in output_object:
                    add_parameters = '%s&colorList=green%s' % (settings.GRAPHITE_GRAPH_SETTINGS, graph_title)
                image_url += add_parameters
                current_logger.info('Ionosphere graphite NOW url - %s' % (image_url))

        if settings.ENABLE_DEBUG:
            current_logger.info('graphite image url - %s' % (str(image_url)))
        # image_url_timeout = int(connect_timeout)

        image_data = None

        # @modified 20191025 - Task #3290: Handle urllib2 in py3
        #                      Branch #3262: py3
        # Use urlretrieve
        # try:
        #     # @modified 20170913 - Task #2160: Test skyline with bandit
        #     # Added nosec to exclude from bandit tests
        #     image_data = urllib2.urlopen(image_url, timeout=image_url_timeout).read()  # nosec
        #     current_logger.info('url OK - %s' % (image_url))
        # except urllib2.URLError:
        #     image_data = None
        #     current_logger.error('error :: url bad - %s' % (image_url))

        # @added 20191025 - Task #3290: Handle urllib2 in py3
        #                   Branch #3262: py3
        # Use urlretrieve
        try:
            image_data = get_graphite_graph_image(
                current_skyline_app, image_url, graphite_image_file)
        except:
            current_logger.error(traceback.format_exc())
            fail_msg = 'error :: %s :: get_graphite_graph_image :: failed to get_graphite_graph_image(%s, %s, %s)' % (
                str(current_skyline_app), str(url), str(graphite_image_file))
            current_logger.error(fail_msg)

        # if image_data is not None:
        if image_data == 'disabled_for_testing':
            with open(graphite_image_file, 'w') as f:
                f.write(image_data)
            current_logger.info('retrieved - %s' % (graphite_image_file))
            if python_version == 2:
                os.chmod(graphite_image_file, 0o644)
            if python_version == 3:
                os.chmod(graphite_image_file, mode=0o644)
        # else:
        #    current_logger.error(
        #         'error :: failed to retrieve - %s' % (graphite_image_file))

        if not os.path.isfile(graphite_image_file):
            msg = 'retrieve failed to surface %s graph from Graphite' % (metric)
            current_logger.error('error :: %s' % msg)
        # @added 20170107 - Feature #1842: Ionosphere - Graphite now graphs
        # In order to determine whether a Graphite image was retrieved or not
        # this needs to return here.  This should not be backwards incompatible
        # as it has never been used to determine the outcome before it appears,
        # which as they say is my bad.
            return False
        else:
            return True

    if data_type == 'json':

        if output_object != 'object':
            if os.path.isfile(output_object):
                return True

        msg = 'surfacing timeseries data for %s from graphite from %s to %s' % (
            metric, graphite_from, graphite_until)
        current_logger.info('%s' % msg)
        if requests.__version__ >= '2.4.0':
            use_timeout = (int(connect_timeout), int(read_timeout))
        else:
            use_timeout = int(connect_timeout)
        if settings.ENABLE_DEBUG:
            current_logger.info('use_timeout - %s' % (str(use_timeout)))

        graphite_json_fetched = False
        try:
            r = requests.get(url, timeout=use_timeout)
            graphite_json_fetched = True
        except:
            datapoints = [[None, str(graphite_until)]]
            current_logger.error('error :: data retrieval from Graphite failed')

        if graphite_json_fetched:
            try:
                js = r.json()
                datapoints = js[0]['datapoints']
                if settings.ENABLE_DEBUG:
                    current_logger.info('data retrieved OK')
            except:
                datapoints = [[None, str(graphite_until)]]
                current_logger.error('error :: failed to parse data points from retrieved json')

        converted = []
        for datapoint in datapoints:
            try:
                new_datapoint = [float(datapoint[1]), float(datapoint[0])]
                converted.append(new_datapoint)
            # @modified 20170913 - Task #2160: Test skyline with bandit
            # Added nosec to exclude from bandit tests
            except:  # nosec
                continue

        if output_object != 'object':
            with open(output_object, 'w') as f:
                f.write(json.dumps(converted))
            if python_version == 2:
                os.chmod(output_object, 0o644)
            if python_version == 3:
                os.chmod(output_object, mode=0o644)
            if settings.ENABLE_DEBUG:
                current_logger.info('json file - %s' % output_object)
        else:
            timeseries_json = json.dumps(converted)
            return timeseries_json

        if not os.path.isfile(output_object):
            current_logger.error(
                'error :: failed to surface %s json from graphite' % (metric))
            return False
        else:
            return True

    return True


# @added 20170206 - Bug #1904: Handle non filesystem friendly metric names in check files
def filesafe_metricname(metricname):
    '''
    Returns a file system safe name for a metric name in terms of creating
    check files, etc
    '''
    keepchars = ('.', '_', '-')
    try:
        sane_metricname = ''.join(c for c in str(metricname) if c.isalnum() or c in keepchars).rstrip()
        return str(sane_metricname)
    except:
        return False


# @added 20160922 - Branch #922: Ionosphere
# Added the send_anomalous_metric_to function for Analyzer and Mirage
# @modified 20161228 Feature #1828: ionosphere - mirage Redis data features
# Added full_duration which needs to be recorded to allow Mirage metrics
# to be profiled on Redis timeseries data at FULL_DURATION
# e.g. mirage.redis.24h.json
# @modified 20170127 - Feature #1886: Ionosphere learn - child like parent with evolutionary maturity
# Added parent_id, always zero from Analyzer and Mirage


def send_anomalous_metric_to(
    current_skyline_app, send_to_app, timeseries_dir, metric_timestamp,
        base_name, datapoint, from_timestamp, triggered_algorithms, timeseries,
        full_duration, parent_id):
    """
    Assign a metric and timeseries to Crucible or Ionosphere.
    """
    try:
        os.getpid()
    except:
        import os

    # @modified 20191115 - Branch #3262: py3
    # try:
    #     python_version
    # except:
    #     from sys import version_info
    #     python_version = int(version_info[0])

    current_skyline_app_logger = str(current_skyline_app) + 'Log'
    current_logger = logging.getLogger(current_skyline_app_logger)

    # @added 20170116 - Feature #1854: Ionosphere learn
    added_by_context = str(current_skyline_app)

    # @added 20170117 - Feature #1854: Ionosphere learn
    # Added the ionosphere_learn_to_ionosphere to fix ionosphere_learn not being
    # logged.
    if str(send_to_app) == 'ionosphere_learn_to_ionosphere':
        added_by_context = 'ionosphere_learn'
        new_send_to_app = 'ionosphere'
        send_to_app = new_send_to_app

    # @added 20170206 - Bug #1904: Handle non filesystem friendly metric names in check files
    sane_metricname = filesafe_metricname(str(base_name))

    if str(send_to_app) == 'crucible':
        anomaly_dir = '%s/%s/%s' % (
            settings.CRUCIBLE_DATA_FOLDER, str(timeseries_dir), str(metric_timestamp))
        check_file = '%s/%s.%s.txt' % (
            settings.CRUCIBLE_CHECK_PATH, str(metric_timestamp), str(sane_metricname))
        check_dir = '%s' % (settings.CRUCIBLE_CHECK_PATH)
    if str(send_to_app) == 'ionosphere':
        # @modified 20161121 - Branch #922: ionosphere
        # Use the timestamp as the parent dir for training_data, it is easier
        # to manage and clean on timestamps.  Walking through 1000s of Graphite
        # style dirs with dotted metric namespaces for timestamps.
        # anomaly_dir = settings.IONOSPHERE_DATA_FOLDER + '/' + timeseries_dir + '/' + metric_timestamp
        anomaly_dir = '%s/%s/%s' % (
            settings.IONOSPHERE_DATA_FOLDER, str(metric_timestamp),
            str(timeseries_dir))
        check_file = '%s/%s.%s.txt' % (
            settings.IONOSPHERE_CHECK_PATH, str(metric_timestamp),
            str(sane_metricname))
        check_dir = '%s' % (settings.IONOSPHERE_CHECK_PATH)

        # @added 20170116 - Feature #1854: Ionosphere learn
        # So as to not have to backport a context to all the instances of
        # send_anomalous_metric_to identify what this was added_by, here the
        # learn directory path string overrides the dirs if it is a learn
        # related check
        # @modified 20170117 - Feature #1854: Ionosphere learn
        # The ionosphere_learn_to_ionosphere to fix ionosphere_learn not being
        # logged.
        # if str(settings.IONOSPHERE_LEARN_FOLDER) in anomaly_dir:
        #     added_by_context = 'ionosphere_learn'
        if added_by_context == 'ionosphere_learn':
            current_logger.info('send_anomalous_metric_to :: this is an ionosphere_learn check')

    if not os.path.exists(check_dir):
        os.makedirs(check_dir, mode=0o755)

    if not os.path.exists(anomaly_dir):
        os.makedirs(anomaly_dir, mode=0o755)

    # Note:
    # The values are enclosed is single quoted intentionally
    # as the imp.load_source used in crucible results in a
    # shift in the decimal position when double quoted, e.g.
    # value = "5622.0" gets imported as
    # 2016-03-02 12:53:26 :: 28569 :: metric variable - value - 562.2
    # single quoting results in the desired,
    # 2016-03-02 13:16:17 :: 1515 :: metric variable - value - 5622.0
    now_timestamp = int(time())
    # @modified 20161228 Feature #1828: ionosphere - mirage Redis data features
    # Added full_duration
    # @modified 20170116 - Feature #1854: Ionosphere learn
    # Changed added_by parameter from current_skyline_app to added_by_context
    # @modified 20170127 - Feature #1886: Ionosphere learn - child like parent with evolutionary maturity
    # Added ionosphere_parent_id, always zero from Analyzer and Mirage
    anomaly_data = 'metric = \'%s\'\n' \
                   'value = \'%s\'\n' \
                   'from_timestamp = \'%s\'\n' \
                   'metric_timestamp = \'%s\'\n' \
                   'algorithms = %s\n' \
                   'triggered_algorithms = %s\n' \
                   'anomaly_dir = \'%s\'\n' \
                   'graphite_metric = True\n' \
                   'run_crucible_tests = False\n' \
                   'added_by = \'%s\'\n' \
                   'added_at = \'%s\'\n' \
                   'full_duration = \'%s\'\n' \
                   'ionosphere_parent_id = \'%s\'\n' \
        % (str(base_name), str(datapoint), str(from_timestamp),
            str(metric_timestamp), str(settings.ALGORITHMS),
            str(triggered_algorithms), anomaly_dir, added_by_context,
            str(now_timestamp), str(int(full_duration)), str(parent_id))

    # @modified 20170116 - Feature #1854: Ionosphere learn
    # In the Ionosphere context there is no requirement to create a timeseries
    # json file or anomaly_file, just the check
    if added_by_context != 'ionosphere_learn':

        # Create an anomaly file with details about the anomaly
        anomaly_file = '%s/%s.txt' % (anomaly_dir, str(base_name))
        try:
            write_data_to_file(str(current_skyline_app), anomaly_file, 'w', anomaly_data)
            current_logger.info('added %s anomaly file :: %s' % (str(send_to_app), anomaly_file))
        except:
            current_logger.info(traceback.format_exc())
            current_logger.error(
                'error :: failed to add %s anomaly file :: %s' %
                (str(send_to_app), anomaly_file))

        # Create timeseries json file with the timeseries
        json_file = '%s/%s.json' % (anomaly_dir, str(base_name))
        timeseries_json = str(timeseries).replace('[', '(').replace(']', ')')
        try:
            write_data_to_file(str(current_skyline_app), json_file, 'w', timeseries_json)
            current_logger.info('added %s timeseries file :: %s' % (str(send_to_app), json_file))
        except:
            current_logger.error(
                'error :: failed to add %s timeseries file :: %s' %
                (str(send_to_app), json_file))
            current_logger.info(traceback.format_exc())

    # Create a check file
    try:
        write_data_to_file(str(current_skyline_app), check_file, 'w', anomaly_data)
        current_logger.info(
            'added %s check :: %s,%s' % (
                str(send_to_app), str(base_name), str(metric_timestamp)))
    except:
        current_logger.info(traceback.format_exc())
        current_logger.error('error :: failed to add %s check file :: %s' % (str(send_to_app), check_file))


def RepresentsInt(s):
    '''
    As per http://stackoverflow.com/a/1267145 and @Aivar I must agree with
    @Triptycha > "This 5 line function is not a complex mechanism."
    '''
    try:
        int(s)
        return True
    except ValueError:
        return False


################################################################################


def mysql_select(current_skyline_app, select):
    """
    Select data from mysql database

    :param current_skyline_app: the Skyline app that is calling the function
    :param select: the select string
    :type current_skyline_app: str
    :type select: str
    :return: tuple
    :rtype: tuple, boolean

    - **Example usage**::

        from skyline_functions import mysql_select
        query = 'select id, metric from anomalies'
        results = mysql_select(query)

    - **Example of the 0 indexed results tuple, which can hold multiple results**::

        >> print('results: %s' % str(results))
        results: [(1, u'test1'), (2, u'test2')]

        >> print('results[0]: %s' % str(results[0]))
        results[0]: (1, u'test1')

    .. note::
        - If the MySQL query fails a boolean will be returned not a tuple
            * ``False``
            * ``None``

    """
    ENABLE_DEBUG = False
    try:
        if settings.ENABLE_PANORAMA_DEBUG:
            ENABLE_DEBUG = True
    except:
        ENABLE_DEBUG = False
    try:
        if settings.ENABLE_WEBAPP_DEBUG:
            ENABLE_DEBUG = True
    except:
        ENABLE_DEBUG = False
    try:
        if settings.ENABLE_IONOSPHERE_DEBUG:
            ENABLE_DEBUG = True
    except:
        ENABLE_DEBUG = False

    current_skyline_app_logger = str(current_skyline_app) + 'Log'
    current_logger = logging.getLogger(current_skyline_app_logger)
    if ENABLE_DEBUG:
        current_logger.info('debug :: entering mysql_select')

    try:
        mysql.connector
    except:
        import mysql.connector
    try:
        conversion
    except:
        from mysql.connector import conversion
    try:
        re
    except:
        import re

    try:
        cnx = mysql.connector.connect(**config)
        if ENABLE_DEBUG:
            current_logger.info('debug :: connected to mysql')
    except mysql.connector.Error as err:
        current_logger.error('error :: mysql error - %s' % str(err))
        current_logger.error('error :: failed to connect to mysql')
        return False

    if cnx:
        try:
            if ENABLE_DEBUG:
                current_logger.info('debug :: %s' % (str(select)))

            # NOTE: when a LIKE SELECT is made the query string is not run
            # through the conversion.MySQLConverter().escape method as it
            # it would not work and kept on returning mysql error - 1064 with
            # multiple single quotes e.g. use near '\'carbon%\'' at line 1
            # Various patterns were attempted to no avail, it seems to be
            # related to % character. pseudo basic HTTP auth has been added to
            # the webapp just in someone does not secure it properly, a little
            # defence in depth, so added WEBAPP_ALLOWED_IPS too.
            pattern_match = None
            try:
                pattern_match = re.search(' LIKE ', str(select))
            except:
                current_logger.error('error :: pattern_match - %s' % traceback.format_exc())
            if not pattern_match:
                try:
                    pattern_match = re.search(' like ', str(select))
                except:
                    current_logger.error('error :: pattern_match - %s' % traceback.format_exc())

            # TBD - not sure how to get it escaping safely
            pattern_match = True
            if pattern_match:
                query = str(select)
                if ENABLE_DEBUG:
                    current_logger.info('debug :: unescaped query - %s' % (str(query)))
                cursor = cnx.cursor()
                cursor.execute(query)
            else:
                query = conversion.MySQLConverter().escape(select)
                if ENABLE_DEBUG:
                    current_logger.info('debug :: escaped query - %s' % (str(query)))
                cursor = cnx.cursor()
                cursor.execute(query.format(query))

            # cursor = cnx.cursor()
            # cursor.execute(query)
            result = cursor.fetchall()
            cursor.close()
            cnx.close()
            return result
        except mysql.connector.Error as err:
            current_logger.error('error :: mysql error - %s' % str(err))
            current_logger.error(
                'error :: failed to query database - %s' % (str(select)))
            try:
                cnx.close()
                return False
            except:
                return False
    else:
        if ENABLE_DEBUG:
            current_logger.error('error - failed to connect to mysql')

    # Close the test mysql connection
    try:
        cnx.close()
        return False
    except:
        return False

    return False


# @added 20170602 - Feature #2034: analyse_derivatives
def nonNegativeDerivative(timeseries):
    """
    This function is used to convert an integral or incrementing count to a
    derivative by calculating the delta between subsequent datapoints.  The
    function ignores datapoints that trend down and is useful for metrics that
    increase over time and then reset.
    This based on part of the Graphite render function nonNegativeDerivative at:
    https://github.com/graphite-project/graphite-web/blob/1e5cf9f659f5d4cc0fa53127f756a1916e62eb47/webapp/graphite/render/functions.py#L1627
    """

    derivative_timeseries = []
    prev = None

    for timestamp, datapoint in timeseries:
        if None in (prev, datapoint):
            # derivative_timeseries.append((timestamp, None))
            prev = datapoint
            continue

        diff = datapoint - prev
        if diff >= 0:
            derivative_timeseries.append((timestamp, diff))
        # else:
        #    derivative_timeseries.append((timestamp, None))

        prev = datapoint

    return derivative_timeseries


def strictly_increasing_monotonicity(timeseries):
    """
    This function is used to determine whether timeseries is strictly increasing
    monotonically, it will only return True if the values are strictly
    increasing, an incrementing count.
    """
    import numpy as np

    test_ts = []
    for timestamp, datapoint in timeseries:
        # This only identifies and handles positive, strictly increasing
        # monotonic timeseries
        if datapoint < 0.0:
            return False
        test_ts.append(datapoint)

    # Exclude timeseries that are all the same value, these are not increasing
    if len(set(test_ts)) == 1:
        return False

    # Exclude timeseries that sum to 0, these are not increasing
    ts_sum = sum(test_ts[1:])
    if ts_sum == 0:
        return False

    diff_ts = np.asarray(test_ts)
    return np.all(np.diff(diff_ts) >= 0)


def in_list(metric_name, check_list):
    """
    Check if the metric is in list.

    # @added 20170602 - Feature #2034: analyse_derivatives
    #                   Feature #1978: worker - DO_NOT_SKIP_LIST
    This is a part copy of the SKIP_LIST allows for a string match or a match on
    dotted elements within the metric namespace used in Horizon/worker

    """

    metric_namespace_elements = metric_name.split('.')
    metric_in_list = False
    for in_list in check_list:
        if in_list in metric_name:
            metric_in_list = True
            break
        in_list_namespace_elements = in_list.split('.')
        elements_matched = set(metric_namespace_elements) & set(in_list_namespace_elements)
        if len(elements_matched) == len(in_list_namespace_elements):
            metric_in_list = True
            break

    if metric_in_list:
        return True

    return False


# @added 20170825 - Task #2132: Optimise Ionosphere DB usage
# Get the metric db object data to memcache
def get_memcache_metric_object(current_skyline_app, base_name):
    """
    Return the metrics_db_object from memcache if it exists.

    """
    try:
        pymemcache_Client
    except:
        from pymemcache.client.base import Client as pymemcache_Client

    try:
        literal_eval
    except:
        from ast import literal_eval

    # @added 20191029 - Task #3304: py3 - handle pymemcache bytes not str
    #                   Branch #3262: py3
    try:
        python_version
    except:
        from sys import version_info
        python_version = int(version_info[0])

    # @added 20191030 - Task #3304: py3 - handle pymemcache bytes not str
    #                   Branch #3262: py3
    if python_version == 3:
        try:
            codecs
        except:
            import codecs

    current_skyline_app_logger = str(current_skyline_app) + 'Log'
    current_logger = logging.getLogger(current_skyline_app_logger)

    if settings.MEMCACHE_ENABLED:
        memcache_client = pymemcache_Client((settings.MEMCACHED_SERVER_IP, settings.MEMCACHED_SERVER_PORT), connect_timeout=0.1, timeout=0.2)
    else:
        return False

    memcache_metrics_db_object = None
    metrics_db_object_key = 'metrics_db_object.%s' % str(base_name)
    memcache_result = None
    if settings.MEMCACHE_ENABLED:
        try:
            # @modified 20191029 - Task #3304: py3 - handle pymemcache bytes not str
            # memcache_result = memcache_client.get(metrics_db_object_key)
            if python_version == 2:
                memcache_result = memcache_client.get(metrics_db_object_key)
            else:
                # memcache_result = memcache_client.get(metrics_db_object_key)
                # @modified 20200109 - Task #3304: py3 - handle pymemcache bytes not str
                # Handle when memcache returns None as the new decode errors with
                # AttributeError: 'NoneType' object has no attribute 'decode'
                try:
                    memcache_result = memcache_client.get(metrics_db_object_key).decode('utf-8')
                except (TypeError, AttributeError):
                    memcache_result = None
        except:
            current_logger.error(traceback.format_exc())
            current_logger.error('error :: skyline_functions :: get_memcache_metric_object :: failed to get %s from memcache' % metrics_db_object_key)
        try:
            memcache_client.close()
        # @modified 20170913 - Task #2160: Test skyline with bandit
        # Added nosec to exclude from bandit tests
        except:  # nosec
            pass

    memcache_metric_dict = None
    if memcache_result:
        try:
            memcache_metrics_db_object = literal_eval(memcache_result)
            memcache_metric_dict = {}
            # for k, v in memcache_result_list:
            # @modified 20191030 - Branch #3262: py3
            # for k, v in memcache_metrics_db_object.iteritems():
            if python_version == 2:
                for k, v in memcache_metrics_db_object.iteritems():
                    key_name = str(k)
                    key_value = str(v)
                    memcache_metric_dict[key_name] = key_value
            else:
                for k, v in memcache_metrics_db_object.items():
                    key_name = str(k)
                    key_value = str(v)
                    memcache_metric_dict[key_name] = key_value
        except:
            current_logger.error(traceback.format_exc())
            current_logger.error('error :: skyline_functions :: get_memcache_metric_object :: failed to process data with iter of memcache memcache_metrics_db_object key dict %s' % metrics_db_object_key)
            memcache_metric_dict = False
        try:
            memcache_client.close()
        # @modified 20170913 - Task #2160: Test skyline with bandit
        # Added nosec to exclude from bandit tests
        except:  # nosec
            pass

    if memcache_metric_dict:
        # metrics_id = int(memcache_metric_dict['id'])
        # metric_ionosphere_enabled = int(memcache_metric_dict['ionosphere_enabled'])
        metrics_db_object = memcache_metric_dict
        current_logger.info('get_memcache_metric_object :: returned memcache data for key - %s' % metrics_db_object_key)
        return metrics_db_object

    return False


# @added 20170826 - Task #2132: Optimise Ionosphere DB usage
# Get the fp_ids list object data to memcache
def get_memcache_fp_ids_object(current_skyline_app, base_name):
    """
    Return the fp_ids list from memcache if it exists.

    .. warning: this returns a list and to mimic the MySQL results rows, when
        this is used the list item must to turned into a dict to match the
        MySQL/SQLAlchemy format.

    """
    try:
        pymemcache_Client
    except:
        from pymemcache.client.base import Client as pymemcache_Client

    try:
        literal_eval
    except:
        from ast import literal_eval

    # @added 20191029 - Task #3304: py3 - handle pymemcache bytes not str
    #                   Branch #3262: py3
    try:
        python_version
    except:
        from sys import version_info
        python_version = int(version_info[0])

    current_skyline_app_logger = str(current_skyline_app) + 'Log'
    current_logger = logging.getLogger(current_skyline_app_logger)

    if settings.MEMCACHE_ENABLED:
        memcache_client = pymemcache_Client((settings.MEMCACHED_SERVER_IP, settings.MEMCACHED_SERVER_PORT), connect_timeout=0.1, timeout=0.2)
    else:
        return False

    fp_ids_list_object_key = 'fp_ids_list_object.%s' % (str(base_name))

    memcache_result = None
    if settings.MEMCACHE_ENABLED:
        try:
            # @modified 20191029 - Task #3304: py3 - handle pymemcache bytes not str
            # memcache_result = memcache_client.get(fp_ids_list_object_key)
            if python_version == 2:
                memcache_result = memcache_client.get(fp_ids_list_object_key)
            else:
                memcache_result = memcache_client.get(fp_ids_list_object_key).decode('utf-8')
        except:
            current_logger.error(traceback.format_exc())
            current_logger.error('error :: skyline_functions :: get_memcache_fp_ids_object :: failed to get %s from memcache' % fp_ids_list_object_key)
        try:
            memcache_client.close()
        # @modified 20170913 - Task #2160: Test skyline with bandit
        # Added nosec to exclude from bandit tests
        except:  # nosec
            pass

    result = None
    if memcache_result:
        try:
            result = literal_eval(memcache_result)
        except:
            current_logger.error('error :: skyline_functions :: get_memcache_fp_ids_object :: failed to process data from memcache key with literal_eval %s' % fp_ids_list_object_key)
            result = False
        try:
            memcache_client.close()
        # @modified 20170913 - Task #2160: Test skyline with bandit
        # Added nosec to exclude from bandit tests
        except:  # nosec
            pass

    return result


# @added 20171216 - Task #2236: Change Boundary to only send to Panorama on alert
def move_file(current_skyline_app, dest_dir, file_to_move):
    """
    Move a file.

    :param current_skyline_app: the skyline app using this function
    :param dest_dir: the directory the file is to be moved to
    :param file_to_move: path and filename of the file to move
    :type current_skyline_app: str
    :type dest_dir: str
    :type file_to_move: str
    :return: ``True``, ``False``
    :rtype: boolean

    """
    try:
        os.getpid()
    except:
        import os

    try:
        shutil
    except:
        import shutil

    try:
        python_version
    except:
        from sys import version_info
        python_version = int(version_info[0])

    current_skyline_app_logger = str(current_skyline_app) + 'Log'
    current_logger = logging.getLogger(current_skyline_app_logger)

    if not os.path.exists(dest_dir):
        try:
            mkdir_p(dest_dir)
            current_logger.info(
                'created dest_dir - %s' % str(dest_dir))
        except:
            current_logger.info(traceback.format_exc())
            current_logger.error(
                'error :: failed to create dest_dir - %s' %
                str(dest_dir))
            return False

    move_file_name = os.path.basename(str(file_to_move))
    moved_file = '%s/%s' % (dest_dir, move_file_name)

    try:
        shutil.move(file_to_move, moved_file)
        if python_version == 2:
            os.chmod(moved_file, 0o644)
        if python_version == 3:
            os.chmod(moved_file, mode=0o644)

        current_logger.info('moved file to - %s' % moved_file)
        return True
    except OSError:
        current_logger.info(traceback.format_exc())
        msg = 'failed to move file to - %s' % moved_file
        current_logger.error('error :: %s' % msg)
        pass

    return False


# @added 20180107 - Branch #2270: luminosity
def is_derivative_metric(current_skyline_app, base_name):
    """
    Determine if a metric is a known derivative metric.

    :param current_skyline_app: the Skyline app that is calling the function
    :type current_skyline_app: str
    :param base_name: The metric base_name
    :type base_name: str
    :return: boolean
    :rtype: boolean

    """

    current_skyline_app_logger = str(current_skyline_app) + 'Log'
    current_logger = logging.getLogger(current_skyline_app_logger)

    # Feature #2034: analyse_derivatives
    known_derivative_metric = False
    REDIS_CONN_DECODED = None
#    try:
#        # @modified 20180519 - Feature #2378: Add redis auth to Skyline and rebrow
#        if settings.REDIS_PASSWORD:
#            REDIS_CONN = StrictRedis(password=settings.REDIS_PASSWORD, unix_socket_path=settings.REDIS_SOCKET_PATH)
#        else:
#            REDIS_CONN = StrictRedis(unix_socket_path=settings.REDIS_SOCKET_PATH)
#    except:
#        from redis import StrictRedis
#    if not REDIS_CONN:
#        try:
#            # @modified 20180519 - Feature #2378: Add redis auth to Skyline and rebrow
#            if settings.REDIS_PASSWORD:
#                REDIS_CONN = StrictRedis(password=settings.REDIS_PASSWORD, unix_socket_path=settings.REDIS_SOCKET_PATH)
#            else:
#                REDIS_CONN = StrictRedis(unix_socket_path=settings.REDIS_SOCKET_PATH)
#        except:
#            current_logger.error('error :: known_derivative_metric - Redis connection failed')
    try:
        REDIS_CONN_DECODED = get_redis_conn_decoded(current_skyline_app)
    except:
        current_logger.error('error :: known_derivative_metric - get_redis_conn failed')

    try:
        derivative_metrics = list(REDIS_CONN_DECODED.smembers('derivative_metrics'))
    except:
        derivative_metrics = []
    try:
        non_derivative_metrics = list(REDIS_CONN_DECODED.smembers('non_derivative_metrics'))
    except:
        non_derivative_metrics = []
    try:
        non_derivative_monotonic_metrics = settings.NON_DERIVATIVE_MONOTONIC_METRICS
    except:
        non_derivative_monotonic_metrics = []

    redis_metric_name = '%s%s' % (settings.FULL_NAMESPACE, str(base_name))
    if redis_metric_name in derivative_metrics:
        known_derivative_metric = True

    # First check if it has its own Redis z.derivative_metric key
    # that has not expired
    derivative_metric_key = 'z.derivative_metric.%s' % str(base_name)
    if not known_derivative_metric:
        last_derivative_metric_key = False
        try:
            REDIS_CONN = None
            try:
                REDIS_CONN = get_redis_conn(current_skyline_app)
            except:
                current_logger.error('error :: %s :: is_derivative_metric :: last_derivative_metric_key - get_redis_conn failed')
            last_derivative_metric_key = REDIS_CONN.get(derivative_metric_key)
        except Exception as e:
            current_logger.error('error :: could not query Redis for last_derivative_metric_key: %s' % e)

        if last_derivative_metric_key:
            # Until the z.derivative_metric key expires, it is classed
            # as such
            known_derivative_metric = True

    skip_derivative = in_list(base_name, non_derivative_monotonic_metrics)
    if skip_derivative:
        known_derivative_metric = False

    if known_derivative_metric:
        if redis_metric_name in non_derivative_metrics:
            known_derivative_metric = False

    if known_derivative_metric:
        return True

    return False


# @added 20180804 - Feature #2488: Allow user to specifically set metric as a derivative metric in training_data
def set_metric_as_derivative(current_skyline_app, base_name):
    """
    Add the metric to the derivative_metrics Redis set and create a
    z_derivative_metrics Redis key.

    :param current_skyline_app: the Skyline app that is calling the function
    :type current_skyline_app: str
    :param base_name: the metric base_name
    :type base_name: str
    :return: boolean
    :rtype: boolean

    """

    current_skyline_app_logger = str(current_skyline_app) + 'Log'
    current_logger = logging.getLogger(current_skyline_app_logger)

    current_logger.info('set_metric_as_derivative :: %s' % str(base_name))
    return_boolean = True
    REDIS_CONN = None

    # @modified 20191025 - Bug #3266: py3 Redis binary objects not strings
    #                      Branch #3262: py3
    # Added, charset='utf-8', decode_responses=True to REDIS_CONN
    try:
        if settings.REDIS_PASSWORD:
            REDIS_CONN = StrictRedis(password=settings.REDIS_PASSWORD, unix_socket_path=settings.REDIS_SOCKET_PATH, charset='utf-8', decode_responses=True)
        else:
            REDIS_CONN = StrictRedis(unix_socket_path=settings.REDIS_SOCKET_PATH, charset='utf-8', decode_responses=True)
    except:
        from redis import StrictRedis
    if not REDIS_CONN:
        try:
            if settings.REDIS_PASSWORD:
                REDIS_CONN = StrictRedis(password=settings.REDIS_PASSWORD, unix_socket_path=settings.REDIS_SOCKET_PATH, charset='utf-8', decode_responses=True)
            else:
                REDIS_CONN = StrictRedis(unix_socket_path=settings.REDIS_SOCKET_PATH, charset='utf-8', decode_responses=True)
        except:
            current_logger.error('error :: set_metric_as_derivative - Redis connection failed')
    if not REDIS_CONN:
        return return_boolean

    metric_name = '%s%s' % (str(settings.FULL_NAMESPACE), str(base_name))
    try:
        REDIS_CONN.sadd('derivative_metrics', metric_name)
        current_logger.info('set_metric_as_derivative :: %s added to derivative_metrics Redis set' % str(metric_name))
    except:
        current_logger.info(traceback.format_exc())
        current_logger.error('error :: failed to add metric to Redis derivative_metrics set')
        return_boolean = False

    derivative_metric_key = 'z.derivative_metric.%s' % str(base_name)
    try:
        last_expire_set = int(time())
        REDIS_CONN.setex(
            derivative_metric_key, settings.FULL_DURATION, last_expire_set)
    except Exception as e:
        current_logger.error('error :: could not set Redis derivative_metric key: %s' % e)
        return_boolean = False

    # @added 20190108 - Bug #2796: set_metric_as_derivative remove from non_derivative_metrics
    # Remove the metric from the non_derivative_metrics Redis set.  This set is
    # normally managed by Analyzer and although the metric is added to the
    # derivative_metrics set above, it only gets removed from the
    # non_derivative_metrics set after the Analyzer Redis key
    # analyzer.derivative_metrics_expiry expires, which can be up to 300 seconds
    # when the derivative_metrics and non_derivative_metrics Redis sets are
    # recreated
    current_logger.info('set_metric_as_derivative :: removing %s from non_derivative_metrics' % str(base_name))
    try:
        non_derivative_metrics = list(REDIS_CONN.smembers('non_derivative_metrics'))
    except:
        non_derivative_metrics = []
    new_non_derivative_metrics = []
    len_non_derivative_metrics = len(non_derivative_metrics)
    current_logger.info('set_metric_as_derivative :: %s metrics in non_derivative_metrics before the removal' % str(len_non_derivative_metrics))
    if non_derivative_metrics:
        for i_metric_name in non_derivative_metrics:
            if i_metric_name != metric_name:
                new_non_derivative_metrics.append(i_metric_name)
    if new_non_derivative_metrics:
        all_redis_ops_ok = True
        for i_metric_name in new_non_derivative_metrics:
            try:
                REDIS_CONN.sadd('new_non_derivative_metrics', i_metric_name)
            except:
                all_redis_ops_ok = False
                current_logger.error('error :: failed to add metric %s to Redis new_non_derivative_metrics set' % str(i_metric_name))
        if all_redis_ops_ok:
            try:
                REDIS_CONN.rename('non_derivative_metrics', 'old_non_derivative_metrics')
            except Exception as e:
                current_logger.error('error :: could not rename Redis set non_derivative_metrics to old_non_derivative_metrics: %s' % str(e))
                return_boolean = False
            try:
                REDIS_CONN.rename('new_non_derivative_metrics', 'non_derivative_metrics')
                current_logger.info('set_metric_as_derivative :: removed %s from non_derivative_metrics' % str(base_name))
            except Exception as e:
                current_logger.error('error :: could not rename Redis set new_non_derivative_metrics to non_derivative_metrics: %s' % str(e))
                return_boolean = False
            try:
                REDIS_CONN.delete('old_non_derivative_metrics')
            except Exception as e:
                current_logger.error('error :: could not delta Redis set old_non_derivative_metrics: %s' % str(e))
    try:
        non_derivative_metrics = list(REDIS_CONN.smembers('non_derivative_metrics'))
    except:
        non_derivative_metrics = []
    len_non_derivative_metrics = len(non_derivative_metrics)
    current_logger.info('set_metric_as_derivative :: %s metrics in non_derivative_metrics after the removal of %s' % (str(len_non_derivative_metrics), str(metric_name)))

    return return_boolean


# @added 20190920 - Feature #3230: users DB table
#                   Ideas #2476: Label and relate anomalies
#                   Feature #2516: Add label to features profile
def get_user_details(current_skyline_app, desired_value, key, value):
    """
    Determines the user details for a user given the desired_value, key and
    value.  If you want the username of the user with id 1 then:
    get_user_details('username', 'id', 1)  # Will return 'Skyline'
    get_user_details('id', 'username', 'Skyline')  # Will return '1'

    :param current_skyline_app: the app calling the function so the function
        knows which log to write too.
    :param desired_value: the id or username
    :param key: the field for the value
    :param value: the value of the item you want to query in the key field
    :type current_skyline_app: str
    :type desired_value: str
    :type key: str
    :type value: str or int
    :return: tuple
    :rtype:  (boolean, str)

    """
    current_skyline_app_logger = str(current_skyline_app) + 'Log'
    current_logger = logging.getLogger(current_skyline_app_logger)

    if key == 'id':
        select_field = 'user'
        select_where = 'id'

    if key == 'username':
        select_field = 'id'
        select_where = 'user'

    if desired_value != select_field:
        fail_msg = 'error :: %s :: get_user_details :: desired_value %s is invalid in get_user_details(%s, %s %s)' % (
            str(current_skyline_app), str(desired_value), str(desired_value),
            str(key), str(value))
        current_logger.error('%s' % fail_msg)
        if current_skyline_app == 'webapp':
            # Raise to webbapp
            raise
        else:
            return False, None

    try:
        query = 'select %s from users WHERE %s = \'%s\'' % (select_field, select_where, str(value))  # nosec
        results = mysql_select(str(current_skyline_app), query)
        result = results[0][0]
    except:
        trace = traceback.format_exc()
        current_logger.error('%s' % trace)
        fail_msg = 'error :: %s :: get_user_details :: could not get user %s from database for get_user_details(%s, %s %s)' % (
            str(current_skyline_app), select_field, str(desired_value),
            str(key), str(value))
        current_logger.error('%s' % fail_msg)
        if current_skyline_app == 'webapp':
            # Raise to webbapp
            raise
        else:
            return False, None
        fail_msg = 'error :: %s :: get_user_details :: could not get user %s from database for get_user_details(%s, %s %s)' % (
            str(current_skyline_app), select_field, str(desired_value),
            str(key), str(value))
        current_logger.info('%s :: get_user_details :: determined %s of %s for users.%s = %s' % (
            select_field, str(result), select_where), str(value))
    if select_field == 'id':
        return True, int(result)
    return True, str(result)


# @added 20191106 - Branch #3002: docker
#                   Branch #3262: py3
def get_graphite_port(current_skyline_app):
    """
    Returns graphite port based on configuration in settings.py
    """
    if settings.GRAPHITE_PROTOCOL == 'http':
        graphite_port = '80'
    else:
        graphite_port = '443'
    if settings.GRAPHITE_PORT != '':
        graphite_port = str(settings.GRAPHITE_PORT)
    return graphite_port


def get_graphite_render_uri(current_skyline_app):
    """
    Returns graphite render uri based on configuration in settings.py
    """
    current_skyline_app_logger = str(current_skyline_app) + 'Log'
    current_logger = logging.getLogger(current_skyline_app_logger)
    try:
        graphite_render_uri = str(settings.GRAPHITE_RENDER_URI)
    except:
        current_logger.info('get_graphite_render_uri :: GRAPHITE_RENDER_URI is not declared in settings.py, using default of \'render\'')
        graphite_render_uri = 'render'
    return graphite_render_uri


def get_graphite_custom_headers(current_skyline_app):
    """
    Returns custom http headers
    """
    current_skyline_app_logger = str(current_skyline_app) + 'Log'
    current_logger = logging.getLogger(current_skyline_app_logger)
    headers = dict()
    try:
        headers = settings.GRAPHITE_CUSTOM_HEADERS
    except:
        current_logger.info('get_graphite_custom_headers :: GRAPHITE_CUSTOM_HEADERS is not declared in settings.py, using default of \{\}')
        headers = dict()
    return headers


# @added 20191113 - Feature #: forward_alert
def forward_alert(current_skyline_app, alert_data):
    """
    Sends alert data to a HTTP endpoint

    :param current_skyline_app: the app calling the function so the function
        knows which log to write too.
    :param alert_data: a list containing the alert data
    :type current_skyline_app: str
    :type alert_data: list
    :type key: str
    :type value: str or int
    :return: tuple
    :rtype:  (boolean, str)
    """
    current_skyline_app_logger = str(current_skyline_app) + 'Log'
    current_logger = logging.getLogger(current_skyline_app_logger)
    current_logger.info('%s - never to be implemented.  Implemented as http_alerter' % (
        current_skyline_app))


# @added 20200413 - Feature #3486: analyzer_batch
#                   Feature #3480: batch_processing
def is_batch_metric(current_skyline_app, base_name):
    """
    Determine if the metric is designated as an analyzer batch processing metric

    :param current_skyline_app: the app calling the function so the function
        knows which log to write too.
    :param metric_name: the metric name
    :type current_skyline_app: str
    :type metric_name: str
    :return: False
    :rtype:  boolean
    """
    debug_is_batch_metric = None

    if debug_is_batch_metric:
        current_skyline_app_logger = str(current_skyline_app) + 'Log'
        current_logger = logging.getLogger(current_skyline_app_logger)
    else:
        current_logger = None

    try:
        from settings import BATCH_PROCESSING
    except:
        BATCH_PROCESSING = None
    try:
        from settings import BATCH_PROCESSING_NAMESPACES
    except:
        BATCH_PROCESSING_NAMESPACES = []

    debug_is_batch_metric = None

    batch_metric = False
    if BATCH_PROCESSING:
        batch_metric = True
        if BATCH_PROCESSING_NAMESPACES:
            batch_metric = False
            base_name_namespace_elements = base_name.split('.')
            for batch_processing_namespace in BATCH_PROCESSING_NAMESPACES:
                if batch_processing_namespace in base_name:
                    batch_metric = True
                    if debug_is_batch_metric:
                        current_logger.info('%s - is_batch_metric - namespace - %s matched by being in %s' % (
                            current_skyline_app, base_name, batch_processing_namespace))
                    break
                batch_processing_namespace_namespace_elements = batch_processing_namespace.split('.')
                elements_matched = set(base_name_namespace_elements) & set(batch_processing_namespace_namespace_elements)
                if len(elements_matched) == len(batch_processing_namespace_namespace_elements):
                    batch_metric = True
                    if debug_is_batch_metric:
                        current_logger.info('%s - is_batch_metric - namespace - %s matched by elements of %s' % (
                            current_skyline_app, base_name, batch_processing_namespace))
                    break
    try:
        del base_name_namespace_elements
    except:
        pass
    try:
        del batch_processing_namespace_namespace_elements
    except:
        pass
    try:
        del elements_matched
    except:
        pass

    return batch_metric


# @added 20200424 - Feature #3504: Handle airgaps in batch metrics
#                   Feature #3480: batch_processing
#                   Feature #3486: analyzer_batch
#                   Feature #3400: Identify air gaps in the metric data
# Moved to skyline_functions from its original definition analyzer/algorithms.py
# as is required in analyzer/analyzer.py now as well to identify which batch
# metrics should also be checked for airgaps.
def is_check_airgap_metric(base_name):
    """
    Check if a metric matches a metric namespace in CHECK_AIRGAPS or one in
    SKIP_AIRGAPS.

    :param base_name: the metric base_name
    :type base_name: str
    :return: False
    :rtype:  boolean

    """

    try:
        IDENTIFY_AIRGAPS = settings.IDENTIFY_AIRGAPS
    except:
        IDENTIFY_AIRGAPS = False
    try:
        CHECK_AIRGAPS = settings.CHECK_AIRGAPS
    except:
        CHECK_AIRGAPS = []
    try:
        SKIP_AIRGAPS = settings.SKIP_AIRGAPS
    except:
        SKIP_AIRGAPS = []
    check_metric_for_airgaps = False
    if IDENTIFY_AIRGAPS:
        check_metric_for_airgaps = True
        if CHECK_AIRGAPS:
            check_metric_for_airgaps = False
            try:
                metric_namespace_elements = base_name.split('.')
                for to_check in CHECK_AIRGAPS:
                    if to_check in base_name:
                        check_metric_for_airgaps = True
                        break
                    to_check_namespace_elements = to_check.split('.')
                    elements_matched = set(metric_namespace_elements) & set(to_check_namespace_elements)
                    if len(elements_matched) == len(to_check_namespace_elements):
                        check_metric_for_airgaps = True
                        break
            except:
                pass
        # Allow to skip identifying airgaps on certain metrics and namespaces
        if check_metric_for_airgaps:
            try:
                metric_namespace_elements = base_name.split('.')
                for to_skip in SKIP_AIRGAPS:
                    if to_skip in base_name:
                        check_metric_for_airgaps = False
                        break
                    to_skip_namespace_elements = to_skip.split('.')
                    elements_matched = set(metric_namespace_elements) & set(to_skip_namespace_elements)
                    if len(elements_matched) == len(to_skip_namespace_elements):
                        check_metric_for_airgaps = False
                        break
            except:
                pass

    return check_metric_for_airgaps


# @added 20200506 - Feature #3532: Sort all time series
def sort_timeseries(timeseries):
    """
    This function is used to sort a time series by timestamp to ensure that
    there are no unordered timestamps in the time series which are artefacts of
    the collector or carbon-relay. So all Redis time series are sorted by
    timestamp before analysis.
    """
    sorted_timeseries = []
    if timeseries:
        sorted_timeseries = sorted(timeseries, key=lambda x: x[0])
        del timeseries

    return sorted_timeseries
