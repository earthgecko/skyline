"""
get_anomaly_id.py
"""
import logging
import traceback
from ast import literal_eval

import requests

import settings
from functions.metrics.get_metric_id_from_base_name import get_metric_id_from_base_name


# @added 20210323 - Feature #3642: Anomaly type classification
def get_anomaly_id(current_skyline_app, base_name, timestamp):
    """
    Given a base_name and timestamp, return the anomaly id

    :param current_skyline_app: the Skyline app calling the function
    :param base_name: the base_name of the metric in question
    :param timestamp: the timestamp
    :type current_skyline_app: str
    :type base_name: str
    :type timestamp: int
    :return: id
    :rtype: int

    """

    function_str = 'functions.panorama.get_anomaly_id'
    try:
        current_skyline_app_logger = str(current_skyline_app) + 'Log'
        current_logger = logging.getLogger(current_skyline_app_logger)
    except:
        pass

    # @added 20220722 - Task #2732: Prometheus to Skyline
    #                   Branch #4300: prometheus
    if 'tenant_id="' in base_name:
        labelled_metrics_name = str(base_name)
        current_logger.info('%s :: looking up base_name for %s' % (function_str, labelled_metrics_name))
        try:
            metric_id = get_metric_id_from_base_name(current_skyline_app, base_name)
        except Exception as err:
            current_logger.error('error :: %s :: get_metric_id_from_base_name failed for %s - %s' % (
                function_str, base_name, err))
        current_logger.info('%s :: looked up metric id as %s' % (function_str, str(metric_id)))
        if metric_id:
            base_name = 'labelled_metrics.%s' % str(metric_id)

    panorama_anomaly_id = 0
    # Time shift the requested_timestamp by 120 seconds either way on the
    # from_timestamp and until_timestamp parameter to account for any lag in the
    # insertion of the anomaly by Panorama in terms Panorama only running every
    # 60 second and Analyzer to Mirage to Ionosphere and back introduce
    # additional lags.  Panorama will not add multiple anomalies from the same
    # metric in the time window so there is no need to consider the possibility
    # of there being multiple anomaly ids being returned.
    grace_from_timestamp = int(timestamp) - 300
    grace_until_timestamp = int(timestamp) + 120
    url = '%s/panorama?metric=%s&from_timestamp=%s&until_timestamp=%s&panorama_anomaly_id=true' % (
        settings.SKYLINE_URL, str(base_name), str(grace_from_timestamp),
        str(grace_until_timestamp))
    panorama_resp = None

    # @added 20190519 - Branch #3002: docker
    # Handle self signed certificate on Docker
    verify_ssl = True
    try:
        running_on_docker = settings.DOCKER
    except:
        running_on_docker = False
    if running_on_docker:
        verify_ssl = False

    # @added 20191029 - Branch #3262: py3
    # Allow for the use of self signed SSL certificates even if not running on
    # docker.
    try:
        overall_verify_ssl = settings.VERIFY_SSL
    except:
        overall_verify_ssl = True
    if not overall_verify_ssl:
        verify_ssl = False

    if settings.WEBAPP_AUTH_ENABLED:
        user = str(settings.WEBAPP_AUTH_USER)
        password = str(settings.WEBAPP_AUTH_USER_PASSWORD)
    try:
        if settings.WEBAPP_AUTH_ENABLED:
            r = requests.get(url, timeout=settings.GRAPHITE_READ_TIMEOUT, auth=(user, password), verify=verify_ssl)
        else:
            r = requests.get(url, timeout=settings.GRAPHITE_READ_TIMEOUT, verify=verify_ssl)
        panorama_resp = True
    except:
        current_logger.error(traceback.format_exc())
        current_logger.error('error :: %s :: failed to get anomaly id from panorama: %s' % (
            function_str, str(url)))
    if panorama_resp:
        try:
            data = literal_eval(r.text)
            if str(data) == '[]':
                panorama_anomaly_id = 0
            else:
                panorama_anomaly_id = int(data[0][0])
            current_logger.info('%s :: anomaly id: %s' % (
                function_str, str(panorama_anomaly_id)))
        except:
            current_logger.error(traceback.format_exc())
            current_logger.error('error :: %s :: failed to get anomaly id from panorama response: %s' % (
                function_str, str(r.text)))
    return panorama_anomaly_id
