"""
Get jobs from Prometheus
"""
import logging
import requests


# @added 20211025 - Branch #4300: prometheus
#                   Task #2732: Prometheus to Skyline
# Add a global method to get jobs from a Prometheus host
def get_jobs(current_skyline_app, prometheus_host_dict, log=True):
    """
    Return a list of Prometheus job names from the prometheus host:
    scheme://prometheus_host/api/v1/label/job/values

    jobs = ['prometheus', 'node']

    :param current_skyline_app: the app calling the function
    :param prometheus_host_dict: the prometheus_host_dict from
        settings.PROMETHEUS_SETTINGS (each defined host dict as a dict)
    :param log: whether to log or not, optional, defaults to True
    :type current_skyline_app: str
    :type prometheus_host_dict: dict
    :type log: boolean
    :return: list of jobs
    :rtype: list

    """

    jobs = []

    function_str = '%s :: functions.prometheus.get_jobs' % current_skyline_app

    if log:
        current_skyline_app_logger = current_skyline_app + 'Log'
        current_logger = logging.getLogger(current_skyline_app_logger)
    else:
        current_logger = None

    # Prometheus URL
    prometheus_host = list(prometheus_host_dict.keys())[0]
    scheme = 'https'
    try:
        scheme = prometheus_host_dict[prometheus_host]['scheme']
    except KeyError:
        scheme = 'https'

    port = 443
    try:
        port = prometheus_host_dict[prometheus_host]['port']
    except KeyError:
        port = 443
    add_port = False
    if scheme == 'https':
        if port != 443:
            add_port = True
    if scheme == 'http':
        if port != 80:
            add_port = True

    endpoint = 'api/v1'
    try:
        endpoint = prometheus_host_dict[prometheus_host]['endpoint']
    except KeyError:
        endpoint = 'api/v1'

    query_path = 'label/job/values'
    if not add_port:
        url = '%s://%s/%s/%s' % (scheme, prometheus_host, endpoint, query_path)
    else:
        url = '%s://%s:%s/%s/%s' % (
            scheme, prometheus_host, port, endpoint, query_path)

    response = None
    try:
        response = requests.get(url)
    except Exception as err:
        if not log:
            current_skyline_app_logger = current_skyline_app + 'Log'
            current_logger = logging.getLogger(current_skyline_app_logger)
        current_logger.error('error :: %s :: failed to get response from %s - %s' % (
            function_str, url, str(err)))
        return jobs

    json_response = None
    try:
        json_response = response.json()
    except Exception as err:
        if not log:
            current_skyline_app_logger = current_skyline_app + 'Log'
            current_logger = logging.getLogger(current_skyline_app_logger)
        current_logger.error('error :: %s :: failed to get json from response from %s - %s' % (
            function_str, url, str(err)))
        return jobs

    try:
        jobs = json_response['data']
    except Exception as err:
        if not log:
            current_skyline_app_logger = current_skyline_app + 'Log'
            current_logger = logging.getLogger(current_skyline_app_logger)
        current_logger.error('error :: %s :: failed to get data from json response from %s - %s' % (
            function_str, url, str(err)))

    return jobs
