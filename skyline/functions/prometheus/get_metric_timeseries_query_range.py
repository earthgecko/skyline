"""
Get job series from Prometheus
"""
import logging
import requests


# @added 20211025 - Branch #4300: prometheus
#                   Task #2732: Prometheus to Skyline
# Add a global method to get jobs from a Prometheus host
def get_metric_timeseries_query_range(
        current_skyline_app, prometheus_host_dict, metric, start, end, step,
        log=True):
    """
    Return a metric timeseries as a list using the Prometheus query_range API
    method e.g.
    http://prometheus/api/v1/query_range?query=prometheus_http_response_size_bytes_bucket{alias="Prometheus",handler="/api/v1/query",instance="localhost:9090",job="prometheus",le="100"}&start=1635235300&end=1635321700&step=1m

    :param current_skyline_app: the app calling the function
    :param prometheus_host_dict: the prometheus_host_dict from
        settings.PROMETHEUS_SETTINGS (each defined host dict as a dict)
    :param metric: the Prometheus metric (Prometheus expression query string)
    :param start: start unix timestamp
    :param end: end unix timestamp
    :param step: duration, query resolution step width in duration format or
        float number of seconds.
    :param log: whether to log or not, optional, defaults to True
    :type current_skyline_app: str
    :type prometheus_host_dict: dict
    :type metric: str
    :type start: int
    :type end: int
    :type step: str
    :type log: boolean
    :return: timeseries
    :rtype: list

    """

    timeseries_dict = {}
    timeseries = []
    function_str = '%s :: functions.prometheus.get_metric_timeseries_query_range' % current_skyline_app

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

    query_path = 'query_range'

    query = 'query=%s&start=%s&end=%s&step=%s' % (
        metric, str(start), str(end), str(step))
    if not add_port:
        url = '%s://%s/%s/%s?%s' % (
            scheme, prometheus_host, endpoint, query_path, query)
    else:
        url = '%s://%s:%s/%s/%s?%s' % (
            scheme, prometheus_host, port, endpoint, query_path, query)

    response = None
    try:
        response = requests.get(url)
    except Exception as err:
        if not log:
            current_skyline_app_logger = current_skyline_app + 'Log'
            current_logger = logging.getLogger(current_skyline_app_logger)
        current_logger.error('error :: %s :: failed to get response from %s - %s' % (
            function_str, url, str(err)))
        return timeseries

    try:
        timeseries_dict = response.json()
    except Exception as err:
        if not log:
            current_skyline_app_logger = current_skyline_app + 'Log'
            current_logger = logging.getLogger(current_skyline_app_logger)
        current_logger.error('error :: %s :: failed to get json from response from %s - %s' % (
            function_str, url, str(err)))
        return timeseries

    status = None
    try:
        status = timeseries_dict['status']
        if status != 'success':
            if not log:
                current_skyline_app_logger = current_skyline_app + 'Log'
                current_logger = logging.getLogger(current_skyline_app_logger)
            current_logger.error('error :: %s :: status not success failed to get data from json response from %s. status: %s' % (
                function_str, url, str(status)))
            return timeseries
    except Exception as err:
        if not log:
            current_skyline_app_logger = current_skyline_app + 'Log'
            current_logger = logging.getLogger(current_skyline_app_logger)
        current_logger.error('error :: %s :: failed to get status from json response from %s - %s' % (
            function_str, url, str(err)))
    if status == 'success':
        try:
            timeseries = timeseries_dict['data']['result'][0]['values']
            # Prometheus returns values as str, convert to float
            timeseries = [[int(ts), float(value)] for ts, value in timeseries]
        except Exception as err:
            if not log:
                current_skyline_app_logger = current_skyline_app + 'Log'
                current_logger = logging.getLogger(current_skyline_app_logger)
            current_logger.error('error :: %s :: failed to get values from json response from %s - %s' % (
                function_str, url, str(err)))

    return timeseries
