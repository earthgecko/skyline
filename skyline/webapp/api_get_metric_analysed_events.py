"""
api_get_metric_analysed_events.py
"""
import logging
from flask import request

from settings import FULL_NAMESPACE, REMOTE_SKYLINE_INSTANCES
from skyline_functions import get_redis_conn_decoded
from backend import get_cluster_data
from functions.metrics.get_metric_analysed_events import get_metric_analysed_events


# @added 202200504 - Feature #4530: namespace.analysed_events
def api_get_metric_analysed_events(current_skyline_app, cluster_data=False):
    """
    Return a dict with the number of analysed events for a metric.

    :param current_skyline_app: the app calling the function
    :param cluster_data: the cluster_data parameter from the request
    :type current_skyline_app: str
    :type cluster_data: bool
    :return: analysed_events
    :rtype: dict

    """
    function_str = 'api_get_metric_analysed_events'
    analysed_events = {}

    current_skyline_app_logger = current_skyline_app + 'Log'
    current_logger = logging.getLogger(current_skyline_app_logger)

    base_name = None
    try:
        base_name = request.args.get('metric')
        current_logger.info('/api?get_metric_analysed_events with metric: %s' % base_name)
    except Exception as err:
        current_logger.error('error :: api get_metric_analysed_events request no metric argument - %s' % err)
        # @modified 20220509 - Feature #4530: namespace.analysed_events
        #                      Release #4562 - v3.0.4
        # Does not have the desired result
        # return 'Bad Request', 400
        return 400
    from_timestamp = None
    try:
        from_timestamp = int(request.args.get('from_timestamp'))
        current_logger.info('/api?get_metric_analysed_events with from_timestamp: %s' % str(from_timestamp))
    except Exception as err:
        current_logger.error('error :: api get_metric_analysed_events request no from_timestamp argument - %s' % err)
        # @modified 20220509 - Feature #4530: namespace.analysed_events
        #                      Release #4562 - v3.0.4
        # Does not have the desired result
        # return 'Bad Request', 400
        return 400
    until_timestamp = None
    try:
        until_timestamp = int(request.args.get('until_timestamp'))
        current_logger.info('/api?get_metric_analysed_events with until_timestamp: %s' % str(until_timestamp))
    except Exception as err:
        current_logger.error('error :: api get_metric_analysed_events request no until_timestamp argument - %s' % err)
        # @modified 20220509 - Feature #4530: namespace.analysed_events
        #                      Release #4562 - v3.0.4
        # Does not have the desired result
        # return 'Bad Request', 400
        return 400

    try:
        redis_conn_decoded = get_redis_conn_decoded(current_skyline_app)
    except Exception as err:
        current_logger.error('error :: %s :: %s :: get_redis_conn_decoded failed - %s' % (
            current_skyline_app, function_str, err))
        raise

    redis_metric_name = '%s%s' % (FULL_NAMESPACE, base_name)

    local_metric = False
    try:
        local_metric = redis_conn_decoded.exists(redis_metric_name)
    except Exception as err:
        current_logger.error('error :: %s :: %s :: get_redis_conn_decoded failed - %s' % (
            current_skyline_app, function_str, err))
        return analysed_events

    if not local_metric and not cluster_data:
        current_logger.warning('warning :: %s :: no analysed_events, metric not found in Redis - %s' % (
            function_str, base_name))
        return analysed_events

    if local_metric:
        try:
            analysed_events = get_metric_analysed_events(current_skyline_app, base_name, from_timestamp, until_timestamp)
        except Exception as err:
            current_logger.error('error :: %s :: %s :: get_redis_conn_decoded failed - %s' % (
                current_skyline_app, function_str, err))
            return analysed_events

    if analysed_events:
        current_logger.info('%s :: determined %s' % (
            function_str, str(analysed_events)))
        return analysed_events

    if REMOTE_SKYLINE_INSTANCES and cluster_data:
        analysed_events = {}
        remote_analysed_events = []
        # @modified 20220509 - Feature #4530: namespace.analysed_events
        #                      Release #4562 - v3.0.4
        # Correct parameters append _timestamp
        api_uri = 'get_metric_analysed_events=true&metric=%s&from_timestamp=%s&until_timestamp=%s&cluster_data=false&cluster_call=true' % (
            base_name, from_timestamp, until_timestamp)
        try:
            remote_analysed_events = get_cluster_data(api_uri, 'analysed_events')
        except Exception as err:
            current_logger.error('error :: %s :: failed to get analysed_events from remote instances - %s' % (
                function_str, err))
            raise
        if remote_analysed_events:
            current_logger.info('got %s remote analysed_events from the remote Skyline instances' % str(len(remote_analysed_events)))
            for remote_dict in remote_analysed_events:
                if remote_dict:
                    analysed_events = remote_dict

    current_logger.info('%s :: determined remotely - %s' % (
        function_str, str(analysed_events)))
    return analysed_events
