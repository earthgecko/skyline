"""
api_get_analysed_events.py
"""
import logging
from flask import request

from settings import REMOTE_SKYLINE_INSTANCES
from backend import get_cluster_data
from get_analysed_events import get_analysed_events


# @added 202200504 - Feature #4530: namespace.analysed_events
def api_get_analysed_events(current_skyline_app, cluster_data=False):
    """
    Return a dict with the number of analysed events, estimated the first day
    based on dividing the total of the first day by the elapsed number of
    minutes that have past in the until_timestamp day.

    :param current_skyline_app: the app calling the function
    :param cluster_data: the cluster_data parameter from the request
    :type current_skyline_app: str
    :type cluster_data: bool
    :return: analysed_events
    :rtype: dict

    """
    function_str = 'api_get_analysed_events'
    analysed_events_dict = {}

    current_skyline_app_logger = current_skyline_app + 'Log'
    current_logger = logging.getLogger(current_skyline_app_logger)

    from_timestamp = None
    try:
        from_timestamp = int(request.args.get('from_timestamp'))
        current_logger.info('api_get_analysed_events :: with from_timestamp: %s' % str(from_timestamp))
    except Exception as err:
        current_logger.error('error :: api get_analysed_events request no from_timestamp argument - %s' % err)
        return 'Bad Request', 400
    until_timestamp = None
    try:
        until_timestamp = int(request.args.get('until_timestamp'))
        current_logger.info('api_get_analysed_events :: with until_timestamp: %s' % str(until_timestamp))
    except Exception as err:
        current_logger.error('error :: api get_analysed_events request no until_timestamp argument - %s' % err)
        return 'Bad Request', 400

    namespace = None
    try:
        namespace = request.args.get('namespace')
        current_logger.info('api_get_analysed_events :: with namespace: %s' % str(namespace))
    except:
        namespace = None

    try:
        analysed_events_dict = get_analysed_events(current_skyline_app, from_timestamp, until_timestamp, namespace)
    except Exception as err:
        current_logger.error('error ::  %s :: get_analysed_events failed - %s' % (
            function_str, err))
    if analysed_events_dict:
        current_logger.info('api_get_analysed_events :: determined local analysed_events')
    else:
        current_logger.warning('warning :: api_get_analysed_events :: determined not local analysed_events')

    remote_analysed_events = []
    if REMOTE_SKYLINE_INSTANCES and cluster_data:
        current_logger.info('/api_get_analysed_events :: determining analysed_events from other cluster nodes')
        if namespace:
            api_uri = 'get_analysed_events=true&from=%s&until=%s&namespace=%s&cluster_data=false' % (
                from_timestamp, until_timestamp, namespace)
        else:
            api_uri = 'get_analysed_events=true&from=%s&until=%s&cluster_data=false' % (
                from_timestamp, until_timestamp)
        try:
            remote_analysed_events = get_cluster_data(api_uri, 'analysed_events')
        except Exception as err:
            current_logger.error('error :: %s :: failed to get analysed_events from remote instances - %s' % (
                function_str, err))
            raise
        current_logger.info('%s :: determined %s data sets from remote instances' % (
            function_str, str(len(remote_analysed_events))))

    if remote_analysed_events:
        current_logger.info('got %s remote analysed_events from the remote Skyline instances' % str(len(remote_analysed_events)))
        for remote_analysed_events_dict in remote_analysed_events:
            for key in list(remote_analysed_events_dict.keys()):
                if isinstance(remote_analysed_events_dict[key], int):
                    value = remote_analysed_events_dict[key]
                    try:
                        has_value = analysed_events_dict[key]
                        del has_value
                    except:
                        analysed_events_dict[key] = 0
                    analysed_events_dict[key] += value
                if isinstance(remote_analysed_events_dict[key], dict):
                    for i_key in list(remote_analysed_events_dict[key].keys()):
                        if isinstance(remote_analysed_events_dict[key][i_key], int):
                            value = remote_analysed_events_dict[key][i_key]
                            try:
                                has_value = analysed_events_dict[key][i_key]
                            except:
                                try:
                                    has_parent = analysed_events_dict[key]
                                    del has_parent
                                except:
                                    analysed_events_dict[key] = {}
                                analysed_events_dict[key][i_key] = 0
                        analysed_events_dict[key][i_key] += value

    current_logger.info('%s :: returning %s' % (
        function_str, str(analysed_events_dict)))
    return analysed_events_dict
