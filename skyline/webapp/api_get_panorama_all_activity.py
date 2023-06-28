"""
api_get_panorama_all_activity.py
"""
import logging
from time import time
from flask import request
import requests

import settings


# @added 20230130 - Feature #4834: webapp - api - get_all_activity
#                   Feature #4830: webapp - panorama_plot_anomalies - all_events
def get_panorama_all_activity(current_skyline_app):
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
    function_str = 'api_get_panorama_all_activity'
    all_events_dict = {}

    current_skyline_app_logger = current_skyline_app + 'Log'
    current_logger = logging.getLogger(current_skyline_app_logger)

    request_url = str(request.url)

    try:
        metric = request.args.get('metric')
        current_logger.info('api_get_panorama_all_activity :: request with metric: %s' % metric)
    except Exception as err:
        current_logger.error('error :: api_get_panorama_all_activity :: request no metric argument - %s' % err)
        return 'Bad Request', 400

    until_timestamp = int(time())
    try:
        until_timestamp = int(request.args.get('until_timestamp'))
        current_logger.info('api_get_panorama_all_activity :: with until_timestamp: %s' % str(until_timestamp))
    except KeyError:
        until_timestamp = int(time())
    except Exception as err:
        current_logger.error('error :: api_get_panorama_all_activity request no until_timestamp argument - %s' % err)
        return 500

    from_timestamp = until_timestamp - (86400 * 2)
    try:
        from_timestamp = int(request.args.get('from_timestamp'))
        current_logger.info('api_get_panorama_all_activity :: with from_timestamp: %s' % str(from_timestamp))
    except KeyError:
        from_timestamp = until_timestamp - (86400 * 2)
    except Exception as err:
        current_logger.error('error :: api_get_panorama_all_activity request no until_timestamp argument - %s' % err)
        return 500

    overall_verify_ssl = False
    try:
        overall_verify_ssl = settings.VERIFY_SSL
    except:
        overall_verify_ssl = True
    verify_ssl = overall_verify_ssl

    r = None
    try:
        panorama_all_activity_url = request_url.replace('api?get_all_activity', 'panorama?plot_metric_anomalies', 1)
        url = '%s&format=json' % panorama_all_activity_url
        r = requests.get(
            url, timeout=60,
            auth=(str(settings.WEBAPP_AUTH_USER), str(settings.WEBAPP_AUTH_USER_PASSWORD)),
            verify=verify_ssl)
    except Exception as err:
        current_logger.error('error :: %s :: failed to get %s - %s' % (
            function_str, url, err))

    if r:
        try:
            all_events_dict = r.json()
        except Exception as err:
            current_logger.error('error :: %s :: failed to parse json response - %s' % (
                function_str, err))
    current_logger.info('%s :: returning all_events_dict of length: %s' % (
        function_str, str(len(all_events_dict))))

    return all_events_dict
