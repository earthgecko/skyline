"""
api_populate_redis_metric.py
"""
import logging
import gzip
import json

from flask import request

from functions.redis.populate_redis_metric import populate_redis_metric


# @added 20240520 - Feature #5354: functions.redis.populate_redis_metric
#                   Feature #5352: vista - bigquery
def api_populate_redis_metric(current_skyline_app):
    """
    Return a dict with the timeseries.

    :param current_skyline_app: the app calling the function
    :param cluster_data: the cluster_data parameter from the request
    :type current_skyline_app: str
    :type cluster_data: bool
    :return: bq_backfill_job_dict
    :rtype: dict

    """
    function_str = 'api_populate_redis_metric'
    response_dict = {'timeseries': None, 'success': False, 'status_code': 200}

    current_skyline_app_logger = current_skyline_app + 'Log'
    current_logger = logging.getLogger(current_skyline_app_logger)

    post_data = {}

    content_encoding = None
    try:
        content_encoding = request.headers.get('content-encoding')
    except:
        content_encoding = None
    if content_encoding == 'gzip':
        decompressed_post_data = None
        try:
            decompressed_post_data = gzip.decompress(request.data)
        except Exception as err:
            current_logger.error('error :: %s - gzip.decompress(request.data) failed, err: %s' % (
                function_str, err))
            current_logger.info('%s, return 400 bad POST data' % function_str)
            response_dict['status_code'] = 400
            response_dict['error'] = 'post data could be be decompressed'
            return response_dict
        if decompressed_post_data:
            try:
                data = json.loads(decompressed_post_data)
                if data:
                    post_data = {'data': data}
            except Exception as err:
                current_logger.error('error :: %s - json.loads failed on decompressed_post_data, err: %s' % (
                    function_str, err))
                current_logger.info('%s, return 400 bad POST data' % function_str)
                response_dict['status_code'] = 400
                response_dict['error'] = 'post data not json'
                return response_dict

    if not post_data:
        try:
            post_data = request.get_json()
        except Exception as err:
            current_logger.error('error :: %s - no POST data - %s' % (
                function_str, err))
            current_logger.info('%s, return 400 no POST data' % function_str)
            response_dict['status_code'] = 400
            response_dict['error'] = 'no post data'
            return response_dict
    try:
        base_name = post_data['data']['base_name']
    except Exception as err:
        current_logger.error('error :: %s :: no base_name in POST data, err: %s' % (
            function_str, err))
        response_dict['status_code'] = 400
        response_dict['error'] = 'no base_name passed in post data'
        return response_dict
    try:
        from_timestamp = post_data['data']['from_timestamp']
    except Exception as err:
        current_logger.error('error :: %s :: no from_timestamp in POST data, err: %s' % (
            function_str, err))
        response_dict['status_code'] = 400
        response_dict['error'] = 'no from_timestamp passed in post data'
        return response_dict
    try:
        until_timestamp = post_data['data']['until_timestamp']
    except Exception as err:
        current_logger.error('error :: %s :: no until_timestamp in POST data, err: %s' % (
            function_str, err))
        response_dict['status_code'] = 400
        response_dict['error'] = 'no until_timestamp passed in post data'
        return response_dict
    try:
        timeseries = post_data['data']['timeseries']
    except Exception as err:
        current_logger.error('error :: %s :: no timeseries in POST data, err: %s' % (
            function_str, err))
        response_dict['status_code'] = 400
        response_dict['error'] = 'no timeseries passed in post data'
        return response_dict
    try:
        replace = post_data['data']['replace']
    except Exception as err:
        current_logger.error('error :: %s :: no replace in POST data, err: %s' % (
            function_str, err))
        response_dict['status_code'] = 400
        response_dict['error'] = 'no replace passed in post data'
        return response_dict
    try:
        verify_ssl = post_data['data']['verify_ssl']
    except Exception as err:
        current_logger.error('error :: %s :: no verify_ssl in POST data, err: %s' % (
            function_str, err))
        response_dict['status_code'] = 400
        response_dict['error'] = 'no verify_ssl passed in post data'
        return response_dict
    populated_timeseries = None
    try:
        populated_timeseries = populate_redis_metric(
                                'webapp', base_name, from_timestamp=from_timestamp,
                                until_timestamp=until_timestamp, timeseries=timeseries,
                                replace=replace, verify_ssl=verify_ssl)
        response_dict['timeseries'] = populated_timeseries
    except Exception as err:
        current_logger.error('error :: %s :: populate_redis_metric failed for %s, err: %s' % (
            function_str, base_name, err))
        response_dict['status_code'] = 500
        response_dict['error'] = 'failed to populate Redis metric time series'
    if populated_timeseries:
        current_logger.info('%s :: populated Redis with time series of length %s for %s' % (
            function_str, str(len(populated_timeseries)), base_name))
    return response_dict
