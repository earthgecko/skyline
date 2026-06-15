"""
get_batch_processing_namespaces.py
"""
import logging

from skyline_functions import get_redis_conn_decoded

# @added 20240518 - Feature #5356: get_batch_processing_namespaces
#                   Feature #5352: vista - bigquery
#                   Feature #3480: batch_processing 
def get_batch_processing_namespaces(current_skyline_app):
    """
    Determine batch_processing_namespaces.

    :param current_skyline_app: the app calling the function
    :type current_skyline_app: str
    :return: external_bq_accounts
    :rtype: dict

    """
    function_str = 'functions.settings.get_batch_processing_namespaces'
    current_skyline_app_logger = current_skyline_app + 'Log'
    batch_processing_namespaces = []

    try:
        redis_conn_decoded = get_redis_conn_decoded(current_skyline_app)
    except Exception as err:
        current_logger = logging.getLogger(current_skyline_app_logger)
        current_logger.error('error :: %s :: get_redis_conn_decoded failed, err: %s' % (
            function_str, err))
        return batch_processing_namespaces

    try:
        batch_processing_namespaces = redis_conn_decoded.smembers('metrics_manager.batch_processing_namespaces')
    except Exception as err:
        current_logger = logging.getLogger(current_skyline_app_logger)
        current_logger.error('error :: %s :: could not get metrics_manager.batch_processing_namespaces Redis set, err: %s' % (
            function_str, err))
    return batch_processing_namespaces
