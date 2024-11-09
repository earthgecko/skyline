"""
translate_prometheus_metricname.py
"""
import logging
import traceback
import re

from skyline_functions import get_redis_conn_decoded


# @added 20220607 - Task #2732: Prometheus to Skyline
#                   Branch #4300: prometheus
def translate_prometheus_metricname(
        current_skyline_app, prometheus_metricname, dotted_metricname,
        prefix=None):
    """
    Given a prometheus_metricname or dotted_metricname determine the translated
    metric name

    :param current_skyline_app: the Skyline app calling the function
    :param prometheus_metricname: the prometheus metric name
    :param dotted_metricname: the translated dotted prometheus metric name
    :type current_skyline_app: str
    :type prometheus_metricname: str
    :type dotted_metricname: str
    :return: translated_metric_name
    :rtype: str

    """
    translated_metric_name = None
    function_str = 'functions.metrics.translate_prometheus_metricname'
    logger_init = True

    redis_key = 'skyline.prometheus_to_dotted'
    redis_key_dotted = 'skyline.dotted_to_prometheus'
    query_metric = prometheus_metricname
    if dotted_metricname != '':
        if dotted_metricname:
            redis_key = redis_key_dotted
            query_metric = dotted_metricname

    try:
        redis_conn_decoded = get_redis_conn_decoded(current_skyline_app)
    except Exception as err:
        current_skyline_app_logger = current_skyline_app + 'Log'
        current_logger = logging.getLogger(current_skyline_app_logger)
        logger_init = False
        current_logger.error(traceback.format_exc())
        current_logger.error('error :: %s :: %s :: get_redis_conn_decoded failed - %s' % (
            current_skyline_app, function_str, err))
        return translated_metric_name

    try:
        translated_metric_name = redis_conn_decoded.hget(redis_key, query_metric)
    except Exception as err:
        current_skyline_app_logger = current_skyline_app + 'Log'
        current_logger = logging.getLogger(current_skyline_app_logger)
        logger_init = False
        current_logger.error(traceback.format_exc())
        current_logger.error('error :: %s :: %s :: failed to get translated_metric_name for %s: %s' % (
            current_skyline_app, function_str, str(query_metric), str(err)))

    if not translated_metric_name and prometheus_metricname:
        if prefix:
            remove_prefix = '%s.' % prefix
            unprefixed_prometheus_metricname = prometheus_metricname.replace(remove_prefix, '')
        else:
            unprefixed_prometheus_metricname = str(prometheus_metricname)
        translated_metric_name = re.sub('[^0-9a-zA-Z\\{\\}=",]+', '_', unprefixed_prometheus_metricname)
        translated_metric_name = translated_metric_name.replace('{', '.')
        translated_metric_name = translated_metric_name.replace(',', '.')
        translated_metric_name = translated_metric_name.replace('=', '.')
        translated_metric_name = translated_metric_name.replace('"', '')
        translated_metric_name = translated_metric_name.replace('}', '')
        if prefix:
            translated_metric_name = '%s%s' % (remove_prefix, translated_metric_name)

        try:
            redis_conn_decoded.hset(redis_key, query_metric, translated_metric_name)
        except Exception as err:
            if not logger_init:
                current_skyline_app_logger = current_skyline_app + 'Log'
                current_logger = logging.getLogger(current_skyline_app_logger)
            current_logger.error(traceback.format_exc())
            current_logger.error('error :: %s :: %s :: failed to hset %s to %s in %s: %s' % (
                current_skyline_app, function_str, str(query_metric),
                translated_metric_name, redis_key, str(err)))
        try:
            redis_conn_decoded.hset(redis_key_dotted, translated_metric_name, query_metric)
        except Exception as err:
            if not logger_init:
                current_skyline_app_logger = current_skyline_app + 'Log'
                current_logger = logging.getLogger(current_skyline_app_logger)
            current_logger.error(traceback.format_exc())
            current_logger.error('error :: %s :: %s :: failed to hset %s to %s in %s: %s' % (
                current_skyline_app, function_str, str(query_metric),
                translated_metric_name, redis_key, str(err)))

    return translated_metric_name
