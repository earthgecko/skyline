"""
is_skyline_busy.py
"""
import logging


def is_skyline_busy(self, current_skyline_app):
    """

    Determine if Skyline is busy

    :param self: the self object
    :param current_skyline_app: the current_skyline_app
    :type self: object
    :type current_skyline_app: str
    :return: skyline_busy
    :rtype: bool

    """
    function_str = 'is_skyline_busy'
    current_skyline_app_logger = current_skyline_app + 'Log'
    current_logger = logging.getLogger(current_skyline_app_logger)

    skyline_busy = False
    load_shedding_hash_exists = False
    try:
        load_shedding_hash_exists = self.redis_conn_decoded.exists('analyzer.metrics.last_analysis_timestamp')
    except Exception as err:
        current_logger.error('error :: %s :: failed to exists on Redis hash analyzer.metrics.last_analysis_timestamp, err: %s' % (
            function_str, err))
    if load_shedding_hash_exists:
        skyline_busy = True
        current_logger.info('is_skyline_busy :: analyzer.metrics.last_analysis_timestamp load shedding hash exists')
        return skyline_busy

    if not skyline_busy:
        analyzer_labelled_metrics_busy = False
        try:
            analyzer_labelled_metrics_busy = self.redis_conn_decoded.get('analyzer_labelled_metrics.busy')
        except Exception as err:
            current_logger.error('error :: is_skyline_busy :: failed to get analyzer_labelled_metrics.busy Redis key, err: %s' % (
                err))
        if analyzer_labelled_metrics_busy:
            skyline_busy = True
            current_logger.info('is_skyline_busy - analyzer_labelled_metrics.busy hash exists')
            return skyline_busy
    if not skyline_busy:
        mirage_busy = False
        try:
            mirage_busy = self.redis_conn_decoded.exists('mirage.busy')
        except Exception as err:
            current_logger.error('error :: is_skyline_busy :: failed to exists on Redis key mirage.busy - %s' % (
                err))
        if mirage_busy:
            skyline_busy = True
            current_logger.info('is_skyline_busy - mirage.busy key exists')
            return skyline_busy
    return skyline_busy
