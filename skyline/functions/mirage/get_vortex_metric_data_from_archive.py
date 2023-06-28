"""
get_vortex_metric_data_archive_filename.py
"""
import logging
from os import path
import gzip
import json


# @added 20221207 - Feature #4734: mirage_vortex
#                   Feature #4732: flux vortex
def get_vortex_metric_data_from_archive(current_skyline_app, metric_data_archive):
    """
    Return the vortex_metric_data dict from the metric_data_archive.

    :param current_skyline_app: the app calling the function so the function
        knows which log to write too.
    :param metric_data_archive: the metric_data_archive, path and filename
    :type current_skyline_app: str
    :param metric_data_archive: str
    :return: vortex_metric_data
    :rtype: dict

    """
    function_str = 'functions.mirage.get_vortex_metric_data_from_archive'

    vortex_metric_data = {}

    current_skyline_app_logger = current_skyline_app + 'Log'
    current_logger = logging.getLogger(current_skyline_app_logger)

    if not path.isfile(metric_data_archive):
        current_logger.error('error :: %s :: metric_data_archive file not found %s' % (
            function_str, metric_data_archive))
        return vortex_metric_data
    try:
        with gzip.open(metric_data_archive, 'rb') as f:
            file_content = f.read()
        vortex_metric_data = json.loads(file_content)
    except Exception as err:
        current_logger.error('error :: %s :: failed ungzip and load metric_data from %s - %s' % (
            function_str, metric_data_archive, err))
    return vortex_metric_data
