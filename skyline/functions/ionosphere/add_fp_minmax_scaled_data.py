"""
add_fp_minmax_scaled_data.py
"""
import logging

from settings import MEMCACHE_ENABLED

from functions.database.queries.set_fp_minmax_scaled_data import set_fp_minmax_scaled_data
from functions.memcache.set_memcache_key import set_memcache_key


# @added 20221026 - Feature #4708: ionosphere - store and cache fp minmax data
def add_fp_minmax_scaled_data(current_skyline_app, fp_minmax_scaled_dict):
    """
    Add the fp minmax scaled features for a fp to the DB and memcache, e.g.:

    fp_minmax_scaled_dict = {
        'id': 123,
        'min_fp_value': 0.100004,
        'max_fp_value': 4.678987,
        'values_count': 1009,
        'features_count': 140,
        'features_sum': 3787.6024799786837,
        'tsfresh_version': '0.17.9',
        'calc_time': 1.186837,
    }

    :param current_skyline_app: the app calling the function
    :param fp_minmax_scaled_dict: the dictionary of the features profile minmax
        scaled features.
    :type current_skyline_app: str
    :type fp_minmax_scaled_dict: dict
    :return: success
    :rtype: boolean

    """

    function_str = 'functions.ionosphere.add_fp_minmax_scaled_data'
    current_skyline_app_logger = current_skyline_app + 'Log'
    current_logger = logging.getLogger(current_skyline_app_logger)
    success = False

    current_logger.info('%s :: adding minmax scaled data: %s' % (
        function_str, str(fp_minmax_scaled_dict)))

    try:
        fp_id = fp_minmax_scaled_dict['id']
        min_fp_value = fp_minmax_scaled_dict['min_fp_value']
        max_fp_value = fp_minmax_scaled_dict['max_fp_value']
        values_count = fp_minmax_scaled_dict['values_count']
        features_count = fp_minmax_scaled_dict['features_count']
        features_sum = fp_minmax_scaled_dict['features_sum']
        tsfresh_version = fp_minmax_scaled_dict['tsfresh_version']
        calc_time = fp_minmax_scaled_dict['calc_time']
    except Exception as err:
        current_logger.error('error :: %s :: failed to determine minmax scaled details from %s - %s' % (
            function_str, str(fp_minmax_scaled_dict), err))
        return success

    # Insert into DB
    try:
        success = set_fp_minmax_scaled_data(current_skyline_app, fp_minmax_scaled_dict)
    except Exception as err:
        current_logger.error('error :: %s :: set_fp_minmax_scaled_data failed - %s' % (
            function_str, err))

    current_logger.info('%s :: set_fp_minmax_scaled_data returned %s' % (
        str(function_str), str(success)))

    # Add to memcache
    if success and MEMCACHE_ENABLED:
        try:
            memcache_key = 'ionosphere.fp.minmax_scaled_data.%s' % str(fp_id)
            success = set_memcache_key(current_skyline_app, memcache_key, fp_minmax_scaled_dict)
            if success:
                current_logger.info('%s :: set memcache %s key' % (str(function_str), memcache_key))
        except Exception as err:
            current_logger.error('error :: %s :: set_memcache_key failed to set %s - %s' % (
                function_str, memcache_key, err))

    return success
