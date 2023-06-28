"""
get_fp_minmax_scaled_data.py
"""
import logging

from settings import MEMCACHE_ENABLED

from functions.memcache.get_memcache_key import get_memcache_key
from functions.memcache.set_memcache_key import set_memcache_key
from functions.database.queries.get_fp_minmax_scaled_data import get_fp_minmax_scaled_data as db_get_fp_minmax_scaled_data


# @added 20221027 - Feature #4708: ionosphere - store and cache fp minmax data
def get_fp_minmax_scaled_data(current_skyline_app, fp_id):
    """
    Get the fp minmax scaled features for a fp from memcache or the DB, e.g.:

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
    :param fp_id: the features profile id
    :type current_skyline_app: str
    :type fp_id: int
    :return: fp_minmax_scaled_dict
    :rtype: dict

    """

    function_str = 'functions.ionosphere.get_fp_minmax_scaled_data'
    current_skyline_app_logger = current_skyline_app + 'Log'
    current_logger = logging.getLogger(current_skyline_app_logger)
    fp_minmax_scaled_dict = {}

    if MEMCACHE_ENABLED:
        memcache_key = 'ionosphere.fp.minmax_scaled_data.%s' % str(fp_id)
        try:
            memcache_fp_minmax_scaled_dict = get_memcache_key(current_skyline_app, memcache_key)
            if memcache_fp_minmax_scaled_dict:
                fp_minmax_scaled_dict = memcache_fp_minmax_scaled_dict
                current_logger.info('got minmax data from memcache for fp id %s, fp_minmax_scaled_dict: %s' % (
                    str(fp_id), str(fp_minmax_scaled_dict)))
        except Exception as err:
            current_logger.error('error :: get_memcache_key failed on %s - %s' % (
                str(memcache_key), err))
    if fp_minmax_scaled_dict:
        if 'id' in fp_minmax_scaled_dict:
            if fp_minmax_scaled_dict['id'] == fp_id:
                current_logger.info('%s :: got minmax data from memcache for fp id: %s' % (
                    function_str, str(fp_id)))
                fp_minmax_scaled_dict['data_source'] = 'memcache'
                return fp_minmax_scaled_dict

    # Lookup in DB
    try:
        fp_minmax_scaled_dict = db_get_fp_minmax_scaled_data(current_skyline_app, fp_id)
    except Exception as err:
        current_logger.error('error :: %s :: get_fp_minmax_scaled_data failed - %s' % (
            function_str, err))
    if fp_minmax_scaled_dict:
        current_logger.info('%s :: got minmax data from DB for fp id %s, fp_minmax_scaled_dict: %s' % (
            function_str, str(fp_id), str(fp_minmax_scaled_dict)))
        fp_minmax_scaled_dict['data_source'] = 'database'
        # Set in memcache
        if MEMCACHE_ENABLED:
            try:
                success = set_memcache_key(current_skyline_app, memcache_key, fp_minmax_scaled_dict)
                if success:
                    current_logger.info('%s :: set memcache %s key' % (str(function_str), memcache_key))
            except Exception as err:
                current_logger.error('error :: %s :: set_memcache_key failed to set %s - %s' % (
                    function_str, memcache_key, err))
    else:
        current_logger.info('%s :: did not find minmax data in memcache or the DB for fp id %s' % (
            function_str, str(fp_id)))

    return fp_minmax_scaled_dict
