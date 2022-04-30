"""
get_last_metric_data.py
"""
from ast import literal_eval

try:
    from settings import MEMCACHE_ENABLED
except:
    MEMCACHE_ENABLED = False
from skyline_functions import get_redis_conn_decoded
from functions.memcache.get_memcache_key import get_memcache_key


# @added 20220429 - Feature #4536: Handle Redis failure
# Swap to using a Redis hash instead of the flux.last.<metric> keys
def get_last_metric_data(current_skyline_app, base_name):
    """
    Get the last metric data from Redis or memcache and return a dict of the
    timestamp and value, e.g. {'timestamp': 1651211940, 'value': 0.92}

    :param current_skyline_app: the app calling the function
    :param base_name: the metric base_name
    :type current_skyline_app: str
    :type base_name: str
    :return: last_metric_data
    :rtype: dict

    """
    last_metric_data_dict = {}
    last_metric_data_dict['from_redis'] = False
    last_metric_data_dict['from_memcache'] = False
    last_metric_data_dict['error'] = None
    try:
        redis_conn_decoded = get_redis_conn_decoded(current_skyline_app)
    except Exception as err:
        last_metric_data_dict['error'] = [err]
        redis_conn_decoded = None

    redis_last_metric_data_dict_str = None
    try:
        redis_last_metric_data_dict_str = redis_conn_decoded.hget('flux.last.metric_data', base_name)
        if redis_last_metric_data_dict_str:
            last_metric_data_dict = literal_eval(redis_last_metric_data_dict_str)
            if last_metric_data_dict:
                last_metric_data_dict['from_redis'] = True
                last_metric_data_dict['from_memcache'] = False
    except Exception as err:
        error = last_metric_data_dict['error']
        if error:
            error.append(err)
        else:
            error = [err]
        last_metric_data_dict['error'] = error
        last_metric_data_dict['from_redis'] = False
        last_metric_data_dict['from_memcache'] = False
        if MEMCACHE_ENABLED:
            try:
                memcache_flux_last_metric_data = get_memcache_key('flux', 'flux.last.metric_data')
                if not memcache_flux_last_metric_data:
                    # Only check once and fail once
                    memcache_flux_last_metric_data = False
            except Exception as err:
                error = last_metric_data_dict['error']
                if error:
                    error.append(err)
                else:
                    error = [err]
                last_metric_data_dict['error'] = error
                memcache_flux_last_metric_data = False

            if memcache_flux_last_metric_data:
                try:
                    error = last_metric_data_dict['error']
                    last_metric_data_dict = literal_eval(memcache_flux_last_metric_data[base_name])
                    last_metric_data_dict['from_redis'] = False
                    last_metric_data_dict['from_memcache'] = True
                    if error:
                        last_metric_data_dict['error'] = error
                except:
                    error = last_metric_data_dict['error']
                    if error:
                        error.append(err)
                    else:
                        error = [err]
                    last_metric_data_dict['error'] = error
                    last_metric_data_dict['from_redis'] = False
                    last_metric_data_dict['from_memcache'] = False

    return last_metric_data_dict
