"""
get_dawn_status.py
"""
import datetime
from skyline_functions import get_redis_conn_decoded

# @added 20260221 - Feature #5712: skyline.dawn
def dawn_status(current_skyline_app, dawn_redis_key):
    """
    Get the expiry timestamp of the passed dawn key if it exists and return the
    expiry date string.

    :param current_skyline_app: the skyline app
    :param dawn_redis_key: the skyline.dawn key
    :type current_skyline_app: str
    :type dawn_redis_key: str
    :return: expiry_date
    :rtype: str

    """
    expiry_date = None
    try:
        redis_conn_decoded = get_redis_conn_decoded(current_skyline_app)
    except:
        raise

    expiry_timestamp = 0
    try:
        expiry_timestamp = redis_conn_decoded.get(dawn_redis_key)
    except:
        pass
    if expiry_timestamp:
        try:
            expiry_date = datetime.datetime.fromtimestamp(int(expiry_timestamp)).strftime('%Y-%m-%d %H:%M:%S')
        except:
            raise
    return expiry_date
