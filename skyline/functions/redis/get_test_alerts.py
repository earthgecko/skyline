import logging
# import traceback
from ast import literal_eval

from skyline_functions import get_redis_conn_decoded


# @added 20220315 - Feature #4482: Test alerts
def get_test_alerts(current_skyline_app):
    """
    Return the dictionary of test alerts for an app.

    :param current_skyline_app: the app calling the function
    :type current_skyline_app: str
    :return: test_alerts
    :rtype: dict

    """

    function_str = 'functions.redis.get_test_alerts'
    test_alerts = {}
    current_skyline_app_logger = current_skyline_app + 'Log'
    current_logger = logging.getLogger(current_skyline_app_logger)

    redis_conn_decoded = None
    try:
        redis_conn_decoded = get_redis_conn_decoded(current_skyline_app)
    except Exception as err:
        # current_logger.error(traceback.format_exc())
        current_logger.error('error :: %s :: failed to connect to Redis - %s' % (
            function_str, err))

    if not redis_conn_decoded:
        return test_alerts

    test_alerts_data = None
    redis_key = '%s.test_alerts' % current_skyline_app
    try:
        test_alerts_data = redis_conn_decoded.hgetall(redis_key)
    except Exception as err:
        # current_logger.error(traceback.format_exc())
        current_logger.error('error :: %s :: failed to hgetall Redis key %s - %s' % (
            function_str, redis_key, err))
        test_alerts = {}
    if test_alerts_data:
        for key in list(test_alerts_data.keys()):
            try:
                test_alerts[float(key)] = literal_eval(test_alerts_data[key])
            except Exception as err:
                # current_logger.error(traceback.format_exc())
                current_logger.error('error :: %s :: failed to literal_eval %s - %s' % (
                    function_str, str(test_alerts_data[key]), err))

    return test_alerts
