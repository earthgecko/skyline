"""
cloudburst_get_metric_ids_to_check.py
"""
import logging
from time import time, strftime, gmtime
from ast import literal_eval

from skyline_functions import get_redis_conn_decoded


# @added 20220920 - Feature #4674: cloudburst active events only
# Only check metrics that have been active recently in the relevant
# analyzer.illuminance.all.YYYY-MM-DD key/s handle day roll overs.
def cloudburst_get_metric_ids_to_check(current_skyline_app, unique_metric_ids):
    """
    Determine metric ids that have recent activity that need to be checked.
    """

    current_skyline_app_logger = current_skyline_app + 'Log'
    current_logger = logging.getLogger(current_skyline_app_logger)

    function_str = 'functions.luminosity.cloudburst_get_metrics_to_check'

    try:
        redis_conn_decoded = get_redis_conn_decoded(current_skyline_app)
    except Exception as err:
        current_logger.error('error :: %s :: get_redis_conn_decoded failed - %s' % (function_str, (err)))

    last_active_metrics_check_timestamp = int(time()) - 3600
    last_active_metrics_check_key = 'luminosity.cloudburst.last_active_metrics_timestamp_check'
    try:
        last_active_metrics_check_timestamp_str = redis_conn_decoded.get(last_active_metrics_check_key)
        if last_active_metrics_check_timestamp_str:
            last_active_metrics_check_timestamp = int(float(last_active_metrics_check_timestamp_str))
    except Exception as err:
        current_logger.error('error :: %s :: fail to get %s from Redis - %s' % (
            function_str, last_active_metrics_check_key, err))
        last_active_metrics_check_timestamp = int(time()) - 3600

    hour_string = str(strftime('%H', gmtime()))
    illuminance_data = {}
    yesterday_illuminance_data = {}

    # @added 20230321 - Feature #4674: cloudburst active events only
    # The keys are very large when not pruned so only get the keys and
    # then only get the data for the relevant keys
    yesterday_illuminance_timestamps = []
    yesterday_illuminance_redis_hash = None

    if hour_string == '00':
        yesterday_timestamp = int(time()) - 7200
        date_string = str(strftime('%Y-%m-%d', gmtime(yesterday_timestamp)))
        illuminance_redis_hash = 'analyzer.illuminance.all.%s' % date_string
        yesterday_illuminance_redis_hash = str(illuminance_redis_hash)
        try:
            # @modified 20230321 - Feature #4674: cloudburst active events only
            # The keys are very large when not pruned so only get the keys and
            # then only get the data for the relevant keys
            # yesterday_illuminance_data = redis_conn_decoded.hgetall(illuminance_redis_hash)
            yesterday_illuminance_timestamps = redis_conn_decoded.hkeys(illuminance_redis_hash)
        except Exception as err:
            current_logger.error('error :: %s :: fail to get %s from Redis - %s' % (
                function_str, illuminance_redis_hash, err))
    date_string = str(strftime('%Y-%m-%d', gmtime()))
    illuminance_redis_hash = 'analyzer.illuminance.all.%s' % date_string

    # @added 20230321 - Feature #4674: cloudburst active events only
    # The keys are very large when not pruned so only get the keys and
    # then only get the data for the relevant keys
    today_illuminance_timestamps = []

    try:
        # @modified 20230321 - Feature #4674: cloudburst active events only
        # The keys are very large when not pruned so only get the keys and
        # then only get the data for the relevant keys
        # today_illuminance_data = redis_conn_decoded.hgetall(illuminance_redis_hash)
        today_illuminance_timestamps = redis_conn_decoded.hkeys(illuminance_redis_hash)
    except Exception as err:
        current_logger.error('error :: %s :: fail to get %s from Redis - %s' % (
            function_str, illuminance_redis_hash, err))
    keys_to_remove = []
    now = int(time())

    # @modified 20230321 - Feature #4674: cloudburst active events only
    # Get the key data individually rather than the entire hash key
    # for ts_key in list(yesterday_illuminance_data.keys()):
    for ts_key in yesterday_illuminance_timestamps:
        ts = int(float(ts_key))
        if ts <= last_active_metrics_check_timestamp:
            continue
        # @modified 20230321 - Feature #4674: cloudburst active events only
        # Get the key data
        # illuminance_data[ts_key] = yesterday_illuminance_data[ts_key]
        yesterday_illuminance_key_data = {}
        try:
            yesterday_illuminance_key_data = redis_conn_decoded.hget(yesterday_illuminance_redis_hash, ts_key)
        except Exception as err:
            current_logger.error('error :: %s :: fail to get %s from Redis - %s' % (
                function_str, yesterday_illuminance_redis_hash, err))
        illuminance_data[ts_key] = yesterday_illuminance_key_data

    # @modified 20230321 - Feature #4674: cloudburst active events only
    # Get the key data individually rather than the entire hash key
    # for ts_key in list(today_illuminance_data.keys()):
    for ts_key in today_illuminance_timestamps:
        ts = int(float(ts_key))
        if ts <= last_active_metrics_check_timestamp:
            if ts <= (now - 7200):
                keys_to_remove.append(ts_key)
            continue
        # @modified 20230321 - Feature #4674: cloudburst active events only
        # Get the key data
        # illuminance_data[ts_key] = today_illuminance_data[ts_key]
        illuminance_key_data = {}
        try:
            illuminance_key_data = redis_conn_decoded.hget(illuminance_redis_hash, ts_key)
        except Exception as err:
            current_logger.error('error :: %s :: fail to get %s from Redis - %s' % (
                function_str, illuminance_redis_hash, err))
        illuminance_data[ts_key] = illuminance_key_data

    metric_ids_to_evaluate = []
    current_logger.info('%s :: %s timestamp keys to evaluate from %s' % (
        function_str, str(len(list(illuminance_data.keys()))), illuminance_redis_hash))

    for ts_key in list(illuminance_data.keys()):
        ts_dict = literal_eval(illuminance_data[ts_key])
        for metric_id_str in ts_dict:
            metric_id = int(float(metric_id_str))
            check = False
            if metric_id in unique_metric_ids:
                check = True
            if check and len(ts_dict[metric_id_str]['a']) >= 2:
                metric_ids_to_evaluate.append(metric_id)
    metric_ids_to_check = list(set(metric_ids_to_evaluate))

    # @modified 20230317 - why?
    # Why were keys being pruned here?  This resulted in keys being removed
    # from the current analyzer.illuminance.all.YYYY-MM-DD hash, which is not
    # desired.
    keys_to_remove = []

    if keys_to_remove:
        current_logger.info('%s :: pruning %s keys from %s' % (
            function_str, str(len(keys_to_remove)), illuminance_redis_hash))
        removed = 0
        try:
            removed = redis_conn_decoded.hdel(illuminance_redis_hash, *keys_to_remove)
        except Exception as err:
            current_logger.error('error :: %s :: fail to remove keys from %s - %s' % (
                function_str, illuminance_redis_hash, err))
        current_logger.info('%s :: removed %s keys from %s' % (
            function_str, str(removed), illuminance_redis_hash))

    return metric_ids_to_check
