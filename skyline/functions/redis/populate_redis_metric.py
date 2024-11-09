"""
populate_redis_metric.py
"""
import copy
import gzip
import json
import logging
import os

from msgpack import packb
from redis import WatchError
import requests

import settings

from skyline_functions import get_redis_conn_decoded, get_graphite_metric
from functions.cluster.is_shard_metric import is_shard_metric
from functions.cluster.shard_host import shard_host
from functions.redis.redis_rename_key import redis_rename_key

try:
    HORIZON_SHARDS = copy.deepcopy(settings.HORIZON_SHARDS)
except:
    HORIZON_SHARDS = {}
try:
    HORIZON_SHARD_DEBUG = settings.HORIZON_SHARD_DEBUG
except:
    HORIZON_SHARD_DEBUG = True
number_of_horizon_shards = 0
this_host = str(os.uname()[1])
HORIZON_SHARD = 0
if HORIZON_SHARDS:
    number_of_horizon_shards = len(HORIZON_SHARDS)
    HORIZON_SHARD = HORIZON_SHARDS[this_host]
number_of_horizon_test_shards = 0
HORIZON_TEST_SHARD = 0
try:
    HORIZON_TEST_SHARDS = copy.deepcopy(settings.HORIZON_TEST_SHARDS)
except:
    HORIZON_TEST_SHARDS = {}
if HORIZON_TEST_SHARDS:
    number_of_horizon_shards = len(HORIZON_TEST_SHARDS)
    HORIZON_TEST_SHARD = HORIZON_TEST_SHARDS[this_host]


# @added 20240520 - Feature #5354: functions.redis.populate_redis_metric
#                   Feature #5352: vista - bigquery
def populate_redis_metric(
        current_skyline_app, base_name, from_timestamp=0,
        until_timestamp=0, timeseries=[], replace=False, verify_ssl=True):
    """
    Populate a tradition Redis metric time series key with data passed or from
    Graphite with data from_timestamp to until_timestamp.  If a timeseries is
    passed it must be passed as a list e.g.
    [[ts, value], [ts, value], ..., [ts, value]]
    This function automatically handles cluster sharding and populates the
    shard host Redis.
    Returns the time series that was inserted into Redis.

    :param current_skyline_app: the app calling the function
    :param base_name: the metric base_name
    :param from_timestamp: the from timestamp
    :param until_timestamp: the until timestamp
    :param timeseries: a time series list
    :param replace: whether to replace existing key data for the metric
    :param verify_ssl: whether to verify_ssl or not
    :type current_skyline_app: str
    :type base_name: str
    :type from_timestamp: int
    :type until_timestamp: int
    :type timeseries: list
    :type replace: bool
    :type verify_ssl: bool
    :return: timeseries
    :rtype: list

    """

    function_str = 'functions.redis.populate_redis_metric'
    current_skyline_app_logger = current_skyline_app + 'Log'
    current_logger = logging.getLogger(current_skyline_app_logger)
    submitted_timeseries = []

    shost = None
    if HORIZON_SHARDS:
        shard_metric = is_shard_metric(base_name)
        if not shard_metric:
            try:
                shost = shard_host(base_name, return_fqdn=False)
            except Exception as err:
                current_logger.error('error :: %s :: shard_host failed with %s, err: %s' % (
                    function_str, base_name, err))
    if shost:
        for remote_url, remote_user, remote_password, shostname in settings.REMOTE_SKYLINE_INSTANCES:
            if shostname == shost:
                break
        try:
            # Although a POST request query parameters are added for tracking
            # and debugging in the log
            shard_url = '%s/api?populate_redis_metric&base_name=%s&from_timestamp=%s&until_timestamp=%s' % (
                remote_url, base_name, str(from_timestamp), str(until_timestamp))
        except Exception as err:
            current_logger.error('error :: %s :: failed to construct the shard_url for %s, err: %s' % (
                function_str, base_name, err))
        postData = {
            'base_name': base_name, 'from_timestamp': from_timestamp,
            'until_timestamp': until_timestamp, 'timeseries': timeseries,
            'replace': replace, 'verify_ssl': verify_ssl
        }
        r = None
        try:
            headers = {'content-encoding': 'gzip', "content-type": "application/json"}
            payload = gzip.compress(json.dumps(postData).encode('utf-8'))
            r = requests.post(shard_url, auth=(remote_user, remote_password), data=payload, headers=headers, timeout=10, verify=verify_ssl)
            current_logger.info('%s :: %s returned status_code: %s' % (
                function_str, shost, str(r.status_code)))
        except Exception as err:
            current_logger.error('error :: %s :: request failed on %s, err: %s' % (
                function_str, str(shard_url), err))
        if r:
            try:
                response_json = r.json()
                submitted_timeseries = response_json['data']['timeseries']
            except Exception as err:
                current_logger.error('error :: %s :: failed to get timeseries from shard response, err: %s' % (
                    function_str, err))
        return submitted_timeseries

    metric_name = '%s' % str(base_name)
    if len(settings.FULL_NAMESPACE) > 0:
        metric_name = '%s%s' % (settings.FULL_NAMESPACE, str(base_name))

    # If the timeseries was passed in ensure there are only float values
    if timeseries:
        cleaned_timeseries = []
        for datapoint in timeseries:
            try:
                new_datapoint = [int(datapoint[0]), float(datapoint[1])]
                cleaned_timeseries.append(new_datapoint)
            # Added nosec to exclude from bandit tests
            except:  # nosec
                continue
        timeseries = list(cleaned_timeseries)

    # If no timeseries was passed surface the data from Graphite
    if len(timeseries) == 0:
        if not from_timestamp or not isinstance(from_timestamp, int):
            current_logger.error('error :: %s :: no valid from_timestamp passed' % function_str)
            return submitted_timeseries
        if not until_timestamp or not isinstance(until_timestamp, int):
            current_logger.error('error :: %s :: no valid until_timestamp passed' % function_str)
            return submitted_timeseries
        try:
            timeseries = get_graphite_metric(current_skyline_app, base_name, from_timestamp, until_timestamp, 'list', 'object')
            if timeseries:
                current_logger.info('%s :: fetched time series for %s from Graphite, len(timeseries): %s, from_timestamp: %s, until_timestamp:%s' % (
                    function_str, base_name, str(len(timeseries)),
                    str(from_timestamp), str(until_timestamp)))
        except Exception as err:
            current_logger.error('error :: %s :: get_graphite_metric failed, err: %s' % (function_str, err))

    if not timeseries:
        current_logger.warning('warning :: %s :: no Graphite timeseries from %s to populate in Redis key %s' % (
            function_str, base_name, metric_name))
        return submitted_timeseries

    try:
        redis_conn_decoded = get_redis_conn_decoded(current_skyline_app)
    except Exception as err:
        current_logger.error('error :: %s :: get_redis_conn_decoded failed, err: %s' % (
            function_str, err))

    key_exists = False
    if replace:
        redis_key_renamed = False
        new_key = 'populate_redis_metric.replaced.%s' % metric_name
        try:
            key_exists = redis_conn_decoded.exists(metric_name)
        except Exception as err:
            current_logger.error('error :: %s :: exists failed on %s, err: %s' % (
                function_str, metric_name, err))
        if key_exists:
            try:
                redis_key_renamed = redis_rename_key(current_skyline_app, metric_name, new_key, log=True)
            except Exception as err:
                current_logger.error('error :: %s :: redis_rename_key failed renaming %s to %s, err: %s' % (
                    function_str, metric_name, new_key, err))
            if redis_key_renamed:
                current_logger.info('%s :: renamed Redis key %s to %s' % (
                    function_str, metric_name, new_key))

    full_uniques = '%sunique_metrics' % settings.FULL_NAMESPACE
    try:
        pipe = redis_conn_decoded.pipeline()
    except Exception as err:
        current_logger.error('error :: get_metric_timeseries failed no backup_key_data, err: %s' % err)
    key = str(metric_name)
    success = False
    try:
        for item in timeseries:
            try:
                pipe.append(str(key), packb(item))
                submitted_timeseries.append(item)
            except WatchError:
                current_logger.error('error :: %s :: WatchError - %s' % (function_str, str(key)))
            except Exception as err:
                current_logger.error('error :: %s :: error on pipe.append to %s, err: %s' % (
                    function_str, str(key), err))
        try:
            pipe.sadd(full_uniques, str(key))
        except WatchError:
            current_logger.error('error :: %s :: WatchError - %s' % (function_str, str(key)))
        except Exception as err:
            current_logger.error('error :: %s :: error on pipe.sadd, err: %s' % (function_str, err))
        try:
            pipe.execute()
            success = True
        except WatchError:
            current_logger.error('error :: %s :: WatchError - %s' % (function_str, str(key)))
            submitted_timeseries = []
        except Exception as err:
            current_logger.error('error :: %s :: error on pipe.execute: %s' % (function_str, err))
            submitted_timeseries = []
    except WatchError:
        current_logger.error('error :: %s :: WatchError - %s' % (function_str, str(key)))
        submitted_timeseries = []
    except Exception as err:
        current_logger.error('error :: failed to add timeseries data to Redis key, err: %s' % err)
        submitted_timeseries = []

    if replace and not success:
        if key_exists:
            current_logger.error('error :: %s :: success: %s, replacing the Redis key %s failed, restoring original key' % (
                function_str, str(success), metric_name))
            redis_key_renamed = False
            try:
                redis_key_renamed = redis_rename_key(current_skyline_app, new_key, metric_name, log=True)
            except Exception as err:
                current_logger.error('error :: %s :: redis_rename_key failed renaming %s to %s, err: %s' % (
                    function_str, new_key, metric_name, err))
            if redis_key_renamed:
                current_logger.info('%s :: renamed Redis key %s to %s' % (
                    function_str, new_key, metric_name))

    if replace and success and key_exists:
        current_logger.info('%s :: success: %s, removing back up key %s' % (
            function_str, str(success), new_key))
        try:
            redis_conn_decoded.delete(new_key)
        except Exception as err:
            current_logger.error('error :: %s :: failed to delete %s, err: %s' % (
                function_str, new_key, err))

    return submitted_timeseries
