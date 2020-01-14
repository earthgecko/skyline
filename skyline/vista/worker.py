from __future__ import division
import logging
from time import time, sleep
from multiprocessing import Process
import os
from os import kill
from os import remove as os_remove
import traceback
from sys import version_info
import os.path
from ast import literal_eval
import datetime

# @added 20200107 - Task #3376: Enable vista and flux to deal with lower frequency data
from collections import Counter

# from redis import StrictRedis
import requests
import pandas as pd

import settings
from skyline_functions import (
    send_graphite_metric,
    # @added 20191111 - Bug #3266: py3 Redis binary objects not strings
    #                   Branch #3262: py3
    get_redis_conn, get_redis_conn_decoded)

parent_skyline_app = 'vista'
child_skyline_app = 'worker'
skyline_app_logger = '%sLog' % parent_skyline_app
logger = logging.getLogger(skyline_app_logger)
skyline_app = '%s.%s' % (parent_skyline_app, child_skyline_app)
skyline_app_logfile = '%s/%s.log' % (settings.LOG_PATH, parent_skyline_app)
skyline_app_loglock = '%s.lock' % skyline_app_logfile
skyline_app_logwait = '%s.wait' % skyline_app_logfile

try:
    SERVER_METRIC_PATH = '.%s' % settings.SERVER_METRICS_NAME
    if SERVER_METRIC_PATH == '.':
        SERVER_METRIC_PATH = ''
except:
    SERVER_METRIC_PATH = ''

skyline_app_graphite_namespace = 'skyline.%s%s.worker' % (parent_skyline_app, SERVER_METRIC_PATH)

python_version = int(version_info[0])

this_host = str(os.uname()[1])

LOCAL_DEBUG = False


class Worker(Process):
    """
    The worker process retrieves time series published to the Vista Fetcher
    Redis set, vista.fetcher.metrics.json (and in future the
    vista.fetcher.metrics.csv Redis set) and validates the latest data points
    for each metric and submits it to Skyline Flux or directly to Graphite.
    """
    def __init__(self, parent_pid):
        super(Worker, self).__init__()
        self.parent_pid = parent_pid
        self.daemon = True
        # @modified 20191111 - Bug #3266: py3 Redis binary objects not strings
        #                      Branch #3262: py3
        # if settings.REDIS_PASSWORD:
        #     self.redis_conn = StrictRedis(password=settings.REDIS_PASSWORD, unix_socket_path=settings.REDIS_SOCKET_PATH)
        # else:
        #     self.redis_conn = StrictRedis(unix_socket_path=settings.REDIS_SOCKET_PATH)
        # @added 20191111 - Bug #3266: py3 Redis binary objects not strings
        #                   Branch #3262: py3
        # Added a single functions to deal with Redis connection and the
        # charset='utf-8', decode_responses=True arguments required in py3
        self.redis_conn = get_redis_conn(skyline_app)
        self.redis_conn_decoded = get_redis_conn_decoded(skyline_app)

    def check_if_parent_is_alive(self):
        """
        Self explanatory.
        """
        try:
            kill(self.parent_pid, 0)
        except:
            exit(0)

    def run(self):
        """
        Called when the process intializes.
        """
        # Log management to prevent overwriting
        # Allow the bin/<skyline_app>.d to manage the log
        # In Vista the log management is handled be fetcher, the worker just
        # waits for the fetcher to do the log managment
        now = int(time())
        log_wait_for = now + 5
        while now < log_wait_for:
            if os.path.isfile(skyline_app_loglock):
                sleep(.1)
                now = int(time())
            else:
                now = log_wait_for + 1

        logger.info('worker :: starting log management')
        if os.path.isfile(skyline_app_loglock):
            logger.error('error :: worker :: bin/%s.d log management seems to have failed, continuing' % skyline_app)
            try:
                os_remove(skyline_app_loglock)
                logger.info('worker :: log lock file removed')
            except OSError:
                logger.error('error :: worker :: failed to remove %s, continuing' % skyline_app_loglock)
                pass
        else:
            logger.info('worker :: bin/%s.d log management done' % skyline_app)

        logger.info('worker :: starting worker')

        try:
            VISTA_ENABLED = settings.VISTA_ENABLED
            logger.info('worker :: VISTA_ENABLED is set to %s' % str(VISTA_ENABLED))
        except:
            VISTA_ENABLED = False
            logger.info('worker :: warning :: VISTA_ENABLED is not declared in settings.py, defaults to False')

        last_sent_to_graphite = int(time())
        metrics_sent_to_flux = 0

        # python-2.x and python3.x handle while 1 and while True differently
        # while 1:
        running = True
        while running:

            # Make sure Redis is up
            redis_up = False
            while not redis_up:
                try:
                    redis_up = self.redis_conn.ping()
                    if LOCAL_DEBUG:
                        logger.info('worker :: redis is up')
                except:
                    logger.error('worker :: cannot connect to redis at socket path %s' % (settings.REDIS_SOCKET_PATH))
                    sleep(2)

                    # @modified 20191111 - Bug #3266: py3 Redis binary objects not strings
                    #                      Branch #3262: py3
                    # if settings.REDIS_PASSWORD:
                    #     self.redis_conn = StrictRedis(password=settings.REDIS_PASSWORD, unix_socket_path=settings.REDIS_SOCKET_PATH)
                    # else:
                    #     self.redis_conn = StrictRedis(unix_socket_path=settings.REDIS_SOCKET_PATH)
                    self.redis_conn = get_redis_conn(skyline_app)
                    self.redis_conn_decoded = get_redis_conn_decoded(skyline_app)

            metrics_data = []
            redis_set = 'vista.fetcher.metrics.json'
            try:
                # Get a metric to validate from the Redis set

                # @modified 20191111 - Bug #3266: py3 Redis binary objects not strings
                #                      Branch #3262: py3
                # metrics_data = self.redis_conn.smembers(redis_set)
                metrics_data = self.redis_conn_decoded.smembers(redis_set)

                if LOCAL_DEBUG:
                    logger.info('worker :: got redis set data - %s' % redis_set)
            except:
                logger.error(traceback.format_exc())
                logger.error('error :: worker :: retrieving Redis set %s data' % str(redis_set))

            if not metrics_data:
                if LOCAL_DEBUG:
                    logger.info('worker :: no data from Redis set %s' % str(redis_set))
                sleep(5)

            for str_metric_data in metrics_data:
                delete_set_record = False
                remote_host_type = None
                try:

                    # @modified 20191111 - Bug #3266: py3 Redis binary objects not strings
                    #                      Branch #3262: py3
                    # Rather using get_redis_conn_decoded
                    # if python_version == 3:
                    #     str_metric_data = str_metric_data.decode('UTF-8')

                    metric_data = literal_eval(str_metric_data)
                    remote_host_type = str(metric_data[0]['remote_host_type'])
                    if LOCAL_DEBUG:
                        logger.info('worker :: got data from Redis set for remote_host_type %s' % str(remote_host_type))
                except:
                    logger.error(traceback.format_exc())
                    logger.error('error :: worker :: failed to determine remote_host_type from %s' % str(str_metric_data))
                    delete_set_record = True
                if not delete_set_record:
                    try:
                        remote_target = str(metric_data[0]['remote_target'])
                        if LOCAL_DEBUG:
                            logger.info('worker :: got data from Redis set for target %s' % str(remote_target))
                    except:
                        logger.error(traceback.format_exc())
                        logger.error('error :: worker :: failed to determine target from %s' % str(str_metric_data))
                        delete_set_record = True
                metric = None
                if not delete_set_record:
                    try:
                        metric = str(metric_data[0]['metric'])
                        if LOCAL_DEBUG:
                            logger.info('worker :: got data from Redis set for metric %s' % str(metric))
                    except:
                        logger.error(traceback.format_exc())
                        logger.error('error :: worker :: failed to determine metric from %s' % str(str_metric_data))
                        delete_set_record = True

                namespace_prefix = ''
                if not delete_set_record:
                    try:
                        namespace_prefix = str(metric_data[0]['namespace_prefix'])
                        namespace_prefix = '%s.' % namespace_prefix
                        if not namespace_prefix:
                            namespace_prefix = ''
                        if LOCAL_DEBUG:
                            logger.info('worker :: got data from Redis set for namespace_prefix %s' % str(namespace_prefix))
                    except:
                        logger.error(traceback.format_exc())
                        logger.error('error :: worker :: failed to determine namespace_prefix from %s' % str(str_metric_data))
                        delete_set_record = True

                have_data = False
                if not delete_set_record:
                    last_flux_metric_data = None
                    cache_key = 'flux.last.%s' % (metric)
                    try:
                        if python_version == 3:
                            redis_last_flux_metric_data = self.redis_conn.get(cache_key).decode('UTF-8')
                        else:
                            redis_last_flux_metric_data = self.redis_conn.get(cache_key)
                        redis_last_flux_metric_data = redis_last_flux_metric_data
                        last_flux_metric_data = literal_eval(redis_last_flux_metric_data)
                        if LOCAL_DEBUG:
                            logger.info('worker :: got last_flux_metric_data from Redis')
                    except:
                        logger.error(traceback.format_exc())
                        logger.error('error :: worker :: retrieving Redis key %s data' % str(cache_key))
                        last_flux_metric_data = False

                    last_flux_timestamp = None
                    if last_flux_metric_data:
                        try:
                            last_flux_timestamp = int(last_flux_metric_data[0])
                            if LOCAL_DEBUG:
                                logger.info('worker :: got last_flux_timestamp - %s' % str(last_flux_timestamp))
                        except:
                            logger.error(traceback.format_exc())
                            logger.error('error :: worker :: failed determining last_flux_timestamp')
                            last_flux_timestamp = False

                    # Determine the timestamp of the current minute to apply
                    # VISTA_DO_NOT_SUBMIT_CURRENT_MINUTE
                    time_now = int(time())
                    # current_minute = datetime.datetime.utcfromtimestamp(time_now).strftime('%Y-%m-%d %H:%M')
                    current_minute_hour = int(datetime.datetime.utcfromtimestamp(time_now).strftime('%H'))
                    current_minute_minute = int(datetime.datetime.utcfromtimestamp(time_now).strftime('%M'))
                    current_datetime = datetime.datetime.utcfromtimestamp(time_now).replace(hour=current_minute_hour, minute=current_minute_minute, second=0, microsecond=0)
                    current_minute_timestamp_start = int(current_datetime.strftime('%s'))

                    datapoint = None
                    last_timestamp_with_data = None
                    timeseries = []

                    # @added 20200107 - Task #3376: Enable vista and flux to deal with lower frequency data
                    metric_resolution = 60
                    metric_resolution_determined = False

                    try:
                        if python_version == 3:
                            datapoints_str = literal_eval(metric_data[0]['datapoints'])
                            metric_datapoints = literal_eval(datapoints_str)
                        else:
                            # metric_datapoints = metric_data[0]['datapoints']
                            datapoints_str = literal_eval(metric_data[0]['datapoints'])
                            metric_datapoints = literal_eval(datapoints_str)
                        # for value, timestamp in metric_data[0]['datapoints']:
                        if LOCAL_DEBUG:
                            len_metric_datapoints = len(metric_datapoints)
                            logger.info('worker :: got %s metric_datapoints - %s' % (
                                str(len_metric_datapoints),
                                str(metric_datapoints)))

                        # @added 20200107 - Task #3376: Enable vista and flux to deal with lower frequency data
                        # Determine resolution
                        resolution_timestamps = []
                        for metric_datapoint in metric_datapoints:
                            timestamp = int(metric_datapoint[0])
                            resolution_timestamps.append(timestamp)
                        timestamp_resolutions = []
                        if resolution_timestamps:
                            last_timestamp = None
                            for timestamp in resolution_timestamps:
                                if last_timestamp:
                                    resolution = timestamp - last_timestamp
                                    timestamp_resolutions.append(resolution)
                                    last_timestamp = timestamp
                                else:
                                    last_timestamp = timestamp
                        if timestamp_resolutions:
                            try:
                                timestamp_resolutions_count = Counter(timestamp_resolutions)
                                ordered_timestamp_resolutions_count = timestamp_resolutions_count.most_common()
                                metric_resolution = int(ordered_timestamp_resolutions_count[0][0])
                                if metric_resolution > 0:
                                    metric_resolution_determined = True
                            except:
                                logger.error(traceback.format_exc())
                                logger.error('error :: worker :: failed to determine metric_resolution from %s' % (
                                    str(metric_data)))
                        if metric_resolution_determined:
                            cache_key = 'vista.last.resolution.%s' % metric
                            try:
                                # Update Redis key
                                self.redis_conn.setex(cache_key, 3600, metric_resolution)
                            except:
                                logger.error(traceback.format_exc())
                                logger.error('error :: fetcher :: failed to set Redis key - %s' % (
                                    cache_key))

                        for metric_datapoint in metric_datapoints:
                            # @20191010 - Branch #3140: vista
                            # fetcher passes through preformatted data points that
                            # are in the same format/order for both graphite and
                            # prometheus
                            # if remote_host_type == 'graphite':
                            #     value = float(metric_datapoint[0])
                            #     timestamp = int(metric_datapoint[1])
                            # if remote_host_type == 'prometheus':
                            #     value = float(metric_datapoint[1])
                            #     timestamp = int(metric_datapoint[0])
                            timestamp = int(metric_datapoint[0])
                            value = float(metric_datapoint[1])

                            append_to_timeseries = False
                            if last_flux_timestamp:
                                if int(timestamp) > last_flux_timestamp:
                                    # timeseries.append([timestamp, value])
                                    append_to_timeseries = True
                            else:
                                # timeseries.append([timestamp, value])
                                append_to_timeseries = True

                            # Here if the timestamp of the data point falls
                            # within the current minute, it is discarded and not
                            # sent to flux, to ensure that high frequency metrics
                            # can have their minutely bins fully populated before
                            # they are submitted to Graphite
                            if settings.VISTA_DO_NOT_SUBMIT_CURRENT_MINUTE:
                                if int(timestamp) >= current_minute_timestamp_start:
                                    append_to_timeseries = False
                            if append_to_timeseries:
                                timeseries.append([timestamp, value])

                        last_timestamp_with_data = 0
                        for timestamp, value in timeseries[::-1]:
                            has_value = False
                            if value == 0.0:
                                has_value = True
                            if value:
                                has_value = True
                            if has_value:
                                last_timestamp_with_data = int(timestamp)
                                datapoint = value
                                break
                        if last_timestamp_with_data:
                            have_data = True
                    except:
                        logger.error(traceback.format_exc())
                        logger.error('error :: worker :: failed to determine datapoints from %s' % (
                            str(metric_data)))
                        delete_set_record = True
                if not timeseries:
                    logger.info('worker :: after processing, there were no valid data points in %s' % (
                        str(metric_data)))
                    delete_set_record = True
                if not have_data and timeseries:
                    logger.error('error :: worker :: failed to determine last_timestamp_with_data from %s' % (
                        str(metric_data)))
                    delete_set_record = True
                if delete_set_record:
                    try:
                        redis_set = 'vista.fetcher.metrics.json'
                        self.redis_conn.srem(redis_set, str_metric_data)
                    except:
                        logger.error(traceback.format_exc())
                        logger.error('error :: worker :: failed to delete data from Redis set %s, data - ' % (
                            str(redis_set), str(str_metric_data)))
                    continue

                if not metric:
                    continue

                valid_data = True
                if last_flux_timestamp and last_timestamp_with_data:
                    if int(last_timestamp_with_data) <= last_flux_timestamp:
                        valid_data = False
                if not valid_data:
                    redis_set = 'vista.fetcher.metrics.json'
                    logger.info('worker :: no valid data in fetched data removing from Redis set %s - data - %s' % (
                        redis_set, str(str_metric_data)))
                    try:
                        self.redis_conn.srem(redis_set, str_metric_data)
                    except:
                        logger.error(traceback.format_exc())
                        logger.error('error :: worker :: failed to delete data from Redis set %s, data - %s' % (
                            redis_set, str(str_metric_data)))
                    continue

                if valid_data:
                    flux_host = 'http://%s:%s' % (settings.FLUX_IP, settings.FLUX_PORT)

                    # Resample
                    resample_at = None
                    if resample_at == 'none' or resample_at == '0Min':
                        resample_at = False
                    if resample_at == 'None' or resample_at == '0min':
                        resample_at = False
                    if resample_at is None or resample_at == '0' or resample_at == 0:
                        resample_at = False
                    if resample_at:
                        try:
                            df = pd.DataFrame(timeseries)
                            df.columns = ['timestamp', 'value']
                            df['timestamp'] = pd.to_datetime(df['timestamp'], unit='s', origin='unix')
                            df = df.set_index('timestamp')
                            resampled_df = df.resample(resample_at).sum()
                            resampled_timeseries = []
                            for index, row in resampled_df.iterrows():
                                timestamp = int(index.strftime('%s'))
                                resampled_timeseries.append([timestamp, row[0]])
                            timeseries = resampled_timeseries
                            timeseries_length = len(timeseries)
                            logger.info('worker :: time series resampled at %s resulting in %s data points to send to Graphite' % (
                                str(resample_at), str(timeseries_length)))
                        except:
                            logger.error(traceback.format_exc())
                            logger.error('error :: worker :: failed to resample time series at %s for %s with time series %s' % (
                                str(resample_at), str(metric), str(timeseries)))

                    for timestamp, value in timeseries:
                        flux_url = '%s/metric_data?metric=%s&value=%s&timestamp=%s&key=%s' % (
                            flux_host, metric, str(datapoint),
                            str(timestamp),
                            settings.FLUX_SELF_API_KEY)
                        success = False
                        try:
                            response = requests.get(flux_url)
                            if response.status_code == 200:
                                success = True
                            elif response.status_code == 204:
                                success = True
                        except:
                            logger.error(traceback.format_exc())
                            logger.error('error :: worker :: failed to request %s' % str(flux_url))
                        if not success:
                            logger.error('error :: worker :: http status code - %s, reason - %s' % (
                                str(response.status_code), str(response.reason)))

                    if success:
                        metrics_sent_to_flux += 1
                        redis_set = 'vista.fetcher.metrics.json'

                        # @added 20191011 - Task #3258: Reduce vista logging
                        timeseries_length = len(timeseries)

                        # @modified 20191011 - Task #3258: Reduce vista logging
                        # logger.info('worker :: data submitted to flux OK, removing data from Redis set %s' % (
                        #     redis_set))
                        logger.info('worker :: %s data points submitted to flux OK for %s' % (
                            str(timeseries_length), metric))
                        try:
                            self.redis_conn.srem(redis_set, str_metric_data)
                        except:
                            logger.error(traceback.format_exc())
                            logger.error('error :: worker :: failed to delete data from Redis set %s, data - %s' % (
                                redis_set, str(str_metric_data)))

                        redis_set = 'vista.fetcher.unique_metrics'
                        try:
                            self.redis_conn.sadd(redis_set, remote_target)
                        except:
                            logger.error(traceback.format_exc())
                            logger.error('error :: worker :: failed to add %s to Redis set %s' % (
                                remote_target, redis_set))

            time_now = int(time())
            if (time_now - last_sent_to_graphite) >= 60:
                logger.info('worker :: metrics sent_to_flux in last 60 seconds - %s' % str(metrics_sent_to_flux))
                send_metric_name = '%s.metrics_sent_to_flux' % skyline_app_graphite_namespace
                try:
                    send_graphite_metric(parent_skyline_app, send_metric_name, str(metrics_sent_to_flux))
                    last_sent_to_graphite = int(time())
                    metrics_sent_to_flux = 0
                except:
                    logger.error(traceback.format_exc())
                    logger.error('error :: worker :: failed to send_graphite_metric %s with %s' % (
                        send_metric_name, str(metrics_sent_to_flux)))
