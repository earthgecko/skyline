from __future__ import division
import logging
from time import time, sleep
from datetime import datetime, timedelta
from threading import Thread
from multiprocessing import Process
import os
# @modified 20191115 - Branch #3262: py3
# from os import kill, getpid
from os import kill
import traceback
import re
from sys import version_info
import os.path
from ast import literal_eval
try:
    from urllib.parse import quote
except:
    from urllib import quote

# @modified 20191115 - Branch #3262: py3
# from redis import StrictRedis
import requests

import settings
python_version = int(version_info[0])

if True:
    from skyline_functions import (
        send_graphite_metric, filesafe_metricname,
        # @added 20191111 - Bug #3266: py3 Redis binary objects not strings
        #                   Branch #3262: py3
        get_redis_conn, get_redis_conn_decoded,
        # @added 20201009 - Feature #3780: skyline_functions - sanitise_graphite_url
        #                   Bug #3778: Handle single encoded forward slash requests to Graphite
        sanitise_graphite_url)

parent_skyline_app = 'vista'
child_skyline_app = 'fetcher'
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

skyline_app_graphite_namespace = 'skyline.%s%s.fetcher' % (
    parent_skyline_app, SERVER_METRIC_PATH)

python_version = int(version_info[0])

this_host = str(os.uname()[1])

try:
    VISTA_ENABLED = settings.VISTA_ENABLED
except:
    VISTA_ENABLED = False

# @added 20210512 - Feature #4064: VERBOSE_LOGGING
try:
    VERBOSE_LOGGING = settings.VISTA_VERBOSE_LOGGING
except:
    VERBOSE_LOGGING = False

USE_FLUX = False
LOCAL_DEBUG = False


class Fetcher(Thread):
    """
    The fetcher thread asynchronisly retrieves the latest data points for
    metrics from multiple endpoints using asyncio and aiohttp and submits the
    data to the Redis set, vista.fetcher.metrics.json for the worker to process.
    """
    def __init__(self, parent_pid):
        super(Fetcher, self).__init__()
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
            # @added 20201203 - Bug #3856: Handle boring sparsely populated metrics in derivative_metrics
            # Log warning
            logger.warning('warning :: parent process is dead')
            exit(0)

    def fetch_process(self, i, metrics_to_fetch):

        fetch_process_start = time()
        logger.info('fetcher :: fetch_process started')
        metrics_to_fetch_count = len(metrics_to_fetch)
        logger.info('fetcher :: fetch_process to fetch %s metrics' % str(metrics_to_fetch_count))
        if LOCAL_DEBUG:
            logger.info('fetcher :: metrics_to_fetch - %s' % str(metrics_to_fetch))

        # @added 20191127 - Feature #3338: Vista - batch Graphite requests
        def param_value_from_url(url, param_name):
            param_value = None
            for i in url.split('?', 1)[-1].split('&'):
                if i.startswith(param_name + '='):
                    param_value = i.split('=')[-1]
            return param_value

        # @added 20220111 - Feature #4370: Manage vista.fetcher.metrics.json Redis set
        logger.info('fetcher :: managing old entries in Redis set vista.fetcher.metrics.json')
        vista_list = []
        try:
            vista_list = list(self.redis_conn_decoded.smembers('vista.fetcher.metrics.json'))
        except Exception as err:
            logger.error('error :: fetcher :: failed to get Redis set vista.fetcher.metrics.json to manage - %s' % (
                err))
            vista_list = []
        vista_list_manage_error = None
        vista_list_manage_removed = 0
        for item in vista_list:
            last_metric_ts = 0
            try:
                data_list = literal_eval(item)
                last_metric_ts = int(literal_eval(literal_eval(data_list[0]['datapoints']))[-1][0])
            except Exception as err:
                vista_list_manage_error = 'error :: fetcher :: failed to determine timestamp from vista.fetcher.metrics.json data - %s - %s' % (
                    str(item), err)
            if last_metric_ts:
                if last_metric_ts < (int(fetch_process_start) - 86400):
                    try:
                        self.redis_conn_decoded.srem('vista.fetcher.metrics.json', item)
                        vista_list_manage_removed += 1
                    except Exception as err:
                        vista_list_manage_error = 'error :: fetcher :: failed to remove data from vista.fetcher.metrics.json data - %s - %s' % (
                            str(item), err)
        logger.info('fetcher :: manage Redis set vista.fetcher.metrics.json had %s entries, removed %s entries' % (
            str(len(vista_list)), str(vista_list_manage_removed)))
        if vista_list_manage_error:
            logger.error('error :: fetcher :: manage Redis set vista.fetcher.metrics.json had errors')
            logger.error('%s' % vista_list_manage_error)

        # @added 20191127 - Feature #3338: Vista - batch Graphite requests
        # Fetch metrics from the same Graphite host that have the same from
        # parameter in batches
        try:
            graphite_batch_target_count = settings.VISTA_GRAPHITE_BATCH_SIZE
        except:
            graphite_batch_target_count = 20
        graphite_batches = []  # [batch_no, remote_host, from_timestamp, url]
        in_batch_responses = []
        batch_number = 0
        for remote_host_type, frequency, remote_target, graphite_target, metric, url, namespace_prefix, api_key, token, user, password in metrics_to_fetch:
            if remote_host_type != 'graphite':
                continue
            try:
                url_elements = url.split('/')
                remote_host = url_elements[2]
            except Exception as e:
                logger.error('error :: fetcher :: failed to determine the remote_host from the url - %s - %s' % (
                    str(url), e))
            from_timestamp = None
            try:
                from_timestamp_str = param_value_from_url(url, 'from')
                try:
                    from_timestamp = int(from_timestamp_str)
                except:
                    from_timestamp = None
            except Exception as e:
                logger.error('error :: fetcher :: failed to determine the timestamp from the from url parameter - %s - %s' % (
                    str(url), e))
            if not from_timestamp:
                continue
            target = None
            try:
                target = param_value_from_url(url, 'target')
            except Exception as e:
                logger.error('error :: fetcher :: failed to determine the metric from the target url parameter - %s - %s' % (
                    str(url), e))
            added_to_batch = False
            add_to_batch = False
            for batch_number, batch_remote_host, batch_from_timestamp, batch_url in graphite_batches:
                if added_to_batch:
                    continue
                try:
                    if remote_host == batch_remote_host:
                        if str(from_timestamp) == str(batch_from_timestamp):
                            try:
                                if batch_url.count('target') < graphite_batch_target_count:
                                    add_to_batch = int(batch_number)
                            except Exception as e:
                                logger.error('error :: fetcher :: failed to determine whether to add to batch - %s' % e)
                                continue
                except:
                    logger.error('error :: fetcher :: failed to determine whether to add to batch')
            if not add_to_batch:
                batch_number += 1
                batch_url = url
                batch_data = [batch_number, remote_host, from_timestamp_str, url]
                graphite_batches.append(batch_data)
                added_to_batch = batch_number
            if add_to_batch:
                new_graphite_batches = []
                for batch_number, batch_remote_host, batch_from_timestamp, batch_url in graphite_batches:
                    if batch_number == add_to_batch:
                        new_end = '&target=%s&format=json' % remote_target
                        new_url = batch_url.replace('&format=json', new_end)
                        batch_url = new_url
                        added_to_batch = batch_number
                    batch_data = [batch_number, batch_remote_host, batch_from_timestamp, batch_url]
                    new_graphite_batches.append(batch_data)
                graphite_batches = new_graphite_batches
            if added_to_batch:
                in_batch_responses.append(target)
        batch_responses = []
        start_batch_fetches = int(time())
        for batch_number, remote_host, from_timestamp_str, url in graphite_batches:

            # @added 20201009 - Feature #3780: skyline_functions - sanitise_graphite_url
            #                   Bug #3778: Handle single encoded forward slash requests to Graphite
            sanitised = False
            sanitised, url = sanitise_graphite_url(skyline_app, url)

            try:
                batch_response = requests.get(url)
                batch_js = batch_response.json()
                batch_responses.append(batch_js)
            except Exception as e:
                logger.error(traceback.format_exc())
                logger.error('error :: fetcher :: failed to get valid response for batch request %s - %s' % (
                    str(url), e))
        if batch_responses:
            end_batch_fetches = int(time())
            time_to_fetch_batches = end_batch_fetches - start_batch_fetches
            logger.info('fetcher :: %s metric batch requests of %s metrics per batch were fetched in %s seconds' % (
                str(len(batch_responses)), str(graphite_batch_target_count), str(time_to_fetch_batches)))
        if in_batch_responses:
            logger.info('fetcher :: %s metrics were fetched in batch requests' % str(len(in_batch_responses)))

        for remote_host_type, frequency, remote_target, graphite_target, metric, url, namespace_prefix, api_key, token, user, password in metrics_to_fetch:
            success = False

            # @added 20191127 - Feature #3338: Vista - batch Graphite requests
            # Get the metric timeseries from the batch responses and if it is
            # not found no js variable will be set and the metric will be
            # requested individually as per the default behaviour
            js = None
            batched_response = False
            if remote_target in in_batch_responses:
                try:
                    for responses in batch_responses:
                        for i in responses:
                            if str(i['target']) == remote_target:
                                js = i
                                batched_response = True
                                success = True
                                if VERBOSE_LOGGING:
                                    logger.info('fetcher :: data for %s was fetched in a batch' % str(remote_target))
                except Exception as e:
                    logger.error(traceback.format_exc())
                    logger.error('error :: fetcher :: failed to detemine if %s was in batch_responses - %s' % (
                        str(remote_target), e))

            # @added 20201009 - Feature #3780: skyline_functions - sanitise_graphite_url
            #                   Bug #3778: Handle single encoded forward slash requests to Graphite
            sanitised = False
            sanitised, url = sanitise_graphite_url(skyline_app, url)

            # @modified 20191127 - Feature #3338: Vista - batch Graphite requests
            # Wrapped in if not success
            response = None
            if not success:
                try:
                    # @modified 20191011 - Task #3258: Reduce vista logging
                    if LOCAL_DEBUG:
                        logger.info('fetcher :: getting data from %s' % str(url))
                    response = requests.get(url)
                    if response.status_code == 200:
                        success = True
                except Exception as e:
                    logger.error(traceback.format_exc())
                    # @modified 20191115 - Branch #3262: py3
                    # Do not report last response data
                    # logger.error('error :: fetcher :: http status code - %s, reason - %s' % (
                    #     str(response.status_code), str(response.reason)))
                    logger.error('error :: fetcher :: failed to get data from %s - %s' % (
                        str(url), e))
            if not success:
                continue

            # @added 20191111 - Bug #3312: flux - populate_metric_worker - handle None in datapoints
            # And set flux.last key is the returned value from the remote is
            # null so that time series that are mostly null do not keep on
            # getting added to flux populate_metric by Vista
            raw_timeseries = []

            datapoints = None
            try:
                # @modified 20191127 - Feature #3338: Vista - batch Graphite requests
                # js = response.json()
                if not js:
                    js = response.json()
                else:
                    if VERBOSE_LOGGING:
                        logger.info('fetcher :: data for %s was fetched in a batch' % str(remote_target))

                if remote_host_type == 'graphite':
                    # @modified 20191127 - Feature #3338: Vista - batch Graphite requests
                    # datapoints = js[0]['datapoints']
                    if not batched_response:
                        datapoints = js[0]['datapoints']
                    else:
                        datapoints = js['datapoints']

                if remote_host_type == 'prometheus':
                    # TODO:
                    # Maybe iterate through multiple metrics if response has more than one metric
                    # for some public lab metrics
                    datapoints = js['data']['result'][0]['values']
                datapoints_fetched = len(datapoints)
                # @modified 20191011 - Task #3258: Reduce vista logging
                if LOCAL_DEBUG:
                    logger.info('fetcher :: retrieved %s data points from %s' % (
                        str(datapoints_fetched), str(url)))
            except Exception as e:
                logger.error(traceback.format_exc())
                logger.error('error :: fetcher :: failed to get data from %s - %s' % (
                    str(url), e))

            # Example
            # datapoints[0]
            # [7.3, 1556817000]
            # Add each data point and timestamp to the timeseries list so
            # they can be sent to Graphite
            if not datapoints:
                logger.info('fetcher :: failed to get any data from %s' % str(url))
                continue

            # @added 20191111 - Bug #3312: flux - populate_metric_worker - handle None in datapoints
            valid_datapoints = []
            for datapoint in datapoints:
                value = None
                timestamp = None
                if remote_host_type == 'graphite':
                    try:
                        # @added 20191111 - Bug #3312: flux - populate_metric_worker - handle None in datapoints
                        raw_timeseries.append([datapoint[1], datapoint[0]])

                        raw_value = datapoint[0]
                        if raw_value is None:
                            continue
                        value = float(datapoint[0])
                        timestamp = int(datapoint[1])
                        valid_datapoints.append([value, timestamp])
                    except:
                        continue
                if remote_host_type == 'prometheus':
                    try:
                        # @added 20191111 - Bug #3312: flux - populate_metric_worker - handle None in datapoints
                        raw_timeseries.append([datapoint[0], datapoint[1]])

                        raw_value = datapoint[1]
                        if raw_value is None:
                            continue
                        timestamp = int(datapoint[0])
                        value = float(datapoint[1])
                    except:
                        continue
                    valid_datapoints.append([timestamp, value])
            datapoints = valid_datapoints

            # Order the time series by timestamp as the tuple can shift
            # order resulting in more recent data being added before older
            # data
            datapoints.sort()

            # However check if a metric is known to Flux and if so do not
            # use all resolutions just from the last.flux known timestamp
            # for he metric
            last_flux_timestamp = None
            redis_last_flux_metric_data = None
            cache_key = 'flux.last.%s' % metric
            try:
                # if python_version == 3:
                #     redis_last_flux_metric_data = self.redis_conn.get(cache_key).decode('utf-8')
                # else:
                #     redis_last_flux_metric_data = self.redis_conn.get(cache_key)
                redis_last_flux_metric_data = self.redis_conn_decoded.get(cache_key)

                if LOCAL_DEBUG:
                    if redis_last_flux_metric_data:
                        logger.info('fetcher :: Redis key %s is present' % str(cache_key))
                    else:
                        logger.info('fetcher :: Redis key %s is not present' % str(cache_key))
            except AttributeError:
                logger.info('fetcher :: Redis key %s is not present' % str(cache_key))
                last_flux_timestamp = False
                redis_last_flux_metric_data = False
            except Exception as e:
                logger.error(traceback.format_exc())
                logger.error('error :: fetcher :: retrieving Redis key %s data - %s' % (
                    str(cache_key), str(e)))
                redis_last_flux_metric_data = False
            if redis_last_flux_metric_data:
                try:
                    last_flux_metric_data = literal_eval(redis_last_flux_metric_data)
                    last_flux_timestamp = int(last_flux_metric_data[0])
                    if LOCAL_DEBUG:
                        if last_flux_timestamp:
                            logger.info('fetcher :: Redis key %s last_flux_timestamp %s' % (str(cache_key), str(last_flux_timestamp)))
                        else:
                            logger.info('fetcher :: Redis key %s last_flux_timestamp unknown' % (str(cache_key)))
                except Exception as e:
                    logger.error(traceback.format_exc())
                    logger.error('error :: fetch :: failed determining last_flux_timestamp - %s' % e)
                    last_flux_timestamp = False

            value = None
            timestamp = None
            datapoints_added_to_timeseries = 0
            datapoints_already_populated = 0
            datapoints_with_no_value = 0
            timeseries = []
            for datapoint in datapoints:
                try:
                    if remote_host_type == 'graphite':
                        raw_value = datapoint[0]
                        if raw_value is None:
                            continue
                        value = float(datapoint[0])
                        timestamp = int(datapoint[1])
                    if remote_host_type == 'prometheus':
                        timestamp = int(datapoint[0])
                        # value = float(datapoint[1])
                        try:
                            value = float(datapoint[1])
                        except:
                            continue
                    submit_data = True
                    if last_flux_timestamp:
                        if timestamp <= last_flux_timestamp:
                            submit_data = False
                            datapoints_already_populated += 1
                    if submit_data:
                        new_datapoint = [timestamp, value]
                        timeseries.append(new_datapoint)
                        datapoints_added_to_timeseries += 1
                # nosec to exclude from bandit tests
                except:  # nosec
                    datapoints_with_no_value += 1
                    continue

            # @added 20191111 - Bug #3312: flux - populate_metric_worker - handle None in datapoints
            # And set flux.last key is the returned value from the remote is
            # null so that time series that are mostly null do not keep on
            # getting added to flux populate_metric by Vista
            if not timeseries:
                set_flux_key = False
                last_ts = None
                try:
                    sorted_raw_timeseries = sorted(raw_timeseries, key=lambda x: x[0])
                    last_ts = sorted_raw_timeseries[-1][0]
                    if int(last_ts) > (fetch_process_start - 120):
                        if sorted_raw_timeseries[-1][1] is None:
                            set_flux_key = True
                except Exception as e:
                    logger.error(traceback.format_exc())
                    logger.error('error :: fetcher :: failed to determine if last value was null - %s' % e)
                if set_flux_key:
                    cache_key = 'flux.last.%s' % metric
                    try:
                        # Update Redis flux key
                        metric_data = [int(last_ts), None]
                        self.redis_conn.set(cache_key, str(metric_data))
                        if VERBOSE_LOGGING:
                            logger.info('fetcher :: even though no data points so as to not loop round on this metric, set the metric Redis key - %s - %s' % (
                                cache_key, str(metric_data)))
                    except Exception as e:
                        logger.error(traceback.format_exc())
                        logger.error('error :: fetcher :: even though no data points, failed to set Redis key - %s - %s' % (
                            cache_key, e))
                    # Adding to the vista.fetcher.unique_metrics Redis set
                    redis_set = 'vista.fetcher.unique_metrics'
                    data = str(metric)
                    try:
                        self.redis_conn.sadd(redis_set, data)
                        if VERBOSE_LOGGING:
                            logger.info('fetcher :: even though no data points, added %s to Redis set %s' % (
                                remote_target, redis_set))
                    except Exception as e:
                        logger.error(traceback.format_exc())
                        logger.error('error :: fetcher :: even though no data points, failed to add %s to Redis set %s - %s' % (
                            str(data), str(redis_set), e))
                    continue

            if not timeseries:
                if VERBOSE_LOGGING:
                    logger.info('fetcher :: no new data in the timeseries list for the time series for %s' % metric)
                continue

            # Order the time series by timestamp as the tuple can shift
            # order resulting in more recent data being added before older
            # data
            timeseries.sort()
            timeseries_length = len(timeseries)
            if VERBOSE_LOGGING:
                logger.info('fetcher :: %s data points to add to vista.fetcher.metrics.json for %s' % (
                    str(timeseries_length), metric))

            payload = None
            timeseries_str = '"%s"' % timeseries
            try:
                payload = [{
                    'remote_host_type': remote_host_type,
                    'remote_target': remote_target,
                    'graphite_target': graphite_target,
                    'metric': metric,
                    'namespace_prefix': namespace_prefix,
                    'key': settings.FLUX_SELF_API_KEY,
                    'token': token,
                    'user': user,
                    'password': password,
                    'datapoints': timeseries_str
                }]
            except Exception as e:
                logger.error(traceback.format_exc())
                logger.error('error :: fetcher :: could not build the payload json - %s' % e)
            redis_set = 'vista.fetcher.metrics.json'
            data = str(payload)
            try:
                self.redis_conn.sadd(redis_set, data)
                if LOCAL_DEBUG:
                    logger.info('fetcher :: added data from %s to Redis set %s' % (
                        str(url), redis_set))
            except Exception as e:
                logger.error(traceback.format_exc())
                logger.error('error :: failed to add %s to Redis set %s - %s' % (
                    str(data), str(redis_set), e))
            redis_set = 'vista.fetcher.metrics.fetched'
            time_now = int(time())
            data = [str(remote_target), time_now]
            try:
                self.redis_conn.sadd(redis_set, str(data))
            except Exception as e:
                logger.error(traceback.format_exc())
                logger.error('error :: failed to add %s to Redis set %s - %s' % (
                    str(data), str(redis_set), e))
        fetch_process_end = time()
        fetch_time = fetch_process_end - fetch_process_start
        logger.info('fetcher :: metrics fetched in %s seconds' % str(fetch_time))
        return

    def run(self):
        """
        - Called when the process intializes.

        - Determine if Redis is up and discover the number of `unique metrics`.

        - Wait for the processes to finish.

        - Send skyline.vista metrics to `GRAPHITE_HOST`
        """

        # Log management to prevent overwriting
        # Allow the bin/<skyline_app>.d to manage the log
        if os.path.isfile(skyline_app_logwait):
            try:
                os.remove(skyline_app_logwait)
            except OSError:
                logger.error('error - failed to remove %s, continuing' % skyline_app_logwait)
                # pass

        now = time()
        log_wait_for = now + 5
        while now < log_wait_for:
            if os.path.isfile(skyline_app_loglock):
                sleep(.1)
                now = time()
            else:
                now = log_wait_for + 1

        logger.info('starting %s run' % skyline_app)
        if os.path.isfile(skyline_app_loglock):
            logger.error('error - bin/%s.d log management seems to have failed, continuing' % skyline_app)
            try:
                os.remove(skyline_app_loglock)
                logger.info('log lock file removed')
            except OSError:
                logger.error('error - failed to remove %s, continuing' % skyline_app_loglock)
                # pass
        else:
            logger.info('bin/%s.d log management done' % skyline_app)

        try:
            SERVER_METRIC_PATH = '.%s' % settings.SERVER_METRICS_NAME
            if SERVER_METRIC_PATH == '.':
                SERVER_METRIC_PATH = ''
            logger.info('SERVER_METRIC_PATH is set from settings.py to %s' % str(SERVER_METRIC_PATH))
        except:
            SERVER_METRIC_PATH = ''
            logger.info('warning :: SERVER_METRIC_PATH is not declared in settings.py, defaults to \'\'')
        logger.info('skyline_app_graphite_namespace is set to %s' % str(skyline_app_graphite_namespace))
        try:
            VISTA_ENABLED = settings.VISTA_ENABLED
            logger.info('VISTA_ENABLED is set to %s' % str(VISTA_ENABLED))
        except:
            VISTA_ENABLED = True
            logger.info('warning :: VISTA_ENABLED is not declared in settings.py, defaults to True')
        try:
            ASYNCIO_LIMIT = settings.VISTA_ASYNCIO_FETCHER_LIMIT
            logger.info('fetcher :: settings.VISTA_ASYNCIO_FETCHER_LIMIT is set to %s' % str(ASYNCIO_LIMIT))
        except:
            ASYNCIO_LIMIT = 2
            logger.info('fetcher :: warning :: VISTA_ASYNCIO_FETCHER_LIMIT is not declared in settings.py, defaults to 2')

        running = True
        while running:
            begin_fetcher_run = int(time())

            # Make sure Redis is up
            redis_up = False
            while not redis_up:
                try:
                    redis_up = self.redis_conn.ping()
                except:
                    logger.error(traceback.format_exc())
                    logger.error('error :: fetcher :: cannot connect to redis at socket path %s' % settings.REDIS_SOCKET_PATH)
                    sleep(2)
                    try:
                        # @modified 20191111 - Bug #3266: py3 Redis binary objects not strings
                        #                      Branch #3262: py3
                        # if settings.REDIS_PASSWORD:
                        #     self.redis_conn = StrictRedis(password=settings.REDIS_PASSWORD, unix_socket_path=settings.REDIS_SOCKET_PATH)
                        # else:
                        #     self.redis_conn = StrictRedis(unix_socket_path=settings.REDIS_SOCKET_PATH)
                        self.redis_conn = get_redis_conn(skyline_app)
                        self.redis_conn_decoded = get_redis_conn_decoded(skyline_app)
                    except:
                        logger.error(traceback.format_exc())
                        logger.error('error :: fetcher :: cannot connect to redis at socket path %s' % settings.REDIS_SOCKET_PATH)
                    continue

            # Report app up
            try:
                self.redis_conn.setex(skyline_app, 120, begin_fetcher_run)
            except:
                logger.error('error :: fetcher :: could not update the Redis %s key' % skyline_app)
                logger.error(traceback.format_exc())

            # Known fetcher metrics that are known to have already been fetched,
            # metrics in this set are named as follows namespace_prefix.metric
            vista_fetcher_unique_metrics = []
            redis_set = 'vista.fetcher.unique_metrics'
            try:
                # @modified 20191111 - Bug #3266: py3 Redis binary objects not strings
                #                      Branch #3262: py3
                # vista_fetcher_unique_metrics = list(self.redis_conn.smembers(redis_set))
                vista_fetcher_unique_metrics = list(self.redis_conn_decoded.smembers(redis_set))
            except:
                logger.error('error :: fetcher :: could not determine vista_fetcher_unique_metrics from the Redis set %s' % redis_set)
                vista_fetcher_unique_metrics = []
            vista_unique_metrics = []
            if vista_fetcher_unique_metrics:
                for metric in vista_fetcher_unique_metrics:
                    # @modified 20191111 - Bug #3266: py3 Redis binary objects not strings
                    #                      Branch #3262: py3
                    # metric_str = metric.decode('utf-8')
                    metric_str = str(metric)

                    vista_unique_metrics.append(metric_str)

            # Determine metrics to fetch
            metrics_to_fetch = []

            fetcher_sent_to_flux = 0

            if LOCAL_DEBUG:
                try:
                    number_of_metrics = len(settings.VISTA_FETCH_METRICS)
                    logger.info('fetcher :: %s metrics to retrieve' % str(number_of_metrics))
                except:
                    pass

            end_timestamp = int(time())
            start_timestamp = end_timestamp - 300

            # Refer to settings.VISTA_FETCH_METRICS tuple to determine the
            # format of the fetch_tuple
            # for target, graphite_target, fetch_tuple in metrics:
            for remote_host, remote_host_type, frequency, remote_target, graphite_target, uri, namespace_prefix, api_key, token, user, password, populate_at_resolutions in settings.VISTA_FETCH_METRICS:
                try:
                    # remote_host_type = fetch_tuple[1]
                    valid_remote_host_type = False
                    if remote_host_type == 'graphite' or remote_host_type == 'prometheus':
                        valid_remote_host_type = True
                    if not valid_remote_host_type:
                        logger.error('error :: invalid remote_host_type for %s in %s' % (
                            remote_target, str(remote_host_type)))
                        continue

                    if LOCAL_DEBUG:
                        logger.info('fetcher :: processing %s remote_target %s' % (
                            str(remote_host_type), str(remote_target)))

                    remote_graphite_host = None
                    if remote_host_type == 'graphite':
                        remote_graphite_host = remote_host
                        url = '%s%s%s' % (remote_graphite_host, uri, str(remote_target))
                        if LOCAL_DEBUG:
                            logger.info('fetcher :: with url %s' % str(url))

                    default_prometheus_uri = False
                    remote_prometheus_host = None

                    if remote_host_type == 'prometheus':
                        remote_prometheus_host = remote_host
                        # Hardcode the Prometheus api uri
                        # uri = str(fetch_tuple[3])
                        # uri = '/api/v1/query?query=%s[5m]' % str(remote_target)
                        # url encode the Prometheus metric query to handle
                        # labels and query chars in the URI
                        urlencoded_remote_target = quote(remote_target)
                        if uri == 'default':
                            default_prometheus_uri = True
                            uri = '/api/v1/query_range?query=%s&start=%s&end=%s&step=60s' % (
                                str(urlencoded_remote_target),
                                str(start_timestamp), str(end_timestamp))
                        url = '%s%s' % (remote_prometheus_host, uri)
                        if LOCAL_DEBUG:
                            logger.info('fetcher :: with url %s' % str(url))

                    frequency = int(frequency)
                    if LOCAL_DEBUG:
                        logger.info('fetcher :: with frequency %s' % str(frequency))
                    if LOCAL_DEBUG:
                        logger.info('fetcher :: with namespace_prefix %s' % str(namespace_prefix))
                    if namespace_prefix != '':
                        metric = '%s.%s' % (namespace_prefix, graphite_target)
                        metric = filesafe_metricname(metric)
                    else:
                        metric = graphite_target
                        metric = filesafe_metricname(metric)
                    if LOCAL_DEBUG:
                        logger.info('fetcher :: with metric %s' % str(metric))
                    api_key = str(api_key)
                    if LOCAL_DEBUG:
                        # @modified 20210421 - Task #4030: refactoring
                        # semgrep - python-logger-credential-disclosure
                        # logger.info('fetcher :: with api_key %s' % str(api_key))
                        logger.info('fetcher :: with api_key (no disclosure)')
                    token = str(token)
                    if LOCAL_DEBUG:
                        # @modified 20210421 - Task #4030: refactoring
                        # semgrep - python-logger-credential-disclosure
                        # logger.info('fetcher :: with token %s' % str(token))
                        logger.info('fetcher :: with token (no disclosure)')
                    user = str(user)
                    if LOCAL_DEBUG:
                        logger.info('fetcher :: with user %s' % str(user))
                    password = str(password)
                    if LOCAL_DEBUG:
                        # @modified 20210421 - Task #4030: refactoring
                        # semgrep - python-logger-credential-disclosure
                        # logger.info('fetcher :: with password %s' % str(password))
                        logger.info('fetcher :: with password (no disclosure)')
                    populate_at_resolutions_str = str(populate_at_resolutions)
                    if LOCAL_DEBUG:
                        logger.info('fetcher :: with populate_at_resolutions %s' % populate_at_resolutions_str)
                    # Handle if the user passes (None) instead of None
                    if populate_at_resolutions == ():
                        populate_at_resolutions = None
                except Exception as e:
                    logger.error(traceback.format_exc())
                    logger.error('error :: fetcher :: could not determine the required values in VISTA_FETCH_METRICS tuple for - %s - %s' % (
                        str(remote_target), e))
                    continue

                # If the metric is not known to Vista and the metric
                # has a populate_at_resolutions set, send to Flux to
                # pre-populate Graphite
                pre_populate_graphite_metric = False
                if remote_target not in vista_unique_metrics:
                    if remote_host_type == 'graphite' and populate_at_resolutions:
                        pre_populate_graphite_metric = True
                        logger.info('fetcher :: attempting to pre-populate Graphite metric - %s' % (
                            metric))
                    if remote_host_type == 'prometheus' and populate_at_resolutions:
                        pre_populate_graphite_metric = True
                        logger.info('fetcher :: attempting to pre-populate Prometheus metric - %s' % (
                            metric))
                else:
                    if LOCAL_DEBUG:
                        logger.info('fetcher :: remote_target %s is present in vista_unique_metrics' % str(remote_target))

                # However check if a metric is known to Flux and if so do not
                # use all resolutions just from the last.flux known timestamp
                # for he metric
                last_flux_timestamp = None
                redis_last_flux_metric_data = None
                cache_key = None
                try:
                    cache_key = 'flux.last.%s' % metric
                    # @modified 20191111 - Bug #3266: py3 Redis binary objects not strings
                    #                      Branch #3262: py3
                    # @modified 20191128 - Bug #3266: py3 Redis binary objects not strings
                    #                      Branch #3262: py3
                    # if python_version == 3:
                    #     redis_last_flux_metric_data = self.redis_conn.get(cache_key).decode('utf-8')
                    # else:
                    #     redis_last_flux_metric_data = self.redis_conn.get(cache_key)
                    redis_last_flux_metric_data = self.redis_conn_decoded.get(cache_key)

                    if LOCAL_DEBUG:
                        if redis_last_flux_metric_data:
                            logger.info('fetcher :: Redis key %s is present' % str(cache_key))
                        else:
                            logger.info('fetcher :: Redis key %s is not present' % str(cache_key))
                except AttributeError:
                    logger.info('fetcher :: Redis key %s is not present' % str(cache_key))
                    last_flux_timestamp = False
                    redis_last_flux_metric_data = False
                except Exception as e:
                    logger.error(traceback.format_exc())
                    logger.error('error :: fetcher :: retrieving Redis key %s data - %s' % (
                        str(cache_key), str(e)))
                    redis_last_flux_metric_data = False
                if redis_last_flux_metric_data:
                    try:
                        last_flux_metric_data = literal_eval(redis_last_flux_metric_data)
                        last_flux_timestamp = int(last_flux_metric_data[0])
                        if LOCAL_DEBUG:
                            if last_flux_timestamp:
                                logger.info('fetcher :: Redis key %s last_flux_timestamp %s' % (str(cache_key), str(last_flux_timestamp)))
                            else:
                                logger.info('fetcher :: Redis key %s last_flux_timestamp unknown' % (str(cache_key)))
                    except:
                        logger.error(traceback.format_exc())
                        logger.error('error :: fetch :: failed determining last_flux_timestamp')
                        last_flux_timestamp = False

                time_now = int(time())
                if last_flux_timestamp:
                    last_fetch = time_now - last_flux_timestamp
                    if last_fetch < frequency:
                        if LOCAL_DEBUG:
                            logger.info('fetcher :: last fetch was %s seconds ago, less than frequency %s seconds, not fetching' % (str(last_fetch), str(frequency)))
                        continue

                # @added 20200107 - Task #3376: Enable vista and flux to deal with lower frequency data
                # Determine the last known resolution of the metric
                last_vista_metric_resolution = frequency
                try:
                    cache_key = 'vista.last.resolution.%s' % metric
                    last_vista_metric_resolution_data = self.redis_conn_decoded.get(cache_key)
                    if last_vista_metric_resolution_data is None:
                        last_vista_metric_resolution_int = last_vista_metric_resolution
                    else:
                        last_vista_metric_resolution_int = int(last_vista_metric_resolution_data)
                    if last_vista_metric_resolution_int > 0:
                        last_vista_metric_resolution = last_vista_metric_resolution_int
                    if LOCAL_DEBUG:
                        if last_vista_metric_resolution:
                            logger.info('fetcher :: Redis key %s is present' % str(cache_key))
                        else:
                            logger.info('fetcher :: Redis key %s is not present' % str(cache_key))
                except AttributeError:
                    logger.info('fetcher :: Redis key %s is not present' % str(cache_key))
                    last_vista_metric_resolution = False
                except Exception as e:
                    logger.error(traceback.format_exc())
                    logger.error('error :: fetcher :: retrieving Redis key %s data - %s' % (
                        str(cache_key), str(e)))
                    last_vista_metric_resolution = False

                if remote_target in vista_unique_metrics and last_flux_timestamp:
                    last_expected_fetch_time = time_now - (frequency + 420)
                    if last_flux_timestamp < last_expected_fetch_time:

                        # @added 20200107 - Task #3376: Enable vista and flux to deal with lower frequency data
                        # Added older_than_resolution
                        older_than_resolution = True
                        if last_vista_metric_resolution:
                            try:
                                last_expected_data_time = time_now - (frequency + 420 + last_vista_metric_resolution)
                                if last_flux_timestamp > last_expected_data_time:
                                    older_than_resolution = False
                            except:
                                logger.error(traceback.format_exc())
                                logger.error('error :: fetch :: failed determining last_expected_data_time')

                        # @modified 20200107 - Task #3376: Enable vista and flux to deal with lower frequency data
                        # Added older_than_resolution
                        # if populate_at_resolutions:
                        if populate_at_resolutions and older_than_resolution:
                            if remote_host_type == 'graphite' or remote_host_type == 'prometheus':
                                pre_populate_graphite_metric = True
                                behind_by_seconds = time_now - last_flux_timestamp
                                logger.info('fetcher :: last_flux_timestamp is behind by %s seconds, attempting to pre-populate %s' % (
                                    str(behind_by_seconds), metric))

                if remote_target in vista_unique_metrics:
                    if not last_flux_timestamp:
                        if populate_at_resolutions:
                            if remote_host_type == 'graphite' or remote_host_type == 'prometheus':
                                pre_populate_graphite_metric = True

                # Problem with asyncio so using Flux directly
                if remote_target in vista_unique_metrics and last_flux_timestamp and USE_FLUX:
                    if populate_at_resolutions:
                        if remote_host_type == 'graphite' or remote_host_type == 'prometheus':
                            pre_populate_graphite_metric = True

                if pre_populate_graphite_metric:
                    logger.info('fetcher :: attempting to build the pre-populate Graphite metric urls - %s' % (
                        metric))
                if LOCAL_DEBUG:
                    logger.info('fetcher :: pre_populate_graphite_metric - %s - %s' % (
                        str(pre_populate_graphite_metric), metric))

                fetch_resolution_urls = []

                # Build remote Graphite URLs
                if remote_host_type == 'graphite' and pre_populate_graphite_metric:
                    logger.info('fetcher :: building the pre-populate Graphite metric urls - %s' % (
                        metric))
                    try:
                        # Build URLs to submit to flux/HttpPopulateMetric
                        resolutions = []
                        for resolution in populate_at_resolutions:
                            resolutions.append(resolution)
                        number_of_resolutions = len(resolutions)
                        current_resolution_count = 0
                        for resolution in resolutions:
                            append_url = True
                            if current_resolution_count < (number_of_resolutions - 1):
                                resolution_url = None
                                if current_resolution_count == 0:
                                    next_resolution_count = 1
                                else:
                                    next_resolution_count = current_resolution_count + 1
                                next_resolution = resolutions[next_resolution_count]
                                # If there is a known last_flux_timestamp only get data
                                # from that time period until now
                                if last_flux_timestamp:
                                    if 'days' in resolution:
                                        resolution_days = resolution.strip('days')
                                        resolution_hours = int(resolution_days) * 24
                                        d = datetime.today() - timedelta(hours=resolution_hours)
                                    if 'hours' in resolution:
                                        resolution_hours = int(resolution.strip('hours'))
                                        d = datetime.today() - timedelta(hours=resolution_hours)
                                    resolution_timestamp = int(d.strftime('%s'))
                                    if resolution_timestamp < last_flux_timestamp:
                                        append_url = False
                                    else:
                                        append_url = True
                                    # If the last_flux_timestamp falls within
                                    # the range of the resolution period, append
                                    # otherwise the fill will leave an airgap in
                                    # the data
                                    if 'days' in next_resolution:
                                        next_resolution_days = next_resolution.strip('days')
                                        next_resolution_hours = int(next_resolution_days) * 24
                                        d = datetime.today() - timedelta(hours=next_resolution_hours)
                                    if 'hours' in next_resolution:
                                        next_resolution_hours = int(next_resolution.strip('hours'))
                                        d = datetime.today() - timedelta(hours=next_resolution_hours)
                                    next_resolution_timestamp = int(d.strftime('%s'))
                                    if last_flux_timestamp in range(resolution_timestamp, next_resolution_timestamp):
                                        append_url = True
                                    resolution_url = '%s/render/?from=-%s&until=-%s&format=json&target=%s' % (
                                        str(remote_graphite_host), str(resolution),
                                        str(next_resolution), str(remote_target))
                                    if LOCAL_DEBUG:
                                        logger.info('fetcher :: resolution_url - %s - %s' % (
                                            str(resolution_url), metric))
                                else:
                                    resolution_url = '%s/render/?from=-%s&until=-%s&format=json&target=%s' % (
                                        str(remote_graphite_host), str(resolution),
                                        str(next_resolution), str(remote_target))
                                    if LOCAL_DEBUG:
                                        logger.info('fetcher :: resolution_url - %s - %s' % (
                                            str(resolution_url), metric))
                                current_resolution_count += 1
                            else:
                                if last_flux_timestamp:
                                    if 'days' in resolution:
                                        resolution_days = resolution.strip('days')
                                        resolution_hours = int(resolution_days) * 24
                                        d = datetime.today() - timedelta(hours=resolution_hours)
                                    if 'hours' in resolution:
                                        resolution_hours = int(resolution.strip('hours'))
                                        d = datetime.today() - timedelta(hours=resolution_hours)
                                    resolution_timestamp = int(d.strftime('%s'))
                                    if last_flux_timestamp > resolution_timestamp:
                                        append_url = True
                                        fetch_from_timestamp = last_flux_timestamp - 600
                                    else:
                                        append_url = True
                                        fetch_from_timestamp = resolution_timestamp
                                    resolution_url = '%s/render/?from=%s&format=json&target=%s' % (
                                        str(remote_graphite_host), str(fetch_from_timestamp),
                                        str(remote_target))
                                    if LOCAL_DEBUG:
                                        logger.info('fetcher :: resolution_url - %s - %s' % (
                                            str(resolution_url), metric))
                                else:
                                    resolution_url = '%s/render/?from=-%s&format=json&target=%s' % (
                                        str(remote_graphite_host), str(resolution),
                                        str(remote_target))
                                    if LOCAL_DEBUG:
                                        logger.info('fetcher :: resolution_url - %s - %s' % (
                                            str(resolution_url), metric))
                            if append_url:
                                fetch_resolution_urls.append(resolution_url)
                                if LOCAL_DEBUG:
                                    logger.info('fetcher :: appended resolution_url - %s - %s' % (
                                        str(resolution_url), metric))
                    except Exception as e:
                        logger.error(traceback.format_exc())
                        logger.error('error :: fetcher :: could not determine the required resolutions for values in VISTA_FETCH_METRICS tuple for - %s - %s' % (
                            str(remote_target), e))

                # @added 20200108 - Task #3376: Enable vista and flux to deal with lower frequency data
                # Prometheus metrics that use a custom uri cannot be pre populated
                if remote_host_type == 'prometheus' and pre_populate_graphite_metric:
                    if not default_prometheus_uri:
                        logger.info('fetcher :: cannot pre populate Prometheus metric %s as it uses a custom uri' % (
                            metric))
                        pre_populate_graphite_metric = False

                # Build remote Prometheus URLs
                if remote_host_type == 'prometheus' and pre_populate_graphite_metric:
                    # Assuming Prometheus only has a single retention (resolution)
                    try:
                        start_seconds_ago = 1296000  # default to 15 days
                        for resolution in populate_at_resolutions:
                            # Build URL to submit to flux/HttpPopulateMetric
                            # url encode the Prometheus metric query to handle
                            # labels and query chars in the URI
                            if 'm' in resolution:
                                resolution_int = resolution.strip('m')
                                start_seconds_ago = int(resolution_int) * 60
                            if 'h' in resolution:
                                resolution_int = resolution.strip('h')
                                start_seconds_ago = (int(resolution_int) * 60) * 60
                            if 'd' in resolution:
                                resolution_int = resolution.strip('d')
                                start_seconds_ago = ((int(resolution_int) * 24) * 60) * 60
                            if 'w' in resolution:
                                resolution_int = resolution.strip('w')
                                start_seconds_ago = (((int(resolution_int) * 7) * 24) * 60) * 60
                            pop_start_timestamp = end_timestamp - int(start_seconds_ago)
                            urlencoded_remote_target = quote(remote_target)
                            # The query_range query does not return more than
                            # 11000 data points as it is limited as per
                            # https://github.com/prometheus/prometheus/issues/2253#issuecomment-346288842
                            # so resample needed to be reintroduced after being
                            # deleted as the Prometheus query_range was switched
                            # to
                            uri = '/api/v1/query_range?query=%s&start=%s&end=%s&step=60s' % (
                                str(urlencoded_remote_target),
                                str(pop_start_timestamp), str(end_timestamp))
                            # Use query endpoint for more than 11000 data points
                            uri = '/api/v1/query?query=%s[%s]' % (
                                str(urlencoded_remote_target),
                                str(resolution))
                            resolution_url = '%s%s' % (
                                str(remote_prometheus_host), uri)
                            fetch_resolution_urls.append(resolution_url)
                    except Exception as e:
                        logger.error(traceback.format_exc())
                        logger.error('error :: fetcher :: could not determine the required pre-populate URI for values in VISTA_FETCH_METRICS tuple for - %s - %s' % (
                            str(populate_at_resolutions), e))

                if fetch_resolution_urls:
                    set_fetch_resolution_urls = set(fetch_resolution_urls)
                    fetch_resolution_urls = list(set_fetch_resolution_urls)

                flux_url = None
                try:
                    # Submit to flux/populate_metric
                    protocol = 'http://'
                    flux_url = '%s%s:%s/populate_metric' % (
                        protocol, str(settings.FLUX_IP),
                        str(settings.FLUX_PORT))
                except Exception as e:
                    logger.error(traceback.format_exc())
                    logger.error('error :: fetcher :: could not build the flux URL - %s' % e)
                payload = None
                fetch_resolution_urls_str = '"%s"' % fetch_resolution_urls
                if fetch_resolution_urls and pre_populate_graphite_metric:
                    try:
                        payload = {
                            'remote_host_type': remote_host_type,
                            'remote_target': remote_target,
                            'metric': metric,
                            'namespace_prefix': namespace_prefix,
                            'key': settings.FLUX_SELF_API_KEY, 'token': token, 'user': user,
                            'password': password,
                            'fetch_resolution_urls': fetch_resolution_urls_str
                        }
                    except Exception as e:
                        logger.error(traceback.format_exc())
                        logger.error('error :: fetcher :: could not build the payload json - %s' % e)
                if flux_url and payload:
                    try:
                        # @modified 20191011 - Task #3258: Reduce vista logging
                        if LOCAL_DEBUG:
                            logger.info('fetcher :: calling %s with payload - %s' % (
                                flux_url, str(payload)))
                        response = requests.post(flux_url, json=payload)
                        # @modified 20191011 - Task #3258: Reduce vista logging
                        if LOCAL_DEBUG:
                            logger.info('fetcher :: flux /populate_metric response code - %s' % (
                                str(response.status_code)))
                        # @added 20191011 - Task #3258: Reduce vista logging
                        good_response = False

                        if response.status_code == 200:
                            fetcher_sent_to_flux += 1
                            # @added 20191011 - Task #3258: Reduce vista logging
                            good_response = True
                        if response.status_code == 204:
                            fetcher_sent_to_flux += 1
                            # @added 20191011 - Task #3258: Reduce vista logging
                            good_response = True

                        # @added 20191011 - Task #3258: Reduce vista logging
                        if not good_response:
                            logger.error('fetcher :: flux /populate_metric did not respond with 200 or 204, status code - %s for %s' % (
                                str(response.status_code), flux_url))
                    except Exception as e:
                        logger.error(traceback.format_exc())
                        logger.error('error :: fetcher :: could not post data to flux URL - %s, data - %s - %s' % (
                            str(flux_url), str(payload), e))

                if not pre_populate_graphite_metric:
                    if last_flux_timestamp and remote_host_type == 'graphite':
                        try:
                            # Best effort to backfill any missing data
                            url_from = re.sub(r'^.*from=[-]', '', url)
                            url_period = re.sub(r'&.*', '', url_from)
                            if 'days' in url_period:
                                resolution_days = int(url_period.strip('days'))
                                d = datetime.today() - timedelta(days=resolution_days)
                            if 'hours' in url_period:
                                resolution_hours = int(url_period.strip('hours'))
                                d = datetime.today() - timedelta(hours=resolution_hours)
                            if 'minutes' in url_period:
                                resolution_minutes = int(url_period.strip('minutes'))
                                d = datetime.today() - timedelta(minutes=resolution_minutes)
                            from_resolution_timestamp = int(d.strftime('%s'))
                            if from_resolution_timestamp < last_flux_timestamp:
                                rep_str = url_period
                                if 'from=-' in url:
                                    rep_str = '-%s' % url_period
                                fetch_from_timestamp = last_flux_timestamp - 300
                                url = re.sub(rep_str, str(fetch_from_timestamp), url)
                        except Exception as e:
                            logger.error(traceback.format_exc())
                            logger.error('error :: fetcher :: could not determine backfill parameters - %s' % e)

                    if last_flux_timestamp and remote_host_type == 'prometheus':
                        try:
                            # Best effort to backfill any missing data
                            if default_prometheus_uri:
                                pop_start_timestamp = int(last_flux_timestamp) - 120
                                urlencoded_remote_target = quote(remote_target)
                                uri = '/api/v1/query_range?query=%s&start=%s&end=%s&step=60s' % (
                                    str(urlencoded_remote_target),
                                    str(pop_start_timestamp), str(end_timestamp))
                            url = '%s%s' % (str(remote_prometheus_host), uri)
                        except Exception as e:
                            logger.error(traceback.format_exc())
                            logger.error('error :: fetcher :: could not determine backfill parameters - %s' % e)

                    metric_to_fetch = [remote_host_type, frequency, remote_target, graphite_target, metric, url, namespace_prefix, api_key, token, user, password]
                    metrics_to_fetch.append(metric_to_fetch)
                    if LOCAL_DEBUG:
                        logger.info('fetcher :: added metric_to_fetch - %s' % str(metric_to_fetch))

            if LOCAL_DEBUG:
                if metrics_to_fetch:
                    metrics_to_fetch_count = len(metrics_to_fetch)
                    logger.info('fetcher :: there are %s metrics in metrics_to_fetch' % str(metrics_to_fetch_count))

            if metrics_to_fetch:
                # Spawn fetch process/es
                pids = []
                spawned_pids = []
                pid_count = 0
                for i in range(1, settings.VISTA_FETCHER_PROCESSES + 1):
                    if i > len(metrics_to_fetch):
                        logger.info('fetcher :: WARNING: Skyline Vista fetcher is set for more cores than needed.')
                        break
                    try:
                        p = Process(target=self.fetch_process, args=(i, metrics_to_fetch))
                        pids.append(p)
                        pid_count += 1
                        logger.info('fetcher :: starting %s of %s fetch_process/es' % (str(pid_count), str(settings.VISTA_FETCHER_PROCESSES)))
                        p.start()
                        spawned_pids.append(p.pid)
                    except Exception as e:
                        logger.error(traceback.format_exc())
                        logger.error('error :: fetcher :: failed to spawn fetch_process - %s' % e)

                # Self monitor processes and terminate if any fetch_process has run
                # for longer than VISTA_FETCHER_PROCESS_MAX_RUNTIME seconds
                p_starts = time()
                while time() - p_starts <= settings.VISTA_FETCHER_PROCESS_MAX_RUNTIME:
                    if any(p.is_alive() for p in pids):
                        # Just to avoid hogging the CPU
                        sleep(.1)
                    else:
                        # All the processes are done, break now.
                        time_to_run = time() - p_starts
                        logger.info('fetcher :: %s fetch_process/es completed in %.2f seconds' % (str(settings.VISTA_FETCHER_PROCESSES), time_to_run))
                        break
                else:
                    # We only enter this if we didn't 'break' above.
                    logger.info('fetcher :: timed out, killing all fetch_process processes')
                    for p in pids:
                        logger.info('fetcher :: killing fetch_process process')
                        p.terminate()
                        # p.join()
                        logger.info('fetcher :: killed fetch_process process')

                for p in pids:
                    if p.is_alive():
                        logger.info('fetcher :: stopping fetch_process - %s' % (str(p.is_alive())))
                        p.join()

            # Sleep if it went too fast
            process_runtime = int(time()) - begin_fetcher_run
            metrics_fetched_count = 0
            if int(process_runtime) < 60:
                next_run = int(begin_fetcher_run) + 60
                time_now = int(time())
                sleep_for = next_run - time_now
                logger.info('fetcher :: sleeping for %s seconds until next fetch' % str(sleep_for))
                sleep(sleep_for)
                try:
                    del sleep_for
                except:
                    logger.error('error :: fetcher :: failed to del sleep_for')
                try:
                    del next_run
                except:
                    logger.error('error :: fetcher :: failed to del next_run')
                try:
                    del time_now
                except:
                    logger.error('error :: fetcher :: failed to del time_now')
                metrics_fetched = []
                # metrics_fetched_count = 0
                try:
                    redis_set = 'vista.fetcher.metrics.fetched'
                    # @modified 20191111 - Bug #3266: py3 Redis binary objects not strings
                    #                      Branch #3262: py3
                    # metrics_fetched = self.redis_conn.smembers(redis_set)
                    metrics_fetched = self.redis_conn_decoded.smembers(redis_set)
                    metrics_fetched_count = len(list(metrics_fetched))
                    logger.info('fetcher :: %s metrics were fetched' % str(metrics_fetched_count))
                except:
                    logger.error(traceback.format_exc())
                    logger.error('error :: fetcher :: could not get Redis set %s' % redis_set)
                    redis_set = 'vista.worker.to.process'
                try:
                    redis_set = 'vista.fetcher.metrics.fetched'
                    self.redis_conn.delete(redis_set)
                except:
                    logger.error(traceback.format_exc())
                    logger.error('error :: fetcher :: could not delete the Redis set %s' % redis_set)
                if metrics_fetched:
                    timestamps = []
                    for str_metric_fetched in metrics_fetched:
                        try:
                            # @modified 20191111 - Bug #3266: py3 Redis binary objects not strings
                            #                      Branch #3262: py3
                            # Use get_redis_conn_decoded
                            # if python_version == 3:
                            #     str_metric_fetched = str_metric_fetched.decode('UTF-8')
                            metric_fetched = literal_eval(str_metric_fetched)
                            timestamp = int(metric_fetched[1])
                            timestamps.append(timestamp)
                        except:
                            logger.error('error :: fetcher :: failed to determine timestamp from %s' % str(str_metric_fetched))
                    try:
                        timestamps.sort()
                        last_fetch_timestamp = int(timestamps[-1])
                        time_to_fetch = last_fetch_timestamp - begin_fetcher_run
                        logger.info('fetcher :: %s metrics fetched this run in %s seconds' % (
                            str(metrics_fetched_count), str(time_to_fetch)))
                    except:
                        logger.error(traceback.format_exc())
                        logger.error('error :: fetcher :: failed to last_fetch_timestamp from timestamps')
                try:
                    redis_set = 'vista.worker.to.process'
                    # @modified 20191111 - Bug #3266: py3 Redis binary objects not strings
                    #                      Branch #3262: py3
                    # metrics_count_for_workers = len(list(self.redis_conn.smembers(redis_set)))
                    metrics_count_for_workers = len(list(self.redis_conn_decoded.smembers(redis_set)))
                    logger.info('fetcher :: %s of the metrics fetched from this run still need to be processed by a worker' % str(metrics_count_for_workers))
                except:
                    logger.error(traceback.format_exc())
                    logger.error('error :: fetcher :: could not get Redis set %s' % redis_set)

            # Use both the metrics sent to pre_populate_graphite_metric and
            # @added 20220111 - Feature #4370: Manage vista.fetcher.metrics.json Redis set
            # the ones submitted to flux
            metrics_sent_to_flux_count = metrics_fetched_count + fetcher_sent_to_flux

            send_metric_name = '%s.sent_to_flux' % skyline_app_graphite_namespace
            try:
                logger.info('fetcher :: sending Graphite - %s, %s' % (
                    # @modified 20220111 - Feature #4370: Manage vista.fetcher.metrics.json Redis set
                    # send_metric_name, str(fetcher_sent_to_flux)))
                    send_metric_name, str(metrics_sent_to_flux_count)))
                # @modified 20220111 - Feature #4370: Manage vista.fetcher.metrics.json Redis set
                # fetcher_sent_to_flux_str = str(fetcher_sent_to_flux)
                fetcher_sent_to_flux_str = str(metrics_sent_to_flux_count)
                send_graphite_metric(parent_skyline_app, send_metric_name, fetcher_sent_to_flux_str)
            except:
                logger.error(traceback.format_exc())
                logger.error('error :: fetcher :: could not send %s to Graphite' % send_metric_name)

            # @added 20191011 - Feature #3260: vista - fetcher add time_to_fetch metric
            # Added time_to_fetch, metrics_to_fetch, metrics_fetched
            send_metric_name = '%s.time_to_fetch' % skyline_app_graphite_namespace
            try:
                logger.info('fetcher :: sending Graphite - %s, %s' % (
                    send_metric_name, str(process_runtime)))
                fetcher_time_to_fetch_str = str(process_runtime)
                send_graphite_metric(parent_skyline_app, send_metric_name, fetcher_time_to_fetch_str)
            except:
                logger.error(traceback.format_exc())
                logger.error('error :: fetcher :: could not send %s to Graphite' % send_metric_name)

            metrics_to_fetch_count = len(metrics_to_fetch)
            send_metric_name = '%s.metrics_to_fetch' % skyline_app_graphite_namespace
            try:
                logger.info('fetcher :: sending Graphite - %s, %s' % (
                    send_metric_name, str(metrics_to_fetch_count)))
                fetcher_metrics_to_fetch_count_str = str(metrics_to_fetch_count)
                send_graphite_metric(parent_skyline_app, send_metric_name, fetcher_metrics_to_fetch_count_str)
            except:
                logger.error(traceback.format_exc())
                logger.error('error :: fetcher :: could not send %s to Graphite' % send_metric_name)

            send_metric_name = '%s.metrics_fetched' % skyline_app_graphite_namespace
            try:
                logger.info('fetcher :: sending Graphite - %s, %s' % (
                    send_metric_name, str(metrics_fetched_count)))
                fetcher_metrics_fetched_count_str = str(metrics_fetched_count)
                send_graphite_metric(parent_skyline_app, send_metric_name, fetcher_metrics_fetched_count_str)
            except:
                logger.error(traceback.format_exc())
                logger.error('error :: fetcher :: could not send %s to Graphite' % send_metric_name)

            try:
                del process_runtime
            except:
                logger.error('error :: fetcher :: failed to del process_runtime')
