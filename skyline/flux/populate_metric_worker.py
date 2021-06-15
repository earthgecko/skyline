import sys
import os.path
from os import kill
import traceback
# @modified 20191115 - Branch #3262: py3
# from multiprocessing import Queue, Process
from multiprocessing import Process

try:
    from Queue import Empty  # Python 2.7
except ImportError:
    from queue import Empty  # Python 3
from time import sleep, time
from ast import literal_eval
import re
import datetime
import socket

# @modified 20200808 - Task #3608: Update Skyline to Python 3.8.3 and deps
# bandit [B403:blacklist] Consider possible security implications associated
# with pickle module.  These have been considered.
import pickle  # nosec

import struct

# @added 20200107 - Task #3376: Enable vista and flux to deal with lower frequency data
from collections import Counter

import requests
# @modified 20191115 - Bug #3266: py3 Redis binary objects not strings
#                      Branch #3262: py3
# from redis import StrictRedis

import graphyte
import pandas as pd

from logger import set_up_logging
sys.path.append(os.path.join(os.path.dirname(os.path.realpath(__file__)), os.pardir))
sys.path.insert(0, os.path.dirname(__file__))

if True:
    import settings
    from skyline_functions import (
        send_graphite_metric,
        # @added 20191111 - Bug #3266: py3 Redis binary objects not strings
        #                   Branch #3262: py3
        get_redis_conn,
        # @added 20191128 - Bug #3266: py3 Redis binary objects not strings
        #                   Branch #3262: py3
        get_redis_conn_decoded,
        # @added 20201009 - Feature #3780: skyline_functions - sanitise_graphite_url
        #                   Bug #3778: Handle single encoded forward slash requests to Graphite
        sanitise_graphite_url,
        # @added 20201012 - Feature #3780: skyline_functions - sanitise_graphite_url
        #                   Bug #3778: Handle single encoded forward slash requests to Graphite
        encode_graphite_metric_name)

# @modified 20191129 - Branch #3262: py3
# Consolidate flux logging
# logger = set_up_logging('populate_metric_worker')
logger = set_up_logging(None)

this_host = str(os.uname()[1])

try:
    SERVER_METRIC_PATH = '.%s' % settings.SERVER_METRICS_NAME
    if SERVER_METRIC_PATH == '.':
        SERVER_METRIC_PATH = ''
except:
    SERVER_METRIC_PATH = ''

parent_skyline_app = 'flux'
# @added 20191010 - Feature #3250: Allow Skyline to send metrics to another Carbon host
# Added missing skyline_app required for send_graphite_metric
skyline_app = 'flux'

skyline_app_graphite_namespace = 'skyline.%s%s.populate_metric_worker' % (parent_skyline_app, SERVER_METRIC_PATH)

LOCAL_DEBUG = False

GRAPHITE_METRICS_PREFIX = None
CARBON_HOST = settings.FLUX_CARBON_HOST
CARBON_PORT = settings.FLUX_CARBON_PORT
FLUX_CARBON_PICKLE_PORT = settings.FLUX_CARBON_PICKLE_PORT

if settings.FLUX_SEND_TO_CARBON:
    try:
        graphyte.init(CARBON_HOST, port=CARBON_PORT, prefix=None, timeout=5)
        logger.info('populate_metric_worker :: succeeded to graphyte.init with host: %s, port: %s, prefix: %s' % (
            str(CARBON_HOST), str(CARBON_PORT),
            str(GRAPHITE_METRICS_PREFIX)))
    except:
        logger.error(traceback.format_exc())
        logger.error('error :: populate_metric_worker :: failed to run graphyte.init with host: %s, port: %s, prefix: %s' % (
            str(CARBON_HOST), str(CARBON_PORT),
            str(GRAPHITE_METRICS_PREFIX)))


class PopulateMetricWorker(Process):
    """
    The worker grabs metrics from the queue, surfaces the data from the remote
    Graphite and sends them to the Skyline Graphite.
    """
    def __init__(self, queue, parent_pid):
        super(PopulateMetricWorker, self).__init__()
        # @modified 20191111 - Bug #3266: py3 Redis binary objects not strings
        #                      Branch #3262: py3
        # if settings.REDIS_PASSWORD:
        #     self.redis_conn = StrictRedis(password=settings.REDIS_PASSWORD, unix_socket_path=settings.REDIS_SOCKET_PATH)
        #  else:
        #     self.redis_conn = StrictRedis(unix_socket_path=settings.REDIS_SOCKET_PATH)
        # @added 20191111 - Bug #3266: py3 Redis binary objects not strings
        #                   Branch #3262: py3
        # Added a single functions to deal with Redis connection and the
        # charset='utf-8', decode_responses=True arguments required in py3
        self.redis_conn = get_redis_conn(skyline_app)
        # @added 20191128 - Bug #3266: py3 Redis binary objects not strings
        #                   Branch #3262: py3
        self.redis_conn_decoded = get_redis_conn_decoded(skyline_app)

        self.q = queue
        self.parent_pid = parent_pid
        self.daemon = True

    def check_if_parent_is_alive(self):
        """
        Self explanatory.
        """
        try:
            kill(self.parent_pid, 0)
        except:
            # @added 20201203 - Bug #3856: Handle boring sparsely populated metrics in derivative_metrics
            # Log warning
            logger.warn('warning :: parent process is dead')
            exit(0)

    def run(self):
        """
        Called when the process intializes.
        """

        def pickle_data_to_graphite(data):

            message = None
            try:
                payload = pickle.dumps(data, protocol=2)
                header = struct.pack("!L", len(payload))
                message = header + payload
            except:
                logger.error(traceback.format_exc())
                logger.error('error :: populate_metric_worker :: failed to pickle to send to Graphite')
                return False
            if message:
                try:
                    sock = socket.socket()
                    sock.connect((CARBON_HOST, FLUX_CARBON_PICKLE_PORT))
                    sock.sendall(message)
                    sock.close()
                except:
                    logger.error(traceback.format_exc())
                    logger.error('error :: populate_metric_worker :: failed to send pickle data to Graphite')
                    return False
            else:
                logger.error('error :: populate_metric_worker :: failed to pickle metric data into message')
                return False
            return True

        logger.info('populate_metric_worker :: starting worker')

        # Populate API keys and tokens in memcache
        # python-2.x and python3.x handle while 1 and while True differently
        # while 1:
        running = True
        while running:
            # Make sure Redis is up
            redis_up = False
            while not redis_up:
                try:
                    redis_up = self.redis_conn.ping()
                except:
                    logger.error('populate_metric_worker :: cannot connect to Redis at socket path %s' % (settings.REDIS_SOCKET_PATH))
                    sleep(2)

                    # @modified 20191111 - Bug #3266: py3 Redis binary objects not strings
                    #                      Branch #3262: py3
                    # if settings.REDIS_PASSWORD:
                    #     self.redis_conn = StrictRedis(password=settings.REDIS_PASSWORD, unix_socket_path=settings.REDIS_SOCKET_PATH)
                    # else:
                    #     self.redis_conn = StrictRedis(unix_socket_path=settings.REDIS_SOCKET_PATH)
                    # @modified 20201207 - Task #3864: flux - try except everything
                    try:
                        self.redis_conn = get_redis_conn(skyline_app)
                    except Exception as e:
                        logger.error('error :: populate_metric_worker :: get_redis_conn - %s' % str(e))

                    # @added 20191128 - Bug #3266: py3 Redis binary objects not strings
                    #                   Branch #3262: py3
                    # @modified 20201207 - Task #3864: flux - try except everything
                    try:
                        self.redis_conn_decoded = get_redis_conn_decoded(skyline_app)
                    except Exception as e:
                        logger.error('error :: populate_metric_worker :: get_redis_conn_decoded - %s' % str(e))

            metricDict = None
            try:
                # Get a metric from the queue with a 1 second timeout, each
                # metric item on the queue is a list e.g.
                # metric_json = [metricName, metricValue, metricTimestamp]
                metricDict = self.q.get(True, 1)
                logger.info('populate_metric_worker :: processing queue item')
            except Empty:
                logger.info('populate_metric_worker :: queue is empty and timed out, sleeping for 30 seconds')
                sleep(30)
            except NotImplementedError:
                pass
            except KeyboardInterrupt:
                logger.info('populate_metric_worker :: server has been issued a user signal to terminate - KeyboardInterrupt')
            except SystemExit:
                logger.info('populate_metric_worker :: server was interrupted - SystemExit')
            except Exception as e:
                logger.error('error :: populate_metric_worker :: %s' % (str(e)))

            if not metricDict:
                continue

            try:
                remote_host_type = str(metricDict['remote_host_type'])
                remote_target = str(metricDict['remote_target'])
                metric = str(metricDict['metric'])
                namespace_prefix = str(metricDict['namespace_prefix'])
                if not namespace_prefix:
                    namespace_prefix = ''
                if namespace_prefix == 'None':
                    namespace_prefix = ''
                key = str(metricDict['key'])
                token = str(metricDict['token'])
                user = str(metricDict['user'])
                password = str(metricDict['password'])
                if metricDict['fetch_resolution_urls'] == 'None':
                    logger.info('No fetch_resolution_urls declared for %s, nothing to do' % remote_target)
                    continue
                if metricDict['fetch_resolution_urls'] == '()' or metricDict['fetch_resolution_urls'] == ():
                    logger.info('No fetch_resolution_urls declared for %s, nothing to do' % remote_target)
                    continue

                fetch_resolution_urls_str = literal_eval(metricDict['fetch_resolution_urls'])
                fetch_resolution_urls = literal_eval(fetch_resolution_urls_str)
            except:
                logger.error(traceback.format_exc())
                logger.error('error :: populate_metric_worker :: failed to read from metricData')

            if LOCAL_DEBUG:
                try:
                    logger.info('populate_metric_worker :: remote_target from metricData set to %s' % remote_target)
                    logger.info('populate_metric_worker :: metric from metricData set to %s' % metric)
                    logger.info('populate_metric_worker :: namespace_prefix from metricData set to %s' % namespace_prefix)
                    logger.info('populate_metric_worker :: key from metricData set to %s' % key)
                    # @modified 20210421 - Task #4030: refactoring
                    # semgrep - python-logger-credential-disclosure
                    # logger.info('populate_metric_worker :: token from metricData set to %s' % token)
                    logger.info('populate_metric_worker :: token from metricData was set (no discloure)')
                    logger.info('populate_metric_worker :: user from metricData set to %s' % user)
                    logger.info('populate_metric_worker :: password from metricData set to %s' % password)
                    logger.info('populate_metric_worker :: fetch_resolution_urls from metricData set to %s' % str(fetch_resolution_urls))
                    if fetch_resolution_urls:
                        for fetch_url in fetch_resolution_urls:
                            logger.info('populate_metric_worker :: a fetch_url from metricData is set to %s' % str(fetch_url))
                    logger.info('populate_metric_worker :: metric is set to %s' % metric)
                except:
                    logger.error(traceback.format_exc())
                    logger.error('error :: populate_metric_worker :: failed to read from metricData')

            # Best effort to de-duplicate the data sent to Graphite
            # @modified 20201207 - Task #3864: flux - try except everything
            try:
                cache_key = 'flux.last.%s' % metric
            except Exception as e:
                logger.error('error :: populate_metric_worker :: could not interpolate flux.last. cache_key name - %s' % str(e))
                cache_key = None

            last_flux_timestamp = None

            # @modified 20201207 - Task #3864: flux - try except everything
            # Only check if cache_key
            if cache_key:
                try:
                    # @modified 20191128 - Bug #3266: py3 Redis binary objects not strings
                    #                      Branch #3262: py3
                    # redis_last_metric_data = self.redis_conn.get(cache_key).decode('utf-8')
                    redis_last_metric_data = self.redis_conn_decoded.get(cache_key)
                    last_metric_data = literal_eval(redis_last_metric_data)
                    last_flux_timestamp = int(last_metric_data[0])
                except:
                    logger.error(traceback.format_exc())
                    logger.error('error :: populate_metric_worker :: failed to determine last_flux_timestamp from Redis key %s' % cache_key)
                    last_flux_timestamp = False

            recent_last_flux_timestamp_present = False
            if last_flux_timestamp:
                now = int(time())
                if (now - last_flux_timestamp) < 600:
                    recent_last_flux_timestamp_present = True
                    # Skyline has the metric so adding it to the vista.fetcher
                    # Redis set
                    redis_set = 'vista.fetcher.unique_metrics'
                    data = str(remote_target)
                    try:
                        self.redis_conn.sadd(redis_set, data)
                        logger.info('populate_metric_worker :: the last flux update for %s was less than 600 seconds ago, added metric to %s' % (
                            metric, redis_set))
                    except:
                        logger.info(traceback.format_exc())
                        logger.error('error :: populate_metric_worker :: failed to add %s to Redis set %s' % (
                            str(data), str(redis_set)))
                    # continue

            if not last_flux_timestamp:
                # Check Graphite does not have the data or determine what the
                # last data Graphite has is
                logger.info('populate_metric_worker :: no last_flux_timestamp was found in Redis for %s, checking if Graphite has data' % (
                    metric))
                check_graphite_from = ['-50mins', '-6hours', '-24hours', '-7days', '-30days', '-90days']
                timeseries = []
                for graphite_from in check_graphite_from:
                    if last_flux_timestamp:
                        break
                    logger.info('populate_metric_worker :: checking %s in Graphite from %s' % (
                        metric, graphite_from))
                    got_data = False

                    # @added 20201012 - Feature #3780: skyline_functions - sanitise_graphite_url
                    #                   Bug #3778: Handle single encoded forward slash requests to Graphite
                    # @modified 20201207 - Task #3864: flux - try except everything
                    try:
                        encoded_graphite_metric_name = encode_graphite_metric_name(skyline_app, metric)
                    except Exception as e:
                        logger.error('error :: populate_metric_worker :: encode_graphite_metric_name error - %s' % str(e))
                        encoded_graphite_metric_name = metric

                    try:
                        # We use absolute time so that if there is a lag in mirage the correct
                        # timeseries data is still surfaced relevant to the anomalous datapoint
                        # timestamp
                        if settings.GRAPHITE_PORT != '':
                            url = '%s://%s:%s/%s/?from=%s&target=%s&format=json' % (
                                settings.GRAPHITE_PROTOCOL, settings.GRAPHITE_HOST,
                                str(settings.GRAPHITE_PORT),
                                # @modified 20201012 - Feature #3780: skyline_functions - sanitise_graphite_url
                                # settings.GRAPHITE_RENDER_URI, graphite_from, metric)
                                settings.GRAPHITE_RENDER_URI, graphite_from, encoded_graphite_metric_name)
                        else:
                            url = '%s://%s/%s/?from=%s&target=%s&format=json' % (
                                settings.GRAPHITE_PROTOCOL, settings.GRAPHITE_HOST,
                                # @modified 20201012 - Feature #3780: skyline_functions - sanitise_graphite_url
                                # settings.GRAPHITE_RENDER_URI, graphite_from, metric)
                                settings.GRAPHITE_RENDER_URI, graphite_from, encoded_graphite_metric_name)
                        logger.info('populate_metric_worker :: using Graphite URL - %s' % (
                            url))

                        r = requests.get(url)
                        if r.status_code == 200:
                            js = []
                            try:
                                js = r.json()
                            except:
                                logger.info(traceback.format_exc())
                                logger.error('error :: populate_metric_worker :: failed to get data from Graphite')
                                continue
                            if not js:
                                logger.info('populate_metric_worker :: %s not present in Graphite from %s' % (
                                    metric, graphite_from))
                                continue
                            got_data = True
                            logger.info('populate_metric_worker :: %s present in Graphite from %s' % (
                                metric, graphite_from))
                        else:
                            logger.info('populate_metric_worker :: %s not present in Graphite from %s' % (
                                metric, graphite_from))
                            continue
                    except:
                        logger.info(traceback.format_exc())
                        logger.error('error :: populate_metric_worker :: failed to get data from Graphite')
                        continue

                    datapoints = []
                    if got_data:
                        try:
                            js = r.json()
                            datapoints = js[0]['datapoints']
                            logger.info('populate_metric_worker :: %s data points are present in the Graphite %s data' % (
                                str(len(datapoints)), str(graphite_from)))
                        except:
                            logger.info(traceback.format_exc())
                            logger.error('error :: populate_metric_worker :: failed to get data from Graphite')

                    for datapoint in datapoints:
                        try:
                            value = float(datapoint[0])
                            timestamp = int(datapoint[1])
                            new_datapoint = [timestamp, value]
                            timeseries.append(new_datapoint)
                        except:  # nosec
                            continue
                    last_timestamp_with_data = None

                    # @modified 20201207 - Task #3864: flux - try except everything
                    try:
                        for timestamp, value in timeseries[::-1]:
                            has_value = False
                            if value == 0.0:
                                has_value = True
                            if value == 0:
                                has_value = True
                            if value:
                                has_value = True
                            if has_value:
                                last_timestamp_with_data = int(timestamp)
                                datapoint = value
                                break
                        if last_timestamp_with_data:
                            # Here we set this as the missing last_flux_timestamp
                            last_flux_timestamp = last_timestamp_with_data
                            recent_last_flux_timestamp_present = True
                            logger.info('populate_metric_worker :: %s last timestamp in Graphite from %s is %s, using as last_flux_timestamp' % (
                                metric, str(graphite_from), str(last_flux_timestamp)))
                    except Exception as e:
                        logger.error('error :: populate_metric_worker :: error determining last_flux_timestamp - %s' % str(e))

            timeseries = []
            start_populating = int(time())
            datapoints_added_to_timeseries = 0
            datapoints_already_populated = 0
            datapoints_with_no_value = 0
            timestamp = None
            value = None

            # @added 20191111 - Bug #3312: flux - populate_metric_worker - handle None in datapoints
            # And set flux.last key is the returned value from the remote is
            # null so that time series that are mostly null do not keep on
            # getting added to flux populate_metric by Vista
            raw_timeseries = []

            for fetch_url in fetch_resolution_urls:
                # if recent_last_flux_timestamp_present and remote_host_type == 'prometheus':
                # This was for the query query and resample method and not for
                # the query_range query
                if recent_last_flux_timestamp_present and remote_host_type == 'prometheus_query_range_NOT_FOR_GE_11000':
                    try:
                        logger.info('populate_metric_worker :: recent data so replacing fetch_url %s ' % (
                            fetch_url))
                        seconds_to_fetch = int(time()) - last_flux_timestamp
                        minutes_to_fetch = int(seconds_to_fetch / 60) + 2
                        re_mins_to_fetch = '[%sm]' % str(minutes_to_fetch)
                        fetch_url = re.sub(r'\[.*\]', re_mins_to_fetch, fetch_url)
                        encoded_re_mins_to_fetch = '%%5B%sm%%5D' % str(minutes_to_fetch)
                        fetch_url = re.sub(r'%5B.*%5D', encoded_re_mins_to_fetch, fetch_url)
                        logger.info('populate_metric_worker :: replaced fetch_url %s ' % (
                            fetch_url))
                    except:
                        logger.info(traceback.format_exc())
                        logger.error('error :: populate_metric_worker :: failed to rewrite URL')

                if recent_last_flux_timestamp_present and remote_host_type == 'prometheus':
                    try:
                        logger.info('populate_metric_worker :: recent data so replacing fetch_url %s ' % (
                            fetch_url))
                        seconds_to_fetch = int(time()) - last_flux_timestamp
                        minutes_to_fetch = int(seconds_to_fetch / 60) + 2
                        re_mins_to_fetch = '[%sm]' % str(minutes_to_fetch)
                        fetch_url = re.sub(r'\[.*\]', re_mins_to_fetch, fetch_url)
                        encoded_re_mins_to_fetch = '%%5B%sm%%5D' % str(minutes_to_fetch)
                        fetch_url = re.sub(r'%5B.*%5D', encoded_re_mins_to_fetch, fetch_url)
                        logger.info('populate_metric_worker :: replaced fetch_url %s ' % (
                            fetch_url))
                    except:
                        logger.info(traceback.format_exc())
                        logger.error('error :: populate_metric_worker :: failed to rewrite URL')

                # @added 20201009 - Feature #3780: skyline_functions - sanitise_graphite_url
                #                   Bug #3778: Handle single encoded forward slash requests to Graphite
                sanitised = False

                # @modified 20201207 - Task #3864: flux - try except everything
                try:
                    sanitised, fetch_url = sanitise_graphite_url(skyline_app, fetch_url)
                except Exception as e:
                    logger.error('error :: populate_metric_worker :: sanitise_graphite_url - %s' % str(e))

                success = False
                try:
                    logger.info('populate_metric_worker :: getting data from %s' % str(fetch_url))
                    response = requests.get(fetch_url)
                    if response.status_code == 200:
                        success = True
                        logger.info('populate_metric_worker :: got responses from %s' % str(fetch_url))
                except:
                    logger.info(traceback.format_exc())
                    logger.error('error :: populate_metric_worker :: http status code - %s, reason - %s' % (
                        str(response.status_code), str(response.reason)))
                    logger.error('error :: populate_metric_worker :: failed to get data from %s' % str(fetch_url))

                if not success:
                    continue

                datapoints = None
                try:
                    js = response.json()
                    if remote_host_type == 'graphite':
                        datapoints = js[0]['datapoints']
                    if remote_host_type == 'prometheus':
                        datapoints = js['data']['result'][0]['values']
                    datapoints_fetched = len(datapoints)
                    logger.info('populate_metric_worker :: retrieved %s data points from %s' % (
                        str(datapoints_fetched), str(fetch_url)))
                except:
                    logger.info(traceback.format_exc())
                    logger.error('error :: populate_metric_worker :: failed to get data from %s' % str(fetch_url))

                # Example
                # datapoints[0]
                # [7.3, 1556817000]
                # Add each data point and timestamp to the timeseries list so
                # they can be sent to Graphite
                if not datapoints:
                    logger.info('populate_metric_worker :: failed to get any data from %s' % str(fetch_url))
                    continue

                # @added 20191108 - Bug #3312: flux - populate_metric_worker - handle None in datapoints
                valid_datapoints = []

                # @modified 20201207 - Task #3864: flux - try except everything
                try:
                    for datapoint in datapoints:
                        value = None
                        timestamp = None
                        if remote_host_type == 'graphite':
                            # @added 20191111 - Bug #3312: flux - populate_metric_worker - handle None in datapoints
                            raw_timeseries.append([datapoint[1], datapoint[0]])

                            try:
                                raw_value = datapoint[0]
                                if raw_value is None:
                                    datapoints_with_no_value += 1
                                    continue
                                value = float(datapoint[0])
                                timestamp = int(datapoint[1])
                                valid_datapoints.append([value, timestamp])
                            except:
                                continue
                        if remote_host_type == 'prometheus':
                            # @added 20191111 - Bug #3312: flux - populate_metric_worker - handle None in datapoints
                            raw_timeseries.append([datapoint[0], datapoint[1]])

                            try:
                                raw_value = datapoint[1]
                                if raw_value is None:
                                    datapoints_with_no_value += 1
                                    continue
                                timestamp = int(datapoint[0])
                                value = float(datapoint[1])
                            except:
                                continue
                            valid_datapoints.append([timestamp, value])
                except Exception as e:
                    logger.error('error :: populate_metric_worker :: error determining valid_datapoints - %s' % str(e))

                # @modified 20201207 - Task #3864: flux - try except everything
                try:
                    datapoints = valid_datapoints
                    # Order the time series by timestamp as the tuple can shift
                    # order resulting in more recent data being added before older
                    # data
                    datapoints.sort()
                except Exception as e:
                    logger.error('error :: populate_metric_worker :: error set datapoints from valid_datapoints - %s' % str(e))

                # Determine the timestamp of the current minute to apply
                # VISTA_DO_NOT_SUBMIT_CURRENT_MINUTE
                # @modified 20201207 - Task #3864: flux - try except everything
                try:
                    time_now = int(time())
                    current_minute_hour = int(datetime.datetime.utcfromtimestamp(time_now).strftime('%H'))
                    current_minute_minute = int(datetime.datetime.utcfromtimestamp(time_now).strftime('%M'))
                    current_datetime = datetime.datetime.utcfromtimestamp(time_now).replace(hour=current_minute_hour, minute=current_minute_minute, second=0, microsecond=0)
                    current_minute_timestamp_start = int(current_datetime.strftime('%s'))
                except Exception as e:
                    logger.error('error :: populate_metric_worker :: error determining current_minute_timestamp_start - %s' % str(e))
                    current_minute_timestamp_start = int(time())
                datapoints_in_current_minute = 0

                last_error = None
                value = None
                timestamp = None
                for datapoint in datapoints:
                    try:
                        if remote_host_type == 'graphite':
                            try:
                                raw_value = datapoint[0]
                                if raw_value is None:
                                    continue
                                value = float(datapoint[0])
                                timestamp = int(datapoint[1])
                            except:
                                continue
                        if remote_host_type == 'prometheus':
                            # timestamp = int(datapoint[0])
                            try:
                                timestamp = int(datapoint[0])
                                value = float(datapoint[1])
                            except:
                                continue
                        submit_data = True
                        if last_flux_timestamp:
                            if timestamp <= last_flux_timestamp:
                                submit_data = False
                                datapoints_already_populated += 1

                        # Here if the timestamp of the data point falls
                        # within the current minute, it is discarded and not
                        # sent to flux, to ensure that high frequency metrics
                        # can have their minutely bins fully populated before
                        # they are submitted to Graphite
                        if settings.VISTA_DO_NOT_SUBMIT_CURRENT_MINUTE:
                            if timestamp >= current_minute_timestamp_start:
                                submit_data = False
                                datapoints_in_current_minute += 1
                        if submit_data:
                            new_datapoint = [timestamp, value]
                            timeseries.append(new_datapoint)
                            datapoints_added_to_timeseries += 1
                    # nosec to exclude from bandit tests
                    except:  # nosec
                        last_error = traceback.format_exc()
                        datapoints_with_no_value += 1
                        continue

                # @modified 20201207 - Task #3864: flux - try except everything
                try:
                    if last_error:
                        logger.error(last_error)
                        logger.error('error :: populate_metric_worker :: the above is the last_error encountered processing %s' % (
                            str(metric)))
                    if datapoints_with_no_value:
                        logger.info('populate_metric_worker :: %s of the fetched records were discarded as they had value None' % (
                            str(datapoints_with_no_value)))
                    if datapoints_in_current_minute:
                        logger.info('populate_metric_worker :: %s of the fetched records were discarded as they fall within the current minute' % (
                            str(datapoints_in_current_minute)))
                    logger.info('populate_metric_worker :: %s of the fetched data points are older than the last known flux timestamp' % (
                        str(datapoints_already_populated)))
                    logger.info('populate_metric_worker :: added %s data points to the time series to submit to Graphite' % (
                        str(datapoints_added_to_timeseries)))
                except Exception as e:
                    logger.error('error :: populate_metric_worker :: error logging some info - %s' % str(e))

            end_fecthing = int(time())
            seconds_to_fetch = end_fecthing - start_populating
            if timestamp:
                logger.info('populate_metric_worker :: last fetched value - %s, timestamp %s' % (
                    str(value), str(timestamp)))
            logger.info('populate_metric_worker :: %s data point fecthed for %s in %s seconds' % (
                str(datapoints_added_to_timeseries), remote_target,
                str(seconds_to_fetch)))

            # @added 20191111 - Bug #3312: flux - populate_metric_worker - handle None in datapoints
            # And set flux.last key is the returned value from the remote is
            # null so that time series that are mostly null do not keep on
            # getting added to flux populate_metric by Vista
            # @modified 20210504 - Bug #3312: flux - populate_metric_worker - handle None in datapoints
            # Added raw_timeseries to execute only if there is a raw_timeseries
            if not timeseries and raw_timeseries:
                set_flux_key = False
                try:
                    sorted_raw_timeseries = sorted(raw_timeseries, key=lambda x: x[0])
                    # @modified 20210504 - Bug #3312: flux - populate_metric_worker - handle None in datapoints
                    # Only determine the last_ts if there is a sorted_raw_timeseries
                    if sorted_raw_timeseries:
                        last_ts = sorted_raw_timeseries[-1][0]
                        if int(last_ts) > (end_fecthing - 120):
                            if sorted_raw_timeseries[-1][1] is None:
                                set_flux_key = True
                except:
                    logger.error(traceback.format_exc())
                    logger.error('error :: populate_metric_worker :: failed to determine if last value was null')
                if set_flux_key:
                    try:
                        # Update Redis flux key
                        cache_key = 'flux.last.%s' % metric
                        metric_data = [int(last_ts), None]
                        self.redis_conn.set(cache_key, str(metric_data))
                        logger.info('populate_metric_worker :: even though no data points so as to not loop round on this metric, set the metric Redis key - %s - %s' % (
                            cache_key, str(metric_data)))
                    except:
                        logger.error(traceback.format_exc())
                        logger.error('error :: populate_metric_worker :: even though no data points, failed to set Redis key - %s - %s' % (
                            cache_key, str(metric_data)))
                    # Adding to the vista.fetcher.unique_metrics Redis set
                    redis_set = 'vista.fetcher.unique_metrics'
                    data = str(remote_target)
                    try:
                        self.redis_conn.sadd(redis_set, data)
                        logger.info('populate_metric_worker :: even though no data points, added %s to Redis set %s' % (
                            remote_target, redis_set))
                    except:
                        logger.info(traceback.format_exc())
                        logger.error('error :: populate_metric_worker :: even though no data points, failed to add %s to Redis set %s' % (
                            str(data), str(redis_set)))

            if not timeseries:
                logger.info('populate_metric_worker :: no data in the timeseries list for the time series for %s' % metric)
                continue

            # Order the time series by timestamp as the tuple can shift
            # order resulting in more recent data being added before older
            # data
            timeseries.sort()
            timeseries_length = len(timeseries)

            # @added 20200107 - Task #3376: Enable vista and flux to deal with lower frequency data
            # Determine resolution from the last 30 data points
            resolution_timestamps = []
            metric_resolution_determined = False
            for metric_datapoint in timeseries[-30:]:
                # @modified 20201207 - Task #3864: flux - try except everything
                try:
                    timestamp = int(metric_datapoint[0])
                    resolution_timestamps.append(timestamp)
                except Exception as e:
                    logger.error('error :: populate_metric_worker :: error constructing resolution_timestamps - %s' % str(e))

            timestamp_resolutions = []
            if resolution_timestamps:
                last_timestamp = None
                for timestamp in resolution_timestamps:
                    # @modified 20201207 - Task #3864: flux - try except everything
                    try:
                        if last_timestamp:
                            resolution = timestamp - last_timestamp
                            timestamp_resolutions.append(resolution)
                            last_timestamp = timestamp
                        else:
                            last_timestamp = timestamp
                    except Exception as e:
                        logger.error('error :: populate_metric_worker :: error constructing timestamp_resolutions - %s' % str(e))

            if timestamp_resolutions:
                try:
                    timestamp_resolutions_count = Counter(timestamp_resolutions)
                    ordered_timestamp_resolutions_count = timestamp_resolutions_count.most_common()
                    metric_resolution = int(ordered_timestamp_resolutions_count[0][0])
                    if metric_resolution > 0:
                        metric_resolution_determined = True
                except:
                    logger.error(traceback.format_exc())
                    logger.error('error :: populate_metric_worker :: failed to determine metric_resolution from timeseries')
            if metric_resolution_determined:
                cache_key = 'vista.last.resolution.%s' % metric
                try:
                    # Update Redis key
                    self.redis_conn.setex(cache_key, 3600, metric_resolution)
                    logger.info('populate_metric_worker :: set %s to %s' % (cache_key, str(metric_resolution)))
                except:
                    logger.error(traceback.format_exc())
                    logger.error('error :: populate_metric_worker :: failed to set Redis key - %s' % (
                        cache_key))

            # Resample
            resample_at = '1Min'
            # @modified 20210421 - Task #4030: refactoring
            # semgrep - rule:python.lang.correctness.useless-comparison.no-strings-as-booleans
            # if resample_at:
            resample_data = True
            if resample_data:
                try:
                    df = pd.DataFrame(timeseries)
                    df.columns = ['timestamp', 'value']
                    df['timestamp'] = pd.to_datetime(df['timestamp'], unit='s', origin='unix')
                    df = df.set_index('timestamp')
                    # resampled_df = df.resample(resample_at).sum()
                    # Use the mean as Prometheus uses the average in the
                    # query_range API method
                    resampled_df = df.resample(resample_at).mean()
                    resampled_timeseries = []
                    for index, row in resampled_df.iterrows():
                        timestamp = int(index.strftime('%s'))
                        resampled_timeseries.append([timestamp, row[0]])
                    timeseries = resampled_timeseries
                    timeseries_length = len(timeseries)
                    logger.info('populate_metric_worker :: time series resampled at %s resulting in %s data points to send to Graphite' % (
                        str(resample_at), str(timeseries_length)))
                except:
                    logger.error(traceback.format_exc())
                    logger.error('error :: populate_metric_worker :: failed to resample time series for %s' % str(metric))

            logger.info('populate_metric_worker :: %s data points to send to Graphite' % (
                str(timeseries_length)))
            timestamp = None
            value = None
            sent_to_graphite = 0

            # use_pickle = False
            use_pickle = True
            if not use_pickle:
                for timestamp, value in timeseries:
                    try:
                        graphyte.send(metric, float(value), int(timestamp))
                        sent_to_graphite += 1
                        if sent_to_graphite % 1000 == 0:
                            logger.info('populate_metric_worker :: submitted %s of %s data points to Graphite so far' % (
                                str(sent_to_graphite), str(timeseries_length)))
                    except:
                        logger.error(traceback.format_exc())
                        logger.error('error :: populate_metric_worker :: failed to send metric data to Graphite for %s' % str(metric))
            else:
                listOfMetricTuples = []
                try:
                    for timestamp, value in timeseries:
                        tuple_data = (metric, (int(timestamp), float(value)))
                        listOfMetricTuples.append(tuple_data)
                        sent_to_graphite += 1
                except:
                    logger.error(traceback.format_exc())
                    logger.error('error :: populate_metric_worker :: failed to populate listOfMetricTuples for %s' % str(metric))
                if listOfMetricTuples:
                    data_points_sent = 0
                    smallListOfMetricTuples = []
                    tuples_added = 0
                    for data in listOfMetricTuples:
                        # @modified 20201207 - Task #3864: flux - try except everything
                        try:
                            smallListOfMetricTuples.append(data)
                            tuples_added += 1
                            # @modified 20210115 - Task #3864: flux - try except everything
                            # Bring into line with flux worker performance changes
                            # if tuples_added >= 1000:
                            if tuples_added >= 480:
                                pickle_data_sent = pickle_data_to_graphite(smallListOfMetricTuples)
                                if pickle_data_sent:
                                    data_points_sent += tuples_added
                                    logger.info('populate_metric_worker :: sent %s/%s of %s data points to Graphite via pickle for %s' % (
                                        str(tuples_added), str(data_points_sent),
                                        str(timeseries_length), metric))
                                    sent_to_graphite += len(smallListOfMetricTuples)
                                    smallListOfMetricTuples = []
                                    tuples_added = 0
                                # @added 20210115 - Task #3864: flux - try except everything
                                # Bring into line with flux worker performance changes
                                # Reduce the speed of submissions to Graphite
                                # if there are lots of data points
                                if timeseries_length > 4000:
                                    sleep(0.3)
                                else:
                                    logger.error('error :: populate_metric_worker :: failed to send %s data points to Graphite via pickle for %s' % (
                                        str(tuples_added), metric))
                        except Exception as e:
                            logger.error('error :: populate_metric_worker :: error while iterating to pickle_data_to_graphite(smallListOfMetricTuples) - %s' % str(e))

                    if smallListOfMetricTuples:
                        # @modified 20201207 - Task #3864: flux - try except everything
                        try:
                            tuples_to_send = len(smallListOfMetricTuples)
                            pickle_data_sent = pickle_data_to_graphite(smallListOfMetricTuples)
                            if pickle_data_sent:
                                data_points_sent += tuples_to_send
                                logger.info('populate_metric_worker :: sent the last %s/%s of %s data points to Graphite via pickle for %s' % (
                                    str(tuples_to_send), str(data_points_sent),
                                    str(timeseries_length), metric))
                            else:
                                logger.error('error :: populate_metric_worker :: failed to send the last %s data points to Graphite via pickle for %s' % (
                                    str(tuples_to_send), metric))
                        except Exception as e:
                            logger.error('error :: populate_metric_worker :: error while pickle_data_to_graphite(smallListOfMetricTuples) - %s' % str(e))

            logger.info('populate_metric_worker :: sent %s data points to Graphite for %s' % (
                str(sent_to_graphite), metric))
            try:
                skyline_metric = '%s.datapoints_sent_to_graphite' % (skyline_app_graphite_namespace)
                # @modified 20191008 - Feature #3250: Allow Skyline to send metrics to another Carbon host
                # graphyte.send(skyline_metric, float(sent_to_graphite), int(time()))
                send_graphite_metric(skyline_app, skyline_metric, float(sent_to_graphite))
                logger.info('populate_metric_worker :: submitted %s to Graphite for %s' % (
                    str(float(sent_to_graphite)), skyline_metric))
            except:
                logger.error(traceback.format_exc())
                logger.error('error :: populate_metric_worker :: failed to send metric data to Graphite for %s' % str(skyline_metric))

            has_value = False
            if value == 0.0:
                has_value = True
            if value == 0:
                has_value = True
            if value:
                has_value = True

            if timestamp and has_value:
                try:
                    # Update Redis flux key
                    cache_key = 'flux.last.%s' % metric
                    metric_data = [int(timestamp), float(value)]
                    self.redis_conn.set(cache_key, str(metric_data))
                    logger.info('populate_metric_worker :: set the metric Redis key - %s - %s' % (
                        cache_key, str(metric_data)))
                except:
                    logger.error(traceback.format_exc())
                    logger.error('error :: populate_metric_worker :: failed to set Redis key - %s - %s' % (
                        cache_key, str(metric_data)))

                # Adding to the vista.fetcher.unique_metrics Redis set
                redis_set = 'vista.fetcher.unique_metrics'
                data = str(remote_target)
                try:
                    self.redis_conn.sadd(redis_set, data)
                    logger.info('populate_metric_worker :: added %s to Redis set %s' % (
                        remote_target, redis_set))
                except:
                    logger.info(traceback.format_exc())
                    logger.error('error :: populate_metric_worker :: failed to add %s to Redis set %s' % (
                        str(data), str(redis_set)))

            end_populating = int(time())
            seconds_to_run = end_populating - start_populating
            logger.info('populate_metric_worker :: %s populated to Graphite in %s seconds' % (
                metric, str(seconds_to_run)))
