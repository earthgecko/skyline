import sys
import os.path
from os import kill
import traceback
from multiprocessing import Queue, Process
try:
    from Queue import Empty  # Python 2.7
except ImportError:
    from queue import Empty  # Python 3
from time import sleep, time
from ast import literal_eval
import re
import datetime
import socket
import pickle
import struct

import requests
from redis import StrictRedis
import graphyte
import pandas as pd

from logger import set_up_logging
sys.path.append(os.path.join(os.path.dirname(os.path.realpath(__file__)), os.pardir))
sys.path.insert(0, os.path.dirname(__file__))
import settings

logger = set_up_logging('populate_metric_worker')

this_host = str(os.uname()[1])

try:
    SERVER_METRIC_PATH = '.%s' % settings.SERVER_METRICS_NAME
    if SERVER_METRIC_PATH == '.':
        SERVER_METRIC_PATH = ''
except:
    SERVER_METRIC_PATH = ''

parent_skyline_app = 'flux'
skyline_app_graphite_namespace = 'skyline.%s%s.populate_metric_worker' % (parent_skyline_app, SERVER_METRIC_PATH)

# LOCAL_DEBUG = False
LOCAL_DEBUG = True

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
        if settings.REDIS_PASSWORD:
            self.redis_conn = StrictRedis(password=settings.REDIS_PASSWORD, unix_socket_path=settings.REDIS_SOCKET_PATH)
        else:
            self.redis_conn = StrictRedis(unix_socket_path=settings.REDIS_SOCKET_PATH)
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
                    if settings.REDIS_PASSWORD:
                        self.redis_conn = StrictRedis(password=settings.REDIS_PASSWORD, unix_socket_path=settings.REDIS_SOCKET_PATH)
                    else:
                        self.redis_conn = StrictRedis(unix_socket_path=settings.REDIS_SOCKET_PATH)
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
                    logger.info('populate_metric_worker :: token from metricData set to %s' % token)
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
            cache_key = 'flux.last.%s' % metric
            last_flux_timestamp = None
            try:
                redis_last_metric_data = self.redis_conn.get(cache_key).decode('utf-8')
                last_metric_data = literal_eval(redis_last_metric_data)
                last_flux_timestamp = int(last_metric_data[0])
            except:
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
                    try:
                        # We use absolute time so that if there is a lag in mirage the correct
                        # timeseries data is still surfaced relevant to the anomalous datapoint
                        # timestamp
                        if settings.GRAPHITE_PORT != '':
                            url = '%s://%s:%s/%s/?from=%s&target=%s&format=json' % (
                                settings.GRAPHITE_PROTOCOL, settings.GRAPHITE_HOST,
                                str(settings.GRAPHITE_PORT),
                                settings.GRAPHITE_RENDER_URI, graphite_from, metric)
                        else:
                            url = '%s://%s/%s/?from=%s&target=%s&format=json' % (
                                settings.GRAPHITE_PROTOCOL, settings.GRAPHITE_HOST,
                                settings.GRAPHITE_RENDER_URI, graphite_from, metric)
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

            timeseries = []
            start_populating = int(time())
            datapoints_added_to_timeseries = 0
            datapoints_already_populated = 0
            datapoints_with_no_value = 0
            timestamp = None
            value = None

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

                success = False
                try:
                    logger.info('populate_metric_worker :: getting data from %s' % str(fetch_url))
                    response = requests.get(fetch_url)
                    if response.status_code == 200:
                        success = True
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

                # Order the time series by timestamp as the tuple can shift
                # order resulting in more recent data being added before older
                # data
                datapoints.sort()

                # Determine the timestamp of the current minute to apply
                # VISTA_DO_NOT_SUBMIT_CURRENT_MINUTE
                time_now = int(time())
                current_minute_hour = int(datetime.datetime.utcfromtimestamp(time_now).strftime('%H'))
                current_minute_minute = int(datetime.datetime.utcfromtimestamp(time_now).strftime('%M'))
                current_datetime = datetime.datetime.utcfromtimestamp(time_now).replace(hour=current_minute_hour, minute=current_minute_minute, second=0, microsecond=0)
                current_minute_timestamp_start = int(current_datetime.strftime('%s'))
                datapoints_in_current_minute = 0

                last_error = None
                value = None
                timestamp = None
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
                            try:
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

            end_fecthing = int(time())
            seconds_to_fetch = end_fecthing - start_populating
            if timestamp:
                logger.info('populate_metric_worker :: last fetched value - %s, timestamp %s' % (
                    str(value), str(timestamp)))
            logger.info('populate_metric_worker :: %s data point fecthed for %s in %s seconds' % (
                str(datapoints_added_to_timeseries), remote_target,
                str(seconds_to_fetch)))

            if not timeseries:
                logger.info('populate_metric_worker :: no data in the timeseries list for the time series for %s' % metric)
                continue

            # Order the time series by timestamp as the tuple can shift
            # order resulting in more recent data being added before older
            # data
            timeseries.sort()
            timeseries_length = len(timeseries)

            # Resample
            resample_at = '1Min'
            if resample_at:
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
                        smallListOfMetricTuples.append(data)
                        tuples_added += 1
                        if tuples_added >= 1000:
                            pickle_data_sent = pickle_data_to_graphite(smallListOfMetricTuples)
                            if pickle_data_sent:
                                data_points_sent += tuples_added
                                logger.info('populate_metric_worker :: sent %s/%s of %s data points to Graphite via pickle for %s' % (
                                    str(tuples_added), str(data_points_sent),
                                    str(timeseries_length), metric))
                                sent_to_graphite += len(smallListOfMetricTuples)
                                smallListOfMetricTuples = []
                                tuples_added = 0
                            else:
                                logger.error('error :: populate_metric_worker :: failed to send %s data points to Graphite via pickle for %s' % (
                                    str(tuples_added), metric))
                    if smallListOfMetricTuples:
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

            logger.info('populate_metric_worker :: sent %s data points to Graphite for %s' % (
                str(sent_to_graphite), metric))
            try:
                skyline_metric = '%s.datapoints_sent_to_graphite' % (skyline_app_graphite_namespace)
                graphyte.send(skyline_metric, float(sent_to_graphite), int(time()))
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
