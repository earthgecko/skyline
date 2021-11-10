import socket
from os import kill, getpid
try:
    from Queue import Full
except ImportError:
    from queue import Full
from multiprocessing import Process
import sys
from time import time, sleep
import traceback

import logging
import os.path
from os import remove as os_remove
import settings
from skyline_functions import (get_redis_conn_decoded, send_graphite_metric)

parent_skyline_app = 'horizon'
child_skyline_app = 'prometheus'
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

try:
    HORIZON_PROMETHEUS_METRIC_LIMIT = settings.HORIZON_PROMETHEUS_METRIC_LIMIT
except AttributeError:
    HORIZON_PROMETHEUS_METRIC_LIMIT = settings.CHUNK_SIZE

skyline_app_graphite_namespace = 'skyline.%s%s.%s' % (
    parent_skyline_app, SERVER_METRIC_PATH, child_skyline_app)

LOCAL_DEBUG = False


class PrometheusMetrics(Process):
    """
    The PrometheusMetrics process is responsible for getting metrics submitted
    to flux from the flux.prometheus_metrics Redis hash set and adding them to
    the Horizon worker queue to be submitted to Redis.
    """
    def __init__(self, queue, parent_pid):
        super(PrometheusMetrics, self).__init__()
        self.q = queue
        self.daemon = True
        self.parent_pid = parent_pid
        self.current_pid = getpid()
        self.redis_conn_decoded = get_redis_conn_decoded(skyline_app)

    def check_if_parent_is_alive(self):
        """
        Self explanatory
        """
        try:
            kill(self.current_pid, 0)
            kill(self.parent_pid, 0)
        except:
            # @added 20201203 - Bug #3856: Handle boring sparsely populated metrics in derivative_metrics
            # Log warning
            logger.warning('warning :: parent or current process dead')
            exit(0)

    def submit_PrometheusMetrics(self):
        """
        Get Prometheus metrics from the Redis flux.prometheus_metrics hash key
        add submit them to the Horizon worker queue
        """
        last_hash_check = int(time)
        last_log = int(last_hash_check)
        submitted_metrics = 0

        running = True
        while running:
            self.check_if_parent_is_alive()
            now_ts = int(time)

            if now_ts >= (last_log + 59):
                logger.info('prometheus :: submit_PrometheusMetrics - submitted %s metrics to worker in the last 60 seconds' % str(submitted_metrics))
                submitted_metrics = 0
                last_log = int(now_ts)

            if (now_ts - last_hash_check) < 1:
                last_hash_check = int(now_ts)
                continue
            last_hash_check = int(now_ts)
            try:
                prometheus_metrics = {}
                try:
                    prometheus_metrics = self.redis_conn_decoded.hgetall('flux.prometheus_metrics')
                except Exception as err:
                    logger.error('error :: prometheus :: submit_PrometheusMetrics could not determine prometheus_metrics from flux.prometheus_metrics Redis hash - %s' % str(err))
                    prometheus_metrics = {}
                if not prometheus_metrics:
                    continue

                chunk = []
                metrics_added = 0
                prometheus_metrics_list = list(prometheus_metrics.keys())
                for prometheus_metric in prometheus_metrics_list:
                    if metrics_added >= HORIZON_PROMETHEUS_METRIC_LIMIT:
                        break
                    try:
                        metric_data_str = prometheus_metrics[prometheus_metric]
                        # The data is stored by flux in the Redis hash key as
                        # 'metric': 'timestamp,value' to reduce the overhead of
                        # needing to use literal_eval on every call, instead a
                        # simple str split is used
                        timestamp = metric_data_str.split(',')[0]
                        value = metric_data_str.split(',')[1]
                        chunk.append([prometheus_metric, [int(timestamp), float(value)]])
                        metrics_added += 1
                        submitted_metrics += 1
                        self.redis_conn_decoded.hdel('flux.prometheus_metrics', prometheus_metric)
                    except Exception as err:
                        logger.error('error :: prometheus :: submit_PrometheusMetrics could not determine prometheus_metrics from flux.prometheus_metrics Redis hash - %s' % str(err))
                    if len(chunk) > settings.CHUNK_SIZE:
                        break
                if chunk:
                    try:
                        self.q.put(list(chunk), block=False)
                        chunk[:] = []
                    # Drop chunk if queue is full
                    except Full:
                        chunks_dropped = str(len(chunk))
                        logger.info(
                            '%s :: prometheus :: submit_PrometheusMetrics :: worker queue is full, dropping %s Prometheus metric datapoints'
                            % (skyline_app, chunks_dropped))
                        send_metric_name = '%s.prometheus_chunks_dropped' % skyline_app_graphite_namespace
                        send_graphite_metric(skyline_app, send_metric_name, chunks_dropped)
                        chunk[:] = []

            except Exception as err:
                logger.info('%s :: prometheus :: submit_PrometheusMetrics :: error adding check to queue - %s' % (skyline_app, str(err)))

    def run(self):
        """
        Called when process intializes.
        """

        # Log management to prevent overwriting
        # Allow the bin/<skyline_app>.d to manage the log
        if os.path.isfile(skyline_app_logwait):
            try:
                os_remove(skyline_app_logwait)
            except OSError:
                logger.error('error - failed to remove %s, continuing' % skyline_app_logwait)

        now = time()
        log_wait_for = now + 5
        while now < log_wait_for:
            if os.path.isfile(skyline_app_loglock):
                sleep(.1)
                now = time()
            else:
                now = log_wait_for + 1

        logger.info('starting %s Prometheus' % skyline_app)
        if os.path.isfile(skyline_app_loglock):
            logger.error('error - bin/%s.d log management seems to have failed, continuing' % skyline_app)
            try:
                os_remove(skyline_app_loglock)
                logger.info('log lock file removed')
            except OSError:
                logger.error('error - failed to remove %s, continuing' % skyline_app_loglock)
        else:
            logger.info('bin/%s.d log management done' % skyline_app)

        logger.info('%s :: starting submit_PrometheusMetrics' % skyline_app)
        self.submit_PrometheusMetrics()
