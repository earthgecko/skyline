"""
send_graphite_metric.py
"""
import logging
import traceback
import socket
from time import time, sleep

import settings

# @added 20220723 - Task #2732: Prometheus to Skyline
#                   Branch #4300: prometheus
from functions.prometheus.skyline_prometheus_metrics import skyline_prometheus_metric

# @added 20230511 - Feature #4904: Handle send_graphite_metric failure
# from skyline_functions import get_redis_conn_decoded

try:
    # @modified 20190518 - Branch #3002: docker
    # from settings import GRAPHITE_HOST
    from settings import CARBON_HOST
except:
    # @modified 20190518 - Branch #3002: docker
    # GRAPHITE_HOST = ''
    CARBON_HOST = ''
try:
    from settings import CARBON_PORT
except:
    CARBON_PORT = ''

# @added 20191007 - Feature #3250: Allow Skyline to send metrics to another Carbon host
try:
    from settings import SKYLINE_METRICS_CARBON_HOST
    skyline_metrics_carbon_host = SKYLINE_METRICS_CARBON_HOST
except:
    skyline_metrics_carbon_host = CARBON_HOST
try:
    from settings import SKYLINE_METRICS_CARBON_PORT
    skyline_metrics_carbon_port = SKYLINE_METRICS_CARBON_PORT
except:
    skyline_metrics_carbon_port = CARBON_PORT

try:
    EXPOSE_PROMETHEUS_METRICS = settings.EXPOSE_PROMETHEUS_METRICS
except:
    EXPOSE_PROMETHEUS_METRICS = True


# @added 20220722 - Task #2732: Prometheus to Skyline
#                   Branch #4300: prometheus
# Moved from skyline_functions
# @modified 20230511 - Feature #4904: Handle send_graphite_metric failure
# Added timestamp parameter to allow for the actual timestamp from the
# fail queue to be used if it is passed
def send_graphite_metric(self, current_skyline_app, metric, value, timestamp=None):
    """
    Sends the skyline_app metrics to the `GRAPHITE_HOST` if a graphite
    host is defined.

    :param current_skyline_app: the skyline app using this function
    :param metric: the metric namespace
    :param value: the metric value (as a str not an int)
    :param timestamp: the timestamp
    :param processing_failure: used by process_graphite_fail_queue
    :type current_skyline_app: str
    :type metric: str
    :type value: str
    :type timestamp: int
    :type processing_failure: bool
    :return: ``True`` or ``False``
    :rtype: boolean

    """

    # @added 20230511 - Feature #4904: Handle send_graphite_metric failure
    # Check if the skyline.graphite_failure exist
#    redis_conn_decoded = None
#    try:
#        redis_conn_decoded = get_redis_conn_decoded(current_skyline_app)
#    except Exception as err:
#        current_skyline_app_logger = str(current_skyline_app) + 'Log'
#        current_logger = logging.getLogger(current_skyline_app_logger)
#        current_logger.error('error :: send_graphite_metric :: cannot connect to Redis - %s' % err)
    graphite_failure = 0
#    if redis_conn_decoded:
    try:
        graphite_failure = self.redis_conn_decoded.exists('skyline.graphite_fail')
    except Exception as err:
        current_skyline_app_logger = str(current_skyline_app) + 'Log'
        current_logger = logging.getLogger(current_skyline_app_logger)
        current_logger.error('error :: send_graphite_metric :: failed to get Redis key skyline.graphite_failure - %s' % err)

    if graphite_failure:
        processing = 0
        try:
            processing = self.redis_conn_decoded.exists('skyline.processing.graphite_fail_queue')
        except Exception as err:
            current_skyline_app_logger = str(current_skyline_app) + 'Log'
            current_logger = logging.getLogger(current_skyline_app_logger)
            current_logger.error('error :: send_graphite_metric :: failed to get Redis key skyline.processing.graphite_fail_queue - %s' % (
                err))
        if processing:
            slept = 0
            while slept < 3:
                sleep(1)
                slept += 1
                try:
                    graphite_failure = self.redis_conn_decoded.exists('skyline.graphite_fail')
                except Exception as err:
                    current_skyline_app_logger = str(current_skyline_app) + 'Log'
                    current_logger = logging.getLogger(current_skyline_app_logger)
                    current_logger.error('error :: send_graphite_metric :: failed to get Redis key skyline.graphite_failure - %s' % err)
                if not graphite_failure:
                    slept = 5

        if graphite_failure:
            try:
                graphite_ok = self.redis_conn_decoded.exists('skyline.graphite_ok')
                if graphite_ok:
                    graphite_failure = 0
            except Exception as err:
                current_skyline_app_logger = str(current_skyline_app) + 'Log'
                current_logger = logging.getLogger(current_skyline_app_logger)
                current_logger.error('error :: send_graphite_metric :: failed to get Redis key skyline.graphite_ok - %s' % err)

    # @added 20230511 - Feature #4904: Handle send_graphite_metric failure
    # If Skyline has been partitioned from Graphite use the failure queue that
    # is processed by the horizon canary worker
    def graphite_fail(metric, value, ts):
        added = 0
        try:
            data = [metric, value, ts]
            self.redis_conn_decoded.set('skyline.graphite_fail', str(data))
        except Exception as err:
            current_skyline_app_logger = str(current_skyline_app) + 'Log'
            current_logger = logging.getLogger(current_skyline_app_logger)
            current_logger.error('error :: send_graphite_metric :: failed to sadd to Redis set skyline.graphite_failure_queue - %s' % err)
        try:
            data = [metric, value, ts]
            added = self.redis_conn_decoded.sadd('skyline.graphite_fail_queue', str(data))
        except Exception as err:
            current_skyline_app_logger = str(current_skyline_app) + 'Log'
            current_logger = logging.getLogger(current_skyline_app_logger)
            current_logger.error('error :: send_graphite_metric :: failed to sadd to Redis set skyline.graphite_failure_queue - %s' % err)
        return added

    if not timestamp:
        timestamp = int(time())

    # @added 20190518 - Branch #3002: docker
    # If the CARBON_HOST is set to the default do not send_graphite_metric
    # @modified 20191007 - Feature #3250: Allow Skyline to send metrics to another Carbon host
    # if CARBON_HOST == 'YOUR_GRAPHITE_HOST.example.com':
    if skyline_metrics_carbon_host == 'YOUR_GRAPHITE_HOST.example.com':
        current_skyline_app_logger = str(current_skyline_app) + 'Log'
        current_logger = logging.getLogger(current_skyline_app_logger)
        current_logger.info('CARBON_HOST is not configured in settings.py no CARBON_HOST to send metrics to')
        if not EXPOSE_PROMETHEUS_METRICS:
            # skyline_metrics_carbon_host = ''
            return False

    # @added 20220723 - Task #2732: Prometheus to Skyline
    #                   Branch #4300: prometheus
    submitted_prometheus_metric = False
    if EXPOSE_PROMETHEUS_METRICS:
        try:
            submitted_prometheus_metric = skyline_prometheus_metric(current_skyline_app, metric, timestamp, value)
        except Exception as err:
            current_skyline_app_logger = str(current_skyline_app) + 'Log'
            current_logger = logging.getLogger(current_skyline_app_logger)
            current_logger.error('error :: send_graphite :: skyline_prometheus_metric failed - %s' % err)

    # @modified 20190518 - Branch #3002: docker
    # Use the CARBON_HOST rather than GRAPHITE_HOST to allow for the 2 to be
    # on different hosts
    # if GRAPHITE_HOST != '':
    # @modified 20191007 - Feature #3250: Allow Skyline to send metrics to another Carbon host
    # if CARBON_HOST != '':
    if skyline_metrics_carbon_host != '':

        try:
            # @added 20230511 - Feature #4904: Handle send_graphite_metric failure
            # If Skyline has been partitioned from Graphite and submission has
            # failed fail quickly so that the submission timeouts do not delay
            # other analysis
            if graphite_failure:
                try:
                    added = graphite_fail(metric, value, timestamp)
                except Exception as err:
                    current_skyline_app_logger = str(current_skyline_app) + 'Log'
                    current_logger = logging.getLogger(current_skyline_app_logger)
                    current_logger.error('error :: send_graphite_metric :: graphite_fail failed - %s' % err)
                return False

            sock = socket.socket()

            # @modified 20230511 - Feature #4904: Handle send_graphite_metric failure
            # sock.settimeout(10)
            sock.settimeout(3)

            # Handle connection error to Graphite #116 @etsy
            # Fixed as per https://github.com/etsy/skyline/pull/116 and
            # mlowicki:etsy_handle_connection_error_to_graphite
            # Handle connection error to Graphite #7 @ earthgecko
            # merged 1 commit into earthgecko:master from
            # mlowicki:handle_connection_error_to_graphite on 16 Mar 2015
            try:
                # @modified 20190518 - Branch #3002: docker
                # sock.connect((GRAPHITE_HOST, CARBON_PORT))
                # @modified 20191007 - Feature #3250: Allow Skyline to send metrics to another Carbon host
                # sock.connect((CARBON_HOST, CARBON_PORT))
                sock.connect((skyline_metrics_carbon_host, skyline_metrics_carbon_port))
                sock.settimeout(None)
            except socket.error:
                sock.settimeout(None)
                # @modified 20190518 - Branch #3002: docker
                # endpoint = '%s:%d' % (GRAPHITE_HOST, CARBON_PORT)
                # @modified 20191007 - Feature #3250: Allow Skyline to send metrics to another Carbon host
                # endpoint = '%s:%d' % (CARBON_HOST, CARBON_PORT)
                endpoint = '%s:%d' % (skyline_metrics_carbon_host, skyline_metrics_carbon_port)
                current_skyline_app_logger = str(current_skyline_app) + 'Log'
                current_logger = logging.getLogger(current_skyline_app_logger)
                current_logger.error(traceback.format_exc())
                current_logger.error(
                    'error :: cannot connect to Graphite at %s' % endpoint)

                # @added 20230511 - Feature #4904: Handle send_graphite_metric failure
                try:
                    added = graphite_fail(metric, value, timestamp)
                    current_logger.info('send_graphite_metric :: submitted to graphite_fail')
                except Exception as err:
                    current_logger.error('error :: send_graphite_metric :: graphite_fail failed - %s' % err)

                return False

            # For the same reason as above
            # sock.sendall('%s %s %i\n' % (name, value, time()))
            try:
                message = '%s %s %i\n' % (metric, str(value), timestamp)
                sock.sendall(message.encode())
                sock.close()
                return True
            except:
                # @modified 20190518 - Branch #3002: docker
                # endpoint = '%s:%d' % (GRAPHITE_HOST, CARBON_PORT)
                # @modified 20191007 - Feature #3250: Allow Skyline to send metrics to another Carbon host
                # endpoint = '%s:%d' % (CARBON_HOST, CARBON_PORT)
                endpoint = '%s:%d' % (skyline_metrics_carbon_host, skyline_metrics_carbon_port)
                current_skyline_app_logger = str(current_skyline_app) + 'Log'
                current_logger = logging.getLogger(current_skyline_app_logger)
                current_logger.error(
                    'error :: could not send data to Graphite at %s' % endpoint)
                # @added 20230511 - Feature #4904: Handle send_graphite_metric failure
                try:
                    added = graphite_fail(metric, value, timestamp)
                    current_logger.info('send_graphite_metric :: submitted to graphite_fail')
                except Exception as err:
                    current_logger.error('error :: send_graphite_metric :: graphite_fail failed - %s' % err)

                return False

        except Exception as err:
            current_skyline_app_logger = str(current_skyline_app) + 'Log'
            current_logger = logging.getLogger(current_skyline_app_logger)
            current_logger.error(traceback.format_exc())
            current_logger.error('error :: send_graphite_metric :: %s' % err)

    if submitted_prometheus_metric:
        return True

    return False