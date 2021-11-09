import sys
import os
from time import time
import json
import traceback
from multiprocessing import Queue
from logger import set_up_logging
import string
from ast import literal_eval

import falcon

sys.path.append(os.path.join(os.path.dirname(os.path.realpath(__file__)), os.pardir))
sys.path.insert(0, os.path.dirname(__file__))

# @modified 20191115 - Branch #3262: py3
# This prevents flake8 E402 - module level import not at top of file
if True:
    import settings
    import flux
    # @added 20201018 - Feature #3798: FLUX_PERSIST_QUEUE
    # @modified 20210406 - Feature #4004: flux - aggregator.py and FLUX_AGGREGATE_NAMESPACES
    # Added get_redis_conn_decoded
    from skyline_functions import get_redis_conn, get_redis_conn_decoded

# added 20201016 - Feature #3788: snab_flux_load_test
# Wrap per metric logging in if FLUX_VERBOSE_LOGGING
try:
    FLUX_VERBOSE_LOGGING = settings.FLUX_VERBOSE_LOGGING
except:
    FLUX_VERBOSE_LOGGING = True

# @added 20201018 - Feature #3798: FLUX_PERSIST_QUEUE
try:
    FLUX_PERSIST_QUEUE = settings.FLUX_PERSIST_QUEUE
except:
    FLUX_PERSIST_QUEUE = False

# @added 20210406 - Feature #4004: flux - aggregator.py and FLUX_AGGREGATE_NAMESPACES
try:
    FLUX_AGGREGATE_NAMESPACES = settings.FLUX_AGGREGATE_NAMESPACES.copy()
    # aggregate_namespaces = list(FLUX_AGGREGATE_NAMESPACES.keys())
except:
    FLUX_AGGREGATE_NAMESPACES = {}
    # aggregate_namespaces = []
try:
    FLUX_EXTERNAL_AGGREGATE_NAMESPACES = settings.FLUX_EXTERNAL_AGGREGATE_NAMESPACES
except:
    FLUX_EXTERNAL_AGGREGATE_NAMESPACES = False

# @modified 20191129 - Branch #3262: py3
# Consolidate flux logging
# logger = set_up_logging('listen')
logger = set_up_logging(None)

LOCAL_DEBUG = False

# @added 20201018 - Feature #3798: FLUX_PERSIST_QUEUE
redis_conn = get_redis_conn('flux')
redis_conn_decoded = None


class PrometheusMetricDataPost(object):

    def on_post(self, req, resp):
        """
        The /flux/prometheus/write endpoint is called via a POST with the
        Prometheus metric data.

        """
        logger.info('debug :: prometheus :: post request')

        data = None
        try:
            data = req.stream.read()
        except Exception as err:
            logger.error('error :: prometheus :: req.stream.read() falied - %s' % str(err))

        # The prometheus remote_storage_adapter is started with the
        # --influxdb.database=<database> option, when the remote_storage_adapter
        # is used to send data to Skyline, Skyline uses this as the internal
        # metric prefix
        request_params = {}
        for request_param_key, request_param_value in req.params.items():
            request_params[request_param_key] = request_param_value

        prefix_metric = False
        influxdb_db = False
        try:
            influxdb_db = request_params['db']
        except KeyError:
            influxdb_db = False
        except Exception as err:
            logger.error('error :: prometheus :: error determining request_params[\'db\'] - %s' % str(err))
            influxdb_db = False
        if influxdb_db:
            prefix_metric = str(influxdb_db)

        prometheus_metrics = {}
        try:
            metric_str = data.decode('utf-8')
            raw_metrics_data = metric_str.split('\n')
            for raw_metric_data in raw_metrics_data:
                metric = None
                metric_data = None
                # Skip blank lines
                if raw_metric_data == '':
                    continue
                # influxdb format
                # metric_name = 'go_gc_duration_seconds,alias=Prometheus,instance=localhost:9090,job=prometheus,monitor=master,quantile=0.5'
                metric_name = raw_metric_data.split(' ')[0]
                metric_name_elements = metric_name.split(',')
                prometheus_metric_name = '%s{' % metric_name_elements[0]
                for index, element in enumerate(metric_name_elements[1:]):
                    label_name = element.split('=')[0]
                    label = element.split('=')[1]
                    if index == 0:
                        prometheus_metric_name = '%s%s="%s"' % (prometheus_metric_name, label_name, label)
                    else:
                        # Drop the monitor="master" label as it does not exist
                        # on the source
                        if label_name == 'monitor':
                            if label == 'master':
                                continue
                        prometheus_metric_name = '%s,%s="%s"' % (prometheus_metric_name, label_name, label)
                prometheus_metric_name = '%s}' % prometheus_metric_name
                if prefix_metric:
                    prometheus_metric_name = '%s.%s' % (prefix_metric, prometheus_metric_name)
                value_tag = raw_metric_data.split(' ')[1]
                value = value_tag.split('=')[1]
                timestamp_str = raw_metric_data.split(' ')[2]
                timestamp = int(timestamp_str)
                metric_data = '%s,%s' % (str(timestamp), str(value))
                prometheus_metrics[prometheus_metric_name] = metric_data
                # Add to flux.prometheus_metrics Redis hash key in the following
                # format for Horizon prometheus to add the the Horizon worker queue
                # to submit to Redis, hence the strict and usual data format for the
                # hash key
                # redis_conn_decoded.hset('flux.prometheus_metrics', metric, str(timestamp,value))
                # The data is stored by flux in the Redis hash key as
                # 'metric': 'timestamp,value' to reduce the overhead of
                # needing to use literal_eval on every call in Horizon prometheus,
                # instead a simple str split is used
        except Exception as err:
            logger.error(traceback.format_exc())
            logger.error('error :: prometheus :: failed to write data - %s' % str(err))

        if prometheus_metrics:
            try:
                redis_conn.hset('test.flux.prometheus_metrics', mapping=prometheus_metrics)
                logger.info('flux :: prometheus :: added %s metrics to test.flux.prometheus_metrics Redis hash' % str(len(list(prometheus_metrics.keys()))))
            except Exception as err:
                logger.error(traceback.format_exc())
                logger.error('error :: flux :: prometheus :: failed to add metrics to test.flux.prometheus_metrics Redis hash - %s' % str(err))
                resp.status = falcon.HTTP_500

        resp.status = falcon.HTTP_204
        return
