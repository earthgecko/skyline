"""
get_victoriametrics_metric
"""
import logging
import json
import os
import urllib.parse

import requests

import settings
from skyline_functions import get_redis_conn_decoded, mkdir_p

# @added 20220728 - Task #2732: Prometheus to Skyline
#                   Branch #4300: prometheus
from functions.metrics.get_base_name_from_labelled_metrics_name import get_base_name_from_labelled_metrics_name
from functions.plots.plot_timeseries import plot_timeseries
from functions.timeseries.strictly_increasing_monotonicity import strictly_increasing_monotonicity
from functions.metrics.get_metric_id_from_base_name import get_metric_id_from_base_name

# @added 20230102 - Feature #4776: Handle multiple metrics in victoriametrics response
from functions.prometheus.metric_name_labels_parser import metric_name_labels_parser

try:
    VICTORIAMETRICS_ENABLED = settings.VICTORIAMETRICS_ENABLED
except:
    VICTORIAMETRICS_ENABLED = False
vm_url = None
if VICTORIAMETRICS_ENABLED:
    try:
        vm_url = '%s://%s:%s' % (
            settings.VICTORIAMETRICS_OPTS['scheme'],
            settings.VICTORIAMETRICS_OPTS['host'],
            str(settings.VICTORIAMETRICS_OPTS['port'])
        )
    except:
        vm_url = None
    # @added 20221107 - Task #2732: Prometheus to Skyline
    #                   Branch #4300: prometheus
    # Handle cluster select path
    select_path = None
    try:
        select_path = settings.VICTORIAMETRICS_OPTS['select_path']
    except:
        select_path = None
    if select_path and vm_url:
        vm_url = '%s%s' % (vm_url, select_path)

# In Skyline a metric is either a counter (derivative) or a gauge
skyline_metric_types = {'COUNTER': 1, 'GAUGE': 0}
skyline_metric_types_by_id = {}
for key in list(skyline_metric_types.keys()):
    skyline_metric_types_by_id[skyline_metric_types[key]] = key


# @added 20220714 - Task #2732: Prometheus to Skyline
#                   Branch #4300: prometheus
# @modified 20230518 - Task #2732: Prometheus to Skyline
#                      Branch #4300: prometheus
# Added do_not_type
def get_victoriametrics_metric(
    current_skyline_app, metric, from_timestamp, until_timestamp, data_type,
        output_object, metric_data={}, plot_parameters={}, do_not_type=False):
    """
    Fetch data from victoriametrics and return it as object, a graph image or
    save it as file

    :param current_skyline_app: the skyline app using this function
    :param metric: metric name
    :param from_timestamp: unix timestamp
    :param until_timestamp: unix timestamp
    :param data_type: image, json or list
    :param output_object: object or path and filename to save data as, if set to
                          object, the object is returned
    :param metric_data: metric data dict
    :param do_not_type: whether to not use the known type
    :type current_skyline_app: str
    :type metric: str
    :type from_timestamp: str
    :type until_timestamp: str
    :type data_type: str
    :type output_object: str
    :type metric_data: dict
    :return: timeseries, ``True``, ``False``
    :rtype: str or bool

    """

    function_str = 'get_victoriametrics_metric'

    timeseries = False

    # Do not log luminosity classify_metrics calls unless error
    log = True
    if current_skyline_app == 'luminosity' and data_type == 'list' and output_object == 'object':
        log = False

    current_skyline_app_logger = str(current_skyline_app) + 'Log'
    current_logger = logging.getLogger(current_skyline_app_logger)

    # @added 20220728 - Task #2732: Prometheus to Skyline
    #                   Branch #4300: prometheus
    labelled_metric_name = None
    base_name = str(metric)
    metric_id = 0
    if metric.startswith('labelled_metrics.'):
        labelled_metric_name = str(metric)
        metric_id_str = metric.replace('labelled_metrics.', '', 1)
        try:
            metric_id = int(metric_id_str)
        except:
            pass
        try:
            metric_name = get_base_name_from_labelled_metrics_name(current_skyline_app, metric)
            if metric_name:
                labelled_metric_base_name = str(metric)
                metric = str(metric_name)
                base_name = str(metric_name)
        except Exception as err:
            if not log:
                current_skyline_app_logger = current_skyline_app + 'Log'
                current_logger = logging.getLogger(current_skyline_app_logger)
            current_logger.error('error :: %s :: get_base_name_from_labelled_metrics_name failed for %s - %s' % (
                function_str, metric, err))
            return timeseries

    if log:
        current_logger.info('%s :: %s' % (function_str, metric))

    if not metric_id:
        try:
            metric_id = get_metric_id_from_base_name(current_skyline_app, base_name)
        except:
            pass

    # Use skyline.labelled_metrics.id.type rather
    # metric_type_redis_key = None
    # if not metric_data:
    #     try:
    #         metric_dict = metric_name_labels_parser(current_skyline_app, metric)
    #         metric_name = metric_dict['metric']
    #         labels = metric_dict['labels']
    #         tenant_id = labels['_tenant_id']
    #         server_id = labels['_server_id']
    #         metric_type_redis_key = 'metrics_manager.prometheus.metrics_type.%s.%s' % (str(tenant_id), str(server_id))
    #     except Exception as err:
    #         if not log:
    #             current_skyline_app_logger = current_skyline_app + 'Log'
    #             current_logger = logging.getLogger(current_skyline_app_logger)
    #         current_logger.error('error :: %s :: failed to determine labels for %s - %s' % (
    #             function_str, metric, err))
    #         return timeseries

    # Use Graphite timeouts
    connect_timeout = int(settings.GRAPHITE_CONNECT_TIMEOUT)
    read_timeout = int(settings.GRAPHITE_READ_TIMEOUT)
    use_timeout = (int(connect_timeout), int(read_timeout))

    output_format = data_type

    redis_conn_decoded = None
    try:
        redis_conn_decoded = get_redis_conn_decoded(current_skyline_app)
    except Exception as err:
        current_logger.error('error :: %s :: get_redis_conn_decoded failed - %s' % (
            function_str, err))

    metric_type = None
    try:
        skyline_metric_type_str = redis_conn_decoded.hget('skyline.labelled_metrics.id.type', str(metric_id))
        if skyline_metric_type_str:
            skyline_metric_type = int(skyline_metric_type_str)
            metric_type = skyline_metric_types_by_id[skyline_metric_type]
    except Exception as err:
        if not log:
            current_skyline_app_logger = current_skyline_app + 'Log'
            current_logger = logging.getLogger(current_skyline_app_logger)
        current_logger.error('error :: %s :: failed to get %s from skyline.labelled_metrics.id.type - %s' % (
            function_str, str(metric_id), err))

    if log:
        current_logger.info('%s :: %s, metric_type: %s' % (function_str, metric, metric_type))

    # @added 20230518 - Task #2732: Prometheus to Skyline
    #                      Branch #4300: prometheus
    # Added do_not_type
    if do_not_type:
        metric_type = None

    response_dict = {}
    encoded_metric = urllib.parse.quote(metric)
    if metric_type == 'COUNTER':
        rate_metric = 'rate(%s[10m])' % metric
        encoded_metric = urllib.parse.quote(rate_metric)
    else:
        encoded_metric = urllib.parse.quote(metric)

    if log:
        current_logger.info('%s :: encoded_metric: %s' % (function_str, encoded_metric))

    step = 600
    timeseries_duration = int(until_timestamp) - int(from_timestamp)
    if timeseries_duration <= 86401:
        step = 60

    # Use a step of 600 which is effectively the same as summarise at 10m in
    # terms of the number of data points returned
    url = '%s/prometheus/api/v1/query_range?query=%s&start=%s.000&end=%s.000&step=%s&nocache=1' % (
        vm_url, encoded_metric, str(from_timestamp), str(until_timestamp),
        str(step))
    if log:
        current_logger.info('%s :: url: %s' % (function_str, url))

    try:
        r = requests.get(url, timeout=use_timeout)
        response_dict = r.json()
    except Exception as err:
        current_logger.error('error :: %s :: request failed for %s - %s' % (
            function_str, url, err))

    result = []
    if response_dict:
        try:
            result = response_dict['data']['result']
        except Exception as err:
            current_logger.error('error :: %s :: unexpected response for %s response_dict: %s - %s' % (
                function_str, metric, str(response_dict), err))
        if not result:
            current_logger.warning('warning :: %s :: empty result for %s response_dict: %s' % (
                function_str, metric, str(response_dict)))
            timeseries = []
            return False

    # @added 20230102 - Feature #4776: Handle multiple metrics in victoriametrics response
    # If there are multiple metrics that match a query, victoriametrics will
    # return them all in the response. The current code assumes that there will
    # be one metric returned, however if a metric has been renamed or an
    # additional tag has been added then the name may match 2 metrics (or more)
    # so select to correct metric out the results
    result_index = 0
    if len(result) > 1:
        current_logger.info('%s :: more than 1 metric in the VictoriaMetrics results for %s, data for %s metrics returned' % (
            function_str, metric, str(len(result))))
        metric_metric_dict = {}
        try:
            metric_metric_dict = metric_name_labels_parser(current_skyline_app, metric)
        except Exception as err:
            current_logger.error('error :: %s :: metric_name_labels_parser fail on %s - %s' % (
                function_str, metric, err))
        number_of_metric_labels = len(list(metric_metric_dict['labels'].keys())) + 1
        for index, result_dict in enumerate(result):
            labels_found = 0
            not_metric = False
            for label in list(result_dict['metric'].keys()):
                value = result_dict['metric'][label]
                if label == '__name__':
                    if not metric.startswith(value):
                        not_metric = True
                        current_logger.info('%s :: not using data from %s' % (
                            function_str, str(result_dict['metric'])))
                        break
                    labels_found += 1
                    continue
                label_and_value = '%s="%s"' % (str(label), str(value))
                if label_and_value not in metric:
                    not_metric = True
                    current_logger.info('%s :: not using data from %s' % (
                        function_str, str(result_dict['metric'])))
                    break
                labels_found += 1
            if not not_metric and labels_found == number_of_metric_labels:
                result_index = index
                current_logger.info('%s :: match found using data from %s' % (
                    function_str, str(result_dict['metric'])))
                break
        current_logger.info('%s :: using data from result_index: %s' % (
            function_str, str(result_index)))

    if result:
        try:
            # @modified 20230102 - Feature #4776: Handle multiple metrics in victoriametrics response
            # timeseries = response_dict['data']['result'][0]['values']
            timeseries = response_dict['data']['result'][result_index]['values']
        except Exception as err:
            current_logger.error('error :: %s :: unexpected response for %s response_dict: %s - %s' % (
                function_str, metric, str(response_dict), err))

    converted = []
    for datapoint in timeseries:
        try:
            new_datapoint = [int(datapoint[0]), float(datapoint[1])]
            converted.append(new_datapoint)
        except:  # nosec
            continue

    # @modified 20230518 - Task #2732: Prometheus to Skyline
    #                      Branch #4300: prometheus
    # Added do_not_type
    # if not metric_type:
    if not metric_type and not do_not_type:
        is_strictly_increasing_monotonicity = strictly_increasing_monotonicity(converted)
        if is_strictly_increasing_monotonicity:
            metric_type = 'COUNTER'
        else:
            metric_type = 'GAUGE'
        if log:
            current_logger.info('%s :: unknown metric_type determined to be %s' % (function_str, metric_type))
        try:
            redis_conn_decoded.hset('skyline.labelled_metrics.id.type', str(metric_id), skyline_metric_types[metric_type])
            if log:
                current_logger.info('%s :: set %s in skyline.labelled_metrics.id.type to %s' % (function_str, str(metric_id), metric_type))
        except Exception as err:
            current_logger.error('error :: %s :: hset skyline.labelled_metrics.id.type failed - %s' % (
                function_str, err))
        if metric_type == 'COUNTER':
            rate_metric = 'rate(%s[10m])' % metric
            encoded_metric = urllib.parse.quote(rate_metric)
            url = '%s/prometheus/api/v1/query_range?query=%s&start=%s.000&end=%s.000&step=%s&nocache=1' % (
                vm_url, encoded_metric, str(from_timestamp), str(until_timestamp),
                str(step))
            try:
                r = requests.get(url, timeout=use_timeout)
                response_dict = r.json()
            except Exception as err:
                current_logger.error('error :: %s :: request failed for %s - %s' % (
                    function_str, url, err))
            result = []
            if response_dict:
                try:
                    result = response_dict['data']['result']
                except Exception as err:
                    current_logger.error('error :: %s :: unexpected response for %s response_dict: %s - %s' % (
                        function_str, metric, str(response_dict), err))
                if not result:
                    current_logger.warning('warning :: %s :: empty result for %s response_dict: %s' % (
                        function_str, metric, str(response_dict)))
                    timeseries = []
                    return False

            # @added 20230102 - Feature #4776: Handle multiple metrics in victoriametrics response
            # If there are multiple metrics that match a query, victoriametrics will
            # return them all in the response. The current code assumes that there will
            # be one metric returned, however if a metric has been renamed or an
            # additional tag has been added then the name may match 2 metrics (or more)
            # so select to correct metric out the results
            result_index = 0
            if len(result) > 1:
                metric_metric_dict = {}
                try:
                    metric_metric_dict = metric_name_labels_parser(current_skyline_app, metric)
                except Exception as err:
                    current_logger.error('error :: %s :: metric_name_labels_parser fail on %s - %s' % (
                        function_str, metric, err))
                number_of_metric_labels = len(list(metric_metric_dict['labels'].keys())) + 1
                for index, result_dict in enumerate(result):
                    labels_found = 0
                    not_metric = False
                    for label in list(result_dict['metric'].keys()):
                        value = result_dict['metric'][label]
                        if label == '__name__':
                            if not metric.startswith(value):
                                not_metric = True
                                break
                            labels_found += 1
                            continue
                        label_and_value = '%s="%s"' % (str(label), str(value))
                        if label_and_value not in metric:
                            not_metric = True
                            break
                        labels_found += 1
                    if not not_metric and labels_found == number_of_metric_labels:
                        result_index = index
                        break

            try:
                # @modified 20230102 - Feature #4776: Handle multiple metrics in victoriametrics response
                # timeseries = response_dict['data']['result'][0]['values']
                timeseries = response_dict['data']['result'][result_index]['values']
            except Exception as err:
                current_logger.error('error :: %s :: unexpected response for %s response_dict: %s - %s' % (
                    function_str, metric, str(response_dict), err))
            converted = []
            for datapoint in timeseries:
                try:
                    new_datapoint = [int(datapoint[0]), float(datapoint[1])]
                    converted.append(new_datapoint)
                except:  # nosec
                    continue

    if data_type == 'image':

        if log:
            current_logger.info('%s :: timeseries sample: %s' % (function_str, str(timeseries[0:3])))

        outputted_image_file = None
        try:
            success, outputted_image_file = plot_timeseries(
                current_skyline_app, metric, timeseries, output_object,
                plot_parameters)
            if log:
                current_logger.info('%s :: plotted image: %s' % (function_str, str(success)))
        except Exception as err:
            current_logger.error('error :: %s :: plot_timeseries failed - %s' % (
                function_str, err))
        if outputted_image_file:
            return outputted_image_file
        current_logger.error('error :: plot_timeseries failed to return an image file')
        return False

    if output_object != 'object':
        output_object_path = os.path.dirname(output_object)
        if not os.path.isdir(output_object_path):
            try:
                mkdir_p(output_object_path)
                current_logger.info(
                    'output_object_path - %s' % str(output_object_path))
            except Exception as err:
                current_logger.error(
                    'error :: failed to create output_object_path - %s - %s' %
                    str(output_object_path))
                return False

        with open(output_object, 'w') as f:
            f.write(json.dumps(converted))
        os.chmod(output_object, mode=0o644)
        return output_object
    else:
        if data_type == 'list':
            return converted
        if data_type == 'json':
            timeseries_json = json.dumps(converted)
            return timeseries_json

    return timeseries
