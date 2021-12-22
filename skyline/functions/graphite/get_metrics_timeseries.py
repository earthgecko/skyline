import logging
import requests

import settings
from skyline_functions import get_redis_conn_decoded, sanitise_graphite_url, write_data_to_file

try:
    from settings import MAX_GRAPHITE_TARGETS
except:
    MAX_GRAPHITE_TARGETS = 200


# @added 20210708 - Feature #4164: luminosity - cloudbursts
#                   Task #4096: POC cloudbursts
# Add a global method to get multiple metrics timeseries from Graphite
def get_metrics_timeseries(
        current_skyline_app, metrics_functions, from_timestamp,
        until_timestamp, log=True):
    """
    Return dictionary of metrics with their timeseries as a list e.g.

    metrics_timeseries = {
        'metric.1': {
            'timeseries': [[ts, value], [ts, value], ..., [ts, value]],
            'functions': 'nonNegativeDerivative',
        },
        'metric.2': {
            'timeseries': [[ts, value], [ts, value], ..., [ts, value]],
            'functions': None,
        },
        'metric.3': {
            'timeseries': [[ts, value], [ts, value], ..., [ts, value]],
            'functions': {'summarise': {'intervalString': '10min', 'func': 'sum'}, 'integral': None},
        },
    }

    The metrics_functions parameter dictionary allows for metrics and any
    functions to be applied to be specified e.g.

    metrics_functions = {
        'metric.1': {
            'functions': None,
        },
        'metric.2': {
            'functions': None,
        },
        'metric.3': {
            'functions': {'integral': None, 'summarize': {'intervalString': '10min', 'func': 'sum'}},
        },
    }

    Each metric can have one or multiple functions parsed for it using the
    functions key in the dictionary item.  There is NO NEED to ever pass the
    nonNegativeDerivative as the function uses the normal derivative_metrics
    information to do that.
    functions are applied in the order in which they are passed e.g.

    target=integral(summarize(metric.3,"10min"))

    function parameters can be passed with the function as well or declared as
    None if there are no parameters required with the function.

    :param current_skyline_app: the app calling the function
    :param metrics_functions: the metric base_names and any functions to apply
    :param from_timestamp: the from unix timestamp
    :param until_timestamp: the until unix timestamp
    :param log: whether to log or not, optional, defaults to True
    :type current_skyline_app: str
    :type metrics_functions: dict
    :type log: boolean
    :return: dictionary of metric timeseries
    :rtype: dict

    """

    metrics_timeseries = {}

    function_str = '%s :: functions.graphite.get_metrics_timeseries' % current_skyline_app
    if log:
        current_skyline_app_logger = current_skyline_app + 'Log'
        current_logger = logging.getLogger(current_skyline_app_logger)
    else:
        current_logger = None

    # graphite URL
    graphite_port = '80'
    if settings.GRAPHITE_PORT != '':
        graphite_port = str(settings.GRAPHITE_PORT)
    if settings.GRAPHITE_PORT == '443' and settings.GRAPHITE_PROTOCOL == 'https':
        graphite_port = ''
    graphite_url = settings.GRAPHITE_PROTOCOL + '://' + settings.GRAPHITE_HOST + ':' + graphite_port + '/' + settings.GRAPHITE_RENDER_URI + '?from=' + str(from_timestamp) + '&until=' + str(until_timestamp) + '&format=json'
    connect_timeout = int(settings.GRAPHITE_CONNECT_TIMEOUT)
    read_timeout = int(settings.GRAPHITE_READ_TIMEOUT)
    read_timeout = 30
    use_timeout = (int(connect_timeout), int(read_timeout))

    try:
        redis_conn_decoded = get_redis_conn_decoded(current_skyline_app)
    except Exception as e:
        if not log:
            current_skyline_app_logger = current_skyline_app + 'Log'
            current_logger = logging.getLogger(current_skyline_app_logger)
        current_logger.error('error :: %s :: failed to connect to Redis - %s' % (
            function_str, e))

    derivative_metrics = []
    try:
        # @modified 20211012 - Feature #4280: aet.metrics_manager.derivative_metrics Redis hash
        # derivative_metrics = list(redis_conn_decoded.smembers('derivative_metrics'))
        derivative_metrics = list(redis_conn_decoded.smembers('aet.metrics_manager.derivative_metrics'))
    except Exception as e:
        if not log:
            current_skyline_app_logger = current_skyline_app + 'Log'
            current_logger = logging.getLogger(current_skyline_app_logger)
        current_logger.error('error :: %s :: failed to connect to Redis for smembers of derivative_metrics - %s' % (
            function_str, e))
        derivative_metrics = []

    # Add nonNegativeDerivative tranform to derivative_metrics and then fetch
    # from Graphite in batches of MAX_GRAPHITE_TARGETS
    get_metrics_with_functions = {}
    for base_name in list(metrics_functions.keys()):
        redis_metric_name = '%s%s' % (settings.FULL_NAMESPACE, base_name)
        if redis_metric_name in derivative_metrics:
            get_metrics_with_functions[base_name] = metrics_functions[base_name]
            original_functions = metrics_functions[base_name]['functions']
            if original_functions is not None:
                functions = {}
                functions['nonNegativeDerivative'] = None
                for function in list(original_functions.keys()):
                    functions[function] = original_functions[function]
            else:
                functions = {'nonNegativeDerivative': None}
            get_metrics_with_functions[base_name]['functions'] = functions
        else:
            get_metrics_with_functions[base_name] = metrics_functions[base_name]

    metrics_list = list(get_metrics_with_functions.keys())

    headers = {'Content-Type': 'application/x-www-form-urlencoded'}
    if metrics_list:
        metrics_to_get = []
        while len(metrics_list) > 0:
            metrics_to_get = []
            post_content = 'format=json&from=%s&until=%s' % (str(from_timestamp,), str(until_timestamp))
            for index, metric in enumerate(metrics_list):
                if len(metrics_to_get) < MAX_GRAPHITE_TARGETS:
                    metrics_to_get.append(metric)
                    metrics_list.pop(index)
                else:
                    break
            for base_name in metrics_to_get:
                functions = get_metrics_with_functions[base_name]['functions']
                target = base_name
                if functions is not None:
                    for function in list(functions.keys()):
                        function_arguments = functions[function]
                        if function_arguments is None:
                            target = '%s(%s)' % (function, target)
                        if isinstance(function_arguments, int):
                            target = '%s(%s,%s)' % (function, target, function_arguments)
                        if isinstance(function_arguments, str):
                            target = '%s(%s,"%s")' % (function, target, function_arguments)
                        if isinstance(function_arguments, dict):
                            target = '%s(%s' % (function, target)
                            for function_parmeter in list(function_arguments.keys()):
                                function_parmeter_value = function_arguments[function_parmeter]
                                if function_parmeter_value is None:
                                    target = str(target)
                                if isinstance(function_parmeter_value, int):
                                    target = '%s,%s' % (target, function_parmeter_value)
                                if isinstance(function_parmeter_value, str):
                                    target = '%s,"%s"' % (target, function_parmeter_value)
                            target = '%s)' % target
                get_metrics_with_functions[base_name]['target'] = target
                post_content = '%s&target=%s' % (post_content, target)

            graphite_json_fetched = False
            try:
                r = requests.post(graphite_url, data=post_content, headers=headers, timeout=use_timeout)
                graphite_json_fetched = True
            except Exception as e:
                if not log:
                    current_skyline_app_logger = current_skyline_app + 'Log'
                    current_logger = logging.getLogger(current_skyline_app_logger)
                current_logger.error('error :: %s :: data retrieval from Graphite failed - %s' % (
                    function_str, e))

            js = {}
            if graphite_json_fetched:
                try:
                    js = r.json()
                except Exception as e:
                    if not log:
                        current_skyline_app_logger = current_skyline_app + 'Log'
                        current_logger = logging.getLogger(current_skyline_app_logger)
                    current_logger.error('error :: %s :: failed to parse retrieved json - %s' % (
                        function_str, e))

            for item in js:
                data_error = None
                timeseries = None
                base_name = None
                try:
                    target = item['target']
                    for metric_base_name in metrics_to_get:
                        if metric_base_name in target:
                            base_name = metric_base_name
                    if not base_name:
                        if not log:
                            current_skyline_app_logger = current_skyline_app + 'Log'
                            current_logger = logging.getLogger(current_skyline_app_logger)
                        current_logger.error('error :: %s :: failed to determine base_name from get_metrics_with_functions[metric_base_name] with target: %s' % (
                            function_str, str(target)))
                        continue
                    datapoints = item['datapoints']
                    converted = []
                    for datapoint in datapoints:
                        try:
                            new_datapoint = [int(datapoint[1]), float(datapoint[0])]
                            converted.append(new_datapoint)
                        except Exception as e:
                            data_error = e
                    timeseries = converted
                except Exception as e:
                    if not log:
                        current_skyline_app_logger = current_skyline_app + 'Log'
                        current_logger = logging.getLogger(current_skyline_app_logger)
                    current_logger.error('error :: %s :: failed to parse data points from retrieved json data_error: %s - %s' % (
                        function_str, str(data_error), e))
                if base_name:
                    metrics_timeseries[base_name] = {}
                    metrics_timeseries[base_name]['functions'] = get_metrics_with_functions[base_name]['functions']
                    metrics_timeseries[base_name]['timeseries'] = None
                if timeseries:
                    metrics_timeseries[base_name]['timeseries'] = timeseries

    return metrics_timeseries
