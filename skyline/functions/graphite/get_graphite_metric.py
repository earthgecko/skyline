"""
get_graphite_metric
"""
import logging
import traceback
import os
from sys import version_info
import time
import datetime
import re
import json

from requests.utils import quote
import requests

import settings
from skyline_functions import (
    get_redis_conn_decoded, sanitise_graphite_url, mkdir_p,
    get_graphite_graph_image,
)

# @added 20220406 - Feature #4518: settings - LAST_KNOWN_VALUE_NAMESPACES
#                   Feature #4520: settings - ZERO_FILL_NAMESPACES
from functions.metrics.last_known_value_metrics_list import last_known_value_metrics_list
from functions.metrics.zero_fill_metrics_list import zero_fill_metrics_list

try:
    IONOSPHERE_GRAPHITE_NOW_GRAPHS_OVERRIDE = settings.IONOSPHERE_GRAPHITE_NOW_GRAPHS_OVERRIDE
except:
    IONOSPHERE_GRAPHITE_NOW_GRAPHS_OVERRIDE = False

python_version = int(version_info[0])


def get_graphite_metric(
    current_skyline_app, metric, from_timestamp, until_timestamp, data_type,
        output_object):
    """
    Fetch data from graphite and return it as object or save it as file

    :param current_skyline_app: the skyline app using this function
    :param metric: metric name
    :param from_timestamp: unix timestamp
    :param until_timestamp: unix timestamp
    :param data_type: image, json or list
    :param output_object: object or path and filename to save data as, if set to
                          object, the object is returned
    :type current_skyline_app: str
    :type metric: str
    :type from_timestamp: str
    :type until_timestamp: str
    :type data_type: str
    :type output_object: str
    :return: timeseries string, ``True``, ``False``
    :rtype: str or boolean

    """

    # @added 20191025 - Task #3290: Handle urllib2 in py3
    #                   Branch #3262: py3
#    try:
#        urllib.urlretrieve
#    except:
#        try:
#            import urllib as urllib
#        except:
#            import urllib.request
#            import urllib.error

    # @aded 20210309 - Feature #3978: luminosity - classify_metrics
    #                  Feature #3642: Anomaly type classification
    # Do not log luminosity classify_metrics calls unless error
    write_to_log = True
    if current_skyline_app == 'luminosity' and data_type == 'list' and output_object == 'object':
        write_to_log = False

    current_skyline_app_logger = str(current_skyline_app) + 'Log'
    current_logger = logging.getLogger(current_skyline_app_logger)

#    if settings.ENABLE_DEBUG:
    if write_to_log:
        current_logger.info('get_graphite_metric :: graphite_metric - %s' % (metric))

    # @added 20160803 - Unescaped Graphite target - https://github.com/earthgecko/skyline/issues/20
    #                   bug1546: Unescaped Graphite target
    # @modified 20191029 - Branch #3263: py3
    # Commented out colon
    # new_metric_namespace = metric.replace(':', '\:')
    # metric_namespace = new_metric_namespace.replace('(', '\(')
    metric_namespace = metric.replace('(', '\(')
    metric = metric_namespace.replace(')', '\)')

    # Graphite timeouts
    connect_timeout = int(settings.GRAPHITE_CONNECT_TIMEOUT)
    if settings.ENABLE_DEBUG:
        current_logger.debug('debug :: get_graphite_metric :: connect_timeout - %s' % str(connect_timeout))

    read_timeout = int(settings.GRAPHITE_READ_TIMEOUT)
    if settings.ENABLE_DEBUG:
        current_logger.debug('debug :: get_graphite_metric :: read_timeout - %s' % str(read_timeout))

    graphite_from = datetime.datetime.fromtimestamp(int(from_timestamp)).strftime('%H:%M_%Y%m%d')
    if write_to_log:
        current_logger.info('get_graphite_metric :: graphite_from - %s' % str(graphite_from))

    graphite_until = datetime.datetime.fromtimestamp(int(until_timestamp)).strftime('%H:%M_%Y%m%d')
    if write_to_log:
        current_logger.info('get_graphite_metric :: graphite_until - %s' % str(graphite_until))

    output_format = data_type

    # @modified 20170603 - Feature #2034: analyse_derivatives
    # Added deriative functions to convert the values of metrics strictly
    # increasing monotonically to their deriative products in Graphite now
    # graphs
    known_derivative_metric = False
    REDIS_CONN_DECODED = None
    try:
        REDIS_CONN_DECODED = get_redis_conn_decoded(current_skyline_app)
    except:
        current_logger.error(traceback.format_exc())
        current_logger.error('error :: get_graphite_metric - failed to get_redis_conn_decoded')

    try:
        # @modified 20211012 - Feature #4280: aet.metrics_manager.derivative_metrics Redis hash
        # derivative_metrics = list(REDIS_CONN_DECODED.smembers('derivative_metrics'))
        derivative_metrics = list(REDIS_CONN_DECODED.smembers('aet.metrics_manager.derivative_metrics'))
    except:
        derivative_metrics = []
    redis_metric_name = '%s%s' % (settings.FULL_NAMESPACE, str(metric))

    # @added 20180423 - Feature #2034: analyse_derivatives
    #                   Branch #2270: luminosity
    metric_found_in_redis = False

    if redis_metric_name in derivative_metrics:
        known_derivative_metric = True
        metric_found_in_redis = True
    if known_derivative_metric:
        try:
            non_derivative_metrics = list(REDIS_CONN_DECODED.smembers('non_derivative_metrics'))
        except:
            non_derivative_metrics = []
        if redis_metric_name in non_derivative_metrics:
            known_derivative_metric = False
            metric_found_in_redis = True

    # @added 20180423 - Feature #2034: analyse_derivatives
    #                   Branch #2270: luminosity
    if not metric_found_in_redis and settings.OTHER_SKYLINE_REDIS_INSTANCES:
        # @modified 20180519 - Feature #2378: Add redis auth to Skyline and rebrow
        # for redis_ip, redis_port in settings.OTHER_SKYLINE_REDIS_INSTANCES:
        for redis_ip, redis_port, redis_password in settings.OTHER_SKYLINE_REDIS_INSTANCES:
            if not metric_found_in_redis:
                try:
                    if redis_password:
                        other_redis_conn = StrictRedis(host=str(redis_ip), port=int(redis_port), password=str(redis_password))
                    else:
                        other_redis_conn = StrictRedis(host=str(redis_ip), port=int(redis_port))
                    other_derivative_metrics = list(other_redis_conn.smembers('derivative_metrics'))
                except:
                    current_logger.error(traceback.format_exc())
                    current_logger.error('error :: get_graphite_metric :: failed to connect to Redis at %s on port %s' % (str(redis_ip), str(redis_port)))
                if redis_metric_name in other_derivative_metrics:
                    known_derivative_metric = True
                    metric_found_in_redis = True
                    if write_to_log:
                        current_logger.info('get_graphite_metric :: %s found in derivative_metrics in Redis at %s on port %s' % (redis_metric_name, str(redis_ip), str(redis_port)))

    target_metric = metric
    if known_derivative_metric:
        target_metric = 'nonNegativeDerivative(%s)' % str(metric)

    # @added 20220406 - Feature #4518: settings - LAST_KNOWN_VALUE_NAMESPACES
    #                   Feature #4520: settings - ZERO_FILL_NAMESPACES
    last_known_value_metrics = []
    try:
        last_known_value_metrics = last_known_value_metrics_list(current_skyline_app)
    except Exception as err:
        current_logger.error(traceback.format_exc())
        current_logger.error('error :: get_graphite_metric :: last_known_value_metrics_list failed - %s' % err)
    zero_fill_metrics = []
    try:
        zero_fill_metrics = zero_fill_metrics_list(current_skyline_app)
    except Exception as err:
        current_logger.error(traceback.format_exc())
        current_logger.error('error :: get_graphite_metric :: zero_fill_metrics_list failed - %s' % err)
    # Do not zero fill derivative metrics, if the use has defined a metric as a
    # zero fill metric and it is a derivative metric do not break the metric
    # by applying zero filling, rather correct it and apply last_known_value
    if metric in zero_fill_metrics:
        if not known_derivative_metric:
            target_metric = 'transformNull(%s,0)' % str(target_metric)
        else:
            # Fix the badly defined metric
            target_metric = 'keepLastValue(%s)' % str(target_metric)
    if metric in last_known_value_metrics:
        target_metric = 'keepLastValue(%s)' % str(target_metric)

    # graphite URL
    graphite_port = '80'
    if settings.GRAPHITE_PORT != '':
        graphite_port = str(settings.GRAPHITE_PORT)
    # @added 20191002 - Branch #3002: docker
    # Unset the graphite_port if normal https
    if settings.GRAPHITE_PORT == '443' and settings.GRAPHITE_PROTOCOL == 'https':
        graphite_port = ''

    # @modified 20190520 - Branch #3002: docker
    # image_url = settings.GRAPHITE_PROTOCOL + '://' + settings.GRAPHITE_HOST + ':' + graphite_port + '/render?from=' + graphite_from + '&until=' + graphite_until + '&target=' + target_metric
    # image_url = settings.GRAPHITE_PROTOCOL + '://' + settings.GRAPHITE_HOST + ':' + graphite_port + '/api/datasources/proxy/1/render/?from=' + graphite_from + '&until=' + graphite_until + '&target=' + target_metric
    image_url = settings.GRAPHITE_PROTOCOL + '://' + settings.GRAPHITE_HOST + ':' + graphite_port + '/' + settings.GRAPHITE_RENDER_URI + '?from=' + graphite_from + '&until=' + graphite_until + '&target=' + target_metric

    # @modified 20201125 - Feature #3850: webapp - yhat_values API endoint
    # Added list as data_type
    # if data_type == 'json':
    if data_type in ['json', 'list']:
        url = image_url + '&format=json'
    else:
        url = image_url + '&format=' + output_format

    if write_to_log:
        current_logger.info('get_graphite_metric :: graphite url - %s' % (url))

    get_image = False
    if data_type == 'image' and output_object != 'object':
        if not os.path.isfile(output_object):
            get_image = True
        else:
            if settings.ENABLE_DEBUG:
                current_logger.debug('debug :: get_graphite_metric :: graph file exists - %s' % str(output_object))
            return True

    if get_image:
        if write_to_log:
            current_logger.info(
                'get_graphite_metric :: retrieving png - surfacing %s graph from graphite from %s to %s' % (
                    metric, str(graphite_from), str(graphite_until)))

        graphite_image_file = output_object
        if 'width' not in image_url:
            image_url += '&width=586'
        if 'height' not in image_url:
            image_url += '&height=308'
        # @added 20170106 - Feature #1842: Ionosphere - Graphite now graphs
        # settings, color and title
        if str(current_skyline_app) == 'webapp':
            get_ionosphere_graphs = False
            if 'graphite_now' in output_object:
                get_ionosphere_graphs = True
            # @added 20170107 - Feature #1852: Ionosphere - features_profile matched graphite graphs
            # Added graphite_matched_images matched.fp_id
            if 'matched.fp_id' in output_object:
                get_ionosphere_graphs = True
            # @added 20170308 - Feature #1960: ionosphere_layers
            # Added graphite_layers_matched_images matched.layer
            if 'matched.layers.fp_id' in output_object:
                get_ionosphere_graphs = True
            if get_ionosphere_graphs:
                int_hours = int((int(until_timestamp) - int(from_timestamp)) / 60 / 60)
                if 'graphite_now' in output_object:
                    no_extension = os.path.splitext(output_object)[0]
                    _hours = os.path.splitext(no_extension)[1]
                    hours = _hours.replace('.', '')
                    int_hours = hours.replace('h', '')
                str_value = str(int_hours)
                period = 'hours'
                if int(int_hours) > 24:
                    str_value = str(int(int_hours) / 24)
                    period = 'days'
                if 'graphite_now' in output_object:
                    # @added 20200731 - Feature #3654: IONOSPHERE_GRAPHITE_NOW_GRAPHS_OVERRIDE
                    if IONOSPHERE_GRAPHITE_NOW_GRAPHS_OVERRIDE:
                        unencoded_graph_title = 'Graphite THEN at %s %s' % (str_value, period)
                    else:
                        unencoded_graph_title = 'Graphite NOW at %s %s' % (str_value, period)
                # @added 20170308 - Feature #1960: ionosphere_layers
                matched_graph = False
                if 'matched.fp_id' in output_object:
                    matched_graph = True
                if 'matched.layers.fp_id' in output_object:
                    matched_graph = True
                # @modified 20170308 - Feature #1960: ionosphere_layers
                # if 'matched.fp_id' in output_object:
                if matched_graph:
                    human_date = time.strftime('%Y-%m-%d %H:%M:%S %Z', time.localtime(int(until_timestamp)))
                    if 'matched.fp_id' in output_object:
                        tail_piece = re.sub('.*\.fp_id-', '', output_object)
                        matched_fp_id = re.sub('\..*', '', tail_piece)
                        unencoded_graph_title = 'fp_id %s matched - %s at %s hours' % (
                            str(matched_fp_id), str(human_date),
                            str(int_hours))
                        # @added 20210421 - Feature #4014: Ionosphere - inference
                        # Add motif_id to Graph title
                        if 'motif_id-' in output_object:
                            tail_piece = re.sub('.*\.motif_id-', '', output_object)
                            motif_id = re.sub('\..*', '', tail_piece)
                            unencoded_graph_title = 'fp_id %s (motif_id %s) matched - %s for the trailing period' % (
                                str(matched_fp_id), str(motif_id), str(human_date))

                    if 'matched.layers.fp_id' in output_object:
                        # layers_id
                        tail_piece = re.sub('.*\.layers_id-', '', output_object)
                        matched_layers_id = re.sub('\..*', '', tail_piece)
                        unencoded_graph_title = 'layers_id %s matched - %s at %s hours' % (
                            str(matched_layers_id), str(human_date),
                            str(int_hours))
                graph_title_string = quote(unencoded_graph_title, safe='')
                graph_title = '&title=%s' % graph_title_string
                add_parameters = '%s&colorList=blue%s' % (settings.GRAPHITE_GRAPH_SETTINGS, graph_title)
                if 'matched.layers.fp_id' in output_object:
                    add_parameters = '%s&colorList=green%s' % (settings.GRAPHITE_GRAPH_SETTINGS, graph_title)
                image_url += add_parameters
                if write_to_log:
                    current_logger.info('get_graphite_metric :: Ionosphere graphite NOW url - %s' % (image_url))

        if settings.ENABLE_DEBUG:
            current_logger.debug('debug :: get_graphite_metric :: graphite image url - %s' % (str(image_url)))
        # image_url_timeout = int(connect_timeout)

        image_data = None

        # @modified 20191025 - Task #3290: Handle urllib2 in py3
        #                      Branch #3262: py3
        # Use urlretrieve
        # try:
        #     # @modified 20170913 - Task #2160: Test skyline with bandit
        #     # Added nosec to exclude from bandit tests
        #     image_data = urllib2.urlopen(image_url, timeout=image_url_timeout).read()  # nosec
        #     current_logger.info('url OK - %s' % (image_url))
        # except urllib2.URLError:
        #     image_data = None
        #     current_logger.error('error :: url bad - %s' % (image_url))

        # @added 20201009 - Feature #3780: skyline_functions - sanitise_graphite_url
        #                   Bug #3778: Handle single encoded forward slash requests to Graphite
        sanitised = False
        sanitised, image_url = sanitise_graphite_url(current_skyline_app, image_url)

        # @added 20191025 - Task #3290: Handle urllib2 in py3
        #                   Branch #3262: py3
        # Use urlretrieve
        try:
            image_data = get_graphite_graph_image(
                current_skyline_app, image_url, graphite_image_file)
        except Exception as e:
            current_logger.error(traceback.format_exc())
            fail_msg = 'error :: %s :: get_graphite_metric :: failed to get_graphite_graph_image(%s, %s) - %s' % (
                str(current_skyline_app), str(url), str(graphite_image_file), e)
            current_logger.error(fail_msg)

        # if image_data is not None:
        if image_data == 'disabled_for_testing':
            with open(graphite_image_file, 'w') as f:
                f.write(image_data)
            if write_to_log:
                current_logger.info('get_graphite_metric :: retrieved - %s' % (graphite_image_file))
            if python_version == 2:
                os.chmod(graphite_image_file, 0o644)
            if python_version == 3:
                os.chmod(graphite_image_file, mode=0o644)
        # else:
        #    current_logger.error(
        #         'error :: failed to retrieve - %s' % (graphite_image_file))

        if not os.path.isfile(graphite_image_file):
            msg = 'get_graphite_metric :: retrieve failed to surface %s graph from Graphite' % (metric)
            current_logger.error('error :: %s' % msg)
        # @added 20170107 - Feature #1842: Ionosphere - Graphite now graphs
        # In order to determine whether a Graphite image was retrieved or not
        # this needs to return here.  This should not be backwards incompatible
        # as it has never been used to determine the outcome before it appears,
        # which as they say is my bad.
            return False
        else:
            return True

    # @modified 20201125 - Feature #3850: webapp - yhat_values API endoint
    # Added list as data_type
    if data_type in ['json', 'list']:

        if output_object != 'object':
            if os.path.isfile(output_object):
                return True

        if write_to_log:
            msg = 'get_graphite_metric :: surfacing timeseries data for %s from graphite from %s to %s' % (
                metric, graphite_from, graphite_until)
            current_logger.info('%s' % msg)
        if requests.__version__ >= '2.4.0':
            use_timeout = (int(connect_timeout), int(read_timeout))
        else:
            use_timeout = int(connect_timeout)
        if settings.ENABLE_DEBUG:
            current_logger.debug('debug :: get_graphite_metric :: use_timeout - %s' % (str(use_timeout)))

        # @added 20201009 - Feature #3780: skyline_functions - sanitise_graphite_url
        #                   Bug #3778: Handle single encoded forward slash requests to Graphite
        sanitised = False
        sanitised, url = sanitise_graphite_url(current_skyline_app, url)

        graphite_json_fetched = False
        try:
            r = requests.get(url, timeout=use_timeout)
            graphite_json_fetched = True
        except:
            datapoints = [[None, str(graphite_until)]]
            current_logger.error('error :: get_graphite_metric :: data retrieval from Graphite failed')

        if graphite_json_fetched:
            try:
                js = r.json()
                datapoints = js[0]['datapoints']
                if settings.ENABLE_DEBUG:
                    current_logger.debug('debug :: get_graphite_metric :: data retrieved OK')
            except:
                datapoints = [[None, str(graphite_until)]]
                current_logger.error('error :: get_graphite_metric :: failed to parse data points from retrieved json')

        converted = []
        for datapoint in datapoints:
            try:
                new_datapoint = [float(datapoint[1]), float(datapoint[0])]
                converted.append(new_datapoint)
            # @modified 20170913 - Task #2160: Test skyline with bandit
            # Added nosec to exclude from bandit tests
            except:  # nosec
                continue

        if output_object != 'object':

            # @added 20220406 - Feature #4518: settings - LAST_KNOWN_VALUE_NAMESPACES
            #                   Feature #4520: settings - ZERO_FILL_NAMESPACES
            # Add the mkdir_p function to mimic self.surface_graphite_metric_data
            # in mirage so that the same function can be used for all graphite
            # requests
            output_object_path = os.path.dirname(output_object)
            if not os.path.isdir(output_object_path):
                try:
                    mkdir_p(output_object_path)
                    current_logger.info(
                        'output_object_path - %s' % str(output_object_path))
                except Exception as err:
                    current_logger.error(traceback.format_exc())
                    current_logger.error(
                        'error :: failed to create output_object_path - %s - %s' %
                        str(output_object_path))
                    return False

            with open(output_object, 'w') as f:
                f.write(json.dumps(converted))
            if python_version == 2:
                os.chmod(output_object, 0o644)
            if python_version == 3:
                os.chmod(output_object, mode=0o644)
            if settings.ENABLE_DEBUG:
                current_logger.debug('debug :: get_graphite_metric :: json file - %s' % output_object)
        else:
            if data_type == 'list':
                return converted
            if data_type == 'json':
                timeseries_json = json.dumps(converted)
                return timeseries_json

        if not os.path.isfile(output_object):
            current_logger.error(
                'error :: get_graphite_metric :: failed to surface %s json from graphite' % (metric))
            return False
        else:
            return True

    return True
