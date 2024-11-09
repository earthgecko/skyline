"""
get_victoriametrics_metric_link.py
"""
import datetime
import logging
import urllib.parse

from time import time, strftime

import settings
from skyline_functions import get_redis_conn_decoded
from functions.metrics.get_metric_id_from_base_name import get_metric_id_from_base_name

try:
    VICTORIAMETRICS_ENABLED = settings.VICTORIAMETRICS_ENABLED
except:
    VICTORIAMETRICS_ENABLED = False

vmui_url = None
if VICTORIAMETRICS_ENABLED:
    vmui_scheme = None
    try:
        vmui_scheme = settings.VICTORIAMETRICS_OPTS['vmui_scheme']
    except:
        vmui_scheme = None
    vmui_host = None
    try:
        vmui_host = settings.VICTORIAMETRICS_OPTS['vmui_host']
    except:
        vmui_host = None
    vmui_port = None
    try:
        vmui_port = settings.VICTORIAMETRICS_OPTS['vmui_port']
    except:
        vmui_port = None
    if vmui_scheme and vmui_host:
        try:
            base_vmui_url = '%s://%s' % (vmui_scheme, vmui_host)
            vm_url = str(base_vmui_url)
        except:
            base_vmui_url = None
        if base_vmui_url and vmui_port:
            vmui_url = '%s:%s/vmui/#/?g0.' % (base_vmui_url, str(vmui_port))
            vm_url = '%s:%s' % (base_vmui_url, str(vmui_port))

    # Handle cluster select path
    select_path = None
    try:
        select_path = settings.VICTORIAMETRICS_OPTS['select_path']
    except:
        select_path = None
    if select_path and vmui_url:
        if vmui_port:
            vmui_url = '%s://%s:%s%s/vmui/#/?g0.' % (
                vmui_scheme, vmui_host, vmui_port, select_path,
            )
        else:
            vmui_url = '%s://%s:%s/vmui/#/?g0.' % (
                vmui_scheme, vmui_host, select_path,
            )

# In Skyline a metric is either a counter (derivative) or a gauge
skyline_metric_types = {'COUNTER': 1, 'GAUGE': 0}
skyline_metric_types_by_id = {}
for key in list(skyline_metric_types.keys()):
    skyline_metric_types_by_id[skyline_metric_types[key]] = key


# @added 20241108 - Feature #5537: webapp - tsdbs
def get_victoriametrics_url():
    if VICTORIAMETRICS_ENABLED:
        try:
            vm_url = settings.VICTORIAMETRICS_OPTS['public_url']
        except:
            vm_url = vm_url
    return vm_url

# @added 20240118 - Task #2732: Prometheus to Skyline
#                   Branch #4300: prometheus
# Create labelled_metrics link
def get_victoriametrics_metric_link(
    current_skyline_app, metric, from_timestamp, until_timestamp):
    """
    Create a link to the metric graph in VictoriaMetrics vmui

    :param current_skyline_app: the skyline app using this function
    :param metric: metric name
    :param from_timestamp: unix timestamp
    :param until_timestamp: unix timestamp
    :type current_skyline_app: str
    :type metric: str
    :type from_timestamp: int
    :type until_timestamp: int
    :return: metric_graph_url
    :rtype: str

    """

    function_str = 'get_victoriametrics_metric_link'
    current_logger = None
    metric_graph_url = None

    # If this is not a current timeframe graph pad it with a period of data so
    # that to actual data point and time period in question is not right up
    # against the RHS.  This just presents a view that is more useful and shows
    # a few more minutes of data to show what happened during the period in
    # question 
    now = int(time())
    use_until_timestamp = int(until_timestamp)
    if (now - until_timestamp) > 600:
        use_until_timestamp = int(until_timestamp) + 600
    if (now - until_timestamp) > 3600:
        use_until_timestamp = int(until_timestamp) + 3600

    # If the vmui settings are not known return a link to the Skyline webapp
    # /utilities?timeseries_graph page
    if not vmui_url:
        labelled_metric_name = str(metric)
        metric_id = 0
        if '{' in metric and '}' in metric and '_tenant_id="' in metric:
            try:
                metric_id = get_metric_id_from_base_name(current_skyline_app, metric)
            except Exception as err:
                if not current_logger:
                    current_skyline_app_logger = str(current_skyline_app) + 'Log'
                    current_logger = logging.getLogger(current_skyline_app_logger)
                current_logger.error('error :: %s :: get_metric_id_from_base_name failed for %s, err: %s' % (
                    function_str, metric, err))
            if metric_id:
                labelled_metric_name = 'labelled_metrics.%s' % str(metric_id)
        if metric_id:
            try:
                metric_graph_url = '%s/utilities?timeseries_graph=true&metric=%s&from_timestamp=%s&until_timestamp=%s&data_source=victoriametrics&downsample_at=0&downsample_by=mean' % (
                    settings.SKYLINE_URL, labelled_metric_name,
                    str(from_timestamp), str(until_timestamp))
            except Exception as err:
                current_logger.error('error :: %s :: failed to interpolate metric_graph_url, err: %s' % (
                    function_str, err))
                metric_graph_url = False
        return metric_graph_url

    end_input = datetime.datetime.fromtimestamp(int(use_until_timestamp)).strftime('%Y-%m-%dT%H%%3A%M%%3A%S')
    range_seconds = use_until_timestamp - from_timestamp
    # Number of full days
    day = range_seconds // (24 * 3600)
    # Update range_seconds to the remaining seconds after subtracting days
    range_seconds = range_seconds % (24 * 3600)
    # Number of hours in the remaining range_seconds
    hour = range_seconds // 3600
    # Update range_seconds to the remaining seconds after subtracting hours
    range_seconds %= 3600
    range_input = ''
    if day:
        range_input = '%sd' % str(day)
    if hour:
        range_input = '%s%sh' % (range_input, str(hour))

    redis_conn_decoded = None
    try:
        redis_conn_decoded = get_redis_conn_decoded(current_skyline_app)
    except Exception as err:
        current_skyline_app_logger = str(current_skyline_app) + 'Log'
        current_logger = logging.getLogger(current_skyline_app_logger)
        current_logger.error('error :: %s :: get_redis_conn_decoded failed - %s' % (
            function_str, err))

    try:
        metric_id = get_metric_id_from_base_name(current_skyline_app, metric)
    except Exception as err:
        if not current_logger:
            current_skyline_app_logger = str(current_skyline_app) + 'Log'
            current_logger = logging.getLogger(current_skyline_app_logger)
        current_logger.error('error :: %s :: get_metric_id_from_base_name failed for %s, err: %s' % (
            function_str, metric, err))

    metric_type = None
    try:
        skyline_metric_type_str = redis_conn_decoded.hget('skyline.labelled_metrics.id.type', str(metric_id))
        if skyline_metric_type_str:
            skyline_metric_type = int(skyline_metric_type_str)
            metric_type = skyline_metric_types_by_id[skyline_metric_type]
    except Exception as err:
        if not current_logger:
            current_skyline_app_logger = current_skyline_app + 'Log'
            current_logger = logging.getLogger(current_skyline_app_logger)
        current_logger.error('error :: %s :: failed to get %s from skyline.labelled_metrics.id.type - %s' % (
            function_str, str(metric_id), err))

    expr = urllib.parse.quote(metric)
    if metric_type == 'COUNTER':
        rate_metric = 'rate(%s[10m])' % metric
        expr = urllib.parse.quote(rate_metric)
    else:
        expr = urllib.parse.quote(metric)

    step_input = '10m'
    timeseries_duration = int(until_timestamp) - int(from_timestamp)
    if timeseries_duration <= 86401:
        step_input = '60s'

    # http://skyline-test.example.org:8886/vmui/#/?g0.range_input=7d1h&g0.end_input=2024-01-18T10%3A30%3A00&g0.tab=0&g0.relative_time=none&g0.step_input=10m&g0.expr=rate%28process_io_read_bytes_total%7B_tenant_id%3D%221%22%2C_server_id%3D%221%22%2Cinstance%3D%22localhost%3A8428%22%2Cjob%3D%22victoria-metrics%22%7D%29%5B10m%5D
    metric_graph_url = '%srange_input=%s&g0.end_input=%s&g0.tab=0&g0.relative_time=none&g0.step_input=%s&g0.expr=%s' % (
        vmui_url, range_input, end_input, step_input, expr
    )
    return metric_graph_url
