"""
Plot anomalies for a metric
"""
import logging
import traceback
import datetime
import time

from flask import request

import settings

from skyline_functions import filesafe_metricname
from functions.metrics.get_metric_id_from_base_name import get_metric_id_from_base_name
from functions.metrics.get_base_name_from_labelled_metrics_name import get_base_name_from_labelled_metrics_name
from functions.graphite.get_metrics_timeseries import get_metrics_timeseries
from functions.victoriametrics.get_victoriametrics_metric import get_victoriametrics_metric
from functions.redis.get_metric_timeseries import get_metric_timeseries
from functions.plots.plot_timeseries import plot_timeseries
from functions.timeseries.determine_data_frequency import determine_data_frequency
from functions.timeseries.downsample import downsample_timeseries

skyline_app = 'webapp'
skyline_app_logger = '%sLog' % skyline_app
logger = logging.getLogger(skyline_app_logger)
skyline_app_logfile = '%s/%s.log' % (settings.LOG_PATH, skyline_app)
logfile = '%s/%s.log' % (settings.LOG_PATH, skyline_app)


def timeseries_graph():
    """
    Create a plot of the metric and return the path and filename

    :param metric: the name of the metric
    :param from_timestamp: the from timestamp
    :param until_timestamp: the until timestamp
    :param data_source: what data_source to use
    :type metric: str
    :type from_timestamp: int
    :type until_timestamp: int
    :type data_source: str
    :return: path and file
    :rtype:  str

    """

    function_str = 'timeseries_graph'
    graph_image = None

    metric = None
    try:
        metric = request.args.get('metric')
        logger.info('%s :: with metric: %s' % (function_str, str(metric)))
    except Exception as err:
        logger.error('%s :: no metric passed - %s' % (function_str, err))
        raise

    try:
        from_timestamp = request.args.get('from_timestamp')
        logger.info('%s :: with from_timestamp: %s' % (function_str, str(from_timestamp)))
    except Exception as err:
        logger.error('%s :: no from_timestamp passed - %s' % (function_str, err))
        raise

    if ":" in from_timestamp:
        try:
            from_timestamp = int(time.mktime(datetime.datetime.strptime(from_timestamp, '%Y-%m-%d %H:%M').timetuple()))
        except Exception as err:
            logger.error('%s :: failed to determine from_timestamp from %s - %s' % (
                function_str, str(from_timestamp), err))
            raise

    try:
        until_timestamp = request.args.get('until_timestamp')
        logger.info('%s :: with until_timestamp: %s' % (function_str, str(until_timestamp)))
    except Exception as err:
        logger.error('%s :: no until_timestamp passed - %s' % (function_str, err))
        raise

    if ":" in until_timestamp:
        try:
            until_timestamp = int(time.mktime(datetime.datetime.strptime(until_timestamp, '%Y-%m-%d %H:%M').timetuple()))
        except Exception as err:
            logger.error('%s :: failed to determine until_timestamp from %s - %s' % (
                function_str, str(until_timestamp), err))
            raise

    try:
        data_source = request.args.get('data_source')
        logger.info('%s :: with data_source: %s' % (function_str, str(data_source)))
    except Exception as err:
        logger.error('%s :: no data_source passed - %s' % (function_str, err))
        raise

    downsample_at = 0
    try:
        downsample_at_str = request.args.get('downsample_at')
        if downsample_at_str:
            downsample_at = int(float(downsample_at_str))
        logger.info('%s :: with downsample_at: %s' % (function_str, str(downsample_at)))
    except Exception as err:
        downsample_at = 0
    downsample_by = 'mean'
    try:
        downsample_by = request.args.get('downsample_by')
        logger.info('%s :: with downsample_by: %s' % (function_str, str(downsample_by)))
    except Exception as err:
        downsample_by = 'mean'

    logger.info('%s - metric: %s, from_timestamp: %s, until_timestamp: %s, data_source: %s, downsample_at: %s, downsample_by: %s' % (
        function_str, str(metric), str(from_timestamp),
        str(until_timestamp), str(data_source), str(downsample_at),
        str(downsample_by)))

    labelled_metric_name = None
    labelled_metric_base_name = None
    use_metric = str(metric)
    file_metric_name = str(metric)
    if metric.startswith('labelled_metrics.'):
        if data_source == 'graphite':
            logger.error('%s :: data_source passed was graphite, graphite is not a data_source for labelled_metrics' % function_str)
            return graph_image
        labelled_metric_name = str(metric)
        try:
            metric_name = get_base_name_from_labelled_metrics_name(skyline_app, metric)
            if metric_name:
                labelled_metric_base_name = str(metric_name)
        except Exception as err:
            logger.error('error :: %s :: get_base_name_from_labelled_metrics_name failed for %s - %s' % (
                function_str, metric, err))
            raise
    if '_tenant_id="' in metric:
        labelled_metric_base_name = str(metric)
        try:
            metric_id = get_metric_id_from_base_name(skyline_app, metric)
            if metric_id:
                file_metric_name = 'labelled_metrics.%s' % str(metric_id)
                use_metric = str(file_metric_name)
        except Exception as err:
            logger.error('error :: %s :: get_metric_id_from_base_name failed for %s - %s' % (
                function_str, metric, err))
            raise

    if not until_timestamp:
        until_timestamp = int(time.time())
        logger.info('%s :: no until_timestamp set, using until_timestamp: %s' % (function_str, str(until_timestamp)))
    if not from_timestamp:
        from_timestamp = int(until_timestamp - settings.FULL_DURATION)
        logger.info('%s :: no from_timestamp set, using from_timestamp: %s' % (function_str, str(from_timestamp)))

    timeseries = []
    if data_source == 'graphite':
        metrics_functions = {}
        metrics_functions[metric] = {}
        metrics_functions[metric]['functions'] = None
        try:
            metrics_timeseries = get_metrics_timeseries(skyline_app, metrics_functions, int(from_timestamp), int(until_timestamp), log=False)
        except Exception as err:
            logger.error(traceback.format_exc())
            logger.error('error :: %s :: get_metrics_timeseries failed - %s' % (
                function_str, err))
            raise
        try:
            timeseries = metrics_timeseries[metric]['timeseries']
            # Truncate the first and last timestamp, just in case they are not
            # filled buckets
            timeseries = timeseries[1:-1]
        except Exception as err:
            logger.error(traceback.format_exc())
            logger.error('error :: %s :: failed retrieve timeseries from Graphite for %s - %s' % (
                function_str, metric, err))
            raise

    if data_source == 'victoriametrics':
        try:
            # get_victoriametrics_metric automatically applies the rate and
            # step required no downsampling or nonNegativeDerivative is
            # required.
            timeseries = get_victoriametrics_metric(
                skyline_app, labelled_metric_base_name, int(from_timestamp), int(until_timestamp),
                'list', 'object')
        except Exception as err:
            logger.error('error :: %s :: get_victoriametrics_metric failed - %s' % (
                function_str, err))
            raise

    if data_source == 'redis':
        try:
            timeseries = get_metric_timeseries(skyline_app, metric, int(from_timestamp), int(until_timestamp))
        except Exception as err:
            logger.error('error :: %s :: get_metric_timeseries failed - %s' % (
                function_str, err))
            raise

    if downsample_at and downsample_by:
        logger.info('%s :: downsampling at %s seconds by %s - timeseries length before downsampling: %s' % (
            function_str, str(downsample_at), str(downsample_by),
            str(len(timeseries))))
        try:

            resolution = determine_data_frequency(skyline_app, timeseries)
            logger.info('%s :: determined resolution: %s' % (
                function_str, str(resolution)))
            timeseries = downsample_timeseries(
                skyline_app, timeseries, resolution,
                downsample_at, downsample_by, 'end')
        except Exception as err:
            logger.error('error :: %s :: failed to downsample timeseries - %s' % (
                function_str, err))
            raise
        logger.info('%s :: timeseries length after downsampling: %s' % (
            function_str, str(len(timeseries))))

    sane_metricname = filesafe_metricname(file_metric_name)
    save_to_file = '%s/timeseries_graph.%s.%s.%s.png' % (
        settings.SKYLINE_TMP_DIR, sane_metricname, str(from_timestamp),
        str(until_timestamp))

    start_date = time.strftime('%Y-%m-%d %H:%M:%S %Z', time.localtime(int(from_timestamp)))
    end_date = time.strftime('%Y-%m-%d %H:%M:%S %Z', time.localtime(int(until_timestamp)))

    title = '%s\n%s - %s to %s' % (str(use_metric), data_source, start_date, end_date)
    plot_parameters = {
        'title': title, 'line_color': 'blue', 'bg_color': 'black',
        'figsize': (8, 4)
    }
    try:
        success, graph_image = plot_timeseries(
            skyline_app, metric, timeseries, save_to_file,
            plot_parameters)
        if success:
            logger.info('%s :: plotted %s' % (function_str, str(graph_image)))
    except Exception as err:
        logger.error('error :: %s :: plot_timeseries failed - %s' % (
            function_str, err))
        raise

    return graph_image
