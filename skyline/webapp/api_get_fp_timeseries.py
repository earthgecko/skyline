"""
api_get_fp_timeseries.py
"""
import logging
# @added 20220722 - Task #4624: Change all dict copy to deepcopy
import copy

from flask import request

from functions.database.queries.get_ionosphere_fp_row import get_ionosphere_fp_db_row
from functions.metrics.get_base_name_from_metric_id import get_base_name_from_metric_id
from functions.metrics.get_metric_id_from_base_name import get_metric_id_from_base_name
from functions.database.queries.fp_timeseries import get_db_fp_timeseries

# @added 20221020 - Feature #4578: webapp - api_get_fp_timeseries
# Allow for a metric and timestamp to be passed only and look up the metric
# and fp ids
from functions.database.queries.get_fps_for_metric import get_fps_for_metric


# @added 20220517 - Feature #4578: webapp - api_get_fp_timeseries
def api_get_fp_timeseries(current_skyline_app):
    """
    Return a dict with the metric and fp timeseries.

    :param current_skyline_app: the app calling the function
    :type current_skyline_app: str
    :return: fp_timeseries_dict
    :rtype: dict

    """
    fp_timeseries_dict = {}

    current_skyline_app_logger = current_skyline_app + 'Log'
    current_logger = logging.getLogger(current_skyline_app_logger)

    fp_id = None
    base_name = None
    metric_id = 0
    from_timestamp = 0
    until_timestamp = 0
    duration = 0

    # @added 20221020 - Feature #4578: webapp - api_get_fp_timeseries
    # Allow for a metric and timestamp to be passed only and look up the metric
    # and fp ids and handle labelled_metrics being passed
    metric = None
    if 'fp_id' not in request.args:
        if 'metric' in request.args:
            try:
                base_name = request.args.get('metric')
                metric = str(base_name)
            except Exception as err:
                current_logger.info('api_get_fp_timeseries request no metric argument passed - %s' % err)
        if base_name.startswith('labelled_metrics.'):
            metric = str(base_name)
            metric_id_str = metric.replace('labelled_metrics.', '', 1)
            try:
                base_name = get_base_name_from_metric_id(current_skyline_app, int(metric_id_str))
                metric_id = int(metric_id_str)
            except Exception as err:
                current_logger.info('api_get_fp_timeseries get_base_name_from_metric_id failed for - %s' % err)
            current_logger.info('api_get_fp_timeseries :: labelled metric - determined metric_id: %s, base_name: %s' % (
                str(metric_id), str(base_name)))
        if base_name and 'from_timestamp' in request.args and 'until_timestamp' in request.args:
            try:
                from_timestamp = int(request.args.get('from_timestamp'))
            except Exception as err:
                current_logger.info('api_get_fp_timeseries request no valid from_timestamp argument passed - %s' % err)
            try:
                until_timestamp = int(request.args.get('until_timestamp'))
            except Exception as err:
                current_logger.info('api_get_fp_timeseries request no valid until_timestamp argument passed - %s' % err)
        if base_name and from_timestamp and until_timestamp:
            if not metric_id:
                current_logger.info('/api?api_get_fp_timeseries with metric: %s' % str(base_name))
                try:
                    metric_id = get_metric_id_from_base_name(current_skyline_app, base_name)
                except Exception as err:
                    current_logger.err('error :: api_get_fp_timeseries :: get_metric_id_from_base_name failed with base_name %s - %s' % (
                        str(base_name), err))
                current_logger.info('api_get_fp_timeseries :: determined metric_id - %s' % str(metric_id))
            if not base_name:
                try:
                    base_name = get_base_name_from_metric_id(current_skyline_app, metric_id)
                except Exception as err:
                    current_logger.err('error :: api_get_fp_timeseries :: get_base_name_from_metric_id failed with metric_id %s - %s' % (
                        str(metric_id), err))
            duration = until_timestamp - from_timestamp
        if metric_id and until_timestamp and duration:
            fps_dict = {}
            try:
                fps_dict = get_fps_for_metric(current_skyline_app, metric_id)
            except Exception as err:
                current_logger.error('api_get_fp_timeseries :: %s :: get_fps_for_metric failed - %s' % (
                    str(err)))
            for i_fp_id in fps_dict.keys():
                if fps_dict[i_fp_id]['anomaly_timestamp'] == until_timestamp:
                    if fps_dict[i_fp_id]['full_duration'] == duration:
                        fp_id = i_fp_id
                        current_logger.info('/api?api_get_fp_timeseries determine fp_id: %s for metric_id: %s, timestamp: %s, duration: %s' % (
                            str(fp_id), str(metric_id), str(until_timestamp),
                            str(duration)))
                        break

    if not fp_id:
        try:
            fp_id = int(request.args.get('fp_id'))
            current_logger.info('/api?api_get_fp_timeseries with fp_id: %s' % str(fp_id))
        except Exception as err:
            current_logger.error('error :: api api_get_fp_timeseries request no fp_id argument (or metric and timestamp) - %s' % err)
            # return 400
            return fp_timeseries_dict

    if not base_name:
        try:
            base_name = request.args.get('metric')
        except:
            current_logger.info('api_get_fp_timeseries request no metric argument passed')

    current_logger.info('/api?api_get_fp_timeseries getting fp_id_row')
    fp_id_row = {}
    try:
        fp_id_row = get_ionosphere_fp_db_row(current_skyline_app, fp_id)
    except Exception as err:
        current_logger.err('error :: api_get_fp_timeseries :: get_ionosphere_fp_db_row failed - %s' % err)
    if fp_id_row:
        try:
            metric_id = fp_id_row['metric_id']
            current_logger.info('api_get_fp_timeseries :: determined metric_id as %s' % str(metric_id))
        except Exception as err:
            current_logger.err('error :: api_get_fp_timeseries :: failed to determine metric_id from fp_id_row - %s' % err)

    if not base_name:
        if metric_id:
            try:
                base_name = get_base_name_from_metric_id(current_skyline_app, metric_id)
            except Exception as err:
                current_logger.err('error :: api_get_fp_timeseries :: get_base_name_from_metric_id failed with metric_id %s - %s' % (
                    str(metric_id), err))
        current_logger.info('api_get_fp_timeseries :: determined metric - %s' % str(base_name))
    else:
        current_logger.info('/api?api_get_fp_timeseries with metric: %s' % str(base_name))
        try:
            metric_id = get_metric_id_from_base_name(current_skyline_app, base_name)
        except Exception as err:
            current_logger.err('error :: api_get_fp_timeseries :: get_metric_id_from_base_name failed with base_name %s - %s' % (
                str(base_name), err))
        current_logger.info('api_get_fp_timeseries :: determined metric_id - %s' % str(metric_id))

    try:
        fp_timeseries = get_db_fp_timeseries(current_skyline_app, metric_id, fp_id)
    except Exception as err:
        current_logger.error('error :: api_get_fp_timeseries :: get_db_fp_timeseries failed - %s' % (
            err))
    if fp_timeseries:
        # @modified 20220722 - Task #4624: Change all dict copy to deepcopy
        # fp_timeseries_dict = fp_id_row.copy()
        fp_timeseries_dict = copy.deepcopy(fp_id_row)
        fp_timeseries_dict['fp_id'] = fp_id
        fp_timeseries_dict['metric'] = base_name
        fp_timeseries_dict['timeseries'] = fp_timeseries
    else:
        current_logger.error('error :: api_get_fp_timeseries :: failed to get timeseries for fp_id %s' % (
            str(fp_id)))

    return fp_timeseries_dict
