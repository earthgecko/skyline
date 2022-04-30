"""
set_metrics_as_inactive.py
"""
import logging
import traceback
from time import time

from database import get_engine, engine_disposal, metrics_table_meta
from functions.metrics.get_base_name_from_metric_id import get_base_name_from_metric_id
from functions.database.queries.get_all_db_metric_names import get_all_db_metric_names


# @added 20220228 - Feature #4468: flux - remove_namespace_quota_metrics
#                   Feature #4464: flux - quota - cluster_sync
def set_metrics_as_inactive(current_skyline_app, metric_ids, metrics, dry_run):
    """
    Set all the metrics in the given list as inactive in the metrics table.

    :param current_skyline_app: the app calling the function
    :param metric_ids: a list of metric ids (as in int(id), as ints)
    :param metrics: a list of base_names, optional and will be ignored if
        metric_ids is passed.
    :param dry_run: whether to execute or just report what would have been done
    :type current_skyline_app: str
    :type metric_ids: list
    :type metrics: list
    :type dry_run: boolean
    :return: metrics_set_as_inactive
    :rtype: list

    """
    metrics_set_as_inactive = []
    function_str = 'database.queries.set_metrics_as_inactive'

    current_skyline_app_logger = current_skyline_app + 'Log'
    current_logger = logging.getLogger(current_skyline_app_logger)

    if not dry_run:
        dry_run_str = ''
    else:
        dry_run_str = ':: DRY RUN'

    current_logger.info('%s :: called with %s metric_ids %s' % (
        function_str, str(len(metric_ids)), dry_run_str))
    current_logger.info('%s :: called with %s metrics %s' % (
        function_str, str(len(metrics)), dry_run_str))
    current_logger.info('%s :: metrics: %s' % (
        function_str, str(metrics)))

    metrics_to_set_as_inactive = []
    for metric_id in metric_ids:
        base_name = None
        try:
            base_name = get_base_name_from_metric_id(current_skyline_app, metric_id, False)
        except Exception as err:
            current_logger.error(traceback.format_exc())
            current_logger.error('error :: %s :: base_name_from_metric_id failed - %s' % (
                function_str, (err)))
        if base_name:
            metrics_to_set_as_inactive.append(base_name)

    if not metrics_to_set_as_inactive and metrics:
        base_names = []
        base_names_with_ids = {}
        try:
            with_ids = True
            base_names, base_names_with_ids = get_all_db_metric_names(current_skyline_app, with_ids)
            del base_names
        except Exception as err:
            current_logger.error(traceback.format_exc())
            current_logger.error('error :: metrics_manager :: failed to get_all_active_db_metric_names - %s' % (
                err))
        for base_name in metrics:
            current_logger.info('%s :: determining id of %s' % (
                function_str, base_name))
            metric_id = 0
            try:
                metric_id = base_names_with_ids[base_name]
                current_logger.info('%s :: will set %s with id %s as inactive' % (
                    function_str, base_name, str(metric_id)))
            except KeyError:
                metric_id = 0
            except Exception as err:
                current_logger.error(traceback.format_exc())
                current_logger.error('error :: %s :: failed to get base_name from base_names_with_ids - %s' % (
                    function_str, (err)))
            if metric_id:
                metrics_to_set_as_inactive.append(base_name)
                metric_ids.append(int(metric_id))

    if not metrics_to_set_as_inactive:
        current_logger.warning('warning :: %s :: no metrics to set as inactive %s' % (
            function_str, dry_run_str))
        return metrics_set_as_inactive

    current_logger.info('%s :: setting %s metrics as inactive %s' % (
        function_str, str(len(metric_ids)), dry_run_str))

    try:
        engine, fail_msg, trace = get_engine(current_skyline_app)
    except Exception as err:
        trace = traceback.format_exc()
        current_logger.error(trace)
        fail_msg = 'error :: %s :: could not get a MySQL engine - %s' % (function_str, err)
        current_logger.error('%s' % fail_msg)
        if current_skyline_app == 'webapp':
            # Raise to webapp
            raise
        return []

    try:
        metrics_table, fail_msg, trace = metrics_table_meta(current_skyline_app, engine)
        current_logger.info(fail_msg)
    except Exception as err:
        trace = traceback.format_exc()
        current_logger.error('%s' % trace)
        fail_msg = 'error :: %s :: failed to get metrics_table meta - %s' % (
            function_str, err)
        current_logger.error('%s' % fail_msg)
        if engine:
            engine_disposal(current_skyline_app, engine)
        if current_skyline_app == 'webapp':
            # Raise to webapp
            raise
        return []

    if not dry_run:
        try:
            connection = engine.connect()
            stmt = metrics_table.update().values(
                inactive=1, inactive_at=int(time())).\
                where(metrics_table.c.id.in_(metric_ids))
            connection.execute(stmt)
            metrics_set_as_inactive = list(metrics_to_set_as_inactive)
        except Exception as err:
            current_logger.error(traceback.format_exc())
            current_logger.error('error :: %s :: failed to update metric_table inactive and inactive_at for metric_ids: %s - %s' % (
                function_str, str(metric_ids), str(err)))
            if engine:
                engine_disposal(current_skyline_app, engine)
            if current_skyline_app == 'webapp':
                # Raise to webapp
                raise
            return []
    else:
        metrics_set_as_inactive = list(metrics_to_set_as_inactive)
        current_logger.info('%s :: %s metrics would have been set as inactive %s' % (
            function_str, str(len(metrics_set_as_inactive)), dry_run_str))

    if engine:
        engine_disposal(current_skyline_app, engine)

    current_logger.info('%s :: %s metrics set as inactive %s' % (
        function_str, str(len(metrics_set_as_inactive)), dry_run_str))

    return metrics_to_set_as_inactive
