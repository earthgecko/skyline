from __future__ import division
import logging
import traceback
from sqlalchemy.sql import select

from database import get_engine, engine_disposal, metrics_table_meta


# @added 20210430  - Task #4022: Move mysql_select calls to SQLAlchemy
# Add a global method to query the DB for all metric names
def get_all_db_metric_names(current_skyline_app, with_ids=False):
    """
    Given return all metric names from the database as a list
    """
    metric_names = []
    metric_names_with_ids = {}
    function_str = 'database.queries.get_all_db_metric_names'

    current_skyline_app_logger = current_skyline_app + 'Log'
    current_logger = logging.getLogger(current_skyline_app_logger)

    try:
        engine, fail_msg, trace = get_engine(current_skyline_app)
    except Exception as err:
        trace = traceback.format_exc()
        current_logger.error(trace)
        fail_msg = 'error :: %s :: could not get a MySQL engine - %s' % (function_str, err)
        current_logger.error('%s' % fail_msg)
        return False, fail_msg, trace

    try:
        metrics_table, fail_msg, trace = metrics_table_meta(current_skyline_app, engine)
        current_logger.info(fail_msg)
    except Exception as e:
        trace = traceback.format_exc()
        current_logger.error('%s' % trace)
        fail_msg = 'error :: %s :: failed to get metrics_table meta - %s' % (
            function_str, e)
        current_logger.error('%s' % fail_msg)
        if engine:
            engine_disposal(current_skyline_app, engine)
        if current_skyline_app == 'webapp':
            # Raise to webapp
            raise
        return False, fail_msg, trace

    # @added 20241026 - Bug #5522: Handle duplicate metric names
    duplicate_metrics = {}
    # @added 20241111 - Bug #5522: Handle duplicate metric names
    duplicate_metrics_errors = []

    try:
        connection = engine.connect()
        if with_ids:
            stmt = select([metrics_table.c.id, metrics_table.c.metric])
        else:
            # @modified 20241111 - Bug #5522: Handle duplicate metric names
            # The id column is now required to deduplicate metrics so actually
            # the with_ids conditional is no longer required but it makes no
            # different the to the iteration of rows
            # stmt = select([metrics_table.c.metric])
            stmt = select([metrics_table.c.id, metrics_table.c.metric])
        result = connection.execute(stmt)
        for row in result:
            base_name = row['metric']
            # @added 20241026 - Bug #5522: Handle duplicate metric names
            if base_name in metric_names:
                # @modified 20241111 - Bug #5522: Handle duplicate metric names
                #duplicate_metrics[row['id']] = base_name
                #continue
                # Wrapped in try
                i_metric_id = None
                try:
                    i_metric_id = row['id']
                except KeyError:
                    continue
                if i_metric_id:
                    try:
                        duplicate_metrics[i_metric_id] = base_name
                        continue
                    except Exception as err:
                        duplicate_metrics_errors.append(['duplicate_metrics dict err', err])

            metric_names.append(base_name)
            if with_ids:
                metric_names_with_ids[base_name] = row['id']
        connection.close()

        # @added 20241111 - Bug #5522: Handle duplicate metric names
        if duplicate_metrics_errors:
            current_logger.error('error :: %s :: %s duplicate_metrics errors reported, last: %s' % (
                function_str, str(len(duplicate_metrics_errors)),
                str(duplicate_metrics_errors[-1])))

        current_logger.info('%s :: determined metric names from the db: %s' % (
            function_str, str(len(metric_names))))
    except Exception as e:
        trace = traceback.format_exc()
        current_logger.error(trace)
        fail_msg = 'error :: %s :: could not determine metric names from DB for - %s' % (
            function_str, e)
        current_logger.error('%s' % fail_msg)
        if engine:
            engine_disposal(current_skyline_app, engine)
        if current_skyline_app == 'webapp':
            # Raise to webapp
            raise
        return False, fail_msg, trace
    if engine:
        engine_disposal(current_skyline_app, engine)

    # @added 20241026 - Bug #5522: Handle duplicate metric names
    if len(duplicate_metrics) > 0:
        current_logger.info('%s :: there are %s duplicate_metrics found' % (
            function_str, str(len(duplicate_metrics))))

    if not metric_names:
        current_logger.error('error :: %s :: no metric names returned from the DB' % (
            function_str))

    # @added 20230202 - Feature #4792: functions.metrics_manager.manage_inactive_metrics
    #                   Feature #4838: functions.metrics.get_namespace_metric.count
    # Return a unique list as metric_names being a list can have multiple entries
    # for the same metric, whereas metric_names_with_ids is a dict so can only
    # have 1.  This just ensures that the metric_names and metric_names_with_ids
    # are the same length to aviod any confusion.
    if metric_names:
        metric_names = list(set(metric_names))

    if with_ids:
        return metric_names, metric_names_with_ids

    return metric_names
