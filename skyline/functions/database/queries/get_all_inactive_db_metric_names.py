import logging
import traceback
from sqlalchemy.sql import select

from database import get_engine, engine_disposal, metrics_table_meta


# @added 20240320 - Feature #4444: webapp - inactive_metrics
def get_all_inactive_db_metric_names(current_skyline_app, with_ids=False):
    """
    Return all inactive metric names from the database as a list and optionally
    with a dict of metric_names and ids
    """

    # @modified 20250304 - Bug #5522: Handle duplicate metric names
    # Optimising just under this issue.  Use a set rather than a list
    #metric_names = []
    metric_names = set()

    metric_names_with_ids = {}
    function_str = 'database.queries.get_all_inactive_db_metric_names'

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
        #connection = engine.connect()
        if with_ids:
            # @modified 20260225 - Task #5176: Migrate to sqlalchemy v2 API
            #                      Task #5628: Build v5.0.0 and test
            #stmt = select([metrics_table.c.id, metrics_table.c.metric]).where(metrics_table.c.inactive == 1)
            stmt = select(metrics_table.c.id, metrics_table.c.metric).where(metrics_table.c.inactive == 1)
        else:
            # @modified 20241111 - Bug #5522: Handle duplicate metric names
            # The id column is now required to deduplicate metrics so actually
            # the with_ids conditional is no longer required but it makes no
            # different the to the iteration of rows
            # @modified 20260225 - Task #5176: Migrate to sqlalchemy v2 API
            #                      Task #5628: Build v5.0.0 and test
            ##stmt = select([metrics_table.c.metric]).where(metrics_table.c.inactive == 1)
            #stmt = select(metrics_table.c.metric).where(metrics_table.c.inactive == 1)
            # @modified 20260225 - Task #5176: Migrate to sqlalchemy v2 API
            #                      Task #5628: Build v5.0.0 and test
            #stmt = select([metrics_table.c.id, metrics_table.c.metric]).where(metrics_table.c.inactive == 1)
            stmt = select(metrics_table.c.id, metrics_table.c.metric).where(metrics_table.c.inactive == 1)

        # @modified 20260226 - Task #5176: Migrate to sqlalchemy v2 API
        #                      Task #5628: Build v5.0.0 and test
        #result = connection.execute(stmt)
        #for row in result:
        with engine.connect() as connection:
            result = connection.execute(stmt)
            results = [dict(row._mapping) for row in result.fetchall()]
        for row in results:
            base_name = row['metric']

            # @added 20241026 - Bug #5522: Handle duplicate metric names
            i_metric_id = None
            if base_name in metric_names:
                # @modified 20241111 - Bug #5522: Handle duplicate metric names
                #duplicate_metrics[row['id']] = base_name
                #continue
                # Wrapped in try
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

            # @modified 20250304 - Bug #5522: Handle duplicate metric names
            # Optimising just under this issue.  Use a set rather than a list
            #metric_names.append(base_name)
            if base_name in metric_names:
                if not i_metric_id:
                    try:
                        i_metric_id = row['id']
                    except KeyError:
                        continue
                    duplicate_metrics[i_metric_id] = base_name
                    continue
            metric_names.add(base_name)
            if with_ids:
                metric_names_with_ids[base_name] = row['id']
        #connection.close()

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

    if metric_names:
        # @modified 20250304 - Bug #5522: Handle duplicate metric names
        #metric_names = list(set(metric_names))
        metric_names = list(metric_names)

    if with_ids:
        return metric_names, metric_names_with_ids

    return metric_names
