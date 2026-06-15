from __future__ import division
import logging
import traceback
#from time import time
from sqlalchemy.sql import select

from database import get_engine, engine_disposal, metrics_table_meta

# @added 20241127 - Bug #5522: Handle duplicate metric names
from skyline_functions import get_redis_conn_decoded


# @added 20210430  - Task #4022: Move mysql_select calls to SQLAlchemy
# Add a global method to query the DB for all metric names
def get_all_db_metric_names_db_only(current_skyline_app, with_ids=False):
    """
    Given return all metric names from the database as a list
    """
    function_str = 'database.queries.get_all_db_metric_names_db_only'

    metric_names = set()
    metric_names_with_ids = {}
    metric_ids_with_names = {}
    new_metrics_to_add_to_redis = {}

    current_skyline_app_logger = current_skyline_app + 'Log'
    current_logger = logging.getLogger(current_skyline_app_logger)

    try:
        redis_conn_decoded = get_redis_conn_decoded(current_skyline_app)
    except Exception as err:
        trace = traceback.format_exc()
        current_logger.error(trace)
        fail_msg = 'error :: %s :: get_redis_conn_decoded failed, err: %s' % (function_str, err)
        current_logger.error('%s' % fail_msg)

    redis_metric_ids_with_names = {}
    try:
        redis_metric_ids_with_names = redis_conn_decoded.hgetall('metrics_manager.all_db_metric_ids_with_names')
    except Exception as err:
        trace = traceback.format_exc()
        current_logger.error(trace)
        fail_msg = 'error :: %s :: could not hgetall metrics_manager.all_db_metric_ids_with_names, err: %s' % (function_str, err)
        current_logger.error('%s' % fail_msg)
    current_logger.info('%s :: determined %s known metric names and ids from the metrics_manager.all_db_metric_ids_with_names' % (
        function_str, str(len(redis_metric_ids_with_names))))
    redis_metric_ids = set([int(mid) for mid in redis_metric_ids_with_names.keys()])

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
            #stmt = select([metrics_table.c.id, metrics_table.c.metric])
            stmt = select(metrics_table.c.id, metrics_table.c.metric)
        else:
            # @modified 20260225 - Task #5176: Migrate to sqlalchemy v2 API
            #                      Task #5628: Build v5.0.0 and test
            #stmt = select([metrics_table.c.id, metrics_table.c.metric])
            stmt = select(metrics_table.c.id, metrics_table.c.metric)

        # @modified 20260226 - Task #5176: Migrate to sqlalchemy v2 API
        #                      Task #5628: Build v5.0.0 and test
        #result = connection.execute(stmt)
        #metric_ids_with_names = {row['id']: row['metric'] for row in result}
        #connection.close()
        with engine.connect() as connection:
            result = connection.execute(stmt)
            results = [dict(row._mapping) for row in result.fetchall()]
        metric_ids_with_names = {row['id']: row['metric'] for row in results}
        current_logger.info('%s :: determined metric names and ids from the db: %s' % (
            function_str, str(len(metric_ids_with_names))))
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

    for metric_id, base_name in metric_ids_with_names.items():
        try:
            if base_name in metric_names:
                try:
                    duplicate_metrics[metric_id] = base_name
                    continue
                except Exception as err:
                    duplicate_metrics_errors.append(['duplicate_metrics dict err', err])
        except Exception as err:
            duplicate_metrics_errors.append(['row err - metric id', metric_id, err])
        metric_names.add(base_name)
        if with_ids:
            metric_names_with_ids[base_name] = metric_id
        if metric_id not in redis_metric_ids:
            new_metrics_to_add_to_redis[str(metric_id)] = base_name

    # Update Redis
    if new_metrics_to_add_to_redis:
        current_logger.info('%s :: adding %s new metrics to metrics_manager.all_db_metric_ids_with_names' % (
            function_str, str(len(new_metrics_to_add_to_redis))))
        try:
            redis_conn_decoded.hset('metrics_manager.all_db_metric_ids_with_names', mapping=new_metrics_to_add_to_redis)
        except Exception as err:
            fail_msg = 'error :: %s :: could not hset metrics_manager.all_db_metric_ids_with_names, err: %s' % (function_str, err)
            current_logger.error('%s' % fail_msg)

    # @added 20241026 - Bug #5522: Handle duplicate metric names
    if len(duplicate_metrics) > 0:
        current_logger.info('%s :: there are %s duplicate_metrics found' % (
            function_str, str(len(duplicate_metrics))))
        # @added 20241203 - Bug #5522: Handle duplicate metric names
        if current_skyline_app == 'panorama':
            try:
                # @modified 20251009 - Bug #5522: Handle duplicate metric names
                # Do not delete and recreate, just add, as this hash is now
                # managed in metrics_manager.py
                #redis_conn_decoded.delete('panorama.duplicate_metrics')
                redis_conn_decoded.hset('panorama.duplicate_metrics', mapping=duplicate_metrics)
                current_logger.info('%s :: panorama.duplicate_metrics Redis hash' % (
                    function_str))
            except Exception as err:
                fail_msg = 'error :: %s :: could not hset panorama.duplicate_metrics, err: %s' % (function_str, err)
                current_logger.error('%s' % fail_msg)

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
