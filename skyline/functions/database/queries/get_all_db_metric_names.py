from __future__ import division
import logging
import traceback
from time import time
from sqlalchemy.sql import select

from database import get_engine, engine_disposal, metrics_table_meta

# @added 20241127 - Bug #5522: Handle duplicate metric names
from skyline_functions import get_redis_conn_decoded


# @added 20210430  - Task #4022: Move mysql_select calls to SQLAlchemy
# Add a global method to query the DB for all metric names
def get_all_db_metric_names(current_skyline_app, with_ids=False):
    """
    Given return all metric names from the database as a list
    """

    # @modified 20251009 - Bug #5522: Handle duplicate metric names
    # Optimise and use a set
    #metric_names = []
    metric_names = set()

    metric_names_with_ids = {}
    # @added 20241127 - Bug #5522: Handle duplicate metric names
    metric_ids_with_names = {}

    function_str = 'database.queries.get_all_db_metric_names'

    current_skyline_app_logger = current_skyline_app + 'Log'
    current_logger = logging.getLogger(current_skyline_app_logger)

    try:
        redis_conn_decoded = get_redis_conn_decoded(current_skyline_app)
    except Exception as err:
        trace = traceback.format_exc()
        current_logger.error(trace)
        fail_msg = 'error :: %s :: get_redis_conn_decoded failed, err: %s' % (function_str, err)
        current_logger.error('%s' % fail_msg)

    # @added 20241127 - Bug #5522: Handle duplicate metric names
    # The DB request gets longer the more metrics, when 10s of 1000s of metrics
    # are present.  Use the Redis metrics_manager.all_db_metric_ids_with_names
    # hash to determine if any need to be added.
    new_metrics_to_add_to_redis = {}
    redis_metric_ids_with_names = {}
    last_redis_metric_id = 0
    duplicate_metric_ids = []
    try:
        redis_metric_ids_with_names = redis_conn_decoded.hgetall('metrics_manager.all_db_metric_ids_with_names')
    except Exception as err:
        trace = traceback.format_exc()
        current_logger.error(trace)
        fail_msg = 'error :: %s :: could not hgetall metrics_manager.all_db_metric_ids_with_names, err: %s' % (function_str, err)
        current_logger.error('%s' % fail_msg)
    current_logger.info('%s :: determined %s known metric names and ids from the metrics_manager.all_db_metric_ids_with_names' % (
        function_str, str(len(redis_metric_ids_with_names))))

    # @added 20250915 - Bug #5522: Handle duplicate metric names
    # If the key is not available load the aet.metrics_manager.inactive_ids_with_metric_names
    # which will often have a majority or bulk of the metrics
    if len(redis_metric_ids_with_names) == 0:
        try:
            redis_metric_ids_with_names = redis_conn_decoded.hgetall('aet.metrics_manager.inactive_ids_with_metric_names')
        except Exception as err:
            trace = traceback.format_exc()
            current_logger.error(trace)
            fail_msg = 'error :: %s :: could not hgetall aet.metrics_manager.inactive_ids_with_metric_names, err: %s' % (function_str, err)
            current_logger.error('%s' % fail_msg)
        current_logger.info('%s :: determined %s known metric names and inactive ids from aet.metrics_manager.inactive_ids_with_metric_names' % (
            function_str, str(len(redis_metric_ids_with_names))))

    if redis_metric_ids_with_names:
        try:
            redis_metric_ids = sorted([int(mid) for mid in redis_metric_ids_with_names.keys()])
            last_redis_metric_id = redis_metric_ids[-1]
            current_logger.info('%s :: determined last known metric id from Redis, last_redis_metric_id: %s' % (
                function_str, str(last_redis_metric_id)))
            # @added 20251009 - Bug #5522: Handle duplicate metric names
            # Optimise and use a set
            redis_metric_ids = set(redis_metric_ids)

            metric_names = list(redis_metric_ids_with_names.values())
            # Deduplicate
            if metric_names:
                # @modified 20251009 - Bug #5522: Handle duplicate metric names
                # Optimise and use a set
                #metric_names = list(set(metric_names))
                metric_names = set(metric_names)
            current_logger.info('%s :: determined %s metric_names from Redis' % (
                function_str, str(len(metric_names))))
            seen_metrics = set()
            # @modified 20251009 - Bug #5522: Handle duplicate metric names
            # Optimise and use a set
            #for mid in set(redis_metric_ids):
            for mid in redis_metric_ids:
                metric = redis_metric_ids_with_names[str(mid)]
                if metric in seen_metrics:
                    duplicate_metric_ids.append(mid)
                    continue
                seen_metrics.add(metric)
            duplicate_metric_ids = set(duplicate_metric_ids)
            current_logger.info('%s :: identified %s duplicate metric from Redis data from %s seen_metrics' % (
                function_str, str(len(duplicate_metric_ids)),
                str(len(seen_metrics))))
            metric_ids_with_names = {mid: redis_metric_ids_with_names[str(mid)] for mid in redis_metric_ids}
            if with_ids:
                metric_names_with_ids = {v: int(k) for k, v in redis_metric_ids_with_names.items() if int(k) not in duplicate_metric_ids}
        except Exception as err:
            trace = traceback.format_exc()
            current_logger.error(trace)
            fail_msg = 'error :: %s :: failed to create resources from redis_metric_ids_with_names, err: %s' % (function_str, err)
            current_logger.error('%s' % fail_msg)
    current_logger.info('%s :: determined %s metric_ids_with_names' % (
        function_str, str(len(metric_ids_with_names))))
    current_logger.info('%s :: determined %s metric_names_with_ids' % (
        function_str, str(len(metric_names_with_ids))))

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

    # @added 20241127 - Bug #5522: Handle duplicate metric names
    # This DB request gets longer the more metrics, when 10s of 1000s of metrics
    # are present.  Use the Redis metrics_manager.all_db_metric_ids_with_names
    # hash to determine if any need to be added.
    last_metric_id = None
    try:
        #connection = engine.connect()
        # @modified 20260225 - Task #5176: Migrate to sqlalchemy v2 API
        #                      Task #5628: Build v5.0.0 and test
        #stmt = select([metrics_table.c.id]).order_by(metrics_table.c.id.desc()).limit(1)
        stmt = select(metrics_table.c.id).order_by(metrics_table.c.id.desc()).limit(1)
        # @modified 20260226 - Task #5176: Migrate to sqlalchemy v2 API
        #                      Task #5628: Build v5.0.0 and test
        #result = connection.execute(stmt)
        #for row in result:
        with engine.connect() as connection:
            result = connection.execute(stmt)
            results = [dict(row._mapping) for row in result.fetchall()]
        for row in results:
            last_metric_id = int(row['id'])
        #connection.close()
    except Exception as err:
        trace = traceback.format_exc()
        current_logger.error(trace)
        fail_msg = 'error :: %s :: could not determine metric names from DB for, err: %s' % (
            function_str, err)
        current_logger.error('%s' % fail_msg)
        if engine:
            engine_disposal(current_skyline_app, engine)
        if current_skyline_app == 'webapp':
            # Raise to webapp
            raise
        return False, fail_msg, trace
    current_logger.info('%s :: determined last metric id from DB, last_metric_id: %s' % (
        function_str, str(last_metric_id)))

    if last_metric_id == last_redis_metric_id:
        current_logger.info('%s :: current Redis data the same as DB, returning %s metrics' % (
            function_str, str(last_metric_id)))
        if engine:
            engine_disposal(current_skyline_app, engine)
        metric_names = list(metric_names)
        if with_ids:
            return metric_names, metric_names_with_ids
        return metric_names

    # @added 20250915 - Bug #5522: Handle duplicate metric names
    # Limit time so the the process eventually populates the Redis hash over a
    # few runs rather than timing out
    start_populate = int(time())

    try:
        #connection = engine.connect()
        if with_ids:
            # @modified 20241127 - Bug #5522: Handle duplicate metric names
            # Do not select all only select id > than what is in Redis
            # @modified 20260225 - Task #5176: Migrate to sqlalchemy v2 API
            #                      Task #5628: Build v5.0.0 and test
            ##stmt = select([metrics_table.c.id, metrics_table.c.metric])
            #stmt = select(metrics_table.c.id, metrics_table.c.metric)
            # @modified 20260225 - Task #5176: Migrate to sqlalchemy v2 API
            #                      Task #5628: Build v5.0.0 and test
            #stmt = select([metrics_table.c.id, metrics_table.c.metric]).where(metrics_table.c.id > last_redis_metric_id)
            stmt = select(metrics_table.c.id, metrics_table.c.metric).where(metrics_table.c.id > last_redis_metric_id)
        else:
            # @modified 20241111 - Bug #5522: Handle duplicate metric names
            # The id column is now required to deduplicate metrics so actually
            # the with_ids conditional is no longer required but it makes no
            # different the to the iteration of rows
            # @modified 20260225 - Task #5176: Migrate to sqlalchemy v2 API
            #                      Task #5628: Build v5.0.0 and test
            ## stmt = select([metrics_table.c.metric])
            # stmt = select(metrics_table.c.metric)
            # @modified 20241127 - Bug #5522: Handle duplicate metric names
            # Do not select all only select id > than what is in Redis
            # @modified 20260225 - Task #5176: Migrate to sqlalchemy v2 API
            #                      Task #5628: Build v5.0.0 and test
            ##stmt = select([metrics_table.c.id, metrics_table.c.metric])
            #stmt = select(metrics_table.c.id, metrics_table.c.metric)
            # @modified 20260225 - Task #5176: Migrate to sqlalchemy v2 API
            #                      Task #5628: Build v5.0.0 and test
            #stmt = select([metrics_table.c.id, metrics_table.c.metric]).where(metrics_table.c.id > last_redis_metric_id)
            stmt = select(metrics_table.c.id, metrics_table.c.metric).where(metrics_table.c.id > last_redis_metric_id)

        # @modified 20260226 - Task #5176: Migrate to sqlalchemy v2 API
        #                      Task #5628: Build v5.0.0 and test
        #result = connection.execute(stmt)
        #for row in result:
        with engine.connect() as connection:
            result = connection.execute(stmt)
            results = [dict(row._mapping) for row in result.fetchall()]
        for row in results:
            # @added 20250915 - Bug #5522: Handle duplicate metric names
            # Limit time so the the process eventually populates the Redis hash over a
            # few runs rather than timing out
            pop_time = int(time())
            if pop_time > (start_populate + 45):
                current_logger.info('%s :: determined %s new_metrics_to_add_to_redis but limiting due to time, not continuing' % (
                    function_str, str(len(new_metrics_to_add_to_redis))))
                break

            try:
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
            except Exception as err:
                duplicate_metrics_errors.append(['row err', str(row), err])

            # @modified 20251009 - Bug #5522: Handle duplicate metric names
            # Optimise and use a set
            #metric_names.append(base_name)
            metric_names.add(base_name)

            if with_ids:
                metric_names_with_ids[base_name] = row['id']

            # @added 20241127 - Bug #5522: Handle duplicate metric names
            # Update Redis
            # @modified 20251009 - Bug #5522: Handle duplicate metric names
            # Optimise and use a set and check only adding if not in set
            #new_metrics_to_add_to_redis[str(row['id'])] = base_name
            metric_id = row['id']
            if metric_id not in redis_metric_ids:
                new_metrics_to_add_to_redis[str(row['id'])] = base_name

        # @modified 20260226 - Task #5176: Migrate to sqlalchemy v2 API
        #                      Task #5628: Build v5.0.0 and test
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

    # @added 20241127 - Bug #5522: Handle duplicate metric names
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
