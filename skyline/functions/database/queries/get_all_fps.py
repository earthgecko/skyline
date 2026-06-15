from __future__ import division
import copy
import logging
import traceback
from sqlalchemy.sql import select

from database import get_engine, engine_disposal, ionosphere_table_meta

# @added 20241127 - Bug #5522: Handle duplicate metric names
from skyline_functions import get_redis_conn_decoded


# @added 20241213 - Feature #5572: get_all_fps
#                   Bug #5571: v5.0.0-alplha - regression on cluster nodes - mysql.aborted_clients
#                   Feature #3890: metrics_manager - sync_cluster_files
#                   Bug #5522: Handle duplicate metric names
# Add a global method to query the Redis and DB for all fps
def get_all_fps(current_skyline_app, enabled_only=False):
    """
    Returns a dict keyed on fp id with dict containing key/values for metric_id,
    anomaly_timestamp and enabled.

    :param current_skyline_app: the skyline app calling the function
    :param enabled_only: whether to only return enabled fps only.
    :type current_skyline_app: str
    :type enabled_only: boolean
    :return: fps
    :rtype: dict

    """
    fps = {}

    function_str = 'database.queries.get_all_fps'

    current_skyline_app_logger = current_skyline_app + 'Log'
    current_logger = logging.getLogger(current_skyline_app_logger)

    try:
        redis_conn_decoded = get_redis_conn_decoded(current_skyline_app)
    except Exception as err:
        trace = traceback.format_exc()
        current_logger.error(trace)
        fail_msg = 'error :: %s :: get_redis_conn_decoded failed, err: %s' % (function_str, err)
        current_logger.error('%s' % fail_msg)

    # To ensure that operations are fast instead of using a hash with dict
    # strings and literal_eval on each (slow), there is a hash for each
    # dimension keyed on the fp id with the value for the dimension.
    # metrics_manager.all_fps.metric_id
    # metrics_manager.all_fps.anomaly_timestamp
    # metrics_manager.all_fps.enabled

    redis_fp_ids_with_metric_id = {}
    redis_fp_ids_with_anomaly_timestamp = {}
    redis_fp_ids_with_enabled = {}

    last_redis_fp_id = 0
    try:
        redis_fp_ids_with_metric_id = redis_conn_decoded.hgetall('ionosphere.fp_ids_with_metric_id')
    except Exception as err:
        trace = traceback.format_exc()
        current_logger.error(trace)
        fail_msg = 'error :: %s :: could not hgetall ionosphere.fp_ids_with_metric_id, err: %s' % (function_str, err)
        current_logger.error('%s' % fail_msg)
    fp_ids_with_metric_id = {int(fp_id): int(mid) for fp_id, mid in redis_fp_ids_with_metric_id.items()}
    redis_fp_ids = sorted(list(fp_ids_with_metric_id.keys()))
    if redis_fp_ids:
        try:
            last_redis_fp_id = redis_fp_ids[-1]
        except Exception as err:
            current_logger.error('error :: %s :: last_redis_fp_id not determined from redis_fp_ids, err: %s' % (function_str, err))
            last_redis_fp_id = 0
    try:
        redis_fp_ids_with_anomaly_timestamp = redis_conn_decoded.hgetall('ionosphere.fp_ids_with_anomaly_timestamp')
    except Exception as err:
        trace = traceback.format_exc()
        current_logger.error(trace)
        fail_msg = 'error :: %s :: could not hgetall ionosphere.fp_ids_with_anomaly_timestamp, err: %s' % (function_str, err)
        current_logger.error('%s' % fail_msg)
    fp_ids_with_anomaly_timestamp = {int(fp_id): int(mid) for fp_id, mid in redis_fp_ids_with_anomaly_timestamp.items()}
    try:
        redis_fp_ids_with_enabled = redis_conn_decoded.hgetall('ionosphere.fp_ids_with_enabled')
    except Exception as err:
        trace = traceback.format_exc()
        current_logger.error(trace)
        fail_msg = 'error :: %s :: could not hgetall ionosphere.fp_ids_with_anomaly_timestamp, err: %s' % (function_str, err)
        current_logger.error('%s' % fail_msg)
    fp_ids_with_enabled = {int(fp_id): int(enabled) for fp_id, enabled in redis_fp_ids_with_enabled.items()}

    fps_to_update = []
    for fp_id in redis_fp_ids:
        try:
            fps[fp_id] = {
                'metric_id': fp_ids_with_metric_id[fp_id],
                'anomaly_timestamp': fp_ids_with_anomaly_timestamp[fp_id],
                'enabled':  fp_ids_with_enabled[fp_id]   
            }
        except:
            fps_to_update.append(fp_id)
    current_logger.info('%s :: determined %s fps, last_redis_fp_id: %s' % (
        function_str, str(len(fps)), str(last_redis_fp_id)))
    if fps_to_update:
        current_logger.info('%s :: determined %s fps need to be updated in Redis' % (
            function_str, str(len(fps_to_update))))

    try:
        engine, fail_msg, trace = get_engine(current_skyline_app)
    except Exception as err:
        trace = traceback.format_exc()
        current_logger.error(trace)
        fail_msg = 'error :: %s :: could not get a MySQL engine, err: %s' % (function_str, err)
        current_logger.error('%s' % fail_msg)

    try:
        ionosphere_table, log_msg, trace = ionosphere_table_meta(current_skyline_app, engine)
        current_logger.info(log_msg)
    except Exception as err:
        trace = traceback.format_exc()
        current_logger.error('%s' % trace)
        fail_msg = 'error :: %s :: ionosphere_table_meta failed, err: %s' % (
            function_str, err)
        current_logger.error('%s' % fail_msg)
        if engine:
            engine_disposal(current_skyline_app, engine)
        if current_skyline_app == 'webapp':
            # Raise to webapp
            raise
        return False, fail_msg, trace

    last_fp_id = 0
    try:
        #connection = engine.connect()
        # @modified 20260225 - Task #5176: Migrate to sqlalchemy v2 API
        #                      Task #5628: Build v5.0.0 and test
        #stmt = select([ionosphere_table.c.id]).order_by(ionosphere_table.c.id.desc()).limit(1)
        stmt = select(ionosphere_table.c.id).order_by(ionosphere_table.c.id.desc()).limit(1)
        # @modified 20260226 - Task #5176: Migrate to sqlalchemy v2 API
        #                      Task #5628: Build v5.0.0 and test
        #result = connection.execute(stmt)
        #for row in result:
        with engine.connect() as connection:
            result = connection.execute(stmt)
            results = [dict(row._mapping) for row in result.fetchall()]
        for row in results:
            last_fp_id = int(row['id'])
            break
        #connection.close()
    except Exception as err:
        current_logger.error(traceback.format_exc())
        current_logger.error('error :: %s :: failed to determine last fp id from ionosphere table, err: %s' % (
            function_str, err))
    current_logger.info('%s :: determined last fp_id: %s from ionosphere DB table' % (
        function_str, str(last_fp_id)))

    fps_to_add_to_redis = {}
    fps_with_errors = []
    if last_redis_fp_id < last_fp_id:
        current_logger.info('%s :: getting fps id > %s from ionosphere DB table' % (
            function_str, str(last_redis_fp_id)))
        try:
            #connection = engine.connect()
            # @modified 20260225 - Task #5176: Migrate to sqlalchemy v2 API
            #                      Task #5628: Build v5.0.0 and test
            #stmt = select([ionosphere_table.c.id, ionosphere_table.c.metric_id, ionosphere_table.c.anomaly_timestamp, ionosphere_table.c.enabled]).where(ionosphere_table.c.id > last_redis_fp_id)
            stmt = select(ionosphere_table.c.id, ionosphere_table.c.metric_id, ionosphere_table.c.anomaly_timestamp, ionosphere_table.c.enabled).where(ionosphere_table.c.id > last_redis_fp_id)
            # @modified 20260226 - Task #5176: Migrate to sqlalchemy v2 API
            #                      Task #5628: Build v5.0.0 and test
            #result = connection.execute(stmt)
            #for row in result:
            with engine.connect() as connection:
                result = connection.execute(stmt)
                results = [dict(row._mapping) for row in result.fetchall()]
            for row in results:
                try:
                    fp_id = int(row['id'])
                    fp_dict = {
                        'metric_id': int(row['metric_id']),
                        'anomaly_timestamp': int(row['anomaly_timestamp']),
                        'enabled': int(row['enabled']),
                    }
                    fps_to_add_to_redis[fp_id] = copy.deepcopy(fp_dict)
                    fps[fp_id] = copy.deepcopy(fp_dict)
                except Exception as err:
                    fps_with_errors.append([{'error': err, 'row': str(row)}])
            #connection.close()
        except Exception as err:
            current_logger.error(traceback.format_exc())
            current_logger.error('error :: %s :: failed to select fps > id: %s from the ionosphere table, err: %s' % (
                function_str, str(last_redis_fp_id), err))
    if fps_with_errors:
        current_logger.error('error :: %s :: fps_with_errors[-1]: %s' % (
            function_str, str(fps_with_errors[-1])))

    current_logger.info('%s :: determined %s fps_to_add_to_redis' % (
        function_str, str(len(fps_to_add_to_redis))))

    current_logger.info('%s :: determined %s fps_to_add_to_redis' % (
        function_str, str(len(fps_to_add_to_redis))))
    
    if fps_to_update:
        try:
            #connection = engine.connect()
            # @modified 20260225 - Task #5176: Migrate to sqlalchemy v2 API
            #                      Task #5628: Build v5.0.0 and test
            #stmt = select([ionosphere_table.c.id, ionosphere_table.c.metric_id, ionosphere_table.c.anomaly_timestamp, ionosphere_table.c.enabled], ionosphere_table.c.id.in_(fps_to_update))
            stmt = select(ionosphere_table.c.id, ionosphere_table.c.metric_id, ionosphere_table.c.anomaly_timestamp, ionosphere_table.c.enabled).\
                    where(ionosphere_table.c.id.in_(fps_to_update))

            # @modified 20260226 - Task #5176: Migrate to sqlalchemy v2 API
            #                      Task #5628: Build v5.0.0 and test
            #result = connection.execute(stmt)
            #for row in result:
            with engine.connect() as connection:
                result = connection.execute(stmt)
                results = [dict(row._mapping) for row in result.fetchall()]
            for row in results:
                try:
                    fp_id = int(row['id'])
                    fp_dict = {
                        'metric_id': int(row['metric_id']),
                        'anomaly_timestamp': int(row['anomaly_timestamp']),
                        'enabled': int(row['enabled']),
                    }
                    fps_to_add_to_redis[fp_id] = copy.deepcopy(fp_dict)
                    fps[fp_id] = copy.deepcopy(fp_dict)
                except Exception as err:
                    fps_with_errors.append([{'error': err, 'row': str(row)}])
            #connection.close()
        except Exception as err:
            current_logger.error(traceback.format_exc())
            current_logger.error('error :: %s :: failed to select fps_to_update from the ionosphere table, err: %s' % (
                function_str, err))
        current_logger.info('%s :: added %s fps_to_update to fps_to_add_to_redis' % (
            function_str, str(len(fps_to_update))))

    if engine:
        engine_disposal(current_skyline_app, engine)

    add_redis_fp_ids_with_metric_id = {}
    add_redis_fp_ids_with_anomaly_timestamp = {}
    add_redis_fp_ids_with_enabled = {}
    for fp_id, fp_dict in fps_to_add_to_redis.items():
        add_redis_fp_ids_with_metric_id[fp_id] = fp_dict['metric_id']
        add_redis_fp_ids_with_anomaly_timestamp[fp_id] = fp_dict['anomaly_timestamp']
        add_redis_fp_ids_with_enabled[fp_id] = fp_dict['enabled']
    if add_redis_fp_ids_with_metric_id:
        try:
            redis_conn_decoded.hset('ionosphere.fp_ids_with_metric_id', mapping=add_redis_fp_ids_with_metric_id)
            current_logger.info('%s :: added %s fps to ionosphere.fp_ids_with_metric_id' % (
                function_str, str(len(add_redis_fp_ids_with_metric_id))))
        except Exception as err:
            current_logger.error('error :: %s :: failed to add %s fps ionosphere.fp_ids_with_metric_id, err: %s' % (
                function_str, str(len(add_redis_fp_ids_with_metric_id)), err))
        try:
            redis_conn_decoded.hset('ionosphere.fp_ids_with_anomaly_timestamp', mapping=add_redis_fp_ids_with_anomaly_timestamp)
            current_logger.info('%s :: added %s fps to ionosphere.fp_ids_with_anomaly_timestamp' % (
                function_str, str(len(add_redis_fp_ids_with_anomaly_timestamp))))
        except Exception as err:
            current_logger.error('error :: %s :: failed to add %s fps ionosphere.fp_ids_with_anomaly_timestamp, err: %s' % (
                function_str, str(len(add_redis_fp_ids_with_anomaly_timestamp)), err))
        try:
            redis_conn_decoded.hset('ionosphere.fp_ids_with_enabled', mapping=add_redis_fp_ids_with_enabled)
            current_logger.info('%s :: added %s fps to ionosphere.fp_ids_with_enabled' % (
                function_str, str(len(add_redis_fp_ids_with_enabled))))
        except Exception as err:
            current_logger.error('error :: %s :: failed to add %s fps ionosphere.fp_ids_with_enabled, err: %s' % (
                function_str, str(len(add_redis_fp_ids_with_enabled)), err))

    if enabled_only:
        enabled_fps = {}
        for fp_id, fp_dict in fps.items():
            if fp_dict['enabled']:
                enabled_fps[fp_id] = copy.deepcopy(fp_dict)
        fps = copy.deepcopy(enabled_fps)

    return fps
