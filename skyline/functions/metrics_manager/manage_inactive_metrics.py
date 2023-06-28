"""
manage_inactive_metrics.py
"""
import logging
import copy
from os import uname as os_uname
from time import time

from functions.database.queries.get_all_active_db_metric_names import get_all_active_db_metric_names
from functions.database.queries.get_all_db_metric_names import get_all_db_metric_names

# @added 20230201 - Feature #4838: functions.metrics.get_namespace_metric.count
import settings
from functions.cluster.is_shard_metric import is_shard_metric
from functions.database.queries.set_metrics_as_inactive import set_metrics_as_inactive

skyline_app = 'analyzer'
skyline_app_logger = '%sLog' % skyline_app
logger = logging.getLogger(skyline_app_logger)

# @added 20230201 - Feature #4838: functions.metrics.get_namespace_metric.count
# Handle shard metrics only
try:
    HORIZON_SHARDS = copy.deepcopy(settings.HORIZON_SHARDS)
except:
    HORIZON_SHARDS = {}
number_of_horizon_shards = 0
this_host = str(os_uname()[1])
if this_host == 'skyline-test-1-fra1':
    DEVELOPMENT = True
    HORIZON_SHARDS = {
        'skyline-test-1-fra1': 0,
        'another-test-node-1': 1,
        'another-test-node-2': 2,
    }
else:
    DEVELOPMENT = False
HORIZON_SHARD = 0
if HORIZON_SHARDS:
    number_of_horizon_shards = len(HORIZON_SHARDS)
    HORIZON_SHARD = HORIZON_SHARDS[this_host]

full_uniques = '%sunique_metrics' % settings.FULL_NAMESPACE

LOCAL_DEBUG = False


# @added 20230105 - Feature #4792: functions.metrics_manager.manage_inactive_metrics
def manage_inactive_metrics(self, unique_base_names, active_labelled_metrics_with_id):
    """
    Determine the inactive metrics and reactivate any metrics the have restarted.

    :param self: the self object
    :type self: object
    :return: purged
    :rtype: int

    """
    function_str = 'metrics_manager :: manage_inactive_metrics'
    start = time()
    logger.info('%s :: managing inactive_metrics' % function_str)

    with_ids = True
    # all_metric_names = []
    all_metric_names_with_ids = {}
    # active_metric_names = []
    active_metric_names_with_ids = {}
    inactive_metric_ids = []
    redis_active_metric_ids = []
    reactivate_metric_ids = []

    # @added 20230201 - Feature #4838: functions.metrics.get_namespace_metric.count
    #                   Task #2732: Prometheus to Skyline
    #                   Branch #4300: prometheus
    inactive_metric_names_with_ids = {}

    db_all_metric_names_with_ids = {}
    try:
        all_metric_names, all_metric_names_with_ids = get_all_db_metric_names(skyline_app, with_ids)
        db_all_metric_names_with_ids = copy.deepcopy(all_metric_names_with_ids)
    except Exception as err:
        logger.error('error :: %s :: get_all_db_metric_names failed - %s' % (
            function_str, err))

    if LOCAL_DEBUG:
        logger.debug('debug :: %s :: len(all_metric_names): %s, len(db_all_metric_names_with_ids): %s' % (
            function_str, str(len(all_metric_names)), str(len(db_all_metric_names_with_ids))))

    # @added 20230201 - Feature #4838: functions.metrics.get_namespace_metric.count
    #                   Task #2732: Prometheus to Skyline
    #                   Branch #4300: prometheus
    # Handle shard metrics only
    db_all_metric_ids_with_names = {}
    for metric in list(db_all_metric_names_with_ids.keys()):
        db_all_metric_ids_with_names[db_all_metric_names_with_ids[metric]] = metric
    is_shard_metric_failures = 0
    if HORIZON_SHARDS:
        logger.info('%s :: found %s all_metric_names before removing metrics not belonging to this shard' % (
            function_str, str(len(all_metric_names))))
        for metric in list(db_all_metric_names_with_ids.keys()):
            shard_metric = is_shard_metric(metric)
            if shard_metric is None:
                is_shard_metric_failures += 1
                continue
            if not shard_metric:
                try:
                    all_metric_names.remove(metric)
                except:
                    pass
                try:
                    del all_metric_names_with_ids[metric]
                except:
                    pass
        if is_shard_metric_failures:
            logger.error('error :: %s :: is_shard_metric_failures reported %s failures iterating all_metric_names' % str(is_shard_metric_failures))
            is_shard_metric_failures = 0
        logger.info('%s :: found %s all_metric_names belonging to this shard' % (
            function_str, str(len(all_metric_names))))

    db_active_metric_names_with_ids = {}
    try:
        active_metric_names, active_metric_names_with_ids = get_all_active_db_metric_names(skyline_app, with_ids)
        db_active_metric_names_with_ids = copy.deepcopy(active_metric_names_with_ids)
    except Exception as err:
        logger.error('error :: %s :: get_all_active_db_metric_names failed - %s' % (
            function_str, err))

    if LOCAL_DEBUG:
        logger.debug('debug :: %s :: len(active_metric_names): %s, len(db_active_metric_names_with_ids): %s' % (
            function_str, str(len(active_metric_names)), str(len(db_active_metric_names_with_ids))))

    # @added 20230201 - Feature #4838: functions.metrics.get_namespace_metric.count
    #                   Task #2732: Prometheus to Skyline
    #                   Branch #4300: prometheus
    # Handle shard metrics only
    is_shard_metric_failures = 0
    if HORIZON_SHARDS:
        logger.info('%s :: found %s active_metric_names before removing metrics not belonging to this shard' % (
            function_str, str(len(active_metric_names))))
        for metric in list(db_active_metric_names_with_ids.keys()):
            shard_metric = is_shard_metric(metric)
            if shard_metric is None:
                is_shard_metric_failures += 1
                continue
            if not shard_metric:
                try:
                    active_metric_names.remove(metric)
                except:
                    pass
                try:
                    del active_metric_names_with_ids[metric]
                except:
                    pass

        if is_shard_metric_failures:
            logger.error('error :: %s :: is_shard_metric_failures reported %s failures iterating active_metric_names' % str(is_shard_metric_failures))
            is_shard_metric_failures = 0
        logger.info('%s :: found %s active_metric_names from DB belonging to this shard' % (
            function_str, str(len(active_metric_names))))

    # @added 20230201 - Feature #4838: functions.metrics.get_namespace_metric.count
    #                   Task #2732: Prometheus to Skyline
    #                   Branch #4300: prometheus
    # Handle shard metrics only
    is_shard_metric_failures = 0
    if HORIZON_SHARDS:
        logger.info('%s :: filtering %s unique_base_names and removing metrics not belonging to this shard' % (
            function_str, str(len(unique_base_names))))

    active_unique_base_names = {}
    all_unique_base_names = list(unique_base_names)
    for base_name in unique_base_names:
        # @added 20230201 - Feature #4838: functions.metrics.get_namespace_metric.count
        #                   Task #2732: Prometheus to Skyline
        #                   Branch #4300: prometheus
        # Handle shard metrics only
        if HORIZON_SHARDS:
            shard_metric = is_shard_metric(base_name)
            if shard_metric is None:
                is_shard_metric_failures += 1
                continue
            if not shard_metric:
                continue
        active_unique_base_names[base_name] = all_metric_names_with_ids[base_name]

    # @added 20230201 - Feature #4838: functions.metrics.get_namespace_metric.count
    #                   Task #2732: Prometheus to Skyline
    #                   Branch #4300: prometheus
    # Handle shard metrics only
    if HORIZON_SHARDS:
        logger.info('%s :: filtered %s unique_base_names belonging to this shard' % (
            function_str, str(len(unique_base_names))))
        if is_shard_metric_failures:
            logger.error('error :: %s :: is_shard_metric_failures reported %s failures iterating unique_base_names' % str(is_shard_metric_failures))
            is_shard_metric_failures = 0

    # @added 20230201 - Feature #4838: functions.metrics.get_namespace_metric.count
    #                   Task #2732: Prometheus to Skyline
    #                   Branch #4300: prometheus
    # Do not classify all active DB metrics as active, only set them to active
    # if they are in Redis
    # if active_metric_names_with_ids:
    #     redis_active_metric_ids = list(active_metric_names_with_ids.values())

    errors = []
    if unique_base_names:
        for unique_base_name in unique_base_names:
            try:
                redis_active_metric_ids.append(db_all_metric_names_with_ids[unique_base_name])
            except Exception as err:
                errors.append(['failed to find id for unique_base_name', unique_base_name])
    if errors:
        logger.error('error :: %s :: %s errors encountered determining ids from unique_base_names, last error - %s' % (
            function_str, str(len(errors)), str(errors[-1])))
    if active_labelled_metrics_with_id:
        all_redis_active_metric_ids = redis_active_metric_ids + list(active_labelled_metrics_with_id.values())
        all_redis_active_metric_ids = list(set(all_redis_active_metric_ids))
    else:
        all_redis_active_metric_ids = list(set(redis_active_metric_ids))

    if all_metric_names_with_ids and active_metric_names_with_ids:
        all_metric_ids_set = set(list(all_metric_names_with_ids.values()))
        active_metric_ids_set = set(list(active_metric_names_with_ids.values()))
        set_difference = all_metric_ids_set.difference(active_metric_ids_set)
        inactive_metric_ids = list(set_difference)

    # @added 20230201 - Feature #4838: functions.metrics.get_namespace_metric.count
    #                   Task #2732: Prometheus to Skyline
    #                   Branch #4300: prometheus
    # Handle shard metrics only
    db_inactive_metric_ids = []
    if HORIZON_SHARDS:
        if db_all_metric_names_with_ids and db_active_metric_names_with_ids:
            all_metric_ids_set = set(list(db_all_metric_names_with_ids.values()))
            active_metric_ids_set = set(list(db_active_metric_names_with_ids.values()))
            set_difference = all_metric_ids_set.difference(active_metric_ids_set)
            db_inactive_metric_ids = list(set_difference)

    # @added 20230201 - Feature #4838: functions.metrics.get_namespace_metric.count
    found_new_inactive_metrics = 0
    newly_found_inactive_metrics = []
    excluded_carbon_metrics = []
    for metric_id in list(active_metric_names_with_ids.values()):
        if metric_id not in all_redis_active_metric_ids:
            # @added 20230201 - Feature #4838: functions.metrics.get_namespace_metric.count
            #                   Task #2732: Prometheus to Skyline
            #                   Branch #4300: prometheus
            # Do not handle carbon metrics on a cluster as they will flip flop
            # because each cluster node only sends its own carbon metrics
            if HORIZON_SHARDS:
                try:
                    base_name = db_all_metric_ids_with_names[metric_id]
                    if base_name.startswith('carbon.'):
                        excluded_carbon_metrics.append([metric_id, base_name])
                        continue
                except:
                    pass

            inactive_metric_ids.append(metric_id)
            newly_found_inactive_metrics.append(metric_id)
            found_new_inactive_metrics += 1
    if excluded_carbon_metrics:
        logger.info('%s :: excluded %s carbon metrics from being set to newly inactive' % (
            function_str, str(len(excluded_carbon_metrics))))

    if found_new_inactive_metrics:
        logger.info('%s :: found %s new inactive metrics not reporting data in Redis' % (
            function_str, str(found_new_inactive_metrics)))
        if LOCAL_DEBUG:
            newly_found_inactive_metric_ids_and_names = {}
            for metric_id in newly_found_inactive_metrics:
                try:
                    newly_found_inactive_metric_ids_and_names[metric_id] = db_all_metric_ids_with_names[metric_id]
                except Exception as err:
                    logger.debug('debug :: error :: %s :: could not find newly_found_inactive_metric metric_id: %s in db_all_metric_ids_with_names - %s' % (
                        function_str, str(metric_id), err))
            logger.debug('debug :: %s :: newly_found_inactive_metric_ids_and_names: %s' % (
                function_str, str(newly_found_inactive_metric_ids_and_names)))

    if inactive_metric_ids and all_redis_active_metric_ids:
        reactivate_metric_ids = list(set(inactive_metric_ids) & set(all_redis_active_metric_ids))

    logger.info('%s :: currently there are %s inactive metrics' % (
        function_str, str(len(inactive_metric_ids))))

    logger.info('%s :: determined %s metrics to reactivate' % (
        function_str, str(len(reactivate_metric_ids))))

    if len(reactivate_metric_ids) > 0:
        if LOCAL_DEBUG:
            reactivate_metric_ids_with_names = {}

        for metric_id in reactivate_metric_ids:
            try:
                inactive_metric_ids.remove(metric_id)
            except:
                pass
            if db_inactive_metric_ids:
                try:
                    db_inactive_metric_ids.remove(metric_id)
                except:
                    pass
            if LOCAL_DEBUG:
                try:
                    reactivate_metric_ids_with_names[metric_id] = db_all_metric_ids_with_names[metric_id]
                except Exception as err:
                    logger.debug('debug :: error :: %s :: could not find reactivate metric_id: %s in db_all_metric_ids_with_names - %s' % (
                        function_str, str(metric_id), err))

        if LOCAL_DEBUG:
            logger.debug('debug :: %s :: reactivate_metric_ids_with_names: %s' % (
                function_str, str(reactivate_metric_ids_with_names)))

        logger.info('%s :: there are now %s inactive metrics after the reactivate_metric_ids have been removed' % (
            function_str, str(len(inactive_metric_ids))))

    # @added 20230202 - Feature #4838: functions.metrics.get_namespace_metric.count
    # Catch any normal metrics that analyzer has not removed from full_uniques
    unique_metrics_to_remove = []
    for metric in unique_base_names:
        try:
            metric_id = db_all_metric_names_with_ids[metric]
        except:
            continue
        if metric_id not in redis_active_metric_ids:
            if not metric.startswith(settings.FULL_NAMESPACE):
                metric = '%s%s' % (settings.FULL_NAMESPACE, metric)
            unique_metrics_to_remove.append(metric)
    if unique_metrics_to_remove:
        logger.info('%s :: would remove %s metrics from %s' % (
            function_str, str(len(unique_metrics_to_remove)), full_uniques))
        try:
            self.redis_conn_decoded.srem(full_uniques, *set(unique_metrics_to_remove))
            logger.info('%s :: called srem on %s with %s possible inactive metrics' % (
                function_str, full_uniques, str(len(unique_metrics_to_remove))))
        except Exception as err:
            logger.error('%s :: failed to srem %s - %s' % (
                function_str, full_uniques, str(err)))

    # @added 20230201 - Feature #4838: functions.metrics.get_namespace_metric.count
    # Although redistimeseries_roomba handles setting labelled_metrics as
    # inactive, the normal metrics roomba does not, so it is done here.
    if newly_found_inactive_metrics:
        logger.info('%s :: setting %s newly found inactive metrics as inactive in the database' % (
            function_str, str(found_new_inactive_metrics)))
        dry_run = False
        try:
            metrics_set_as_inactive = set_metrics_as_inactive('analyzer', newly_found_inactive_metrics, [], dry_run)
        except Exception as err:
            logger.error('error :: %s :: set_metrics_as_inactive failed - %s' % (
                function_str, str(err)))
        if metrics_set_as_inactive:
            logger.info('%s :: set %s metrics as inactive in the database' % (
                function_str, str(len(metrics_set_as_inactive))))

    # @added 20230626 - Task #4962: Build and test skyline v4.0.0
    # When a Skyline instance starts there are no inactive metrics
    inactive_metric_ids_redis_set_exists = False
    try:
        inactive_metric_ids_redis_set_exists = self.redis_conn.exists('metrics_manager.inactive_metric_ids')
    except Exception as err:
        logger.error('error :: %s :: exists failed on metrics_manager.inactive_metric_ids Redis set - %s' % (
            function_str, err))

    # @modified 20230626 - Task #4962: Build and test skyline v4.0.0
    # When a Skyline instance starts there are no inactive metrics so
    # only rename if it exists
    if inactive_metric_ids_redis_set_exists:
        try:
            self.redis_conn.rename('metrics_manager.inactive_metric_ids', 'aet.metrics_manager.inactive_metric_ids')
            logger.info('%s :: created the aet.metrics_manager.inactive_metric_ids Redis set' % function_str)
        except Exception as err:
            logger.error('error :: %s :: failed to created the aet.metrics_manager.inactive_metric_ids Redis set - %s' % (
                function_str, err))

    redis_inactive_metric_ids = list(inactive_metric_ids)
    if db_inactive_metric_ids:
        redis_inactive_metric_ids = list(db_inactive_metric_ids)

    try:
        self.redis_conn_decoded.sadd('metrics_manager.inactive_metric_ids', *set(redis_inactive_metric_ids))
        logger.info('%s :: created the metrics_manager.inactive_metric_ids Redis set with %s metric ids' % (
            function_str, str(len(redis_inactive_metric_ids))))
    except Exception as err:
        logger.error('%s :: failed to set metrics_manager.inactive_metric_ids Redis set - %s' % (
            function_str, str(err)))

    # @added 20230131 - Feature #4838: functions.metrics.get_namespace_metric.count
    #                   Task #2732: Prometheus to Skyline
    #                   Branch #4300: prometheus
    inactive_metric_names_with_ids_errors = []
    if redis_inactive_metric_ids:
        for metric_id in redis_inactive_metric_ids:
            try:
                inactive_metric_names_with_ids[db_all_metric_ids_with_names[metric_id]] = metric_id
            except Exception as err:
                err_msg = 'failed to determine metric for metric id: %s' % str(metric_id)
                inactive_metric_names_with_ids_errors.append([err_msg, err])
        if inactive_metric_names_with_ids_errors:
            logger.error('error :: %s :: creating inactive_metric_names_with_ids reports errors, last 3 reported: %s' % (
                function_str, str(inactive_metric_names_with_ids_errors[-3:])))
    if inactive_metric_names_with_ids:
        try:
            self.redis_conn.rename('metrics_manager.inactive_metric_names_with_ids', 'aet.metrics_manager.inactive_metric_names_with_ids')
            logger.info('%s :: created the aet.metrics_manager.inactive_metric_names_with_ids Redis hash' % function_str)
        except Exception as err:
            logger.error('%s :: failed to created the aet.metrics_manager.inactive_metric_names_with_ids Redis hash - %s' % (
                function_str, err))
        try:
            self.redis_conn_decoded.hset('metrics_manager.inactive_metric_names_with_ids', mapping=inactive_metric_names_with_ids)
            logger.info('%s :: created the metrics_manager.inactive_metric_names_with_ids Redis hash with %s metrics' % (
                function_str, str(len(inactive_metric_names_with_ids))))
        except Exception as err:
            logger.error('%s :: failed to set metrics_manager.inactive_metric_names_with_ids Redis set - %s' % (
                function_str, str(err)))

    logger.info('%s :: completed in %s seconds' % (
        function_str, str(time() - start)))

    return reactivate_metric_ids
