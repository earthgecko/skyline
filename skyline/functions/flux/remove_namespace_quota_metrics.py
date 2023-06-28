import logging
import traceback
from ast import literal_eval

from skyline_functions import get_redis_conn_decoded
from matched_or_regexed_in_list import matched_or_regexed_in_list
from functions.database.queries.set_metrics_as_inactive import set_metrics_as_inactive
from functions.redis.remove_metrics_from_redis import remove_metrics_from_redis


# @added 20220224 - Feature #4468: flux - remove_namespace_quota_metrics
#                   Feature #4464: flux - quota - cluster_sync
def remove_namespace_quota_metrics(
        current_skyline_app, namespace, metrics, patterns, dry_run):
    """
    Remove any entries from a flux.quota.namespace_metrics.<namespace> Redis set

    :param current_skyline_app: the app calling the function
    :param namespace: the top level namespace
    :param metrics: a list of base_names
    :param pattern: list of patterns to matched_or_regexed_in_list
    :param dry_run: whether to execute or just report what would be removed
    :type current_skyline_app: str
    :type namespace: str
    :type metrics: list
    :type patterns: list
    :type dry_run: boolean
    :return: removed_metrics
    :rtype: list

    """

    removed_metrics = []
    function_str = 'functions.flux.remove_namespace_quota_metrics'
    current_skyline_app_logger = current_skyline_app + 'Log'
    current_logger = logging.getLogger(current_skyline_app_logger)

    namespace_metrics_quota_key = 'flux.quota.namespace_metrics.%s' % str(namespace)
    current_logger.info('%s :: removing metrics from the %s Redis set, dry_run: %s' % (
        function_str, namespace_metrics_quota_key, str(dry_run)))

    try:
        redis_conn_decoded = get_redis_conn_decoded(current_skyline_app)
    except Exception as err:
        current_logger.error(traceback.format_exc())
        current_logger.error('error :: %s :: failed to get Redis connection - %s' % (
            function_str, err))
        return removed_metrics

    quota_metrics = []
    try:
        quota_metrics = list(redis_conn_decoded.smembers(namespace_metrics_quota_key))
    except Exception as err:
        current_logger.error(traceback.format_exc())
        current_logger.error('error :: %s :: failed to get Redis set %s - %s' % (
            function_str, namespace_metrics_quota_key, err))

    if not quota_metrics:
        return removed_metrics

    # @added 20230322 - Feature #4468: flux - remove_namespace_quota_metrics
    # Handle labelled_metrics
    tenant_id_namespace = '_tenant_id="%s"' % str(namespace)
    full_namespace = '%s.' % str(namespace)

    metrics_to_remove = []
    for base_name in metrics:
        # @added 20230322 - Feature #4468: flux - remove_namespace_quota_metrics
        # Handle labelled_metrics
        if tenant_id_namespace in base_name:
            if base_name in quota_metrics:
                metrics_to_remove.append(base_name)
                continue
        if base_name in quota_metrics:
            metrics_to_remove.append(base_name)
            continue

        # if not base_name.startswith(namespace):
        if not base_name.startswith(full_namespace):
            base_name = '%s.%s' % (str(namespace), base_name)
        if base_name in quota_metrics:
            metrics_to_remove.append(base_name)
        
    pattern_errors = []
    traceback_sample = None
    if patterns:
        for base_name in quota_metrics:
            try:
                pattern_match, metric_matched_by = matched_or_regexed_in_list(current_skyline_app, base_name, patterns)
                try:
                    del metric_matched_by
                except:
                    pass
                if pattern_match:
                    metrics_to_remove.append(base_name)
            except Exception as err:
                traceback_sample = traceback.format_exc()
                pattern_errors.append('matched_or_regexed_in_list failed on %s against patterns - %s' % (
                    base_name, err))
    if pattern_errors:
        current_logger.error(traceback_sample)
        current_logger.error('error :: %s :: some matched_or_regexed_in_list errors were reported on pattern - %s, traceback sample above and errors follow' % (
            function_str, str(patterns)))

    # @added 20230322 - Feature #4468: flux - remove_namespace_quota_metrics
    #                   Feature #4464: flux - quota - cluster_sync
    # Handle labelled_metrics, do not sync the flux.quota.namespace_metrics
    # keys with analyzer/metrics_manager.py if there have been removals
    # recently
    namespaces_quota_removed_key = 'flux.removed.quota.namespace_metrics.%s' % str(namespace)
    try:
        redis_conn_decoded.setex(namespaces_quota_removed_key, 300, 1)
        current_logger.info('%s :: setex %s Redis key to a TTL of 300' % (
            function_str, namespaces_quota_removed_key))
    except Exception as err:
        current_logger.error('error :: %s :: failed to setex %s Redis key - %s' % (
            function_str, namespaces_quota_removed_key, err))

    if metrics_to_remove:
        metrics_to_remove = list(set(metrics_to_remove))
        current_logger.info('%s :: %s metrics to be removed from the Redis set %s' % (
            function_str, str(len(metrics_to_remove)),
            namespace_metrics_quota_key))
        try:
            removed_metrics = list(set(metrics_to_remove))
            if not dry_run:
                removed_metrics_count = redis_conn_decoded.srem(namespace_metrics_quota_key, *set(removed_metrics))
                current_logger.info('%s :: %s of %s metrics were removed from the Redis set %s' % (
                    function_str, str(removed_metrics_count),
                    str(len(removed_metrics)), namespace_metrics_quota_key))
            else:
                current_logger.info('%s :: DRY RUN :: %s metrics would have been removed from the Redis set %s' % (
                    function_str, str(len(removed_metrics)),
                    namespace_metrics_quota_key))
        except Exception as err:
            current_logger.error(traceback.format_exc())
            current_logger.error('error :: %s :: failed to srem %s metrics from Redis set %s - %s' % (
                function_str, str(len(list(set(metrics_to_remove)))),
                namespace_metrics_quota_key, err))

        # @added 20230322 - Feature #4468: flux - remove_namespace_quota_metrics
        #                   Feature #4464: flux - quota - cluster_sync
        # Handle the single hash that contains all the flux.quota.namespace_metrics
        # namespaces to allow for the cluster sync to make a single call rather
        # than a call per namespace
        metrics_manager_flux_quota_namespace_metrics = []
        metrics_to_keep = []
        try:
            metrics_manager_flux_quota_namespace_metrics_str = redis_conn_decoded.hget('metrics_manager.flux.quota.namespace_metrics', namespace)
            if metrics_manager_flux_quota_namespace_metrics_str:
                metrics_manager_flux_quota_namespace_metrics = literal_eval(metrics_manager_flux_quota_namespace_metrics_str)
            current_logger.info('%s :: got %s metrics for %s namespace from metrics_manager.flux.quota.namespace_metrics' % (
                function_str, str(len(metrics_manager_flux_quota_namespace_metrics)),
                namespace))
        except Exception as err:
            current_logger.error('error :: %s :: failed to setex %s Redis key - %s' % (
                function_str, namespaces_quota_removed_key, err))
        if metrics_manager_flux_quota_namespace_metrics:
            metrics_to_keep = [metric for metric in metrics_manager_flux_quota_namespace_metrics if metric not in removed_metrics]
            if metrics_to_keep and len(metrics_to_keep) != len(metrics_manager_flux_quota_namespace_metrics):
                if not dry_run:
                    try:
                        redis_conn_decoded.hset('metrics_manager.flux.quota.namespace_metrics', namespace, str(metrics_to_keep))
                        current_logger.info('%s :: updated %s namespace in metrics_manager.flux.quota.namespace_metrics with %s metrics_to_keep' % (
                            function_str, namespace, str(len(metrics_to_keep))))
                    except Exception as err:
                        current_logger.error('error :: %s :: failed to hset metrics_manager.flux.quota.namespace_metrics Redis hash for namespace %s - %s' % (
                            function_str, namespace, err))
                else:
                    current_logger.info('%s :: DRY RUN :: would update %s namespace in metrics_manager.flux.quota.namespace_metrics with %s metrics_to_keep' % (
                        function_str, namespace, str(len(metrics_to_keep))))
        else:
            current_logger.warning('warning :: %s :: nothing to update for %s namespace in metrics_manager.flux.quota.namespace_metrics' % (
                function_str, namespace))

        # Set to inactive in the database before removing from the Redis keys
        # because if not when the set_metrics_as_inactive tries to determine
        # ids from the Redis key
        metrics_set_as_inactive = []
        try:
            current_logger.info('%s :: setting %s metrics as inactive in the database' % (
                function_str, str(len(removed_metrics))))
            metrics_set_as_inactive = set_metrics_as_inactive(current_skyline_app, [], removed_metrics, dry_run)
        except Exception as err:
            current_logger.error(traceback.format_exc())
            current_logger.error('error :: %s :: set_metrics_as_inactive failed - %s' % (
                function_str, err))
        if len(metrics_set_as_inactive) != len(removed_metrics):
            current_logger.error('error :: %s :: the number of metrics reported as set as inactive does not match the number of removed_metrics' % (
                function_str))

        flux_not_skipped_metrics_key = 'flux.not_skipped_metrics.%s' % str(namespace)
        flux_skipped_metrics_key = 'flux.skipped_metrics.%s' % str(namespace)
        skip_keys = [flux_not_skipped_metrics_key, flux_skipped_metrics_key]
        for hash_key in skip_keys:
            removed_from_hash_count = 0
            try:
                if not dry_run:
                    removed_from_hash_count = redis_conn_decoded.hdel(hash_key, *set(metrics_to_remove))
                    if removed_from_hash_count:
                        current_logger.info('%s :: %s metrics were removed from the Redis hash %s' % (
                            function_str, str(removed_from_hash_count), hash_key))
                else:
                    current_logger.info('%s :: DRY RUN :: %s metrics would have been removed from the Redis hash %s' % (
                        function_str, str(len(metrics_to_remove)), hash_key))
            except Exception as err:
                current_logger.error(traceback.format_exc())
                current_logger.error('error :: %s :: failed to remove %s metrics from the Redis hash key %s - %s' % (
                    function_str, str(len(metrics_to_remove)), hash_key, err))

        removed_metrics_from_redis = []
        try:
            removed_metrics_from_redis = remove_metrics_from_redis(current_skyline_app, metrics_to_remove, [], dry_run)
        except Exception as err:
            current_logger.error(traceback.format_exc())
            current_logger.error('error :: %s :: remove_metrics_from_redis failed for %s - %s' % (
                function_str, namespace, err))
        if removed_metrics_from_redis:
            if not dry_run:
                current_logger.info('%s :: %s metrics were removed from the Redis resources for %s' % (
                    function_str, str(len(removed_metrics_from_redis)), namespace))
            else:
                current_logger.info('%s :: DRY RUN :: %s metrics would have been removed from the Redis resources for %s' % (
                    function_str, str(len(removed_metrics_from_redis)), namespace))
    else:
        current_logger.info('%s :: no metrics were found to remove from the Redis set %s' % (
            function_str, namespace_metrics_quota_key))

    return removed_metrics
