"""
remove_metrics_from_redis.py

Removes metrics from Redis sets, hashes and metrics keys

"""
import logging
import traceback

from skyline_functions import get_redis_conn_decoded
from matched_or_regexed_in_list import matched_or_regexed_in_list
from functions.metrics.get_base_names_and_metric_ids import get_base_names_and_metric_ids
from settings import FULL_NAMESPACE
# @added 20220803 - Task #2732: Prometheus to Skyline
#                   Branch #4300: prometheus
from functions.metrics.get_base_name_from_labelled_metrics_name import get_base_name_from_labelled_metrics_name


# @added 20220224 - Feature #4468: flux - remove_namespace_quota_metrics
#                   Feature #4464: flux - quota - cluster_sync
def remove_metrics_from_redis(current_skyline_app, metrics, patterns, dry_run):
    """
    Remove metrics from Redis sets, hashes and keys

    :param current_skyline_app: the app calling the function
    :param metrics: a list of base_names
    :param pattern: list of patterns to matched_or_regexed_in_list
    :param dry_run: whether to execute or just report what would be removed
    :type current_skyline_app: str
    :type metrics: list
    :type patterns: list
    :type dry_run: boolean
    :return: removed_metrics
    :rtype: list

    """

    removed_metrics = []
    function_str = 'functions.redis.remove_metrics_from_redis'
    current_skyline_app_logger = current_skyline_app + 'Log'
    current_logger = logging.getLogger(current_skyline_app_logger)

    keys_processed = []
    metrics_to_remove = list(set(metrics))

    current_logger.info('%s :: %s metrics passed to remove from the Redis resources' % (
        function_str, str(len(metrics_to_remove))))

    try:
        redis_conn_decoded = get_redis_conn_decoded(current_skyline_app)
    except Exception as err:
        current_logger.error(traceback.format_exc())
        current_logger.error('error :: %s :: failed to get Redis connection - %s' % (
            function_str, err))
        return removed_metrics

    base_names_with_ids = {}
    try:
        base_names_with_ids = get_base_names_and_metric_ids(current_skyline_app)
    except Exception as err:
        current_logger.error(traceback.format_exc())
        current_logger.error('error :: %s :: get_base_names_and_metric_ids failed - %s' % (
            function_str, str(err)))

    pattern_errors = []
    traceback_sample = None
    if patterns:
        current_logger.info('%s :: determining what additional metrics are to be removed from the Redis resources that match %s' % (
            function_str, str(patterns)))
        for base_name in list(base_names_with_ids.keys()):
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
        current_logger.info('%s :: %s metrics have been determined to remove from the Redis resources' % (
            function_str, str(len(metrics_to_remove))))
    if pattern_errors:
        current_logger.error(traceback_sample)
        current_logger.error('error :: %s :: some matched_or_regexed_in_list errors were reported on pattern - %s, traceback sample above and errors follow' % (
            function_str, str(patterns)))

    removed_metrics = list(set(metrics_to_remove))

    metric_ids_to_remove = []
    metric_specific_namespaces = []
    try:
        full_uniques = '%sunique_metrics' % FULL_NAMESPACE
        unique_remove_metrics = []
        for base_name in metrics_to_remove:

            # @added 20220803 - Task #2732: Prometheus to Skyline
            #                   Branch #4300: prometheus
            # Handle labelled_metrics
            metric_name = None
            if base_name.startswith('labelled_metrics.'):
                metric_name = str(base_name)
                metric_id_str = metric_name.replace('labelled_metrics.', '', 1)
                try:
                    metric_id = int(float(metric_id_str))
                    if metric_id:
                        metric_ids_to_remove.append(metric_id)
                except:
                    pass
                labelled_metric_base_name = None
                try:
                    labelled_metric_base_name = get_base_name_from_labelled_metrics_name(current_skyline_app, metric_name)
                except Exception as err:
                    current_logger.error('error :: %s :: get_base_name_from_labelled_metrics_name failed for %s - %s' % (
                        function_str, metric_name, err))
                if labelled_metric_base_name:
                    unique_remove_metrics.append(labelled_metric_base_name)

            if not metric_name:
                metric_name = '%s%s' % (FULL_NAMESPACE, base_name)
                namespace_elements = base_name.split('.')
                metric_specific_namespaces.append('.'.join(namespace_elements[0:-1]))

            unique_remove_metrics.append(metric_name)

        remove_from_sets = [
            'analyzer.batch_processing_metrics',
            'analyzer.flux_aggregate_metrics',
            'analyzer.flux_zero_fill_metrics',
            'analyzer.non_smtp_alerter_metrics',
            'analyzer.smtp_alerter_metrics',
            'analyzer.mirage_always_metrics',
            'analyzer.unique_base_names',
            'ionosphere.ionosphere_non_smtp_alerter_metrics',
            'ionosphere.ionosphere_smtp_alerter_metrics',
            'horizon.shard.metrics_dropped',
            'horizon.shards.metrics_assigned',
            'flux.sort_and_dedup.metrics',
            'metrics_manager.flux_zero_fill_aggregate_metrics',
            'metrics_manager.flux.aggregate_metrics',
        ]
        for from_set in remove_from_sets:
            keys_processed.append(from_set)
            removed_metrics_count = 0
            try:
                if not dry_run:
                    removed_metrics_count = redis_conn_decoded.srem(from_set, *set(metrics_to_remove))
                    if removed_metrics_count:
                        current_logger.info('%s :: %s metrics were removed from the Redis set %s' % (
                            function_str, str(removed_metrics_count),
                            from_set))
                else:
                    current_logger.info('%s :: DRY RUN :: %s metrics would have been removed from the Redis set %s' % (
                        function_str, str(len(metrics_to_remove)),
                        from_set))
            except Exception as err:
                current_logger.error(traceback.format_exc())
                current_logger.error('error :: %s :: failed to srem %s metrics from Redis set %s - %s' % (
                    function_str, str(len(list(set(metrics_to_remove)))),
                    from_set, err))
        remove_from_unique_sets = [
            full_uniques,
            'mirage.unique_metrics',
            'ionosphere.unique_metrics'
            'analyzer.low_priority_metrics.last_analyzed_timestamp',
            'analyzer.metrics_manager.metrics_fully_populated',
            'analyzer.metrics_manager.metrics_sparsity_decreasing',
            'analyzer.metrics_manager.metrics_sparsity_increasing',
        ]
        for unique_set in remove_from_unique_sets:
            keys_processed.append(unique_set)
            removed_from_set_count = 0
            try:
                if not dry_run:
                    removed_from_set_count = redis_conn_decoded.srem(unique_set, *set(unique_remove_metrics))
                    current_logger.info('%s :: %s metrics were removed from the Redis set %s' % (
                        function_str, str(removed_from_set_count), unique_set))
                else:
                    current_logger.info('%s :: DRY RUN :: %s metrics would have been removed from the Redis set %s' % (
                        function_str, str(len(metrics_to_remove)),
                        unique_set))
            except Exception as err:
                current_logger.error(traceback.format_exc())
                current_logger.error('error :: %s :: failed to srem %s metrics from Redis set %s - %s' % (
                    function_str, str(len(list(set(metrics_to_remove)))),
                    unique_set, err))

        removed_metric_keys = 0
        # Delete the metric timeseries data key - metrics.<base_name>
        for metric_key in unique_remove_metrics:
            deleted = 0
            try:
                if not dry_run:
                    deleted = redis_conn_decoded.delete(metric_key)
                    if deleted:
                        removed_metric_keys += 1
                else:
                    deleted = redis_conn_decoded.type(metric_key)
                    if deleted != 'none':
                        removed_metric_keys += 1
            except Exception as err:
                current_logger.error(traceback.format_exc())
                current_logger.error('error :: %s :: failed to remove %s Redis key - %s' % (
                    function_str, metric_key, err))
        if not dry_run:
            current_logger.info('%s :: %s metric timeseries data keys were deleted from Redis' % (
                function_str, str(removed_metric_keys)))
        else:
            current_logger.info('%s :: DRY RUN :: %s metric timeseries data keys would be deleted from Redis' % (
                function_str, str(removed_metric_keys)))

        # Determine ids of metrics to remove from Redis
        for base_name in metrics_to_remove:
            metric_id = 0
            try:
                metric_id = base_names_with_ids[base_name]
            except KeyError:
                metric_id = 0
            if metric_id:
                metric_ids_to_remove.append(metric_id)

        remove_from_id_hash_keys = [
            'metrics_manager.ids_with_metric_names',
            'aet.metrics_manager.ids_with_metric_names',
        ]
        remove_from_hash_keys = [
            'aet.metrics_manager.ids_with_metric_names',
            'aet.metrics_manager.metric_names_with_ids',
            'aet.metrics_manager.derivative_metrics.timestamps',
            'analyzer.metrics.last_analyzed_timestamp',
            'analyzer.metrics.last_timeseries_timestamp',
            'analyzer.metrics_manager.custom_stale_periods',
            'analyzer.metrics_manager.hash_key.metrics_data_sparsity',
            'analyzer.metrics_manager.resolutions',
            'horizon.do_not_skip_metrics',
            'horizon.skip_metrics',
            'flux.aggregate_metrics.last_flush',
            'flux.aggregate_namespaces.settings',
            'metrics_manager.metric_names_with_ids',
            'metrics_manager.ids_with_metric_names',
            'metrics_manager.boundary_metrics',
            'metrics_manager.flux.aggregate_metrics.hash',
            'metrics_manager.flux.aggregate_namespaces.settings',
            'metrics_manager.namespaces',
            'mirage.hash_key.metrics_expiration_times',
            'mirage.hash_key.metrics_resolutions',
            'mirage.trigger_history',
            'luminosity.related_metrics.metrics',
        ]
        for hash_key in remove_from_hash_keys:
            keys_processed.append(hash_key)
            removed_from_hash_count = 0
            to_remove_list = list(metrics_to_remove)
            if hash_key in remove_from_id_hash_keys:
                to_remove_list = list(metric_ids_to_remove)
            if hash_key == 'metrics_manager.namespaces':
                to_remove_list = list(metric_specific_namespaces)
            if to_remove_list:
                try:
                    if not dry_run:
                        removed_from_hash_count = redis_conn_decoded.hdel(hash_key, *set(to_remove_list))
                        if removed_from_hash_count:
                            current_logger.info('%s :: %s metrics were removed from the Redis hash %s' % (
                                function_str, str(removed_from_hash_count), hash_key))
                    else:
                        current_logger.info('%s :: %s metrics would have been attempted to be removed from the Redis hash %s' % (
                            function_str, str(len(to_remove_list)), hash_key))

                except Exception as err:
                    current_logger.error(traceback.format_exc())
                    current_logger.error('error :: %s :: failed to remove %s metrics from the Redis hash key %s - %s' % (
                        function_str, str(len(metrics_to_remove)), hash_key, err))

        remove_from_unique_hash_keys = [
            'analyzer.metrics_manager.hash_key.metrics_data_sparsity',
            'analyzer.metrics_manager.hash_key.metrics_timestamp_resolutions',
        ]
        for hash_key in remove_from_unique_hash_keys:
            keys_processed.append(hash_key)
            removed_from_hash_count = 0
            if unique_remove_metrics:
                try:
                    if not dry_run:
                        removed_from_hash_count = redis_conn_decoded.hdel(hash_key, *set(unique_remove_metrics))
                        if removed_from_hash_count:
                            current_logger.info('%s :: %s metrics were removed from the Redis hash %s' % (
                                function_str, str(removed_from_hash_count), hash_key))
                    else:
                        current_logger.info('%s :: %s metrics would have been attempted to be removed from the Redis hash %s' % (
                            function_str, str(len(unique_remove_metrics)), hash_key))
                except Exception as err:
                    current_logger.error(traceback.format_exc())
                    current_logger.error('error :: metrics_manager :: failed to remove %s metrics from the Redis hash key %s - %s' % (
                        str(len(metrics_to_remove)), hash_key, err))
    except Exception as err:
        current_logger.error(traceback.format_exc())
        current_logger.error('error :: %s :: failed to remove %s metrics from Redis - %s' % (
            function_str, str(len(metrics_to_remove)), err))

    # Best effort to remove metrics from all keys, because this function may not
    # always be updated when a Redis hash key or set is added.  This does not
    # any keys that are keyed on metric id, other than the ones declared above,
    # it only removes base_name and metrics.base_name keys from any hash keys or
    # sets in which those keys exist.
    all_names_to_remove = metrics_to_remove + unique_remove_metrics

    all_keys = list(redis_conn_decoded.keys('*'))
    # @modified 20230330 - Feature #4468: flux - remove_namespace_quota_metrics
    # Use scan_iter
    # all_keys = []
    # try:
    #     for key in redis_conn_decoded.scan_iter('*'):
    #         all_keys.append(key)
    # except Exception as err:
    #     current_logger.error('error :: %s :: failed to scan_iter Redis - %s' % (
    #         function_str, err))

    # @modified 20230330 - Feature #4468: flux - remove_namespace_quota_metrics
    # Reduce keys to check
    current_logger.info('%s :: determining keys, hashes and sets from %s Redis keys' % (
        function_str, str(len(all_keys))))
    skip_keys = [
        'aet.horizon.metrics_received',
        'horizon.metrics_received',
        'flux.aggregator.queue',
        'flux.metrics_data_sent',
        'flux.prometheus_metrics',
        'flux.workers.',
        'horizon.prometheus_metrics.',
        'luminosity.cloudburst.',
        'flux.prometheus_metadata',
        'flux.prometheus_namespace_cardinality.last',
        'vista.',
        'analyzer_labelled_metrics.last_3sigma_anomalous.',
        'namespace.analysed_events.',
        'horizon.worker',
        'ionosphere.learn_repetitive_patterns.',
        'aet.metrics_manager.inactive_metric_ids',
        'luminosity.classify_anomalies',
        'analyzer_labelled_metrics.last_mad_anomalous.',
        'aet.',
        'metrics_manager.prometheus.metrics_type.',
        'analyzer_labelled_metrics.boring.',
        'analyzer_labelled_metrics.stale.',
        'analyzer_labelled_metrics.no_new_data_metrics.',
        'analyzer_labelled_metrics.tooshort_metrics.',
        'analyzer.boring',
        'analyzer.too_short',
        'analyzer.run_time',
        'analyzer.real_anomalous_metrics',
        'luminosity.dropped_correlations.',
        'analyzer.total_analyzed',
        'analyzer.not_anomalous_metrics',
        'thunder.alerted_on.stale_metrics',
        'ionosphere.panorama.not_anomalous_metrics',
        'webapp.remove_namespace_quota_metrics.',
        'mirage_labelled_metrics.1.',
        'mirage_labelled_metrics.2.',
        'mirage_labelled_metrics.3.',
        'mirage_labelled_metrics.4.',
        'mirage_labelled_metrics.5.',
        'mirage_labelled_metrics.6.',
        'mirage_labelled_metrics.7.',
        'mirage_labelled_metrics.8.',
        'metrics_manager.algorithms.ids',
        'luminosity.metric_group.last_updated',
        'analyzer.illuminance.all.',
    ]

    hashes_and_sets = {}
    redis_keys_to_remove = []
    for key in all_keys:
        if key in keys_processed:
            continue
        # @modified 20230330 - Feature #4468: flux - remove_namespace_quota_metrics
        # Reduce keys to check
        if '.illuminance.2' in key:
            continue
        key_type = None
        if FULL_NAMESPACE:
            if key.startswith(FULL_NAMESPACE):
                key_type = 'string'
        if key.startswith('labelled_metrics.'):
            key_type = 'TSDB-TYPE'
        if not key_type:
            skip = False
            skip_key_list = [1 for skip_key in skip_keys if key.startswith(skip_key)]
            if sum(skip_key_list):
                skip = True
            if skip:
                continue
        if not key_type:
            key_type = redis_conn_decoded.type(key)

        if key_type in ['hash', 'set']:
            hashes_and_sets[key] = key_type
            # @added 20230330 - Feature #4468: flux - remove_namespace_quota_metrics
            # Continue if it is a hash or set because no individual metric will
            # have a has or set specifically dedicated to the metric
            continue

        # Remove metrics specific keys
        # @modified 20230330 - Feature #4468: flux - remove_namespace_quota_metrics
        # Use list comprehensions
        # for metric in all_names_to_remove:
        #     # Specific metric related keys
        #     if key.endswith(metric):
        #         redis_keys_to_remove.append(key)
        #         break
        #     # If the metric name is in the key name at all it is a metric
        #     # related key
        #     if metric in key:
        #         redis_keys_to_remove.append(key)
        #         break
        in_keys = [metric for metric in all_names_to_remove if metric in key]
        if in_keys:
            redis_keys_to_remove = redis_keys_to_remove + in_keys

    current_logger.info('%s :: best effort removing metrics from %s other Redis hash keys and sets' % (
        function_str, str(len(hashes_and_sets))))
    for key in list(hashes_and_sets.keys()):
        key_type = hashes_and_sets[key]
        keys_processed.append(key)
        removed_count = 0
        if all_names_to_remove:
            try:
                if not dry_run:
                    if key_type == 'hash':
                        removed_count = redis_conn_decoded.hdel(key, *set(all_names_to_remove))
                    if key_type == 'set':
                        removed_count = redis_conn_decoded.srem(key, *set(all_names_to_remove))
                    if removed_count:
                        current_logger.info('%s :: %s metrics were removed from the Redis %s %s' % (
                            function_str, str(removed_count), key_type, key))
                else:
                    current_logger.info('%s :: DRY RUN :: %s metrics would have been attempted to be removed from the Redis %s %s' % (
                        function_str, str(len(all_names_to_remove)), key_type, key))
            except Exception as err:
                current_logger.error(traceback.format_exc())
                current_logger.error('error :: %s :: failed to remove %s metrics from the Redis %s %s - %s' % (
                    function_str, str(len(metrics_to_remove)), key_type, key, err))
    current_logger.info('%s :: %s metrics were removed from %s Redis hash keys and sets' % (
        function_str, str(len(removed_metrics)), str(len(keys_processed))))
    for key in redis_keys_to_remove:
        if not dry_run:
            try:
                redis_conn_decoded.delete(key)
                current_logger.info('%s :: deleted Redis key: %s' % (
                    function_str, key))
            except Exception as err:
                current_logger.error(traceback.format_exc())
                current_logger.error('error :: %s :: failed to delete Redis key %s - %s' % (
                    function_str, key, err))
        else:
            current_logger.info('%s :: DRY_RUN :: would have deleted Redis key: %s' % (
                function_str, key))
    if redis_keys_to_remove:
        if not dry_run:
            current_logger.info('%s :: %s specific metric related keys were removed from Redis' % (
                function_str, str(len(redis_keys_to_remove))))
        else:
            current_logger.info('%s :: DRY RUN :: %s specific metric related keys would have been removed from Redis' % (
                function_str, str(len(redis_keys_to_remove))))

    return removed_metrics
