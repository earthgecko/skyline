"""
manage_custom_algorithm_only_metrics.py
"""
import logging

from functions.database.queries.get_apps import get_apps
from matched_or_regexed_in_list import matched_or_regexed_in_list

skyline_app = 'analyzer'
skyline_app_logger = '%sLog' % skyline_app
logger = logging.getLogger(skyline_app_logger)


# @added 20250321 - Feature #5611: custom_algorithm_only
def manage_custom_algorithm_only_metrics(self, CUSTOM_ALGORITHMS, base_names):
    """
    Determines what metrics should be run as custom_algorithm_only for each app
    and creates Redis sets for each app

    :param self: the self object
    :param CUSTOM_ALGORITHMS: the CUSTOM_ALGORITHMS dict to use, this make not
        be only the settings.CUSTOM_ALGORITHMS but one which has also been
        constructed from custom_algorithms in external_settings
    :param base_names: the active base_names list
    :type self: object
    :type CUSTOM_ALGORITHMS: dict
    :type base_names: list
    :return: custom_algorithms_only_per_app
    :rtype: dict

    """
    function_str = 'manage_custom_algorithm_only_metrics'
    custom_algorithms_only_per_app = {}

    logger.info('metrics_manager :: %s :: running' % function_str)

    apps_dict = {}
    try:
        apps_dict = get_apps(skyline_app)
    except Exception as err:
        logger.error('error :: metrics_manager :: %s :: get_apps failed - %s' % (
            function_str, err))
    apps = list(apps_dict.keys())

    for custom_algorithm in CUSTOM_ALGORITHMS:
        custom_algorithm_only = False
        if 'custom_algorithm_only' in CUSTOM_ALGORITHMS[custom_algorithm].keys():
            custom_algorithm_only = True
        if 'algorithm_parameters' in CUSTOM_ALGORITHMS[custom_algorithm].keys():
            if 'custom_algorithm_only' in CUSTOM_ALGORITHMS[custom_algorithm]['algorithm_parameters'].keys():
                custom_algorithm_only = True
        custom_algorithm_metrics = []
        if custom_algorithm_only:
            custom_algorithm_namespaces = []
            try:
                custom_algorithm_namespaces = CUSTOM_ALGORITHMS[custom_algorithm]['namespaces']
            except Exception as err:
                logger.error('error :: metrics_manager :: failed to determine namespaces from custom_algorithm: %s, err: %s' % (
                    custom_algorithm, err))
            if custom_algorithm_namespaces:
                for base_name in base_names:
                    pattern_match = False
                    try:
                        pattern_match, metric_matched_by = matched_or_regexed_in_list(skyline_app, base_name, custom_algorithm_namespaces)
                        if not pattern_match:
                            continue
                    except Exception as err:
                        logger.error('error :: metrics_manager :: failed match %s, err: %s' % (
                            base_name, err))
                    custom_algorithm_metrics.append(base_name)
        exclude_namespaces = []
        if 'exclude_namespaces' in CUSTOM_ALGORITHMS[custom_algorithm]['algorithm_parameters'].keys():
            try:
                exclude_namespaces = CUSTOM_ALGORITHMS[custom_algorithm]['algorithm_parameters']['exclude_namespaces']
            except Exception as err:
                logger.error('error :: metrics_manager :: failed to determine exclude_namespaces from custom_algorithm: %s, err: %s' % (
                    custom_algorithm, err))
            if isinstance(exclude_namespaces, str):
                exclude_namespaces = [exclude_namespaces]
            if not isinstance(exclude_namespaces, list):
                exclude_namespaces = []
        exclude_metrics = []
        if exclude_namespaces and custom_algorithm_metrics:
            for base_name in custom_algorithm_metrics:
                pattern_match = False
                try:
                    pattern_match, metric_matched_by = matched_or_regexed_in_list(skyline_app, base_name, exclude_namespaces)
                    if not pattern_match:
                        continue
                except Exception as err:
                    logger.error('error :: metrics_manager :: failed match %s, err: %s' % (
                        base_name, err))
                exclude_metrics.append(base_name)
        if exclude_metrics:
            exclude_metrics_set = set(exclude_metrics)
            custom_algorithm_metrics = [base_name for base_name in custom_algorithm_metrics if base_name not in exclude_metrics_set]
        # Although it may seem that Redis checks should only be done if there
        # are actually custom_algorithm_metrics, if that were the case and
        # custom_algorithm_only were removed from settings or external_settings
        # they would never be removed.  However an expiry is set on these sets
        # so that is custom_algorithm_only metrics and checks are removed this
        # function is no longer called, they will be removed when the expiry is
        # reached.  If Analyzer failed and metrics_manager was not run for
        # longer than the expiry period they would be removed, but on the first
        # metrics_manager run they would be readded.
        custom_algorithm_metrics_set = set(custom_algorithm_metrics)
        apps_done = []
        for app in apps:
            if app in apps_done:
                continue
            set_expiry = False
            if len(custom_algorithm_metrics_set) > 0:
                set_expiry = True
            else:
                continue
            app_redis_custom_algorithm_only_metrics = []
            redis_set = 'metrics_manager.%s.custom_algorithm_only.metrics' % app
            try:
                app_redis_custom_algorithm_only_metrics = self.redis_conn_decoded.smembers(redis_set)
            except Exception as err:
                logger.error('error :: metrics_manager :: %s :: smembers of %s failed, err: %s' % (
                    function_str, redis_set, err))
            if not isinstance(app_redis_custom_algorithm_only_metrics, set):
                app_redis_custom_algorithm_only_metrics = set(app_redis_custom_algorithm_only_metrics)
            logger.info('metrics_manager :: %s :: %s Redis set has %s members' % (
                function_str, redis_set, str(len(app_redis_custom_algorithm_only_metrics))))
            if app in CUSTOM_ALGORITHMS[custom_algorithm]['use_with']:
                custom_algorithms_only_per_app[app] = custom_algorithm_metrics
                use_custom_algorithm_metrics_set = custom_algorithm_metrics_set
            else:
                use_custom_algorithm_metrics_set = set()
            removed_from_redis_set = 0
            to_remove_from_redis = list(app_redis_custom_algorithm_only_metrics - use_custom_algorithm_metrics_set)
            if to_remove_from_redis:
                try:
                    removed_from_redis_set = self.redis_conn_decoded.srem(redis_set, *set(to_remove_from_redis))
                    logger.info('metrics_manager :: %s :: removed %s metrics from %s Redis set' % (
                        function_str, str(removed_from_redis_set), redis_set))
                except Exception as err:
                    logger.error('error :: metrics_manager :: %s :: srem of %s failed, err: %s' % (
                        function_str, redis_set, err))
            to_add_to_redis = list(use_custom_algorithm_metrics_set - app_redis_custom_algorithm_only_metrics)
            added_to_redis_set = 0
            if to_add_to_redis:
                try:
                    added_to_redis_set = self.redis_conn_decoded.sadd(redis_set, *set(to_add_to_redis))
                    logger.info('metrics_manager :: %s :: added %s metrics to %s Redis set' % (
                        function_str, str(added_to_redis_set), redis_set))
                except Exception as err:
                    logger.error('error :: metrics_manager :: %s :: sadd of %s failed, err: %s' % (
                        function_str, redis_set, err))
            if set_expiry:
                try:
                    self.redis_conn_decoded.expire(redis_set, 600)
                except Exception as err:
                    logger.error('error :: metrics_manager :: %s :: expire failed on %s, err: %s' % (
                        function_str, redis_set, err))
            if added_to_redis_set or removed_from_redis_set:
                logger.info('metrics_manager :: %s :: managed %s Redis set, added_to_redis_set: %s, removed_from_redis_set: %s' % (
                    function_str, redis_set, str(added_to_redis_set),
                    str(removed_from_redis_set)))
            apps_done.append(app)

    logger.info('metrics_manager :: %s :: complete' % function_str)

    return custom_algorithms_only_per_app
