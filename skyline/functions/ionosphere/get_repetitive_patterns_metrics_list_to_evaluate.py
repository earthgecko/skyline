"""
get_repetitive_patterns_metrics_list_to_evaluate.py
"""
import logging
import copy
from time import time

import settings

from matched_or_regexed_in_list import matched_or_regexed_in_list
# @added 20221215 - Feature #4658: ionosphere.learn_repetitive_patterns
from functions.settings.get_external_settings import get_external_settings
from functions.metrics.get_base_names_and_metric_ids import get_base_names_and_metric_ids
# @added 20240308 - Feature #5304: ionosphere.find_repetitive_patterns
from skyline_functions import get_redis_conn_decoded

# @modified 20240308 - Feature #5304: ionosphere.find_repetitive_patterns
# Added parent_namespace
def get_repetitive_patterns_metrics_list_to_evaluate(current_skyline_app, metrics, parent_namespace=None):
    """
    Return a list of metrics that are to be included in repetitive pattern
    learning.

    :param current_skyline_app: the app calling the function
    :param metrics: a list of metrics to evaluate whether to include in learning.
    :param parent_namespace: a parent namespace
    :type current_skyline_app: str
    :type metrics: list
    :type namespace: str
    :return: (include_metrics, exclude_metrics)
    :rtype: tuple

    """

    function_str = 'functions.ionosphere.get_repetitive_patterns_metrics_list_to_evaluate'
    current_skyline_app_logger = current_skyline_app + 'Log'
    current_logger = logging.getLogger(current_skyline_app_logger)
    start = time()

    # @added 20240308 - Feature #5304: ionosphere.find_repetitive_patterns
    # Because this can be a fairly expensive operation, create some Redis cache
    # keys and only operated if they do not exist
    cache_key_exists = False
    within_ttl = False
    if not parent_namespace:
        try:
            redis_conn_decoded = get_redis_conn_decoded(current_skyline_app)
        except Exception as err:
            current_logger.error('error :: %s :: get_redis_conn_decoded failed, err: %s' % (
                function_str, err))
        try:
            cache_key_exists = redis_conn_decoded.exists('ionosphere.repetitive_patterns_metrics.include_metrics')
        except Exception as err:
            current_logger.error('error :: %s :: exists failed on ionosphere.repetitive_patterns_metrics.include_metrics, err: %s' % (
                function_str, err))
        if cache_key_exists:
            try:
                ttl = redis_conn_decoded.ttl('ionosphere.repetitive_patterns_metrics.include_metrics')
                current_logger.info('%s :: ionosphere.repetitive_patterns_metrics.include_metrics exists with TTL %s' % (
                    function_str, str(ttl)))
                if ttl:
                    if ttl > 3:
                        within_ttl = True
            except Exception as err:
                current_logger.error('error :: %s :: ttl failed on ionosphere.repetitive_patterns_metrics.include_metrics, err: %s' % (
                    function_str, err))
        if not cache_key_exists:
            try:
                cache_key_exists = redis_conn_decoded.exists('ionosphere.repetitive_patterns_metrics.exclude_metrics')
            except Exception as err:
                current_logger.error('error :: %s :: exists failed on ionosphere.repetitive_patterns_metrics.exclude_metrics, err: %s' % (
                    function_str, err))
            if cache_key_exists:
                try:
                    ttl = redis_conn_decoded.ttl('ionosphere.repetitive_patterns_metrics.exclude_metrics')
                    current_logger.info('%s :: ionosphere.repetitive_patterns_metrics.exclude_metrics exists with TTL %s' % (
                        function_str, str(ttl)))
                    if ttl:
                        if ttl > 3:
                            within_ttl = True
                except Exception as err:
                    current_logger.error('error :: %s :: ttl failed on ionosphere.repetitive_patterns_metrics.exclude_metrics, err: %s' % (
                        function_str, err))
            else:
                current_logger.info('%s :: ionosphere.repetitive_patterns_metrics.exclude_metrics does not exist' % (
                    function_str))
        if cache_key_exists and within_ttl:
            include_metrics = []
            try:
                include_metrics = list(redis_conn_decoded.smembers('ionosphere.repetitive_patterns_metrics.include_metrics'))
            except Exception as err:
                current_logger.error('error :: %s :: smembers failed on ionosphere.repetitive_patterns_metrics.include_metrics, err: %s' % (
                    function_str, err))
            exclude_metrics = []
            try:
                exclude_metrics = list(redis_conn_decoded.smembers('ionosphere.repetitive_patterns_metrics.exclude_metrics'))
            except Exception as err:
                current_logger.error('error :: %s :: smembers failed on ionosphere.repetitive_patterns_metrics.exclude_metrics, err: %s' % (
                    function_str, err))
            current_logger.info('%s :: returning cache results - %s metrics to include and %s metrics to exclude in repetitive pattern analysis' % (
                function_str, str(len(include_metrics)), str(len(exclude_metrics))))
            return include_metrics, exclude_metrics

    # @added 20221215 - Feature #4658: ionosphere.learn_repetitive_patterns
    # Added external_settings ['learn_repetitive_patterns']['exclude'] to the
    # exclude_metrics list
    include_metrics = []
    exclude_metrics = []
    if not metrics:
        try:
            base_names_with_ids = get_base_names_and_metric_ids(current_skyline_app)
            metrics = list(base_names_with_ids.keys())
        except Exception as err:
            current_logger.error('error :: %s :: get_base_names_and_metric_ids failed - %s' % (
                function_str, err))

    current_logger.info('%s :: determining which of the %s metrics to include in repetitive pattern learning' % (
        function_str, str(len(metrics))))

    # @added 20240308 - Feature #5304: ionosphere.find_repetitive_patterns
    # Allow to filter by parent_namespace
    if parent_namespace:
        labelled_metrics_str = '_tenant_id="%s"' % str(parent_namespace)
        parent_namespace_metrics = [metric for metric in metrics if metric.startswith(parent_namespace) or labelled_metrics_str in metric]
        metrics = list(parent_namespace_metrics)
        current_logger.info('%s :: filtered %s metrics for parent_namespace: %s' % (
            function_str, str(len(metrics)), str(parent_namespace)))

    dicts_defined = []
    include_and_exclude_dicts = {}
    try:
        IONOSPHERE_REPETITIVE_PATTERNS_INCLUDE = copy.deepcopy(settings.IONOSPHERE_REPETITIVE_PATTERNS_INCLUDE)
        dicts_defined.append('include')
    except:
        IONOSPHERE_REPETITIVE_PATTERNS_INCLUDE = {}
        current_logger.warning('%s :: IONOSPHERE_REPETITIVE_PATTERNS_INCLUDE is not defined in settings.py' % (
            function_str))
    include_and_exclude_dicts['include'] = IONOSPHERE_REPETITIVE_PATTERNS_INCLUDE
    try:
        IONOSPHERE_REPETITIVE_PATTERNS_EXCLUDE = copy.deepcopy(settings.IONOSPHERE_REPETITIVE_PATTERNS_EXCLUDE)
        dicts_defined.append('exclude')
    except:
        IONOSPHERE_REPETITIVE_PATTERNS_EXCLUDE = {}
        current_logger.warning('%s :: IONOSPHERE_REPETITIVE_PATTERNS_EXCLUDE is not defined in settings.py' % (
            function_str))

    # @added 20221215 - Feature #4658: ionosphere.learn_repetitive_patterns
    # Added external_settings ['learn_repetitive_patterns']['exclude'] to the
    # exclude_metrics list
    external_settings = {}
    try:
        external_settings = get_external_settings(current_skyline_app, None, True)
    except Exception as err:
        current_logger.error('error :: %s :: get_external_settings failed - %s' % (
            function_str, err))
    current_logger.info('%s :: got %s external_settings to evaluate' % (
        function_str, str(len(external_settings))))

    # @added 20240308 - Feature #5304: ionosphere.find_repetitive_patterns
    # Allow to filter by parent_namespace
    learn_repetitive_patterns_enabled_external_settings_namespaces = []
    learn_repetitive_patterns_disabled_external_settings_namespaces = []
    external_settings_namespaces = []

    # @added 20221215 - Feature #4658: ionosphere.learn_repetitive_patterns
    # Added external_settings ['learn_repetitive_patterns']['exclude'] to the
    # exclude_metrics list
    external_settings_excludes = {}
    for settings_key in list(external_settings.keys()):
        external_setting_keys = list(external_settings[settings_key].keys())
        external_namespace = str(external_settings[settings_key]['namespace'])
        external_settings_namespaces.append(external_namespace)
        learning_enabled = False
        if 'learn_repetitive_patterns' in external_setting_keys:
            if external_settings[settings_key]['learn_repetitive_patterns']['enabled']:
                # @added 20240308 - Feature #5304: ionosphere.find_repetitive_patterns
                # Allow to filter by parent_namespace
                learn_repetitive_patterns_enabled_external_settings_namespaces.append(external_settings[settings_key]['namespace'])
                learning_enabled = True
                if external_settings[settings_key]['learn_repetitive_patterns']['exclude']:
                    external_settings_excludes[external_namespace] = copy.deepcopy(external_settings[settings_key]['learn_repetitive_patterns']['exclude'])
        if not learning_enabled:
            learn_repetitive_patterns_disabled_external_settings_namespaces.append(external_namespace)

    # @added 20240308 - Feature #5304: ionosphere.find_repetitive_patterns
    # Remove all metrics that are part of an external_settings namespace that
    # does not have learn_repetitive_patterns enabled
    for external_namespace in learn_repetitive_patterns_disabled_external_settings_namespaces:
        external_namespace_str = '%s.' % external_namespace
        labelled_metrics_str = '_tenant_id="%s"' % str(external_namespace)
        non_namespace_metrics = [metric for metric in metrics if not metric.startswith(external_namespace_str) and labelled_metrics_str not in metric]
        metrics = list(non_namespace_metrics)

    # @added 20240308 - Feature #5304: ionosphere.find_repetitive_patterns
    # Allow to filter by parent_namespace
    if parent_namespace:
        if parent_namespace in external_settings_namespaces:
            if parent_namespace in learn_repetitive_patterns_enabled_external_settings_namespaces:
                IONOSPHERE_REPETITIVE_PATTERNS_INCLUDE = {}
                IONOSPHERE_REPETITIVE_PATTERNS_EXCLUDE = {}
                dicts_defined = []
            else:
                current_logger.info('%s :: no includes or excludes defined for parent_namespace: %s, returning no includes and no excludes' % (
                    function_str, parent_namespace))
                return [], []
 
    for namespace in list(external_settings_excludes.keys()):

        # @added 20240308 - Feature #5304: ionosphere.find_repetitive_patterns
        # Allow to filter by parent_namespace
        if parent_namespace:
            if namespace != parent_namespace:
                continue

        items = []
        for item in external_settings_excludes[namespace]:
            if len(item) < 2:
                items.append(item[0])
            else:
                items.append(item)
        namespace_dot = '%s\\.' % namespace
        namespace_quote = '%s"' % namespace
        IONOSPHERE_REPETITIVE_PATTERNS_EXCLUDE[namespace_dot] = {namespace_dot: items}
        IONOSPHERE_REPETITIVE_PATTERNS_EXCLUDE[namespace_quote] = {namespace_quote: items}
        if 'exclude' not in dicts_defined:
            dicts_defined.append('exclude')

    # @added 20240308 - Feature #5304: ionosphere.find_repetitive_patterns
    # Add a log just to record external_settings operation time in the log
    if len(external_settings) > 0:
        current_logger.info('%s :: evaluated %s external_settings' % (
            function_str, str(len(external_settings))))

    include_and_exclude_dicts['exclude'] = IONOSPHERE_REPETITIVE_PATTERNS_EXCLUDE
    if not dicts_defined:
        current_logger.info('%s :: no includes or excludes defined returning %s metrics' % (
            function_str, len(str(metrics))))
        return list(set(metrics)), []

    current_logger.debug('debug :: %s :: include_and_exclude_dicts: %s' % (function_str, str(include_and_exclude_dicts)))

    for operation in ['include', 'exclude']:
        if len(list(include_and_exclude_dicts[operation].keys())) == 0:
            # If no includes/excludes are defined skip and move on
            current_logger.info('%s :: no %ss defined, continuing' % (function_str, operation))
            if operation == 'include':
                include_metrics = list(metrics)
            continue

        # NOTE: this method using the primary_filter_keys below is quite complicated
        #       and was quite difficult to develop and test.  Modify with care.
        primary_filter_keys = list(include_and_exclude_dicts[operation].keys())
        current_logger.info('%s :: checking %s against: %s' % (
            function_str, operation, str(primary_filter_keys)))
        errors = []
        primary_matches = 0
        primaries = {}
        skipped = []
        if operation == 'include':
            metrics_list = list(metrics)
        else:
            metrics_list = list(include_metrics)
        current_logger.info('%s :: checking %s metrics for %s against: %s, ' % (
            function_str, str(len(metrics_list)), operation, str(primary_filter_keys)))
        for metric in metrics_list:
            for primary_filter_key in primary_filter_keys:
                matches = []
                to_match = 1
                # Does it match the primary filter
                try:
                    pattern_match, metric_matched_by = matched_or_regexed_in_list(current_skyline_app, metric, [primary_filter_key])
                    if not pattern_match:
                        continue
                    primary_matches += 1
                    first_matched_by = metric_matched_by['matched_namespace']
                    matches.append([metric_matched_by['matched_namespace'], pattern_match])
                    key_set = None
                    try:
                        key_set = primaries[first_matched_by]
                    except:
                        primaries[first_matched_by] = {}
                    try:
                        primaries[first_matched_by][metric][first_matched_by] = True
                    except:
                        primaries[first_matched_by][metric] = {first_matched_by: True}

                    d = copy.deepcopy(include_and_exclude_dicts[operation][metric_matched_by['matched_namespace']])
                    keys = list(d.keys())
                    to_match += 2
                    for key in keys:
                        skip = False
                        # Does it match the secondary filter keys
                        pattern_match, metric_matched_by = matched_or_regexed_in_list(current_skyline_app, metric, [key])
                        try:
                            primaries[first_matched_by][metric][key] = pattern_match
                        except:
                            primaries[first_matched_by][metric] = {}
                            primaries[first_matched_by][metric][key] = pattern_match
                        if pattern_match:
                            matches.append([key, metric_matched_by['matched_namespace']])
                        else:
                            # to_match += 1
                            primaries[first_matched_by][metric]['matches'] = matches
                            primaries[first_matched_by][metric]['to_match'] = to_match
                            primaries[first_matched_by][metric]['matches_count'] = len(matches)
                            continue
                        primaries[first_matched_by][metric]['matches'] = matches
                        primaries[first_matched_by][metric]['matches_count'] = len(matches)
                        if isinstance(d[key], list):
                            pattern = d[key]
                            if not d[key]:
                                pattern = [key]
                            # to_match += 1
                            pattern_match, metric_matched_by = matched_or_regexed_in_list(current_skyline_app, metric, pattern)
                            primaries[first_matched_by][metric][metric_matched_by['matched_namespace']] = pattern_match
                            primaries[first_matched_by][metric]['matches'] = matches
                            if pattern_match:
                                matches.append([pattern, metric_matched_by['matched_namespace']])
                            else:
                                continue
                        if len(matches) >= to_match:
                            if operation == 'include':
                                include_metrics.append(metric)
                            else:
                                exclude_metrics.append(metric)
                            continue
                        if isinstance(d[key], dict):
                            for k, v in d[key].items():
                                if skip:
                                    if operation == 'include':
                                        if metric in include_metrics:
                                            include_metrics.remove(metric)
                                    else:
                                        if metric in exclude_metrics:
                                            exclude_metrics.remove(metric)
                                    break
                                # TODO - Need to debug and fix the _NOT
                                if k == '_NOT' and isinstance(v, dict):
                                    skip_matches_count = len(list(d[key][k].keys()))
                                    skip_matches = []
                                    for nk, nv in d[key][k].items():
                                        pattern_match, metric_matched_by = matched_or_regexed_in_list(current_skyline_app, metric, [nk])
                                        if pattern_match:
                                            pattern_match, metric_matched_by = matched_or_regexed_in_list(current_skyline_app, metric, nv)
                                            if pattern_match:
                                                skip_matches.append(nv)
                                    if len(skip_matches) == skip_matches_count:
                                        skip = True
                                        primaries[first_matched_by][metric]['skip'] = skip
                                        matches = []
                                        skipped.append(metric)
                                        if operation == 'include':
                                            if metric in include_metrics:
                                                include_metrics.remove(metric)
                                        else:
                                            if metric in exclude_metrics:
                                                exclude_metrics.remove(metric)
                                        break
                                if isinstance(v, dict) and not v:
                                    pattern = [k]
                                    to_match += 1
                                    pattern_match, metric_matched_by = matched_or_regexed_in_list(current_skyline_app, metric, pattern)
                                    primaries[first_matched_by][metric][metric_matched_by['matched_namespace']] = pattern_match
                                    primaries[first_matched_by][metric]['matches'] = matches
                                    if pattern_match:
                                        matches.append([pattern, metric_matched_by['matched_namespace']])
                                        primaries[first_matched_by][metric]['matches'] = matches
                                    continue
                                if isinstance(v, dict) and v:
                                    for secondary_key, secondary_value in v.items():
                                        if isinstance(secondary_value, list) and secondary_value:
                                            pattern = list(secondary_value)
                                        else:
                                            pattern = [secondary_key]
                                        to_match += 1
                                        pattern_match, metric_matched_by = matched_or_regexed_in_list(current_skyline_app, metric, secondary_value)
                                        primaries[first_matched_by][metric][metric_matched_by['matched_namespace']] = pattern_match
                                        primaries[first_matched_by][metric]['matches'] = matches
                                        if pattern_match:
                                            matches.append([pattern, metric_matched_by['matched_namespace']])
                                            primaries[first_matched_by][metric]['matches'] = matches
                        if skip:
                            if operation == 'include':
                                if metric in include_metrics:
                                    include_metrics.remove(metric)
                            else:
                                if metric in exclude_metrics:
                                    exclude_metrics.remove(metric)
                            continue
                        primaries[first_matched_by][metric]['to_match'] = to_match
                        primaries[first_matched_by][metric]['matches_count'] = len(matches)
                        if len(matches) >= to_match:
                            if operation == 'include':
                                include_metrics.append(metric)
                            else:
                                exclude_metrics.append(metric)
                        pattern_match = False
                except Exception as err:
                    errors.append([metric, err])
        if errors:
            current_logger.error('error :: %s :: determining %s encountered %s errors, last reported error: %s' % (
                function_str, operation, len(errors), str(errors[-1])))
        if operation == 'include':
            for metric in skipped:
                if metric in include_metrics:
                    include_metrics.remove(metric)
            operation_metrics_count = len(include_metrics)
        else:
            for metric in skipped:
                if metric in exclude_metrics:
                    exclude_metrics.remove(metric)
            operation_metrics_count = len(exclude_metrics)
        current_logger.info('%s :: determined %s metrics to %s in repetitive pattern analysis, skipped(%s)' % (
            function_str, str(operation_metrics_count), operation, str(len(skipped))))
    if exclude_metrics:
        include_metrics = [metric for metric in include_metrics if metric not in exclude_metrics]
        # @added 20240308 - Feature #5304: ionosphere.find_repetitive_patterns
        # Because this can be a fairly expensive operation, create some Redis cache
        # keys and only operated if they do not exist
        if not parent_namespace:
            try:
                redis_conn_decoded.delete('ionosphere.repetitive_patterns_metrics.exclude_metrics')
                redis_conn_decoded.sadd('ionosphere.repetitive_patterns_metrics.exclude_metrics', *set(exclude_metrics))
                redis_conn_decoded.expire('ionosphere.repetitive_patterns_metrics.exclude_metrics', 600)
                current_logger.info('%s :: created ionosphere.repetitive_patterns_metrics.exclude_metrics with 600 expire and %s metrics' % (
                    function_str, str(len(exclude_metrics))))
            except Exception as err:
                current_logger.error('error :: %s :: sadd failed on ionosphere.repetitive_patterns_metrics.exclude_metrics, err: %s' % (
                    function_str, err))

    # @added 20240308 - Feature #5304: ionosphere.find_repetitive_patterns
    # Because this can be a fairly expensive operation, create some Redis cache
    # keys and only operated if they do not exist
    if not parent_namespace and include_metrics:
        try:
            redis_conn_decoded.delete('ionosphere.repetitive_patterns_metrics.include_metrics')
            redis_conn_decoded.sadd('ionosphere.repetitive_patterns_metrics.include_metrics', *set(include_metrics))
            redis_conn_decoded.expire('ionosphere.repetitive_patterns_metrics.include_metrics', 600)
            current_logger.info('%s :: created ionosphere.repetitive_patterns_metrics.include_metrics with 600 expire and %s metrics' % (
                function_str, str(len(include_metrics))))
        except Exception as err:
            current_logger.error('error :: %s :: sadd failed on ionosphere.repetitive_patterns_metrics.include_metrics, err: %s' % (
                function_str, err))

    current_logger.info('%s :: determined %s metrics to include in repetitive pattern analysis, took: %s seconds' % (
        function_str, len(list(set(include_metrics))), (time() - start)))

    return list(set(include_metrics)), list(set(exclude_metrics))
