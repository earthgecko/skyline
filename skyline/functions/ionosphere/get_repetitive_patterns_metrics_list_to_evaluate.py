"""
get_repetitive_patterns_metrics_list_to_evaluate.py
"""
import logging
import copy

import settings

from matched_or_regexed_in_list import matched_or_regexed_in_list
# @added 20221215 - Feature #4658: ionosphere.learn_repetitive_patterns
from functions.settings.get_external_settings import get_external_settings
from functions.metrics.get_base_names_and_metric_ids import get_base_names_and_metric_ids


def get_repetitive_patterns_metrics_list_to_evaluate(current_skyline_app, metrics):
    """
    Return a list of metrics that are to be included in repetitive pattern
    learning.

    :param current_skyline_app: the app calling the function
    :param metrics: a list of metrics to evaluate whether to include in learning.
    :type current_skyline_app: str
    :type metrics: list
    :return: include_metrics
    :rtype: list

    """

    function_str = 'functions.ionosphere.get_repetitive_patterns_metrics_list_to_evaluate'
    current_skyline_app_logger = current_skyline_app + 'Log'
    current_logger = logging.getLogger(current_skyline_app_logger)

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

    # @added 20221215 - Feature #4658: ionosphere.learn_repetitive_patterns
    # Added external_settings ['learn_repetitive_patterns']['exclude'] to the
    # exclude_metrics list
    external_settings = {}
    try:
        external_settings = get_external_settings(current_skyline_app, None, True)
    except Exception as err:
        current_logger.error('error :: %s :: get_external_settings failed - %s' % (
            function_str, err))

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
    external_settings_excludes = {}
    for settings_key in list(external_settings.keys()):
        external_setting_keys = list(external_settings[settings_key].keys())
        if 'learn_repetitive_patterns' in external_setting_keys:
            if external_settings[settings_key]['learn_repetitive_patterns']['enabled']:
                if external_settings[settings_key]['learn_repetitive_patterns']['exclude']:
                    external_settings_excludes[external_settings[settings_key]['namespace']] = copy.deepcopy(external_settings[settings_key]['learn_repetitive_patterns']['exclude'])
    for namespace in list(external_settings_excludes.keys()):
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

    include_and_exclude_dicts['exclude'] = IONOSPHERE_REPETITIVE_PATTERNS_EXCLUDE
    if not dicts_defined:
        current_logger.info('%s :: no includes or excludes defined returning %s metrics' % (
            function_str, len(str(metrics))))
        return metrics

    for operation in ['include', 'exclude']:
        if len(list(include_and_exclude_dicts[operation].keys())) == 0:
            # If no includes/excludes are defined skip and move on
            current_logger.info('%s :: no %ss defined, continuing' % (function_str, operation))
            if operation == 'include':
                include_metrics = list(metrics)
            continue

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
    current_logger.info('%s :: determined %s metrics to include in repetitive pattern analysis' % (
        function_str, len(list(set(include_metrics)))))

    return list(set(include_metrics)), list(set(exclude_metrics))
