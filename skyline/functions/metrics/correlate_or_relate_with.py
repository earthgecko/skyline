"""
correlate_or_relate_with.py
"""
import logging
import traceback
# @added 20220722 - Task #4624: Change all dict copy to deepcopy
import copy

import settings
from matched_or_regexed_in_list import matched_or_regexed_in_list

# @added 20220909 - Task #2732: Prometheus to Skyline
#                   Branch #4300: prometheus
from functions.prometheus.metric_name_labels_parser import metric_name_labels_parser


# @added 20220504 - Task #4544: Handle EXTERNAL_SETTINGS in correlate_or_relate_with
#                   Feature #3858: skyline_functions - correlate_or_relate_with
# Original added the skyline_functions on 20201202 moved to a new function to
# handle external_settings
def correlate_or_relate_with(
        current_skyline_app, metric, metric_to_correlate_or_relate,
        external_settings={}):
    """
    Given a metric name, determine if another metric should be correlated or
    related with it

    :param current_skyline_app: the Skyline app calling the function
    :param metric: the base_name of the metric in question
    :param metric_to_correlate_or_relate: the base_name of the metric you wish
        to determine if it should be correlated or related with
    :param external_settings: the external_settings dict
    :type current_skyline_app: str
    :type metric: str
    :type metric_to_correlate_or_relate: str
    :type external_settings: dict
    :return: False
    :rtype: boolean

    """

    try:
        LUMINOSITY_CORRELATE_ALL = settings.LUMINOSITY_CORRELATE_ALL
    except:
        LUMINOSITY_CORRELATE_ALL = True
    try:
        correlate_namespaces_only = list(settings.LUMINOSITY_CORRELATE_NAMESPACES_ONLY)
    except:
        correlate_namespaces_only = []
    try:
        # @modified 20220722 - Task #4624: Change all dict copy to deepcopy
        # LUMINOSITY_CORRELATION_MAPS = settings.LUMINOSITY_CORRELATION_MAPS.copy()
        LUMINOSITY_CORRELATION_MAPS = copy.deepcopy(settings.LUMINOSITY_CORRELATION_MAPS)
    except:
        LUMINOSITY_CORRELATION_MAPS = {}

    # @added 20220504 - Task #4544: Handle EXTERNAL_SETTINGS in correlate_or_relate_with
    # Override with external_settings if they exist
    metric_namespace = metric.split('.')[0]

    # @added 20220909 - Task #2732: Prometheus to Skyline
    #                   Branch #4300: prometheus
    # Handled labelled_metrics
    metric_dict = {}
    if '_tenant_id="' in metric:
        try:
            metric_dict = metric_name_labels_parser(current_skyline_app, metric)
        except:
            pass

    external_correlate_namespaces_only = []
    external_correlation_maps = {}
    if external_settings:
        for config_id in list(external_settings.keys()):
            namespace = None
            try:
                namespace = external_settings[config_id]['namespace']
            except KeyError:
                namespace = False
            if not namespace:
                continue
            if namespace:

                # @modified 20220909 - Task #2732: Prometheus to Skyline
                #                      Branch #4300: prometheus
                # Handled labelled_metrics
                # if namespace != metric_namespace:
                #     continue
                if not metric_dict:
                    if namespace != metric_namespace:
                        continue
                else:
                    matched = False
                    for label in list(metric_dict['labels'].keys()):
                        if namespace in [label, metric_dict['labels'][label]]:
                            matched = True
                            break
                    if not matched:
                        continue

                # Set a default
                try:
                    external_correlate_namespaces_only = external_settings[config_id]['correlate_namespaces_only']
                except KeyError:
                    external_correlate_namespaces_only = [namespace]
                try:
                    external_correlation_maps = external_settings[config_id]['correlation_maps']
                except KeyError:
                    external_correlation_maps = {}
            if external_correlate_namespaces_only or external_correlation_maps:
                correlate_namespaces_only = list(external_correlate_namespaces_only)
                LUMINOSITY_CORRELATION_MAPS = external_correlation_maps
                break

    if not correlate_namespaces_only:
        if not LUMINOSITY_CORRELATION_MAPS:
            if LUMINOSITY_CORRELATE_ALL:
                return True

    correlate_namespace_to = None
    if correlate_namespaces_only:
        for correlate_namespace in correlate_namespaces_only:
            correlate_namespace_to = None
            try:
                correlate_namespace_to, correlate_namespace_matched_by = matched_or_regexed_in_list(current_skyline_app, metric, [correlate_namespace])
                try:
                    del correlate_namespace_matched_by
                except:
                    pass
            except:
                pass
            if not correlate_namespace_to:
                continue
            correlate_with = None
            try:
                correlate_with, correlate_namespace_matched_by = matched_or_regexed_in_list(current_skyline_app, metric_to_correlate_or_relate, [correlate_namespace])
            except:
                pass
            if not correlate_with:
                continue
            try:
                del correlate_namespaces_only
            except:
                pass
            try:
                del LUMINOSITY_CORRELATION_MAPS
            except:
                pass
            return True

    if LUMINOSITY_CORRELATION_MAPS:
        for correlation_map in LUMINOSITY_CORRELATION_MAPS:
            metric_in_map = False
            if metric in LUMINOSITY_CORRELATION_MAPS[correlation_map]:
                metric_in_map = True
            if metric_in_map:
                if metric_to_correlate_or_relate in LUMINOSITY_CORRELATION_MAPS[correlation_map]:
                    try:
                        del correlate_namespaces_only
                    except:
                        pass
                    try:
                        del LUMINOSITY_CORRELATION_MAPS
                    except:
                        pass
                    return True
    try:
        del correlate_namespaces_only
    except:
        pass
    try:
        del LUMINOSITY_CORRELATION_MAPS
    except:
        pass
    return False
