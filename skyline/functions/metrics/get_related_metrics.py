import logging
import traceback
from ast import literal_eval

import numpy as np

import settings
from skyline_functions import get_redis_conn_decoded
# from functions.database.queries.base_name_from_metric_id import base_name_from_metric_id
# from functions.metrics.get_base_name_from_metric_id import get_base_name_from_metric_id
from functions.metrics.get_metric_ids_and_base_names import get_metric_ids_and_base_names
from functions.metrics.get_base_names_and_metric_ids import get_base_names_and_metric_ids
from functions.database.queries.get_metric_group import get_metric_group
# from functions.metrics.get_metric_id_from_base_name import get_metric_id_from_base_name

LOCAL_DEBUG = False


def get_related_metrics(current_skyline_app, cluster_data, full_details, base_name, metric_id=0):
    """
    Return a dict of the related metrics.

    :param current_skyline_app: the app calling the function
    :param cluster_data: whether this is a cluster_data request
    :param full_details: whether to return full details or just to names
    :param base_name: the base_name of the metric to determine the latest
        anomaly for.  Can be None if metric_id is passed as a positive int
    :param metric_id: the metric id if base_name is passed as None.
    :type current_skyline_app: str
    :type cluster_data: boolean
    :type full_details: boolean
    :type base_name: str
    :type metric_id: int
    :return: related_metrics
    :rtype: dict

    """

    related_metrics = {}

    function_str = 'functions.metrics.get_related_metrics'

    current_skyline_app_logger = current_skyline_app + 'Log'
    current_logger = logging.getLogger(current_skyline_app_logger)
    current_logger.info('%s :: %s :: determining related_metrics for %s id: %s' % (
        current_skyline_app, function_str, base_name, str(metric_id)))

    # Reducing hget()
    ids_with_base_names = {}
    try:
        ids_with_base_names = get_metric_ids_and_base_names(current_skyline_app)
    except Exception as err:
        current_logger.error(traceback.format_exc())
        current_logger.error('error :: %s :: %s :: get_metric_ids_and_basenames failed - %s' % (
            current_skyline_app, function_str, str(err)))
        related_metrics['status'] = {}
        related_metrics['status']['code'] = 500
        related_metrics['status']['error'] = 'failed to get data from Redis'
        return related_metrics

    if not base_name:
        if metric_id:
            try:
                # base_name = base_name_from_metric_id(current_skyline_app, metric_id)
                # base_name = get_base_name_from_metric_id(current_skyline_app, metric_id)
                base_name = ids_with_base_names[metric_id]
            except Exception as err:
                current_logger.error('error :: %s :: %s :: base_name_from_metric_id failed to determine base_name from metric_id: %s - %s' % (
                    current_skyline_app, function_str, str(metric_id), str(err)))

    if not base_name:
        current_logger.error('error :: %s :: %s :: no base_name known returning empty dict' % (
            current_skyline_app, function_str))
        return {}

    base_names_with_ids = {}
    if not metric_id:
        try:
            base_names_with_ids = get_base_names_and_metric_ids(current_skyline_app)
        except Exception as err:
            current_logger.error(traceback.format_exc())
            current_logger.error('error :: %s :: %s :: get_base_names_and_metric_ids failed - %s' % (
                current_skyline_app, function_str, str(err)))
            related_metrics['status'] = {}
            related_metrics['status']['code'] = 500
            related_metrics['status']['error'] = 'failed to get data from Redis'
            return related_metrics

        try:
            # metric_id = get_metric_id_from_base_name(current_skyline_app, base_name)
            metric_id = base_names_with_ids[base_name]
        except Exception as err:
            current_logger.error('error :: %s :: %s :: get_metric_id_from_base_name failed to determine metric_id from base_name: %s - %s' % (
                current_skyline_app, function_str, str(base_name), str(err)))
            metric_id = 0

    related_metrics['metric'] = base_name
    related_metrics['metric_id'] = metric_id
    related_metrics['related_metrics'] = {}
    related_metrics['related_metrics_list'] = []

    related_metrics_str = None
    # If this is a clustered Skyline and the metric does not belong to the
    # instance shard, no related_metrics will be found for it initially in the
    # Redis hash.  If it is a cluster_data request then the full_details method
    # will be used and the related_metrics will be obtained from the database
    # and returned
    if not full_details:
        redis_data_found = False
        try:
            redis_conn_decoded = get_redis_conn_decoded(current_skyline_app)
        except Exception as err:
            current_logger.error(traceback.format_exc())
            current_logger.error('error :: %s :: %s :: get_redis_conn_decoded failed for %s - %s' % (
                current_skyline_app, function_str, base_name, str(err)))
            related_metrics['status'] = {}
            related_metrics['status']['code'] = 500
            related_metrics['status']['error'] = 'Redis unavailable'
            return related_metrics
        try:
            related_metrics_str = redis_conn_decoded.hget('luminosity.related_metrics.metrics', base_name)
            # DEBUG
            current_logger.info('debug :: %s :: %s :: hget(%s, %s)' % (
                current_skyline_app, function_str, 'luminosity.related_metrics.metrics', base_name))
            if related_metrics_str:
                # related_metrics[base_name]['related_metrics'] = literal_eval(str(related_metrics_str))
                related_metrics['related_metrics'] = literal_eval(str(related_metrics_str))
                related_metrics['status'] = {}
                related_metrics['status']['code'] = 200
                redis_data_found = True
        except Exception as err:
            current_logger.error(traceback.format_exc())
            current_logger.error('error :: %s :: %s :: failed to get latest_anomaly for %s from Redis hash %s: %s' % (
                current_skyline_app, function_str, base_name, 'luminosity.related_metrics.metrics', str(err)))
        if not cluster_data:
            return related_metrics
        if cluster_data and redis_data_found:
            return related_metrics

    metric_group = {}
    try:
        metric_group = get_metric_group(current_skyline_app, metric_id)
    except Exception as err:
        current_logger.error(traceback.format_exc())
        current_logger.error('error :: %s :: %s :: get_metric_group failed for %s - %s' % (
            current_skyline_app, function_str, str(metric_id), str(err)))
        related_metrics['status'] = {}
        related_metrics['status']['code'] = 500
        related_metrics['status']['error'] = 'failed to get data from database'
        return related_metrics

    related_metrics_list = []

    avg_coefficients = []
    avg_coefficients_dict = {}
    all_shifted_counts = []
    all_shifted_counts_dict = {}

    if metric_group:
        metric_group_related = metric_group[metric_id]
        for related_metric_id in list(metric_group_related.keys()):
            related_metric = None
            try:
                # related_metric = base_name_from_metric_id(current_skyline_app, related_metric_id)
                # Reducing hget()
                # related_metric = get_base_name_from_metric_id(current_skyline_app, related_metric_id)
                related_metric = ids_with_base_names[related_metric_id]
                if related_metric:
                    related_metrics_list.append(related_metric)
            except Exception as err:
                current_logger.error('error :: %s :: %s :: base_name_from_metric_id failed to determine base_name from metric_id: %s - %s' % (
                    current_skyline_app, function_str, str(related_metric_id),
                    str(err)))
            if related_metric is None:
                related_metric = 'unknown'

            related_metrics['related_metrics'][related_metric] = metric_group_related[related_metric_id]
            avg_coefficient = related_metrics['related_metrics'][related_metric]['avg_coefficient']
            avg_coefficients.append(avg_coefficient)
            avg_coefficients_dict[related_metric_id] = avg_coefficient
            shifted_counts = related_metrics['related_metrics'][related_metric]['shifted_counts']
            shifted_count = 0
            for key in list(shifted_counts.keys()):
                shifted_count += shifted_counts[key]
            all_shifted_counts.append(shifted_count)
            all_shifted_counts_dict[related_metric_id] = shifted_count

        primary_namespaces = []
        secondary_namespaces = []
        tertiary_namespaces = []
        for related_metric in list(related_metrics['related_metrics'].keys()):
            namespace_elements = related_metric.split('.')
            primary_namespaces.append(namespace_elements[0])
            secondary_namespaces.append('.'.join(namespace_elements[0:2]))
            tertiary_namespaces.append('.'.join(namespace_elements[0:3]))
        primary_unique_namespaces = list(set(primary_namespaces))
        secondary_unique_namespaces = list(set(secondary_namespaces))
        tertiary_unique_namespaces = list(set(tertiary_namespaces))
        related_metrics['primary_namespaces'] = len(primary_unique_namespaces)
        related_metrics['secondary_namespaces'] = len(secondary_unique_namespaces)
        related_metrics['tertiary_namespaces'] = len(tertiary_unique_namespaces)

        max_avg_coefficient = max(avg_coefficients)
        max_correlations = max(all_shifted_counts)
        total_correlations = sum(all_shifted_counts)
        np_array = np.array(all_shifted_counts)
        percentile = 40
        if max_correlations > 20:
            percentile = 50
        if max_correlations > 50:
            percentile = 60
        if max_correlations > 100:
            percentile = 70
        # include_correlation_count = np.percentile(np_array, percentile)
        include_correlation_count = (max_correlations / 100) * percentile
        min_correlation_count = np.percentile(np_array, settings.LUMINOSITY_RELATED_METRICS_MIN_CORRELATION_COUNT_PERCENTILE)

        if LOCAL_DEBUG:
            confidence_variables = {
                'max_avg_coefficient': max_avg_coefficient,
                'max_correlations': max_correlations,
                'total_correlations': total_correlations,
                'percentile': percentile,
                'include_correlation_count': include_correlation_count,
                'min_correlation_count': min_correlation_count,
            }
            current_logger.info('%s :: %s :: confidence variables: %s' % (
                current_skyline_app, function_str, str(confidence_variables)))

        base_namespace_elements = base_name.split('.')
        secondary_base_namespace = '.'.join(base_namespace_elements[0:2])
        tertiary_base_namespace = '.'.join(base_namespace_elements[0:3])
        first_degree_base_namespace = '.'.join(base_namespace_elements[0:-1])

        for related_metric in list(related_metrics['related_metrics'].keys()):
            # Calculate a confidence score based on the number of times cross
            # correlations have been found.  This results in a low confidence
            # value for metrics that are probably numeric significant
            # correlations.  This confidence score is calculated dynamically
            # rather than statically stored in the DB, due to the fact that
            # changing correlations on other metrics in the group impact this
            # score. Were it calculated via get_cross_correlation_relationships,
            # every time a metric in a group had a new correlation, the
            # confidence score each every metric in the group would have to be
            # recalculated and stored.  This is a very dynamic variable.
            confidence = 0.0
            related_metric_id = related_metrics['related_metrics'][related_metric]['metric_id']
            correlations_count = all_shifted_counts_dict[related_metric_id]
#            if correlations_count < settings.LUMINOSITY_RELATED_METRICS_MINIMUM_CORRELATIONS_COUNT:
#                continue
            avg_coefficient = avg_coefficients_dict[related_metric_id]
            related_namespace_elements = related_metric.split('.')
            secondary_related_namespace = '.'.join(related_namespace_elements[0:2])
            tertiary_related_namespace = '.'.join(related_namespace_elements[0:3])
            first_degree_related_namespace = '.'.join(related_namespace_elements[0:-1])

            in_namespace = False
            in_tertiary_namespace = False
            if tertiary_related_namespace == tertiary_base_namespace:
                in_tertiary_namespace = True
                in_namespace = True
            in_secondary_namespace = False
            if secondary_related_namespace == secondary_base_namespace:
                in_secondary_namespace = True
                in_namespace = True
            in_first_degree_namespace = False
            if first_degree_related_namespace == first_degree_base_namespace:
                in_first_degree_namespace = True
                in_namespace = True

            if LOCAL_DEBUG:
                related_variables = {
                    'related_metric': related_metric,
                    'correlations_count': correlations_count,
                    'avg_coefficient': avg_coefficient,
                }
                current_logger.info('%s :: %s :: confidence variables for related_metric: %s' % (
                    current_skyline_app, function_str, str(related_variables)))

            adjusted_confidence = 0
            confidence = 0
            new_confidence = True
            if new_confidence:
                # HIGH CONFIDENCE correlations > 8
                if correlations_count in list(range(int(include_correlation_count), int(min_correlation_count))):
                    new_confidence = 0.85
                if correlations_count >= min_correlation_count:
                    new_confidence = 1
                if correlations_count < 8:
                    new_confidence = 0.7
                new_confidence = new_confidence * avg_coefficient
                if in_first_degree_namespace:
                    if new_confidence <= 0.7:
                        new_confidence = new_confidence / 0.7
                elif in_tertiary_namespace:
                    if new_confidence <= 0.7:
                        new_confidence = new_confidence / 0.7
                elif in_secondary_namespace:
                    if new_confidence <= 0.5:
                        new_confidence = new_confidence / 0.7
                elif not in_namespace:
                    if new_confidence >= 0.5:
                        new_confidence = new_confidence * 0.7
                if correlations_count >= include_correlation_count and new_confidence <= 0.7:
                    if avg_coefficient >= 0.95:
                        new_confidence = avg_coefficient * 0.95
                        if not in_namespace:
                            new_confidence = avg_coefficient * 0.9
                # Lower confidence of low volume but high coefficient
                # correlations (normally all 1s)
                if new_confidence == 1 and correlations_count < min_correlation_count:
                    new_confidence = avg_coefficient * 0.95
                if new_confidence >= 0.95 and correlations_count < include_correlation_count:
                    new_confidence = new_confidence * 0.8
                if correlations_count < settings.LUMINOSITY_RELATED_METRICS_MINIMUM_CORRELATIONS_COUNT:
                    new_confidence = new_confidence * 0.5
                # Lower confidence of metrics in the namespace that have a low
                # number of correlations
                if correlations_count < 8 and not in_namespace:
                    new_confidence = new_confidence * 0.5

                confidence = round(new_confidence, 5)
                if LOCAL_DEBUG:
                    current_logger.info('%s :: %s :: confidence - %s, confidence: %s' % (
                        current_skyline_app, function_str, str(related_metric),
                        str(confidence)))
                related_metrics['related_metrics'][related_metric]['confidence'] = round(confidence, 5)

            if not confidence:
                # HIGH CONFIDENCE correlations > 8
                if correlations_count > 8:
                    if correlations_count == max_correlations:
                        confidence = 1.0
                        adjusted_confidence = confidence
                        if LOCAL_DEBUG:
                            current_logger.info('%s :: %s :: confidence - correlations_count > 8, max_correlations matched for %s, confidence and adjusted_confidence: 1' % (
                                current_skyline_app, function_str, str(related_metric)))
                    else:
                        confidence = 1 / (max_correlations / correlations_count)
                        if LOCAL_DEBUG:
                            current_logger.info('%s :: %s :: confidence - correlations_count > 8, %s, 1 / (max_correlations / correlations_count) confidence: %s' % (
                                current_skyline_app, function_str, str(related_metric),
                                str(confidence)))
                    # When there are lots of correlations with a high
                    # avg_coefficient, adjust the confidence
                    # if confidence < 1:
                    if confidence < 0.7:
                        update_confidence = False
                        if in_first_degree_namespace:
                            update_confidence = True
                            if LOCAL_DEBUG:
                                current_logger.info('%s :: %s :: confidence - correlations_count > 8, update confidence in_first_degree_namespace %s' % (
                                    current_skyline_app, function_str, str(related_metric)))
                        if correlations_count > 20 and avg_coefficient >= 0.95:
                            update_confidence = True
                            if LOCAL_DEBUG:
                                current_logger.info('%s :: %s :: confidence - correlations_count > 8, update confidence correlations_count > 20 and avg_coefficient >= 0.95, %s' % (
                                    current_skyline_app, function_str, str(related_metric)))
                        if ((correlations_count / max_correlations) * 100) > 30 and avg_coefficient >= 0.95:
                            update_confidence = True
                            if LOCAL_DEBUG:
                                current_logger.info('%s :: %s :: confidence - correlations_count > 8, update confidence ((correlations_count / max_correlations) * 100) > 30 and avg_coefficient >= 0.95, %s' % (
                                    current_skyline_app, function_str, str(related_metric)))
                        if update_confidence:
                            adjusted_confidence = avg_coefficient * 0.95
                            if LOCAL_DEBUG:
                                current_logger.info('%s :: %s :: confidence - correlations_count > 8, update confidence, %s adjusted_confidence: %s' % (
                                    current_skyline_app, function_str, str(related_metric),
                                    str(adjusted_confidence)))
                else:
                    confidence = 0.4
                    if LOCAL_DEBUG:
                        current_logger.info('%s :: %s :: confidence - correlations_count < 8, %s, confidence: %s' % (
                            current_skyline_app, function_str, str(related_metric),
                            str(confidence)))
                    if in_secondary_namespace:
                        confidence = 0.85
                        adjusted_confidence = confidence * avg_coefficient
                        if LOCAL_DEBUG:
                            current_logger.info('%s :: %s :: confidence - correlations_count < 8, %s, in_secondary_namespace, confidence: %s, adjusted_confidence: %s' % (
                                current_skyline_app, function_str, str(related_metric),
                                str(confidence), str(adjusted_confidence)))
                    if in_tertiary_namespace:
                        confidence = 0.90
                        adjusted_confidence = confidence * avg_coefficient
                        if LOCAL_DEBUG:
                            current_logger.info('%s :: %s :: confidence - correlations_count < 8, %s, in_tertiary_namespace, confidence: %s, adjusted_confidence: %s' % (
                                current_skyline_app, function_str, str(related_metric),
                                str(confidence), str(adjusted_confidence)))
                    if in_first_degree_namespace:
                        confidence = 0.95
                        adjusted_confidence = confidence * avg_coefficient
                        if LOCAL_DEBUG:
                            current_logger.info('%s :: %s :: confidence - correlations_count < 8, %s, in_first_degree_namespace, confidence: %s, adjusted_confidence: %s' % (
                                current_skyline_app, function_str, str(related_metric),
                                str(confidence), str(adjusted_confidence)))
                    if correlations_count == max_correlations:
                        adjusted_confidence = confidence
                        if LOCAL_DEBUG:
                            current_logger.info('%s :: %s :: confidence - correlations_count < 8, %s, correlations_count == max_correlations, adjusted_confidence: %s' % (
                                current_skyline_app, function_str, str(related_metric),
                                str(adjusted_confidence)))
                    else:
                        if not adjusted_confidence:
                            adjusted_confidence = confidence / (max_correlations / correlations_count)
                            if LOCAL_DEBUG:
                                current_logger.info('%s :: %s :: confidence - correlations_count < 8, %s, correlations_count != max_correlations, adjusted_confidence: %s' % (
                                    current_skyline_app, function_str, str(related_metric),
                                    str(adjusted_confidence)))

                    # When there are multiple correlations that are a fair
                    # percentage of the most correlations, adjust the confidence
                    if confidence < 0.7 and avg_coefficient >= 0.97:
                        if correlations_count >= include_correlation_count:
                            if correlations_count < min_correlation_count:
                                if in_first_degree_namespace:
                                    adjusted_confidence = avg_coefficient * 0.95
                                elif in_tertiary_namespace:
                                    adjusted_confidence = avg_coefficient * 0.90
                                elif in_secondary_namespace:
                                    adjusted_confidence = avg_coefficient * 0.85
                                elif in_namespace:
                                    adjusted_confidence = avg_coefficient * 0.80
                                else:
                                    adjusted_confidence = avg_coefficient * 0.75
                                if LOCAL_DEBUG:
                                    current_logger.info('%s :: %s :: confidence - correlations_count < 8, %s, confidence < 0.7 and avg_coefficient >= 0.97 AND AND, adjusted_confidence: %s' % (
                                        current_skyline_app, function_str, str(related_metric),
                                        str(adjusted_confidence)))
                if not adjusted_confidence and avg_coefficient >= 0.95:
                    # If metrics belong to the same secondary or tertiary
                    # namespace with a high avg_coefficient, adjust the
                    # confidence
                    if tertiary_related_namespace == tertiary_base_namespace:
                        adjusted_confidence = avg_coefficient * 0.95
                        if LOCAL_DEBUG:
                            current_logger.info('%s :: %s :: confidence - %s, not adjusted_confidence and avg_coefficient >= 0.95 AND in_tertiary_namespace, adjusted_confidence: %s' % (
                                current_skyline_app, function_str, str(related_metric),
                                str(adjusted_confidence)))
                    if not adjusted_confidence:
                        if secondary_related_namespace == secondary_base_namespace:
                            adjusted_confidence = avg_coefficient * 0.9
                            if LOCAL_DEBUG:
                                current_logger.info('%s :: %s :: confidence - %s, not adjusted_confidence and avg_coefficient >= 0.95 AND in_secondary_namespace, adjusted_confidence: %s' % (
                                    current_skyline_app, function_str, str(related_metric),
                                    str(adjusted_confidence)))
                if not in_namespace:
                    if not adjusted_confidence:
                        adjusted_confidence = confidence * 0.85
                    else:
                        adjusted_confidence = adjusted_confidence * 0.85
                    if LOCAL_DEBUG:
                        current_logger.info('%s :: %s :: confidence - %s, not in_namespace, adjusted_confidence: %s' % (
                            current_skyline_app, function_str, str(related_metric),
                            str(adjusted_confidence)))
                if adjusted_confidence < 0.5:
                    if in_secondary_namespace:
                        adjusted_confidence = 0.5
                        if LOCAL_DEBUG:
                            current_logger.info('%s :: %s :: confidence - %s, adjusted_confidence < 0.5 AND in_secondary_namespace, adjusted_confidence: %s' % (
                                current_skyline_app, function_str, str(related_metric),
                                str(adjusted_confidence)))
                    if in_tertiary_namespace:
                        adjusted_confidence = 0.7
                        if LOCAL_DEBUG:
                            current_logger.info('%s :: %s :: confidence - %s, adjusted_confidence < 0.5 AND in_tertiary_namespace, adjusted_confidence: %s' % (
                                current_skyline_app, function_str, str(related_metric),
                                str(adjusted_confidence)))
                if adjusted_confidence:
                    if not in_namespace:
                        if adjusted_confidence >= 0.7 and avg_coefficient <= 0.96:
                            adjusted_confidence = avg_coefficient * 0.73
                if adjusted_confidence:
                    confidence = adjusted_confidence
                if LOCAL_DEBUG:
                    current_logger.info('%s :: %s :: confidence - %s, confidence: %s' % (
                        current_skyline_app, function_str, str(related_metric),
                        str(confidence)))

        if LOCAL_DEBUG:
            current_logger.info('%s :: %s :: related_metrics: %s' % (
                current_skyline_app, function_str, str(related_metrics)))

        # Sort related_metrics by confidence then by avg_coefficient
        confidence_related_metrics_list = []
        confidence_related_metrics = {}
        related_metrics_keys = []
        for related_metric in list(related_metrics['related_metrics'].keys()):
            related_metric_data = []
            if not related_metrics_keys:
                related_metrics_keys.append('metric')
                for index, key in enumerate(list(related_metrics['related_metrics'][related_metric].keys())):
                    related_metrics_keys.append(key)
                    if key == 'confidence':
                        confidence_index = index + 1
                    if key == 'avg_coefficient':
                        avg_coefficient_index = index + 1
            related_metric_data.append(related_metric)
            for key in related_metrics_keys:
                if key == 'metric':
                    continue
                related_metric_data.append(related_metrics['related_metrics'][related_metric][key])
            confidence_related_metrics_list.append(related_metric_data)

        if LOCAL_DEBUG:
            current_logger.info('%s :: %s :: related_metrics_keys: %s' % (
                current_skyline_app, function_str, str(related_metrics_keys)))

        if confidence_related_metrics_list:
            sorted_confidence_related_metrics_list = sorted(confidence_related_metrics_list, key=lambda x: (x[confidence_index], x[avg_coefficient_index]), reverse=True)
            for item in sorted_confidence_related_metrics_list:
                related_metric = item[0]
                confidence_related_metrics[related_metric] = {}
                for index, key in enumerate(related_metrics_keys):
                    if key == 'metric':
                        continue
                    if key == 'metric_id':
                        confidence_related_metrics[related_metric][key] = item[index]
                    if key == 'confidence':
                        confidence_related_metrics[related_metric][key] = item[index]
                for index, key in enumerate(related_metrics_keys):
                    if key in ['metric', 'metric_id', 'confidence']:
                        continue
                    confidence_related_metrics[related_metric][key] = item[index]
            related_metrics['related_metrics'] = confidence_related_metrics

        related_metrics['related_metrics_list'] = related_metrics_list

    if not full_details:
        related_metrics = {}
        related_metrics['metric'] = base_name
        related_metrics['metric_id'] = metric_id
        related_metrics['related_metrics'] = related_metrics_list

    return related_metrics
