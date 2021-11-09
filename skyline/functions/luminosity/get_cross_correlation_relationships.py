import logging
import os
import sys
import operator
from timeit import default_timer as timer
import traceback
from collections import Counter

import numpy as np

if True:
    sys.path.insert(0, '/opt/skyline/github/skyline/skyline')
    import settings
    from skyline_functions import get_redis_conn_decoded
    # from functions.database.queries.metric_id_from_base_name import metric_id_from_base_name
    from functions.metrics.get_metric_id_from_base_name import get_metric_id_from_base_name
    from functions.database.queries.query_anomalies import get_anomalies
    from functions.database.queries.get_cross_correlations import get_cross_correlations
    # from functions.database.queries.base_name_from_metric_id import base_name_from_metric_id
    from functions.metrics.get_base_name_from_metric_id import get_base_name_from_metric_id

    import warnings
    warnings.filterwarnings('ignore')

skyline_app = 'luminosity'
skyline_app_logger = '%sLog' % skyline_app
logger = logging.getLogger(skyline_app_logger)
skyline_app_logfile = '%s/%s.log' % (settings.LOG_PATH, skyline_app)


def get_cross_correlation_relationships(base_name, metric_names_with_ids={}):
    """
    Given a base_name get and return all the cross correlated metrics
    """

    # logger = logging.getLogger(skyline_app_logger)
    child_process_pid = os.getpid()
    logger.info('related_metrics :: get_cross_correlation_relationships :: running for process_pid - %s for %s' % (
        str(child_process_pid), base_name))

    start = timer()

    cross_correlation_relationships = {}
    cross_correlation_relationships[base_name] = {}
    cross_correlation_relationships[base_name]['cross_correlation_relationships'] = {}

    metric_id = 0
    try:
        # metric_id = metric_id_from_base_name(skyline_app, base_name)
        metric_id = get_metric_id_from_base_name(skyline_app, base_name)
    except Exception as err:
        logger.error(traceback.format_exc())
        logger.error('error :: related_metrics :: get_cross_correlation_relationships :: get_metric_id_from_base_name failed for %s - %s' % (base_name, str(err)))

    cross_correlation_relationships[base_name]['metric_id'] = metric_id

    start_get_anomalies = timer()
    anomalies = {}
    try:
        anomalies = get_anomalies(skyline_app, metric_id)
    except Exception as err:
        logger.error(traceback.format_exc())
        logger.error('error :: related_metrics :: get_cross_correlation_relationships :: get_anomalies failed - %s' % str(err))
    end_get_anomalies = timer()
    cross_correlation_relationships[base_name]['get_anomalies_duration'] = (end_get_anomalies - start_get_anomalies)

    anomaly_ids = []
    if anomalies:
        # for anomaly_id in list(cross_correlation_relationships[base_name]['anomalies'].keys()):
        for anomaly_id in list(anomalies.keys()):
            anomaly_ids.append(int(anomaly_id))

    cross_correlations = {}
    start_cross_correlations = timer()
    if anomaly_ids:
        try:
            cross_correlations = get_cross_correlations(skyline_app, anomaly_ids)
        except Exception as err:
            logger.error(traceback.format_exc())
            logger.error('error :: related_metrics :: get_cross_correlation_relationships :: get_cross_correlations failed - %s' % str(err))
    end_cross_correlations = timer()
    cross_correlation_relationships[base_name]['get_cross_correlations_duration'] = (end_cross_correlations - start_cross_correlations)

    cross_correlated_metrics = {}
    if cross_correlations:
        for anomaly_id in list(cross_correlations.keys()):
            # cross_correlation_relationships[base_name]['anomalies'][anomaly_id]['cross_correlations'] = cross_correlations[anomaly_id]
            for metric_id in list(cross_correlations[anomaly_id].keys()):
                if metric_id not in list(cross_correlated_metrics.keys()):
                    cross_correlated_metrics[metric_id] = {}
                cross_correlated_metrics[metric_id][anomaly_id] = cross_correlations[anomaly_id][metric_id]

    cross_correlated_metrics_summary = {}
    cross_correlated_metrics_list = []
    metric_db_name_requests = 0
    correlation_counts = []
    ids_with_metric_names = {}
    metric_names_with_ids_from_redis = False
    if cross_correlated_metrics:
        if not metric_names_with_ids:
            try:
                redis_conn_decoded = get_redis_conn_decoded(skyline_app)
            except Exception as err:
                logger.error(traceback.format_exc())
                logger.error('error :: related_metrics :: get_cross_correlation_relationships :: get_redis_conn_decoded failed - %s' % str(err))
            start_ids_with_metric_names = timer()
            metric_names_with_ids = {}
            try:
                metric_names_with_ids = redis_conn_decoded.hgetall('aet.metrics_manager.metric_names_with_ids')
                metric_names_with_ids_from_redis = True
            except Exception as err:
                logger.error(traceback.format_exc())
                logger.error('error :: related_metrics :: get_cross_correlation_relationships :: failed to get Redis hash aet.metrics_manager.metric_names_with_ids - %s' % str(err))
        if metric_names_with_ids:
            for c_metric_name in list(metric_names_with_ids.keys()):
                c_metric_id = int(str(metric_names_with_ids[c_metric_name]))
                ids_with_metric_names[c_metric_id] = c_metric_name
        if metric_names_with_ids_from_redis:
            end_ids_with_metric_names = timer()
            cross_correlation_relationships[base_name]['ids_with_metric_names_duration'] = (end_ids_with_metric_names - start_ids_with_metric_names)

        start_cross_correlated_metrics_list = timer()
        metric_db_name_requests = 0
        for c_metric_id in list(cross_correlated_metrics.keys()):
            coefficients = []
            shifts = []
            shifted_coefficients = []
            for anomaly_id in list(cross_correlated_metrics[c_metric_id].keys()):
                coefficients.append(float(cross_correlated_metrics[c_metric_id][anomaly_id]['coefficient']))
                shifts.append(cross_correlated_metrics[c_metric_id][anomaly_id]['shifted'])
                shifted_coefficients.append(float(cross_correlated_metrics[c_metric_id][anomaly_id]['shifted_coefficient']))
            avg_coefficient = sum(coefficients) / len(coefficients)
            # avg_shift = sum(shifts) / len(shifts)
            shifted_counts = dict(Counter(shifts))
            avg_shifted_coefficient = sum(shifted_coefficients) / len(shifted_coefficients)
            metric_base_name = None
            try:
                metric_base_name = ids_with_metric_names[c_metric_id]
            except KeyError:
                try:
                    # metric_base_name = base_name_from_metric_id(skyline_app, c_metric_id, False)
                    metric_base_name = get_base_name_from_metric_id(skyline_app, c_metric_id, False)
                    metric_db_name_requests += 1
                except Exception as err:
                    logger.error(traceback.format_exc())
                    logger.error('error :: related_metrics :: get_cross_correlation_relationships :: base_name_from_metric_id failed - %s' % str(err))
            if metric_base_name is None:
                metric_base_name = c_metric_id
            cross_correlated_metrics_list.append([metric_base_name, c_metric_id, len(coefficients), avg_coefficient, shifted_counts, avg_shifted_coefficient])
            correlation_counts.append(len(coefficients))
        end_cross_correlated_metrics_list = timer()
        cross_correlation_relationships[base_name]['cross_correlated_metrics_list_duration'] = (end_cross_correlated_metrics_list - start_cross_correlated_metrics_list)
        cross_correlation_relationships[base_name]['requests_to_db_for_metric_names'] = metric_db_name_requests

    start_cross_correlated_metrics_summary = timer()
    sorted_cross_correlated_metrics_list = []
    if cross_correlated_metrics_list:
        sorted_cross_correlated_metrics_list = sorted(cross_correlated_metrics_list, key=operator.itemgetter(2, 3), reverse=True)
    if sorted_cross_correlated_metrics_list:
        np_array = np.array(correlation_counts)
        min_correlation_count = np.percentile(np_array, settings.LUMINOSITY_RELATED_METRICS_MIN_CORRELATION_COUNT_PERCENTILE)
        for item in sorted_cross_correlated_metrics_list:
            # Only class a metric as having a cross correlation relationship if
            # it has correlated within the 95th percentile times
            if item[2] < min_correlation_count:
                logger.info('related_metrics :: get_cross_correlation_relationships :: dropping as number of correlations %s is less than min_correlation_count %s - %s' % (
                    str(item[2]), str(min_correlation_count), str(item)))
                continue
            if item[2] < settings.LUMINOSITY_RELATED_METRICS_MINIMUM_CORRELATIONS_COUNT:
                # if item[3] < 0.98:
                #     continue
                logger.info('related_metrics :: get_cross_correlation_relationships :: dropping as number of correlations %s is less than LUMINOSITY_RELATED_METRICS_MINIMUM_CORRELATIONS_COUNT %s - %s' % (
                    str(item[2]), str(settings.LUMINOSITY_RELATED_METRICS_MINIMUM_CORRELATIONS_COUNT), str(item)))
                continue
            metric_base_name = item[0]
            metric_dict = {
                'metric_id': item[1],
                'cross_correlations_count': item[2],
                'avg_coefficient': item[3],
                'shifted_counts': item[4],
                'avg_shifted_coefficient': item[5],
            }
            cross_correlated_metrics_summary[metric_base_name] = metric_dict
    end_cross_correlated_metrics_summary = timer()
    cross_correlation_relationships[base_name]['cross_correlated_metrics_summary_duration'] = (end_cross_correlated_metrics_summary - start_cross_correlated_metrics_summary)

    cross_correlation_relationships[base_name]['cross_correlation_relationships'] = cross_correlated_metrics_summary

    end = timer()
    cross_correlation_relationships[base_name]['duration'] = (end - start)

    return cross_correlation_relationships
