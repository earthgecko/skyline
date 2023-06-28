"""
labelled_metrics_data_sparsity.py
"""
import logging
import traceback
from time import time, strftime, gmtime

from settings import FULL_DURATION
from functions.timeseries.determine_data_frequency import determine_data_frequency
from functions.timeseries.determine_data_sparsity import determine_data_sparsity

skyline_app = 'analyzer'
skyline_app_logger = '%sLog' % skyline_app
logger = logging.getLogger(skyline_app_logger)

MAX_SAMPLE_AGE = FULL_DURATION


# @added 20230406 - Task #2732: Prometheus to Skyline
#                   Branch #4300: prometheus
def get_metrics_to_update(self, active_labelled_ids_with_metrics, run_every):
    """
    Determine the list of labelled metric ids that need to be updated based on
    their last 
    """
    start_gmtu = time()
    metrics_to_update = []
    try:

        # max_metrics_to_check = int((len(active_labelled_ids_with_metrics) / int(FULL_DURATION / 1800)) / (1800 / run_every))
        max_metrics_to_check = int(len(active_labelled_ids_with_metrics) / (1800 / run_every))

    #    metrics_last_timeseries_timestamp_hash_key = 'analyzer_labelled_metrics.last_timeseries_timestamp'
        right_now = int(start_gmtu)
        labelled_metrics_resolution_sparsity_last_checked_dict = {}
        labelled_metrics_resolution_sparsity_last_checked = []
        metric_ids_last_checked = []
        labelled_metrics_resolution_sparsity_last_checked_hash_key = 'labelled_metrics.resolution_sparsity_last_checked'
        try:
            labelled_metrics_resolution_sparsity_last_checked_dict = self.redis_conn_decoded.hgetall(labelled_metrics_resolution_sparsity_last_checked_hash_key)
        except Exception as err:
            logger.error('error :: metrics_manager :: update_labelled_metrics_data_sparsity :: :: failed to hgetall %s - %s' % (
                labelled_metrics_resolution_sparsity_last_checked_hash_key, err))
            labelled_metrics_resolution_sparsity_last_checked_dict = {}
        if labelled_metrics_resolution_sparsity_last_checked_dict:
            metric_ids_last_checked = [int(float(metric_id_str)) for metric_id_str, ts in labelled_metrics_resolution_sparsity_last_checked_dict.items()]
            labelled_metrics_resolution_sparsity_last_checked = [[metric_id_str, int(float(timestamp_str))] for metric_id_str, timestamp_str in labelled_metrics_resolution_sparsity_last_checked_dict.items() if right_now > (int(float(timestamp_str)) + 1800)]
        labelled_metrics_resolution_sparsity_recently_checked = len(labelled_metrics_resolution_sparsity_last_checked_dict) - len(labelled_metrics_resolution_sparsity_last_checked)
        logger.info('metrics_manager :: update_labelled_metrics_data_sparsity :: not checking %s metrics that have been recently checked' % (
            str(labelled_metrics_resolution_sparsity_recently_checked)))
        metrics_add_with_no_last_check_timestamp = 0
        for metric_id in list(active_labelled_ids_with_metrics.keys()):
            if metric_id not in metric_ids_last_checked:
                metrics_add_with_no_last_check_timestamp += 1
                labelled_metrics_resolution_sparsity_last_checked.append([str(metric_id), 0])
        if metrics_add_with_no_last_check_timestamp:
            logger.info('metrics_manager :: update_labelled_metrics_data_sparsity :: added %s metrics that have no last checked timestamp' % (
                str(metrics_add_with_no_last_check_timestamp)))
        if labelled_metrics_resolution_sparsity_last_checked:
            labelled_metrics_resolution_sparsity_last_checked = sorted(labelled_metrics_resolution_sparsity_last_checked, key=lambda x: x[1])
        if labelled_metrics_resolution_sparsity_last_checked:
            max_metrics_to_check = int(len(labelled_metrics_resolution_sparsity_last_checked) / (1800 / run_every))
            if metrics_add_with_no_last_check_timestamp:
                if metrics_add_with_no_last_check_timestamp >= 1000:
                    max_metrics_to_check += 1000
                else:
                    max_metrics_to_check += metrics_add_with_no_last_check_timestamp
            metrics_to_update = labelled_metrics_resolution_sparsity_last_checked[0:max_metrics_to_check]
        logger.info('metrics_manager :: update_labelled_metrics_data_sparsity :: determined a total of %s metrics to update but only updating %s due to max_metrics_to_update' % (
            str(len(labelled_metrics_resolution_sparsity_last_checked)),
            str(len(metrics_to_update))))
        logger.info('metrics_manager :: update_labelled_metrics_data_sparsity :: get_metrics_to_update took %s seconds' % (
            str((time() - start_gmtu))))
    except Exception as err:
        logger.error(traceback.format_exc())
        logger.error('error :: metrics_manager :: update_labelled_metrics_data_sparsity :: :: get_metrics_to_update failed with - %s' % (
            err))

    return metrics_to_update


def update_metrics(self, metrics_to_update):
    """
    The labelled_metrics by id e.g. labelled_metrics.1234
    """
    update_start = time()
    updated_labelled_metrics_sparsity = {}
    metrics_last_checked_dict = {}
    labelled_metrics_resolutions = {}
    labelled_metrics_sparsity = {}
    labelled_metrics_resolution_sparsity_checked_dict = {}
    errors = []
    redis_timings = []
    resolution_and_sparsity_timings = []
    until_timestamp = int(int(time()) // 60 * 60)
    from_timestamp = until_timestamp - FULL_DURATION
    for metric_id_str, ts in metrics_to_update:
        timeseries = []
        r_start = time()
        right_now = int(r_start)
        labelled_metric = 'labelled_metrics.%s' % str(metric_id_str)
        metrics_last_checked_dict[metric_id_str] = right_now
        full_duration_timeseries = []
        try:
            full_duration_timeseries = self.redis_conn_decoded.ts().range(labelled_metric, (from_timestamp * 1000), (until_timestamp * 1000))
        except Exception as err:
            if str(err) == 'TSDB: the key does not exist':
                redis_timings.append(time() - r_start)
                continue
            errors.append([labelled_metric, 'ts().range', str(err)])
            full_duration_timeseries = []
        redis_timings.append(time() - r_start)
        if full_duration_timeseries:
            timeseries = full_duration_timeseries
            timeseries = [[int(mts / 1000), value] for mts, value in full_duration_timeseries]
        if not timeseries:
            continue

        start_resolution_and_sparsity_timing = time()
        metric_resolution = 0
        data_sparsity = None
        try:
            metric_resolution, timestamp_resolutions_count = determine_data_frequency(skyline_app, timeseries, False)
        except Exception as err:
            errors.append([labelled_metric, 'determine_data_frequency', str(err)])
            metric_resolution = 0
        if metric_resolution:
            labelled_metrics_resolutions[str(metric_id_str)] = metric_resolution
            try:
                data_sparsity = determine_data_sparsity(skyline_app, timeseries, resolution=metric_resolution, log=False)
            except Exception as err:
                errors.append([labelled_metric, 'determine_data_sparsity', str(err)])
                data_sparsity = None
        if isinstance(data_sparsity, float):
            labelled_metrics_sparsity[str(metric_id_str)] = data_sparsity
            labelled_metrics_resolution_sparsity_checked_dict[str(metric_id_str)] = int(right_now)
        resolution_and_sparsity_timings.append(time() - start_resolution_and_sparsity_timing)

    if labelled_metrics_resolutions:
        hash_key = 'labelled_metrics.metric_resolutions'
        try:
            self.redis_conn.hset(hash_key, mapping=labelled_metrics_resolutions)
            logger.info('metrics_manager :: update_labelled_metrics_data_sparsity :: updated %s metrics in %s Redis hash' % (
                str(len(labelled_metrics_resolutions)), hash_key))
        except Exception as err:
            logger.error(traceback.format_exc())
            logger.error('error :: metrics_manager :: update_labelled_metrics_data_sparsity :: failed to set %s Redis hash - %s' % (
                hash_key, err))
    if labelled_metrics_sparsity:
        hash_key = 'labelled_metrics.data_sparsity'
        try:
            self.redis_conn.hset(hash_key, mapping=labelled_metrics_sparsity)
            logger.info('metrics_manager :: update_labelled_metrics_data_sparsity :: updated %s metrics in %s Redis hash' % (
                str(len(labelled_metrics_sparsity)), hash_key))
            updated_labelled_metrics_sparsity = labelled_metrics_sparsity
        except Exception as err:
            logger.error(traceback.format_exc())
            logger.error('error :: metrics_manager :: update_labelled_metrics_data_sparsity :: failed to set %s Redis hash - %s' % (
                hash_key, err))
    if metrics_last_checked_dict:
        labelled_metrics_resolution_sparsity_last_checked_hash_key = 'labelled_metrics.resolution_sparsity_last_checked'
        hash_key = 'labelled_metrics.resolution_sparsity_last_checked'
        try:
            self.redis_conn.hset(hash_key, mapping=metrics_last_checked_dict)
            logger.info('metrics_manager :: update_labelled_metrics_data_sparsity :: updated %s metrics in %s Redis hash' % (
                str(len(labelled_metrics_resolution_sparsity_checked_dict)), hash_key))
        except Exception as err:
            logger.error(traceback.format_exc())
            logger.error('error :: metrics_manager :: update_labelled_metrics_data_sparsity :: failed to set %s Redis hash - %s' % (
                hash_key, err))

    timings = {
        'redis_timings': sum(redis_timings),
        'resolution_and_sparsity_timings': sum(resolution_and_sparsity_timings),
    }
    logger.info('metrics_manager :: update_labelled_metrics_data_sparsity :: updated metrics in %s seconds, timings: %s' % (
        str((time() - update_start)), str(timings)))

    return updated_labelled_metrics_sparsity


def update_labelled_metrics_data_sparsity(self, active_labelled_ids_with_metrics, run_every):
    """

    Update the resolution and sparsity of labelled_metrics that have not had
    their resolution and sparsity updated by analyzer_labelled_metrics in the
    last (settings.FULL_DURATION / 48) seconds, with a maximum check number per
    per.

    :param self: the self object
    :param active_labelled_ids_with_metrics: the active_labelled_ids_with_metrics
    :param run_every: the metrics_manager RUN_EVERY
    :type self: object
    :type active_labelled_ids_with_metrics: dict
    :type run_every: int
    :return: updated_labelled_metrics_sparsity
    :rtype: dict

    """
    start_op = time()
    updated_labelled_metrics_sparsity = {}
    logger.info('metrics_manager :: update_labelled_metrics_data_sparsity :: determining metrics to update from the %s active labelled_metrics' % (
        str(len(active_labelled_ids_with_metrics))))

    # Determine the list of labelled metric_id_strs and timestamps that need to
    # be checked and updated
    metrics_to_update = []
    try:
        metrics_to_update = get_metrics_to_update(self, active_labelled_ids_with_metrics, run_every)
    except Exception as err:
        logger.error('error :: metrics_manager :: update_labelled_metrics_data_sparsity :: get_metrics_to_update failed - %s' % (
            err))
    if not metrics_to_update:
        logger.info('metrics_manager :: update_labelled_metrics_data_sparsity :: no metrics to update')
        return updated_labelled_metrics_sparsity

    try:
        updated_labelled_metrics_sparsity = update_metrics(self, metrics_to_update)
    except Exception as err:
        logger.error('error :: metrics_manager :: update_labelled_metrics_data_sparsity :: update_metrics failed - %s' % (
            err))

    logger.info('metrics_manager :: update_labelled_metrics_data_sparsity :: %s metrics to updated in %s seconds' % (
        str(len(updated_labelled_metrics_sparsity)), str((time() - start_op))))

    return updated_labelled_metrics_sparsity