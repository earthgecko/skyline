"""
redistimeseries_roomba.py
"""
import logging
import traceback
from time import time, strftime, gmtime

from settings import FULL_DURATION
from functions.database.queries.set_metrics_as_inactive import set_metrics_as_inactive
from functions.metrics.get_base_name_from_metric_id import get_base_name_from_metric_id

skyline_app = 'analyzer'
skyline_app_logger = '%sLog' % skyline_app
logger = logging.getLogger(skyline_app_logger)

MAX_SAMPLE_AGE = FULL_DURATION


# @added 20220627 - Task #2732: Prometheus to Skyline
#                   Branch #4300: prometheus
def get_metrics_to_remove(self):
    """
    Determine the list of labelled metric id flagged to be removed by analyzer
    apps
    """
    metrics_to_remove = []
    try:
        metrics_to_remove = list(self.redis_conn_decoded.smembers('redistimeseries_roomba.metrics_to_remove'))

        if metrics_to_remove:
            self.redis_conn_decoded.delete('redistimeseries_roomba.metrics_to_remove')
    except Exception as err:
        logger.error('error :: metrics_manager :: redistimeseries_roomba :: failed to get and delele redistimeseries_roomba.metrics_to_remove - %s' % (
            err))
    logger.info('metrics_manager :: redistimeseries_roomba :: determined %s metrics to remove from redistimeseries_roomba.metrics_to_remove Redis set' % str(len(metrics_to_remove)))
    return metrics_to_remove


def get_labelled_metrics(self):
    """
    The labelled_metrics by id e.g. labelled_metrics.1234
    """
    labelled_metrics = []
    try:
        labelled_metrics = list(self.redis_conn_decoded.smembers('labelled_metrics.unique_labelled_metrics'))
    except Exception as err:
        logger.error('error :: metrics_manager :: redistimeseries_roomba :: failed to get and delele redistimeseries_roomba.metrics_to_remove - %s' % (
            err))
    logger.info('metrics_manager :: redistimeseries_roomba :: determined %s labelled_metrics from labelled_metrics.unique_labelled_metrics Redis set' % str(len(labelled_metrics)))
    return labelled_metrics


def get_metrics_to_check(metrics_to_remove, labelled_metrics):
    """
    Remove the already known metrics to be removed from the list of metrics to
    check
    """
    metrics_to_check = []
    if labelled_metrics and metrics_to_remove:
        try:
            metrics_to_remove_set = set(metrics_to_remove)
            labelled_metrics_set = set(labelled_metrics)
            metrics_to_check = list(labelled_metrics_set.difference(metrics_to_remove_set))
        except:
            logger.error(traceback.format_exc())
            logger.error('error :: metrics_manager :: redistimeseries_roomba :: failed to determine metrics_to_check')
    else:
        metrics_to_check = list(labelled_metrics)
    return metrics_to_check


def check_last_timestamp(self, metrics_to_check, metrics_to_remove):
    """
    Check the last timestamp of the metrics deemed to be check
    """
    timestamp = int(time())
    logger.info('metrics_manager :: redistimeseries_roomba :: checking %s metrics to remove based on oldest timestamp' % str(len(metrics_to_check)))
    logger.info('metrics_manager :: redistimeseries_roomba :: %s metrics already passed in metrics_to_remove' % str(len(metrics_to_remove)))
    error_logged = False
    metrics_with_current_data = 0
    metrics_by_hour = {}

    for metric in metrics_to_check:
        try:
            last_sample = self.redis_conn_decoded.ts().get(metric)
            last_timestamp = int(last_sample[0] / 1000)

            hour_aligned_last_timestamp = int(last_timestamp // 3600 * 3600)
            date_string = str(strftime('%Y-%m-%d %H:%M:%S', gmtime(hour_aligned_last_timestamp)))
            try:
                metrics_by_hour[date_string] += 1
            except:
                metrics_by_hour[date_string] = 1

            sample_age = timestamp - last_timestamp
            if sample_age > MAX_SAMPLE_AGE:
                metrics_to_remove.append(metric)
                logger.debug('debug :: metrics_manager :: redistimeseries_roomba :: metric to remove %s - last_timestamp: %s' % (
                    str(metric), str(last_timestamp)))
            else:
                metrics_with_current_data += 1
        except Exception as err:
            if str(err) == 'TSDB: the key does not exist':
                metrics_to_remove.append(metric)
                logger.debug('debug :: metrics_manager :: redistimeseries_roomba :: metric to remove %s - TSDB the key does not exist' % (
                    str(metric)))
                continue
            if not error_logged:
                logger.error(traceback.format_exc())
                logger.error('error :: metrics_manager :: redistimeseries_roomba :: failed to determine last_sample from %s Redis key - %s' % (
                    str(len(metric)), err))
                error_logged = True
    key = 'metrics_manager.redistimeseries_roomba.metrics_last_timestamp_by_hour.%s' % str(timestamp)
    if metrics_by_hour:
        try:
            self.redis_conn_decoded.hset(key, mapping=metrics_by_hour)
            self.redis_conn_decoded.expire(key, 600)
        except Exception as err:
            logger.error(traceback.format_exc())
            logger.error('error :: metrics_manager :: redistimeseries_roomba :: failed to hset %s - %s' % (
                key, err))

    logger.info('metrics_manager :: redistimeseries_roomba :: determined %s old metrics to remove' % str(len(metrics_to_remove)))
    logger.info('metrics_manager :: redistimeseries_roomba :: %s metrics have data within FULL_DURATION' % str(metrics_with_current_data))
    return metrics_to_remove


def create_labelled_base_names(labelled_metrics, active_labelled_ids_with_metrics, ids_with_labelled_metric_names):
    """
    Create a list of labelled_metrics base_names
    """
    labelled_basenames = []
    errors = []
    lookup_base_name_count = 0
    unknown_base_names = 0
    active_labelled_metric_ids = []
    if labelled_metrics:
        for labelled_metric in labelled_metrics:
            # labelled_basename = labelled_metric.replace('labelled_metrics.', '')
            # labelled_basenames.append(labelled_basename)
            try:
                metric_id_str = labelled_metric.replace('labelled_metrics.', '', 1)
                metric_id = int(float(metric_id_str))
                active_labelled_metric_ids.append(metric_id)
            except Exception as err:
                errors.append(['labelled_basenames no metric id', labelled_metric, str(err)])
                continue
            base_name = None
            try:
                base_name = active_labelled_ids_with_metrics[metric_id]
            except KeyError:
                try:
                    base_name = active_labelled_ids_with_metrics[str(metric_id)]
                except:
                    pass
            except:
                pass
            if not base_name:
                try:
                    base_name = ids_with_labelled_metric_names[metric_id]
                except:
                    pass
            if not base_name:
                try:
                    base_name = get_base_name_from_metric_id('analyzer', metric_id)
                    lookup_base_name_count += 1
                except Exception as err:
                    errors.append(['labelled_basenames get_base_name_from_metric_id failed', labelled_metric, str(err)])
            if not base_name:
                unknown_base_names += 1
                continue
            labelled_basenames.append(base_name)
    if errors:
        logger.error('error :: metrics_manager :: redistimeseries_roomba :: %s errors reported in labelled_basenames interpolation of metric name from id, last - %s' % (
            str(len(errors)), str(errors[-1])))
        del errors
    logger.info('metrics_manager :: redistimeseries_roomba :: looked up %s base_names from the datatbase that were not in active_labelled_ids_with_metrics, %s base_names were not found' % (
        str(lookup_base_name_count), str(unknown_base_names)))
    return labelled_basenames


def get_remove_metric_ids(metrics_to_remove, ids_with_labelled_metric_names):
    """
    Determine the metric ids and unique labelled base_names to remove
    """
    errors = []
    metric_ids_to_remove = []
    # Remove the metrics
    unique_labelled_basenames_to_remove = []

    try:
        unique_metrics_to_remove = list(set(metrics_to_remove))
        for labelled_metric in unique_metrics_to_remove:
            # labelled_basename = labelled_metric.replace('labelled_metrics.', '')
            # unique_labelled_basenames_to_remove.append(labelled_basename)
            try:
                metric_id_str = labelled_metric.replace('labelled_metrics.', '', 1)
                metric_id = int(float(metric_id_str))
                labelled_metric_base_name = None
                try:
                    labelled_metric_base_name = ids_with_labelled_metric_names[metric_id]
                except:
                    pass
                if not labelled_metric_base_name:
                    try:
                        labelled_metric_base_name = get_base_name_from_metric_id('analyzer', metric_id)
                    except Exception as err:
                        errors.append(['get_remove_metric_ids - labelled_basenames get_base_name_from_metric_id failed', str(metric_id), str(err)])
                if labelled_metric_base_name:
                    unique_labelled_basenames_to_remove.append(labelled_metric_base_name)
                    metric_ids_to_remove.append(int(float(metric_id_str)))
                else:
                    errors.append(['get_remove_metric_ids - labelled_metric_base_name not found', str(labelled_metric)])
            except Exception as err:
                errors.append(['interpolation of metric name', labelled_metric, str(err)])
        if errors:
            logger.error('error :: metrics_manager :: redistimeseries_roomba :: get_remove_metric_ids :: %s errors reported in interpolation of metric name from id, last - %s' % (
                str(len(errors)), str(errors[-1])))
            del errors
    except Exception as err:
        logger.error(traceback.format_exc())
        logger.error('error :: metrics_manager :: redistimeseries_roomba :: get_remove_metric_ids failed - %s' % (
            err))
    return metric_ids_to_remove, unique_labelled_basenames_to_remove


def remove_redistimeseries_keys(self, metrics_to_remove):
    """
    Delete the redistimeseries keys
    """
    removed_metrics = []
    try:
        deleted_keys_count = 0
        unique_metrics_to_remove = list(set(metrics_to_remove))
        if unique_metrics_to_remove:
            try:
                deleted_keys_count = self.redis_conn_decoded.delete(*unique_metrics_to_remove)
                removed_metrics = list(unique_metrics_to_remove)
            except Exception as err:
                logger.error(traceback.format_exc())
                logger.error('error :: metrics_manager :: redistimeseries_roomba :: remove_redistimeseries_keys failed to delete %s Redis keys - %s' % (
                    str(len(unique_metrics_to_remove)), err))
        logger.info('metrics_manager :: redistimeseries_roomba :: remove_redistimeseries_keys deleted %s labelled_metrics. Redis keys of the %s unique_metrics_to_remove' % (
            str(deleted_keys_count), str(len(unique_metrics_to_remove))))
    except Exception as err:
        logger.error(traceback.format_exc())
        logger.error('error :: metrics_manager :: redistimeseries_roomba :: remove_redistimeseries_keys failed - %s' % (
            err))
    return removed_metrics


def remove_from_labelled_metrics(self, metrics_to_remove):
    """
    Delete from the labelled_metrics set
    """
    deleted_item_count = 0
    try:
        unique_metrics_to_remove = list(set(metrics_to_remove))
        if unique_metrics_to_remove:
            try:
                deleted_item_count = self.redis_conn_decoded.srem('labelled_metrics.unique_labelled_metrics', *unique_metrics_to_remove)
            except Exception as err:
                logger.error(traceback.format_exc())
                logger.error('error :: metrics_manager :: redistimeseries_roomba :: remove_from_labelled_metrics failed to delete %s items from Redis set - %s' % (
                    str(len(unique_metrics_to_remove)), err))
        logger.info('metrics_manager :: redistimeseries_roomba :: remove_from_labelled_metrics deleted %s labelled_metrics. from Redis set of the %s unique_metrics_to_remove' % (
            str(deleted_item_count), str(len(unique_metrics_to_remove))))
    except Exception as err:
        logger.error(traceback.format_exc())
        logger.error('error :: metrics_manager :: redistimeseries_roomba :: remove_from_labelled_metrics failed - %s' % (
            err))
    return deleted_item_count


def remove_from_hash_keys(self, unique_labelled_basenames_to_remove):
    """
    Delete from the hash keys
    """
    hash_keys = [
        'metrics_manager.prometheus_skip_dict',
        # 'skyline.prometheus_to_dotted',
    ]
    deleted_from_hash_keys = []
    if unique_labelled_basenames_to_remove:
        for hash_key in hash_keys:
            try:
                # Delete the labelled metric base_names from the Redis hash
                self.redis_conn_decoded.hdel(hash_key, *unique_labelled_basenames_to_remove)
                logger.info('metrics_manager :: redistimeseries_roomba :: remove_from_hash_keys :: hdel %s metrics from %s Redis hash' % (
                    str(len(unique_labelled_basenames_to_remove)), hash_key))
                deleted_from_hash_keys.append(hash_key)
            except Exception as err:
                logger.error(traceback.format_exc())
                logger.error('error :: metrics_manager :: redistimeseries_roomba :: remove_from_hash_keys :: failed to hdel %s metrics from %s Redis hash - %s' % (
                    str(len(unique_labelled_basenames_to_remove)), hash_key, err))
    return deleted_from_hash_keys


def clean_up_hash_keys(self):
    """
    Clean up hash keys
    """
    hash_keys = [
        'analyzer_labelled_metrics.last_timeseries_timestamp',
        'analyzer_labelled_metrics.stationary_metrics',
    ]
    cleaned_up = {}
    timestamp = int(time())
    for hash_key in hash_keys:
        hash_dict = {}
        logger.info('metrics_manager :: redistimeseries_roomba :: clean up - checking for entries to remove from %s Redis hash' % (
            hash_key))
        try:
            hash_dict = self.redis_conn_decoded.hgetall(hash_key)
        except Exception as err:
            logger.error(traceback.format_exc())
            logger.error('error :: metrics_manager :: redistimeseries_roomba :: failed to hgetall from %s Redis set - %s' % (
                hash_key, err))
        keys_to_remove = []
        error_logged = False
        for item in hash_dict.items():
            try:
                last_ts = None
                if hash_key == 'analyzer_labelled_metrics.last_timeseries_timestamp':
                    last_ts = int(float(item[1]))
                if hash_key == 'analyzer_labelled_metrics.stationary_metrics':
                    stationary_str = item[1]
                    if stationary_str:
                        stationary_str_elements = stationary_str.split(',')
                        last_ts = int(float(stationary_str_elements[1]))
                if last_ts < (timestamp - FULL_DURATION):
                    keys_to_remove.append(item[0])
            except Exception as err:
                if not error_logged:
                    logger.error(traceback.format_exc())
                    logger.error('error :: metrics_manager :: redistimeseries_roomba :: failed to determine if key to be removed from %s Redis hash for item %s - %s' % (
                        hash_key, str(item), err))
                error_logged = True
        logger.info('metrics_manager :: redistimeseries_roomba :: clean up - %s entries to remove from %s Redis hash' % (
            str(len(keys_to_remove)), hash_key))
        cleaned_up[hash_key] = len(keys_to_remove)
        if keys_to_remove:
            try:
                self.redis_conn.hdel(hash_key, *set(keys_to_remove))
                logger.info('metrics_manager :: redistimeseries_roomba :: clean up - removed %s entries from %s Redis hash' % (
                    str(len(keys_to_remove)), hash_key))
            except:
                logger.error(traceback.format_exc())
                logger.error('error :: metrics_manager :: failed to %s remove entries from Redis hash key %s' % (
                    str(len(keys_to_remove)), hash_key))
    return cleaned_up


def set_db_metric_ids_to_inactive(metric_ids_to_remove, new_labelled_metrics, ids_with_labelled_metric_names):
    """
    Determine the metrics id that need to be set as inactive in the database and
    set to inactive.
    """
    metrics_set_as_inactive = []
    db_metric_ids_to_set_inactive = list(set(metric_ids_to_remove))
    try:
        logger.info('metrics_manager :: redistimeseries_roomba :: checking if any of the %s active labelled metrics from the database need to be set as inactive' % str(len(ids_with_labelled_metric_names)))
        db_labelled_metrics = []
        for metric_id in list(ids_with_labelled_metric_names.keys()):
            labelled_metric = 'labelled_metrics.%s' % str(metric_id)
            db_labelled_metrics.append(labelled_metric)
        logger.info('metrics_manager :: redistimeseries_roomba :: determined %s labelled_metrics from labelled_metrics.unique_labelled_metrics Redis set' % str(len(db_labelled_metrics)))
        old_labelled_metrics = list(set(db_labelled_metrics) - set(new_labelled_metrics))
        logger.info('metrics_manager :: redistimeseries_roomba :: there are %s labelled metrics in the database marked as active that are not active' % str(len(old_labelled_metrics)))
        old_labelled_metric_ids = []
        if old_labelled_metrics:
            for old_labelled_metric in old_labelled_metrics:
                metric_id_str = old_labelled_metric.replace('labelled_metrics.', '', 1)
                old_labelled_metric_ids.append(int(metric_id_str))
        if old_labelled_metric_ids:
            logger.info('metrics_manager :: redistimeseries_roomba :: determined %s old labelled_metrics to set as inactive in the database' % str(len(old_labelled_metric_ids)))
            db_metric_ids_to_set_inactive = list(set(metric_ids_to_remove + old_labelled_metric_ids))
    except Exception as err:
        logger.error(traceback.format_exc())
        logger.error('error :: metrics_manager :: redistimeseries_roomba :: set_db_metric_ids_to_inactive failed - %s' % (
            err))
    # Set metrics in database as inactive
    if db_metric_ids_to_set_inactive:
        logger.info('metrics_manager :: redistimeseries_roomba :: setting %s metrics as inactive in the database' % (
            str(len(db_metric_ids_to_set_inactive))))
        dry_run = False
        try:
            metrics_set_as_inactive = set_metrics_as_inactive('analyzer', db_metric_ids_to_set_inactive, [], dry_run)
        except Exception as err:
            logger.error(traceback.format_exc())
            logger.error('error :: metrics_manager :: redistimeseries_roomba :: set_metrics_as_inactive failed - %s' % (
                str(err)))
        if metrics_set_as_inactive:
            logger.info('metrics_manager :: redistimeseries_roomba :: set %s metrics as inactive in the database' % (
                str(len(metrics_set_as_inactive))))
    return metrics_set_as_inactive


def redistimeseries_roomba(self, active_labelled_ids_with_metrics, ids_with_labelled_metric_names):
    """

    Manage the labelled_metrics timeseries data in Redis.

    :param self: the self object
    :type self: object
    :return: removed_metrics
    :rtype: list

    """
    removed_metrics = []

    logger.info('metrics_manager :: redistimeseries_roomba :: determining metrics to remove')

    # Determine the list of labelled metric id flagged to be removed by analyzer
    # apps
    metrics_to_remove = get_metrics_to_remove(self)

    # The labelled_metrics by id e.g. labelled_metrics.1234
    labelled_metrics = get_labelled_metrics(self)

    # Remove the already known metrics to be removed from the list of metrics to
    # check
    metrics_to_check = get_metrics_to_check(metrics_to_remove, labelled_metrics)

    # Check the last timestamp of the metrics deemed to check
    metrics_to_remove = check_last_timestamp(self, metrics_to_check, metrics_to_remove)

    # Get the active labelled_metrics if not passed
    logger.info('metrics_manager :: redistimeseries_roomba :: %s active_labelled_ids_with_metrics' % str(len(active_labelled_ids_with_metrics)))
    if not active_labelled_ids_with_metrics:
        try:
            active_labelled_ids_with_metrics = self.redis_conn_decoded.hgetall('aet.metrics_manager.active_labelled_ids_with_metric')
            logger.info('metrics_manager :: redistimeseries_roomba :: fetched %s active_labelled_ids_with_metrics from Redis' % str(len(active_labelled_ids_with_metrics)))
        except Exception as err:
            logger.error('error :: metrics_manager :: redistimeseries_roomba :: hgetall aet.metrics_manager.active_labelled_ids_with_metric failed - %s' % (
                err))

    # Create a list of labelled_metrics base_names
    # labelled_basenames = create_labelled_base_names(labelled_metrics, active_labelled_ids_with_metrics, ids_with_labelled_metric_names)

    metric_ids_to_remove, unique_labelled_basenames_to_remove = get_remove_metric_ids(metrics_to_remove, ids_with_labelled_metric_names)

    if metrics_to_remove:
        removed_metrics = remove_redistimeseries_keys(self, metrics_to_remove)
        logger.info('metrics_manager :: redistimeseries_roomba :: removed %s redistimeseries metric keys' % str(len(removed_metrics)))
        deleted_item_count = remove_from_labelled_metrics(self, metrics_to_remove)
        logger.info('metrics_manager :: redistimeseries_roomba :: removed %s metrics from labelled_metrics.unique_labelled_metrics' % str(deleted_item_count))

    if unique_labelled_basenames_to_remove:
        deleted_from_hash_keys = remove_from_hash_keys(self, unique_labelled_basenames_to_remove)
        logger.info('metrics_manager :: redistimeseries_roomba :: deleted %s metrics from hash keys - %s' % (
            str(len(unique_labelled_basenames_to_remove)), str(deleted_from_hash_keys)))

    cleaned_up_dict = clean_up_hash_keys(self)
    logger.info('metrics_manager :: redistimeseries_roomba :: cleaned up keys - %s' % str(cleaned_up_dict))

    # The labelled_metrics by id e.g. labelled_metrics.1234
    new_labelled_metrics = get_labelled_metrics(self)
    metrics_set_as_inactive = set_db_metric_ids_to_inactive(metric_ids_to_remove, new_labelled_metrics, ids_with_labelled_metric_names)
    logger.info('metrics_manager :: redistimeseries_roomba :: %s metrics set as inactive in the database' % (
        str(len(metrics_set_as_inactive))))

    if metric_ids_to_remove:
        metric_ids_to_remove_strs = [str(item) for item in metric_ids_to_remove]
        try:
            metric_ids_to_removed_type = self.redis_conn_decoded.hdel('skyline.labelled_metrics.id.type', *metric_ids_to_remove_strs)
            logger.info('metrics_manager :: redistimeseries_roomba :: removed %s ids from skyline.labelled_metrics.id.type Redis hash' % str(metric_ids_to_removed_type))
        except Exception as err:
            logger.error('error :: metrics_manager :: redistimeseries_roomba :: hdel skyline.labelled_metrics.id.type failed - %s' % (
                err))

    return removed_metrics
