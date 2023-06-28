"""
manage_labelled_metrics_longterm_metric_type.py
"""
import logging
import traceback
from random import randint
from time import time

from functions.victoriametrics.get_victoriametrics_metric import get_victoriametrics_metric
from functions.timeseries.strictly_increasing_monotonicity import strictly_increasing_monotonicity

skyline_app = 'analyzer'
skyline_app_logger = '%sLog' % skyline_app
logger = logging.getLogger(skyline_app_logger)


# @added 20230518 - metric_type.longterm_expire
def manage_labelled_metrics_longterm_metric_type(self, run_every, active_labelled_metrics_with_id):
    """

    Create and manage the metrics_manager.untrainable_new_metrics Redis hash
    This is a very complicated process because the metric_type can be set by
    Prometheus metadata (default - possibility for incorrect developer definition),
    analyzer_labelled_metrics (3h or 24h), mirage_labelled_metrics (7d),
    get_victoriametrics_metric (various and optional) and by metrics_manager (> 24h).
    The skyline.labelled_metrics.id.type should be authorative but can be set by
    multiple things.  The reliability of the type should be consider 30d as the
    most authorative, then 7d and then 3h/24h.

    :param self: the self object
    :param new_metrics: the new metrics
    :type self: object
    :type new_metrics: dict
    :return: managed_new_metrics_dict
    :rtype: dict

    """
    updated_types = 0
    now = int(time())
    logger.info('metrics_manager :: manage_labelled_metrics_longterm_metric_type :: starting')

    skyline_labelled_metrics_id_type = {}
    try:
        skyline_labelled_metrics_id_type = self.redis_conn_decoded.hgetall('skyline.labelled_metrics.id.type')
    except Exception as err:
        logger.error('error :: metrics_manager :: manage_labelled_metrics_longterm_metric_type :: failed to hdel from %s - %s' % (
            hash_key, err))

    # A dict of metrics to check, which is later ordered by timestamp
    # to determine which metrics to check in this run
    to_check = {}
    default_last_checked = (now - (86400 * 30) + 3600)
    max_checks_per_run = len(active_labelled_metrics_with_id) / (1440 / run_every)
    active_metric_ids_with_base_names = {}
    for base_name in list(active_labelled_metrics_with_id.keys()):
        try:
            metric_id = int(active_labelled_metrics_with_id[base_name])
            active_metric_ids_with_base_names[metric_id] = base_name
            # Start out that every active metric should be checked
            # defaulting to last checked time being > 30d ago
            to_check[metric_id] = {'metric': base_name, 'last_checked': default_last_checked}
        except:
            continue

    max_checks_per_run = int(len(to_check) / (86400 / run_every))
    within_expiry = 0

    # longterm expiry timestamps
    hash_key = 'skyline.labelled_metrics.id.type.longterm_expire'
    skyline_labelled_metrics_id_type_longterm = {}
    try:
        skyline_labelled_metrics_id_type_longterm = self.redis_conn_decoded.hgetall(hash_key)
    except Exception as err:
        logger.error('error :: metrics_manager :: manage_labelled_metrics_longterm_metric_type :: failed to hgetall %s - %s' % (
            hash_key, err))
    for metric_id_str in list(skyline_labelled_metrics_id_type_longterm.keys()):
        try:
            expiry_ts_str = skyline_labelled_metrics_id_type_longterm[metric_id_str]
            if expiry_ts_str:
                expiry_ts = int(expiry_ts_str)
                if expiry_ts > now:
                    within_expiry += 1
                    del to_check[int(metric_id_str)]
                else:
                    # Even though the 30d and 7d check can set the expiry at less than
                    # 30d or 7d it is assumed it was last checked at least 7 days ago
                    to_check[int(metric_id_str)]['last_checked'] = expiry_ts - (86400 * 7) + randint(0, 3600)
        except:
            continue

    logger.info('metrics_manager :: manage_labelled_metrics_longterm_metric_type :: of the total %s active labelled_metrics, not checking %s which are still within the expiry period' % (
        str(len(active_labelled_metrics_with_id)), str(within_expiry)))

    to_check_list = []
    for metric_id in list(to_check.keys()):
        to_check_list.append([metric_id, to_check[metric_id]['metric'], to_check[metric_id]['last_checked']])
    to_check_list = sorted(to_check_list, key=lambda x: x[2])
    logger.info('metrics_manager :: manage_labelled_metrics_longterm_metric_type :: there are a total of %s labelled_metrics longterm monotonicity to check' % (
        str(len(to_check_list))))
    
    changed_metric_type = []
    update_types = {}
    check_list = to_check_list[0:max_checks_per_run]
    if check_list:
        logger.info('metrics_manager :: manage_labelled_metrics_longterm_metric_type :: only checking %s (max %s) labelled_metrics for longterm monotonicity' % (
            str(len(check_list)), str(max_checks_per_run)))        
    until_timestamp = (now - 600)
    from_timestamp = until_timestamp - (86400 * 30)
    timestamp_30d = now - (86400 * 29)
    timestamp_7d = now - int(86400 * 6.8)
    errors = []
    longterm_expires = {}
    checked_ids = []
    for item in check_list:
        try:
            metric_type = None
            expire = None
            metric_id = item[0]
            base_name = item[1]
            timeseries = []
            try:
                timeseries = get_victoriametrics_metric(skyline_app, base_name, from_timestamp, until_timestamp, 'list', 'object', metric_data={}, plot_parameters={}, do_not_type=True)
            except Exception as err:
                errors.append(['get_victoriametrics_metric', base_name, err])
                continue
            if not timeseries:
                errors.append(['no timeseries', base_name, err])
                continue
            checked_ids.append(metric_id)
            first_timestamp = timeseries[0][0]
            if first_timestamp < timestamp_30d:
                # Check it again after 14 days with some randomness
                expire = now + (86400 * 14) + randint(0, 7200)
            if not expire:
                if first_timestamp < timestamp_7d:
                    # Check it again after 1 days with some randomness
                    # because data at 7d change change monotonicity over
                    # 12 days say
                    expire = now + (86400) + randint(0, 7200)
            if not expire:
                # Check it again after between 1 and 5 hours
                expire = now + 3600 + randint(0, 7200)
            longterm_expires[str(metric_id)] = str(expire)
            is_strictly_increasing_monotonic = strictly_increasing_monotonicity(timeseries)
            if is_strictly_increasing_monotonic:
                metric_type = '1'
            else:
                metric_type = '0'

            current_metric_type = None
            update_type = False
            try:
                current_metric_type = skyline_labelled_metrics_id_type[str(metric_id)]
            except KeyError:
                changed_metric_type.append(item)
                update_type = True
            if current_metric_type:
                if current_metric_type != metric_type:
                    changed_metric_type.append(item)
                    update_type = True
            if update_type:
                update_types[str(metric_id)] = metric_type
                logger.info('metrics_manager :: manage_labelled_metrics_longterm_metric_type :: changed metric_type id from %s to %s for metric_id: %s - %s' % (
                    str(current_metric_type), metric_type, str(metric_id), str(base_name)))

        except Exception as err:
            logger.error('error :: metrics_manager :: manage_labelled_metrics_longterm_metric_type :: failed to determine monotonicity for %s - %s' % (
                str(item), err))

    if errors:
        logger.error('error :: metrics_manager :: manage_labelled_metrics_longterm_metric_type :: encountered errors, sample follows: %s' % (
            str(errors[-2:])))

    if update_types:
        logger.info('metrics_manager :: manage_labelled_metrics_longterm_metric_type :: updating %s metric ids that have changed type in skyline.labelled_metrics.id.type' % (
            str(len(update_types))))
        try:
            updated_types = self.redis_conn_decoded.hset('skyline.labelled_metrics.id.type', mapping=update_types)
        except Exception as err:
            logger.error('error :: metrics_manager :: manage_labelled_metrics_longterm_metric_type :: failed to update skyline.labelled_metrics.id.type - %s' % (
                err))
        if updated_types:
            try:
                self.redis_conn_decoded.setex('skyline.labelled_metrics.id.type.changed', 60, updated_types)
            except Exception as err:
                logger.error(traceback.format_exc())
                logger.error('error :: metrics_manager :: manage_labelled_metrics_longterm_metric_type :: failed to create Redis key skyline.labelled_metrics.id.type.changed - %s' % (
                    err))

    if longterm_expires:
        logger.info('metrics_manager :: manage_labelled_metrics_longterm_metric_type :: updating %s metric ids expires in skyline.labelled_metrics.id.type.longterm_expire' % (
            str(len(longterm_expires))))
        try:
            longterm_expired = self.redis_conn_decoded.hset('skyline.labelled_metrics.id.type.longterm_expire', mapping=longterm_expires)
        except Exception as err:
            logger.error('error :: metrics_manager :: manage_labelled_metrics_longterm_metric_type :: failed to update skyline.labelled_metrics.id.type.longterm_expire - %s' % (
                err))

    return (len(checked_ids), len(update_types))
