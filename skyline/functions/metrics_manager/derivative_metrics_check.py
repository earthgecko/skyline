"""
derivative_metric_check.py
"""
import logging
import traceback
from time import time

from settings import FULL_NAMESPACE
from skyline_functions import get_graphite_metric
from functions.timeseries.strictly_increasing_monotonicity import strictly_increasing_monotonicity

skyline_app = 'analyzer'
skyline_app_logger = '%sLog' % skyline_app
logger = logging.getLogger(skyline_app_logger)


# @added 20220419 - Feature #4528: metrics_manager - derivative_metric_check
#                   Feature #3866: MIRAGE_ENABLE_HIGH_RESOLUTION_ANALYSIS
def derivative_metrics_check(self):
    """

    Long period derivative metric check.  Certain types of metrics can display
    strictly increasing monotonicity over a 24 hour period, however they are not
    over a long period, this has been identified in some Redis usec metrics.
    Surface Graphite data at 28 days and apply strictly_increasing_monotonicity
    check and remove any incorrectly identified metrics and create a hash of non
    derivative metrics.

    :param self: the self object
    :param base_name: the base_name of the metric to check
    :type self: object
    :type base_name: str
    :return: longterm_non_derivative_metrics
    :rtype: list

    """

    longterm_non_derivative_metrics = []
    current_derivative_metrics = []
    try:
        current_derivative_metrics = list(self.redis_conn_decoded.smembers('derivative_metrics'))
        logger.info('metrics_manager :: derivative_metric_check :: found %s derivative metrics from derivative_metrics Redis set' % str(len(current_derivative_metrics)))
    except Exception as err:
        logger.error('error :: metrics_manager :: derivative_metric_check :: failed to get derivative_metrics Redis set - %s' % str(err))
    aet_metrics_manager_derivative_metrics = []
    try:
        aet_metrics_manager_derivative_metrics_timestamps = self.redis_conn_decoded.hgetall('aet.metrics_manager.derivative_metrics.timestamps')
        aet_metrics_manager_derivative_metrics = list(aet_metrics_manager_derivative_metrics_timestamps.keys())
        logger.info('metrics_manager :: derivative_metric_check :: found %s derivative metrics from aet.metrics_manager.derivative_metrics.timestamps Redis hash' % str(len(aet_metrics_manager_derivative_metrics)))
    except Exception as err:
        logger.error('error :: metrics_manager :: derivative_metric_check :: failed to hgetall aet.metrics_manager.derivative_metrics.timestamps to check derivative_metric timestamps - %s' % str(err))

    all_derivative_metrics = list(set(current_derivative_metrics + aet_metrics_manager_derivative_metrics))
    logger.info('metrics_manager :: derivative_metric_check :: found %s unique derivative metrics' % str(len(all_derivative_metrics)))

    now = int(time())
    try:
        longterm_non_derivative_metrics_dict = self.redis_conn_decoded.hgetall('metrics_manager.longterm_non_derivative_metrics')
    except Exception as err:
        logger.error('error :: metrics_manager :: derivative_metric_check :: failed to hgetall metrics_manager.longterm_non_derivative_metrics - %s' % str(err))
    for metric in list(longterm_non_derivative_metrics_dict.keys()):
        if (now - int(float(longterm_non_derivative_metrics_dict[metric]))) > 88000:
            try:
                self.redis_conn_decoded.hdel('metrics_manager.longterm_non_derivative_metrics', metric)
            except Exception as err:
                logger.error('error :: metrics_manager :: derivative_metric_check :: failed to hdel %s from metrics_manager.longterm_non_derivative_metrics - %s' % (
                    metric, str(err)))
            continue
        longterm_non_derivative_metrics.append(metric)

    # Only check once per 24 hours
    do_not_check_derivative_metrics = []
    try:
        metrics_manager_last_check_derivative_metrics_dict = self.redis_conn_decoded.hgetall('metrics_manager.last_check_derivative_metrics')
    except Exception as err:
        logger.error('error :: metrics_manager :: derivative_metric_check :: failed to hgetall metrics_manager.last_check_derivative_metrics - %s' % str(err))
    metrics_manager_last_check_derivative_metrics = list(metrics_manager_last_check_derivative_metrics_dict.keys())
    for metric in metrics_manager_last_check_derivative_metrics:
        if (now - int(float(metrics_manager_last_check_derivative_metrics_dict[metric]))) < 86400:
            do_not_check_derivative_metrics.append(metric)
        if (now - int(float(metrics_manager_last_check_derivative_metrics_dict[metric]))) > 88000:
            try:
                self.redis_conn_decoded.hdel('metrics_manager.last_check_derivative_metrics', metric)
            except Exception as err:
                logger.error('error :: metrics_manager :: derivative_metric_check :: failed to hdel metrics_manager.last_check_derivative_metrics - %s' % str(err))

    number_checked = 0
    number_to_check = int(len(all_derivative_metrics) / (86400 / 300))
    if number_to_check < 1:
        number_to_check = 1

    remove_from_derivative_metrics = []
    check_for_derivative = False

    check_for_one_minute = False
    if len(metrics_manager_last_check_derivative_metrics) < (len(all_derivative_metrics) - 10):
        check_for_one_minute = True
        logger.info('metrics_manager :: derivative_metric_check :: checking derivative metrics for 60 seconds this run')
    if not check_for_one_minute:
        logger.info('metrics_manager :: derivative_metric_check :: checking %s derivative metrics this run' % (
            str(number_to_check)))

    for metric in all_derivative_metrics:
        if 'skyline_set_as_of_' in metric:
            continue
        # Try to get through checking all the derivative metrics in 24 hours
        if not check_for_one_minute:
            if number_checked == number_to_check:
                logger.info('metrics_manager :: derivative_metric_check :: checked %s derivative metrics this run' % (
                    str(number_checked)))
                break
        else:
            if (int(time()) - now) >= 60:
                logger.info('metrics_manager :: derivative_metric_check :: checked %s derivative metrics this run' % (
                    str(number_checked)))
                break

        if metric in longterm_non_derivative_metrics:
            remove_from_derivative_metrics.append(metric)
            continue
        if metric in do_not_check_derivative_metrics:
            continue
        derivative_metric = False
        until_timestamp = (int(time()) - 1800)
        from_timestamp = until_timestamp - (86400 * 28)
        if metric.startswith(FULL_NAMESPACE):
            base_name = metric.replace(FULL_NAMESPACE, '', 1)
        else:
            base_name = str(metric)
        timeseries = []
        try:
            timeseries = get_graphite_metric(
                skyline_app, base_name, from_timestamp, until_timestamp, 'list',
                'object', check_for_derivative)
        except Exception as err:
            logger.error(traceback.format_exc())
            logger.error('error :: metrics_manager :: derivative_metric_check :: get_graphite_metric failed for %s - %s' % (
                base_name, err))
        number_checked += 1

        # Only check if the first timestamp in the timeseries is older than 1 day
        first_timestamp = now
        if timeseries:
            try:
                first_timestamp = int(timeseries[0][0])
            except Exception as err:
                logger.warning('metrics_manager :: derivative_metric_check :: could not determine first timestamp for %s - %s' % (
                    base_name, err))
                first_timestamp = now

        if timeseries and first_timestamp < (now - 86400):
            try:
                derivative_metric = strictly_increasing_monotonicity(timeseries)
            except Exception as err:
                logger.error(traceback.format_exc())
                logger.error('error :: metrics_manager :: derivative_metric_check :: strictly_increasing_monotonicity failed for %s - %s' % (
                    base_name, err))
                # If there is any error determining is the timeseries is monotonic
                # default to the derivative state that it has been classified as
                derivative_metric = True
            if not derivative_metric:
                remove_from_derivative_metrics.append(metric)
                logger.info('metrics_manager :: derivative_metric_check :: %s is not strictly increasing monotonically over the long term, removing from derivative_metrics' % base_name)
            else:
                logger.info('metrics_manager :: derivative_metric_check :: OK %s is strictly increasing monotonically over the long term' % base_name)
        try:
            self.redis_conn_decoded.hset('metrics_manager.last_check_derivative_metrics', metric, int(time()))
        except Exception as err:
            logger.error('error :: metrics_manager :: derivative_metric_check :: failed to hset metrics_manager.last_check_derivative_metrics - %s' % str(err))
    remove_from_derivative_metrics = list(set(remove_from_derivative_metrics))
    logger.info('metrics_manager :: derivative_metric_check :: found %s derivative metrics to remove' % str(len(remove_from_derivative_metrics)))
    if remove_from_derivative_metrics:
        remove_from_derivative_metrics_dict = {}
        for metric in remove_from_derivative_metrics:
            remove_from_derivative_metrics_dict[metric] = now
        logger.info('metrics_manager :: derivative_metric_check :: removing %s metrics from derivative metrics sets and hashes' % (
            str(len(remove_from_derivative_metrics))))
        try:
            self.redis_conn_decoded.hset('metrics_manager.longterm_non_derivative_metrics', mapping=remove_from_derivative_metrics_dict)
        except Exception as err:
            logger.error('error :: metrics_manager :: derivative_metric_check :: failed to hset %s metrics to metrics_manager.longterm_non_derivative_metrics - %s' % (
                str(len(remove_from_derivative_metrics)), str(err)))
        try:
            self.redis_conn_decoded.sadd('non_derivative_metrics', *set(remove_from_derivative_metrics))
        except Exception as err:
            logger.error('error :: metrics_manager :: derivative_metric_check :: failed to sadd %s metrics to non_derivative_metrics - %s' % (
                str(len(remove_from_derivative_metrics)), str(err)))
        try:
            self.redis_conn_decoded.srem('derivative_metrics', *set(remove_from_derivative_metrics))
        except Exception as err:
            logger.error('error :: metrics_manager :: derivative_metric_check :: failed to srem %s metrics from derivative_metrics - %s' % (
                str(len(remove_from_derivative_metrics)), str(err)))
        try:
            self.redis_conn_decoded.srem('metrics_manager.always_derivative_metrics', *set(remove_from_derivative_metrics))
        except Exception as err:
            logger.error('error :: metrics_manager :: derivative_metric_check :: failed to srem %s metrics from derivative_metrics - %s' % (
                str(len(remove_from_derivative_metrics)), str(err)))
        try:
            self.redis_conn_decoded.srem('aet.metrics_manager.derivative_metrics', *set(remove_from_derivative_metrics))
        except Exception as err:
            logger.error('error :: metrics_manager :: derivative_metric_check :: failed to srem %s metrics from aet.metrics_manager.derivative_metrics - %s' % (
                str(len(remove_from_derivative_metrics)), str(err)))
        try:
            self.redis_conn_decoded.hdel('aet.metrics_manager.derivative_metrics.timestamps', *set(remove_from_derivative_metrics))
        except Exception as err:
            logger.error('error :: metrics_manager :: derivative_metric_check :: failed to hdel %s metrics from aet.metrics_manager.derivative_metrics.timestamps - %s' % (
                str(len(remove_from_derivative_metrics)), str(err)))
    for metric in remove_from_derivative_metrics:
        if metric.startswith(FULL_NAMESPACE):
            base_name = metric.replace(FULL_NAMESPACE, '', 1)
        else:
            base_name = str(metric)
        z_key = 'z.derivative_metric.%s' % base_name
        try:
            self.redis_conn_decoded.delete(z_key)
        except Exception as err:
            logger.error('error :: metrics_manager :: derivative_metric_check :: failed to delete %s Redis key - %s' % (
                z_key, str(err)))
        zz_key = 'z%s' % z_key
        try:
            self.redis_conn_decoded.delete(zz_key)
        except Exception as err:
            logger.error('error :: metrics_manager :: derivative_metric_check :: failed to delete %s Redis key - %s' % (
                zz_key, str(err)))

    try:
        longterm_non_derivative_metrics_dict = self.redis_conn_decoded.hgetall('metrics_manager.longterm_non_derivative_metrics')
        longterm_non_derivative_metrics = list(longterm_non_derivative_metrics_dict.keys())
    except Exception as err:
        logger.error('error :: metrics_manager :: derivative_metric_check :: failed to hgetall metrics_manager.longterm_non_derivative_metrics - %s' % str(err))
    return longterm_non_derivative_metrics
