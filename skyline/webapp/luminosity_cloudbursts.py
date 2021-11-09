import logging
import os
from ast import literal_eval
import traceback

import settings
import skyline_version
from skyline_functions import get_redis_conn_decoded
from matched_or_regexed_in_list import matched_or_regexed_in_list
from sqlalchemy.sql import select
from database import get_engine, cloudburst_table_meta

skyline_version = skyline_version.__absolute_version__
skyline_app = 'webapp'
skyline_app_logger = '%sLog' % skyline_app
logger = logging.getLogger(skyline_app_logger)
skyline_app_logfile = '%s/%s.log' % (settings.LOG_PATH, skyline_app)
logfile = '%s/%s.log' % (settings.LOG_PATH, skyline_app)
try:
    ENABLE_WEBAPP_DEBUG = settings.ENABLE_WEBAPP_DEBUG
except EnvironmentError as err:
    logger.error('error :: cannot determine ENABLE_WEBAPP_DEBUG from settings - %s' % err)
    ENABLE_WEBAPP_DEBUG = False

this_host = str(os.uname()[1])


def get_filtered_metrics(redis_conn_decoded, namespaces):
    """
    Get create a list of filter_by_metrics.

    :param redis_conn_decoded: the redis_conn_decoded object
    :param namespaces: the namespaces to match
    :param from_timestamp: the from_timestamp
    :param until_timestamp: the until_timestamp
    :type redis_conn_decoded: str
    :return: list of metrics
    :rtype:  list
    """
    function_str = 'get_cloudbursts :: get_filtered_metrics'
    filter_by_metrics = []
    redis_key = 'analyzer.metrics_manager.db.metric_names'
    unique_base_names = []
    try:
        unique_base_names = list(redis_conn_decoded.smembers(redis_key))
        if unique_base_names:
            logger.info('%s :: got %s unique_base_names' % (
                function_str, str(len(unique_base_names))))
    except Exception as err:
        logger.error(traceback.format_exc())
        logger.error('error :: %s :: failed to get Redis key %s - %s' % (
            function_str, redis_key, err))
        raise
    for base_name in unique_base_names:
        try:
            pattern_match, metric_matched_by = matched_or_regexed_in_list(skyline_app, base_name, namespaces)
            if pattern_match:
                filter_by_metrics.append(base_name)
        except Exception as err:
            logger.error(traceback.format_exc())
            logger.error('error :: %s :: failed to get Redis key %s - %s' % (
                function_str, redis_key, err))
    return filter_by_metrics


def get_metric_ids(redis_conn_decoded, filter_by_metrics):
    """
    Get create a list of metric ids and dict of metric_names_with_ids.

    :param redis_conn_decoded: the redis_conn_decoded object
    :param namespaces: the namespaces to match
    :param from_timestamp: the from_timestamp
    :param until_timestamp: the until_timestamp
    :type redis_conn_decoded: str
    :return: (list of metrics, dict of metric_names_with_ids)
    :rtype:  (list, dict)
    """
    function_str = 'get_cloudbursts :: get_metric_ids'
    metric_ids = []
    try:
        metric_names_with_ids = redis_conn_decoded.hgetall('aet.metrics_manager.metric_names_with_ids')
    except Exception as err:
        logger.error(traceback.format_exc())
        logger.error('error :: %s :: failed to get Redis hash aet.metrics_manager.metric_names_with_ids - %s' % (
            function_str, err))
        raise
    for base_name in filter_by_metrics:
        try:
            metric_ids.append(int(metric_names_with_ids[base_name]))
        except Exception as err:
            logger.error(traceback.format_exc())
            logger.error('error :: %s :: failed to add metric id to metric_ids for %s from metric_names_with_ids - %s' % (
                function_str, base_name, err))
    return (metric_ids, metric_names_with_ids)


def get_cloudbursts(metric, namespaces, from_timestamp, until_timestamp):
    """
    Get create a dict of all the cloudbursts.

    :param metric: the name of the metric
    :param namespaces: the namespaces to match
    :param from_timestamp: the from_timestamp
    :param until_timestamp: the until_timestamp
    :type metric: str
    :type namespaces: list
    :type from_timestamp: int
    :type until_timestamp: int
    :return: dict of cloudbursts
    :rtype:  {}

    Returns a dict of cloudbursts
    {
        "cloudbursts": {
            <id>: {
                'metric_id': <int>,
                'metric': <str>,
                'timestamp': <int>,
                'end': <int>,
                'duration': <int>,
                'duration': <int>,
                'from_timestamp': <int>,
                'resolution': <int>,
                'full_duration': <int>,
                'anomaly_id': <int>,
                'match_id': <int>,
                'fp_id': <int>,
                'layer_id': <int>,
                'added_at': <int>,
            },
        }
    }

    """

    function_str = 'get_cloudbursts'
    cloudbursts_dict = {}
    engine = None
    metric_ids = []
    use_filter_by_metrics = False
    filter_by_metrics = []
    metric_names_with_ids = {}
    ids_with_metric_names = {}

    logger.info(
        'get_cloudbursts - metric: %s, namespaces: %s, from_timestamp: %s, until_timestamp: %s' % (
            str(metric), str(namespaces), str(from_timestamp),
            str(until_timestamp)))

    if metric != 'all':
        filter_by_metrics = [metric]
        use_filter_by_metrics = True
        if not namespaces:
            namespaces = [metric]

    try:
        redis_conn_decoded = get_redis_conn_decoded(skyline_app)
    except Exception as err:
        logger.error(traceback.format_exc())
        logger.error('error :: %s :: get_redis_conn_decoded failed - %s' % (
            function_str, err))
        raise

    filter_by_metrics = get_filtered_metrics(redis_conn_decoded, namespaces)
    if namespaces:
        use_filter_by_metrics = True

    if metric != 'all':
        try:
            metric_id = int(redis_conn_decoded.hget('aet.metrics_manager.metric_names_with_ids', metric))
            if metric_id:
                metric_names_with_ids[metric] = metric_id
                metric_ids.append(metric_id)
        except Exception as err:
            logger.error(traceback.format_exc())
            logger.error('error :: %s :: failed to get %s from Redis hash aet.metrics_manager.metric_names_with_ids - %s' % (
                function_str, metric, err))
            raise

    metric_ids, metric_names_with_ids = get_metric_ids(redis_conn_decoded, filter_by_metrics)
    if len(filter_by_metrics) > 1:
        use_filter_by_metrics = True

    for base_name in list(metric_names_with_ids.keys()):
        metric_id = int(metric_names_with_ids[base_name])
        ids_with_metric_names[metric_id] = base_name

    try:
        engine, log_msg, trace = get_engine(skyline_app)
    except Exception as err:
        logger.error(traceback.format_exc())
        logger.error('error :: %s :: failed to get engine - %s' % (
            function_str, err))
        raise

    try:
        cloudburst_table, log_msg, trace = cloudburst_table_meta(skyline_app, engine)
    except Exception as err:
        logger.error(traceback.format_exc())
        logger.error('error :: %s :: failed to get cloudburst_table - %s' % (
            function_str, err))
        raise

    try:
        connection = engine.connect()
        if use_filter_by_metrics:
            stmt = select([cloudburst_table], cloudburst_table.c.metric_id.in_(metric_ids))
            if from_timestamp > 0 and until_timestamp == 0:
                stmt = select([cloudburst_table], cloudburst_table.c.metric_id.in_(metric_ids)).\
                    where(cloudburst_table.c.timestamp >= from_timestamp)
            if from_timestamp == 0 and until_timestamp > 0:
                stmt = select([cloudburst_table], cloudburst_table.c.metric_id.in_(metric_ids)).\
                    where(cloudburst_table.c.timestamp <= until_timestamp)
            if from_timestamp > 0 and until_timestamp > 0:
                stmt = select([cloudburst_table], cloudburst_table.c.metric_id.in_(metric_ids)).\
                    where(cloudburst_table.c.timestamp >= from_timestamp).\
                    where(cloudburst_table.c.timestamp <= until_timestamp)
        else:
            stmt = select([cloudburst_table])
            if from_timestamp > 0 and until_timestamp == 0:
                stmt = select([cloudburst_table]).\
                    where(cloudburst_table.c.timestamp >= from_timestamp)
            if from_timestamp == 0 and until_timestamp > 0:
                stmt = select([cloudburst_table]).\
                    where(cloudburst_table.c.timestamp <= until_timestamp)
            if from_timestamp > 0 and until_timestamp > 0:
                stmt = select([cloudburst_table]).\
                    where(cloudburst_table.c.timestamp >= from_timestamp).\
                    where(cloudburst_table.c.timestamp <= until_timestamp)
        results = connection.execute(stmt)
        for row in results:
            cloudburst_id = row['id']
            metric_id = row['metric_id']
            cloudbursts_dict[cloudburst_id] = dict(row)
            cloudbursts_dict[cloudburst_id]['metric'] = ids_with_metric_names[metric_id]
        connection.close()
    except Exception as err:
        logger.error(traceback.format_exc())
        logger.error('error :: %s :: failed to get cloudburst_table - %s' % (
            function_str, err))
        raise

    # Reorder the dict keys for the page
    cloudbursts_dict_keys = []
    key_ordered_cloudbursts_dict = {}
    if cloudbursts_dict:
        cloudburst_ids = list(cloudbursts_dict.keys())
        first_cloudburst_id = cloudburst_ids[0]
        for key in list(cloudbursts_dict[first_cloudburst_id].keys()):
            cloudbursts_dict_keys.append(key)
        for cloudburst_id in cloudburst_ids:
            key_ordered_cloudbursts_dict[cloudburst_id] = {}
            for key in cloudbursts_dict[cloudburst_id]:
                if key == 'id':
                    key_ordered_cloudbursts_dict[cloudburst_id][key] = cloudbursts_dict[cloudburst_id][key]
                    key_ordered_cloudbursts_dict[cloudburst_id]['metric'] = cloudbursts_dict[cloudburst_id]['metric']
            for key in cloudbursts_dict[cloudburst_id]:
                if key not in ['id', 'metric']:
                    key_ordered_cloudbursts_dict[cloudburst_id][key] = cloudbursts_dict[cloudburst_id][key]
        cloudbursts_dict = key_ordered_cloudbursts_dict
        cloudburst_ids = list(cloudbursts_dict.keys())
        cloudburst_ids.reverse()
        desc_cloudbursts_dict = {}
        for c_id in cloudburst_ids:
            desc_cloudbursts_dict[c_id] = cloudbursts_dict[c_id]
        cloudbursts_dict = desc_cloudbursts_dict

    logger.info('%s :: found %s cloudbursts' % (
        function_str, str(len(list(cloudbursts_dict.keys())))))

    return cloudbursts_dict
