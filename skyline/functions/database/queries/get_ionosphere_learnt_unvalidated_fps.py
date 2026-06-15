"""
get_ionosphere_learnt_unvalidated_fps.py
"""
import copy
import logging
import traceback
from os import uname as os_uname
from sqlalchemy.sql import select

import settings
from database import get_engine, engine_disposal, ionosphere_table_meta
from functions.metrics.get_base_name_from_metric_id import get_base_name_from_metric_id
from skyline_functions import get_redis_conn_decoded
# @added 20250829 - Feature #5644: ionosphere.learn_self_validation
from functions.cluster.is_shard_metric import is_shard_metric
try:
    HORIZON_SHARDS = copy.deepcopy(settings.HORIZON_SHARDS)
except:
    HORIZON_SHARDS = {}
number_of_horizon_shards = 0
this_host = str(os_uname()[1])
HORIZON_SHARD = 0
if HORIZON_SHARDS:
    number_of_horizon_shards = len(HORIZON_SHARDS)
    HORIZON_SHARD = HORIZON_SHARDS[this_host]


# @added 20250728 - Feature #5644: ionosphere.learn_self_validation
def get_ionosphere_learnt_unvalidated_fps(
        current_skyline_app, from_timestamp, until_timestamp, limit=2,
            namespace=None, exclude_done=True, adhoc_fp_ids=[]):
    """
    Return the unvalidated learnt fps as a dict.  This function defaults to a
    limit of returning 5 unvalidated learnt features profiles because it's
    initial use is for ionosphere.learn_self_validation which does not require
    more than 5 per run.

    :param current_skyline_app: 
    :param from_timestamp: 
    :param until_timestamp: 
    :param limit: the number of unvalidated learnt features profiles to return
    :param namespace: a namespace if desired
    :param exclude_done: whether to exclude fp_ids that have been done already
    :param adhoc_fp_ids: a list of fp ids to run adhoc on
    :type current_skyline_app: str
    :type from_timestamp: int
    :type until_timestamp: int
    :type limit: int
    :type namespace: str
    :type exclude_done: bool
    :type adhoc_fp_ids: list
    :return: unvalidated_learnt_fps
    :rtype: dict

    """
    function_str = 'get_ionosphere_learnt_unvalidated_fps'
    current_skyline_app_logger = current_skyline_app + 'Log'
    current_logger = logging.getLogger(current_skyline_app_logger)

    unvalidated_learnt_fps = {}

    current_logger.info('%s :: determining all unvalidated feature profiles for namespace %s' % (
        function_str, str(namespace)))

    try:
        engine, fail_msg, trace = get_engine(current_skyline_app)
    except Exception as err:
        trace = traceback.format_exc()
        current_logger.error('%s' % trace)
        fail_msg = 'error :: %s :: failed to get MySQL engine, err: %s' % (function_str, err)
        current_logger.error('%s' % fail_msg)
        if current_skyline_app == 'webapp':
            # Raise to webapp
            raise
        return unvalidated_learnt_fps

    try:
        ionosphere_table, fail_msg, trace = ionosphere_table_meta(current_skyline_app, engine)
    except Exception as err:
        trace = traceback.format_exc()
        current_logger.error('%s' % trace)
        fail_msg = 'error ::  %s :: failed to get ionosphere_table meta, err: %s' % (function_str, err)
        current_logger.error('%s' % fail_msg)
        try:
            engine.dispose()
        except Exception as dispose_err:
            current_logger.error(traceback.format_exc())
            current_logger.error('error :: %s :: calling engine.dispose(), err: %s' % (function_str, dispose_err))
        if current_skyline_app == 'webapp':
            # Raise to webapp
            raise
        return unvalidated_learnt_fps

    try:
        redis_conn_decoded = get_redis_conn_decoded(current_skyline_app)
    except Exception as err:
        current_logger.error('error :: %s :: get_redis_conn_decoded failed, err: %s' % (
            function_str, str(err)))

    # @added 20250829 - Feature #5644: ionosphere.learn_self_validation
    # Handle cluster.  Get all the ids with metrics to prevent hitting the DB
    # for every metric when learn_self_validation is enabled and there are 1000s
    # of LEARNT features profiles to validate
    redis_key = 'aet.metrics_manager.ids_with_metric_names'
    ids_with_base_names = {}
    if HORIZON_SHARDS:
        try:
            ids_with_base_names = redis_conn_decoded.hgetall(redis_key)
        except Exception as err:
            current_logger.error('error :: %s :: %s :: hgetall failed on %s, err: %s' % (
                current_skyline_app, function_str, redis_key, str(err)))

    self_validated_fps = {}
    self_validated_fp_ids = []
    if exclude_done:
        try:
            self_validated_fps = redis_conn_decoded.hgetall('ionosphere.learn_self_validation.fps_done')
        except Exception as err:
            current_logger.error('error :: %s :: hgetall failed on ionosphere.learn_self_validation.fps_done, err: %s' % (
                function_str, str(err)))
        if self_validated_fps:
            self_validated_fp_ids = [int(i_fp_id) for i_fp_id in list(self_validated_fps.keys()) if int(i_fp_id) not in adhoc_fp_ids]

    # An iteration is required to count the results as the
    # LegacyCursorResult has no len()
    results_count = 0
    limit_reached = False

    try:
        #connection = engine.connect()
        # @modified 20260225 - Task #5176: Migrate to sqlalchemy v2 API
        #                      Task #5628: Build v5.0.0 and test
        #stmt = select([ionosphere_table]).\
        stmt = select(ionosphere_table).\
            where(ionosphere_table.c.anomaly_timestamp >= from_timestamp).\
            where(ionosphere_table.c.anomaly_timestamp <= until_timestamp).\
            where(ionosphere_table.c.validated == 0).\
            where(ionosphere_table.c.generation > 1).\
            where(ionosphere_table.c.echo_fp == 0).\
            where(ionosphere_table.c.enabled == 1)

        # @added 20250728 - Feature #5644: ionosphere.learn_self_validation
        if adhoc_fp_ids:
            # @modified 20260225 - Task #5176: Migrate to sqlalchemy v2 API
            #                      Task #5628: Build v5.0.0 and test
            #stmt = select([ionosphere_table]).\
            stmt = select(ionosphere_table).\
                where(ionosphere_table.c.id == adhoc_fp_ids[0]).\
                where(ionosphere_table.c.validated == 0).\
                where(ionosphere_table.c.generation > 1).\
                where(ionosphere_table.c.echo_fp == 0).\
                where(ionosphere_table.c.enabled == 1)

        # @modified 20260226 - Task #5176: Migrate to sqlalchemy v2 API
        #                      Task #5628: Build v5.0.0 and test
        #results = connection.execute(stmt)
        with engine.connect() as connection:
            result = connection.execute(stmt)
            results = [dict(row._mapping) for row in result.fetchall()]

        for row in results:
            try:
                # @added 20250829 - Feature #5644: ionosphere.learn_self_validation
                # Handle cluster.  This distributes the learn_self_validation
                # to each cluster node so that nodes will only validate their
                # own metrics
                shard_metric = False
                if HORIZON_SHARDS:
                    metric_id = int(row['metric_id'])
                    base_name = None
                    try:
                        base_name = ids_with_base_names[str(metric_id)]
                    except Exception as err:
                        base_name = None
                    # Only validate if the features profile is for an active
                    # metric
                    #if not base_name and metric_id:
                    #    try:
                    #        base_name = get_base_name_from_metric_id(current_skyline_app, metric_id)
                    #    except Exception as err:
                    #        current_logger.error('error :: %s :: get_base_name_from_metric_id failed to determine base_name for metric_id: %s, err: %s' % (
                    #            function_str, str(metric_id), str(err)))
                    #        base_name = None
                    if base_name:
                        shard_metric = is_shard_metric(base_name)
                    if not shard_metric:
                        continue

                results_count += 1
                fp_id = int(row['id'])
                if fp_id in self_validated_fp_ids:
                    continue
                if limit_reached:
                    continue
                unvalidated_learnt_fps[fp_id] = dict(row)
                # Coerce for json
                for key, item in unvalidated_learnt_fps[fp_id].items():
                    if 'decimal.Decimal' in str(type(unvalidated_learnt_fps[fp_id][key])):
                        unvalidated_learnt_fps[fp_id][key] = float(row[key])
                    if 'datetime.datetime' in str(type(unvalidated_learnt_fps[fp_id][key])):
                        unvalidated_learnt_fps[fp_id][key] = str(row[key])
            except Exception as row_err:
                current_logger.error('error :: %s :: bad row data, row: %s, err: %s' % (
                    function_str, str(row), row_err))
            if limit:
                if len(unvalidated_learnt_fps) >= limit:
                    limit_reached = True
        #connection.close()
    except Exception as err:
        trace = traceback.format_exc()
        current_logger.error('%s' % trace)
        current_logger.error('error :: %s :: select error, err: %s' % (
            function_str, err))
        if current_skyline_app == 'webapp':
            # Raise to webapp
            raise
        return unvalidated_learnt_fps

    if limit_reached:
        current_logger.info('%s :: limited to %s unvalidated feature profiles of a total of %s unvalidated feature profiles' % (
            function_str, str(limit), str(results_count)))

    try:
        engine.dispose()
    except Exception as dispose_err:
        current_logger.error(traceback.format_exc())
        current_logger.error('error :: %s :: calling engine.dispose(), err: %s' % (function_str, dispose_err))

    remove_non_namespace_fps = []
    for fp_id, item in unvalidated_learnt_fps.items():
        metric_id = item['metric_id']
        try:
            base_name = ids_with_base_names[str(metric_id)]
        except Exception as err:
            base_name = 'unknown'
        if base_name == 'unknown':
            try:
                base_name = get_base_name_from_metric_id(current_skyline_app, metric_id)
            except Exception as err:
                current_logger.error('error :: %s :: failed to determine base_name for metric_id: %s, err: %s' % (
                    function_str, str(metric_id), str(err)))
                base_name = 'unknown'

        if namespace and namespace not in base_name:
            remove_non_namespace_fps.append(fp_id)
            continue
        unvalidated_learnt_fps[fp_id]['metric'] = base_name
        use_base_name = str(base_name)
        labelled_metric_name = None
        if '{' in base_name and '}' in base_name and '_tenant_id="' in base_name:
            labelled_metric_name = 'labelled_metrics.%s' % str(metric_id)
        if labelled_metric_name:
            use_base_name = str(labelled_metric_name)
        unvalidated_learnt_fps[fp_id]['labelled_metric_name'] = labelled_metric_name
        validation_link = '%s/ionosphere?fp_view=true&timestamp=%s&metric=%s&validate_fp=true&format=json' % (
            settings.SKYLINE_URL, str(item['anomaly_timestamp']), use_base_name)
        unvalidated_learnt_fps[fp_id]['validation_link'] = validation_link
        validation_uri = '?fp_view=true&timestamp=%s&metric=%s&validate_fp=true&format=json' % (
            str(item['anomaly_timestamp']), use_base_name)
        unvalidated_learnt_fps[fp_id]['validation_uri'] = validation_uri
        adhoc = False
        if fp_id in adhoc_fp_ids:
            adhoc = True
        unvalidated_learnt_fps[fp_id]['adhoc'] = adhoc

    if remove_non_namespace_fps:
        current_logger.info('%s :: removing %s unvalidated feature profiles that are not for namespace %s' % (
            function_str, str(len(remove_non_namespace_fps)), str(namespace)))
        for fp_id in remove_non_namespace_fps:
            del unvalidated_learnt_fps[fp_id]

    if engine:
        engine_disposal(current_skyline_app, engine)
    del fail_msg
    del trace
    return unvalidated_learnt_fps
