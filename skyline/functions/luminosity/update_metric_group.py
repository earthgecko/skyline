import logging
import os
import sys
from time import time
import traceback
import warnings
import json

# from sqlalchemy.sql import delete

sys.path.insert(0, '/opt/skyline/github/skyline/skyline')
if True:
    import settings
    from skyline_functions import get_redis_conn_decoded
    from functions.database.queries.get_metric_group import get_metric_group
    from functions.database.queries.get_metric_group_info import get_metric_group_info
    from database import (
        get_engine, engine_disposal, metric_group_table_meta,
        metric_group_info_table_meta)

warnings.filterwarnings('ignore')

skyline_app = 'luminosity'
skyline_app_logger = '%sLog' % skyline_app
logger = logging.getLogger(skyline_app_logger)
skyline_app_logfile = '%s/%s.log' % (settings.LOG_PATH, skyline_app)

DEBUG = False


def update_metric_group(base_name, metric_id, cross_correlation_relationships, ids_with_metric_names):
    """
    Given a cross_correlation_relationships dictionary update the metric_group
    rows in the
    """
    updated_metric_group = False
    # base_name = list(cross_correlation_relationships.keys())[0]
    # metric_id = cross_correlation_relationships[base_name]['metric_id']
    logger.info('related_metrics :: update_metric_group :: updating metric_group for %s id: %s' % (
        base_name, str(metric_id)))
    timestamp = int(time())

    metric_group_info = {}
    try:
        metric_group_info = get_metric_group_info(skyline_app, metric_id)
    except Exception as err:
        logger.error(traceback.format_exc())
        logger.error('error :: related_metrics :: update_metric_group :: get_metric_group_info failed - %s' % str(err))

    metric_group = {}
    try:
        metric_group = get_metric_group(skyline_app, metric_id)
    except Exception as err:
        logger.error(traceback.format_exc())
        logger.error('error :: related_metrics :: update_metric_group :: get_metric_group failed for %s - %s' % (
            str(metric_id), str(err)))
    metric_group_to_be_updated = False
    if len(metric_group[metric_id].keys()) == 0:
        metric_group = {}
        metric_group_to_be_updated = True
    else:
        metric_group = metric_group[metric_id]
    current_related_metric_ids = []
    if metric_group:
        try:
            for related_metric_id in list(metric_group.keys()):
                current_related_metric_ids.append(related_metric_id)
        except Exception as err:
            logger.error('error :: related_metrics :: update_metric_group :: error occurred building current_related_metric_ids - %s' % (
                str(err)))
    new_related_metric_ids = []
    try:
        for related_metric in list(cross_correlation_relationships.keys()):
            new_related_metric_ids.append(cross_correlation_relationships[related_metric]['metric_id'])
    except Exception as err:
        logger.error(traceback.format_exc())
        logger.error('error :: related_metrics :: update_metric_group :: error occurred building new_related_metric_ids - %s' % (
            str(err)))
    add_related_metric_ids = []
    update_related_metric_ids = []
    for related_metric_id in new_related_metric_ids:
        if related_metric_id == metric_id:
            continue
        if related_metric_id not in current_related_metric_ids:
            add_related_metric_ids.append(related_metric_id)
            metric_group_to_be_updated = True
    remove_related_metric_ids = []
    for related_metric_id in current_related_metric_ids:
        if related_metric_id not in new_related_metric_ids:
            remove_related_metric_ids.append(related_metric_id)
            metric_group_to_be_updated = True
        else:
            try:
                related_metric = ids_with_metric_names[related_metric_id]
                current_avg_coefficient = metric_group[related_metric_id]['avg_coefficient']
                new_avg_coefficient = round(cross_correlation_relationships[related_metric]['avg_coefficient'], 5)
                if current_avg_coefficient != new_avg_coefficient:
                    logger.info('related_metrics :: update_metric_group :: updating %s in metric_group for %s as avg_coefficient has changed - current: %s, new: %s' % (
                        related_metric, base_name, str(current_avg_coefficient),
                        str(new_avg_coefficient)))
                    update_related_metric_ids.append(related_metric_id)
                    metric_group_to_be_updated = True
            except Exception as err:
                logger.error(traceback.format_exc())
                logger.error('error :: related_metrics :: update_metric_group :: could not get determine while to update related_metric_id %s in metric_group - %s' % (
                    str(related_metric_id), str(err)))

    if not add_related_metric_ids:
        if not remove_related_metric_ids:
            if not update_related_metric_ids:
                logger.info('related_metrics :: update_metric_group :: no change to metric_group for %s, all related metric ids and avg_coefficient values are the same' % (
                    str(base_name)))

    try:
        engine, fail_msg, trace = get_engine(skyline_app)
        if fail_msg != 'got MySQL engine':
            logger.error('error :: related_metrics :: update_metric_group :: could not get a MySQL engine fail_msg - %s' % str(fail_msg))
        if trace != 'none':
            logger.error('error :: related_metrics :: update_metric_group :: could not get a MySQL engine trace - %s' % str(trace))
    except Exception as err:
        logger.error(traceback.format_exc())
        logger.error('error :: related_metrics :: update_metric_group :: could not get a MySQL engine - %s' % str(err))

    if engine:
        try:
            metric_group_table, fail_msg, trace = metric_group_table_meta(skyline_app, engine)
            if fail_msg != 'metric_group meta reflected OK':
                logger.error('error :: related_metrics :: update_metric_group :: could not get metric_group_table_meta fail_msg - %s' % str(fail_msg))
            if trace != 'none':
                logger.error('error :: related_metrics :: update_metric_group :: could not get metric_group_table_meta trace - %s' % str(trace))
        except Exception as err:
            logger.error(traceback.format_exc())
            logger.error('error :: related_metrics :: update_metric_group :: metric_group_table_meta - %s' % str(err))

    if metric_group_to_be_updated:
        try:
            connection = engine.connect()
        except Exception as err:
            logger.error(traceback.format_exc())
            logger.error('error :: related_metrics :: update_metric_group :: engine.connect failed to add related_metric_ids - %s' % str(err))

        # Delete any metrics from the metric_group that are no longer valid
        if len(remove_related_metric_ids) > 0:
            logger.info('debug :: related_metrics :: update_metric_group :: %s metric_group: %s, current_related_metric_ids: %s' % (
                base_name, str(metric_id), str(current_related_metric_ids)))
            logger.info('debug :: related_metrics :: update_metric_group :: %s metric_group: %s, new_related_metric_ids: %s' % (
                base_name, str(metric_id), str(new_related_metric_ids)))
            logger.info('debug :: related_metrics :: update_metric_group :: %s metric_group: %s, metric_group: %s' % (
                base_name, str(metric_id), str(metric_group)))
            logger.info('debug :: related_metrics :: update_metric_group :: %s metric_group: %s, cross_correlation_relationships%s' % (
                base_name, str(metric_id), str(cross_correlation_relationships)))

            logger.info('debug :: related_metrics :: update_metric_group :: deleting related_metric_ids from %s metric_group: %s, remove_related_metric_ids: %s' % (
                base_name, str(metric_id), str(remove_related_metric_ids)))

            for related_metric_id in remove_related_metric_ids:
                related_metric = ids_with_metric_names[related_metric_id]
                logger.info('debug :: related_metrics :: update_metric_group :: removing entry from %s metric_group for %s: %s' % (
                    base_name, related_metric, str(metric_group[related_metric_id])))

            try:
                stmt = metric_group_table.delete().\
                    where(metric_group_table.c.metric_id == metric_id).\
                    where(metric_group_table.c.related_metric_id.in_(remove_related_metric_ids))
                result = connection.execute(stmt)
                if result:
                    updated_metric_group = True
            except Exception as err:
                logger.error(traceback.format_exc())
                logger.error('error :: related_metrics :: update_metric_group :: failed to delete related_metric_ids from metric_group_table - %s' % str(err))

        if len(add_related_metric_ids) > 0:
            logger.info('related_metrics :: update_metric_group :: adding %s related_metric_ids to %s metric_group: %s' % (
                str(len(add_related_metric_ids)), base_name, str(metric_id)))
            logger.info('related_metrics :: update_metric_group :: adding related_metric_ids to %s metric_group: %s, related_metric_ids: %s' % (
                base_name, str(metric_id), str(add_related_metric_ids)))

            for related_metric in list(cross_correlation_relationships.keys()):
                cross_correlation_data = cross_correlation_relationships[related_metric]
                related_metric_id = cross_correlation_data['metric_id']
                if related_metric_id not in add_related_metric_ids:
                    continue
                if related_metric_id == metric_id:
                    continue
                logger.info('related_metrics :: update_metric_group :: adding %s related_metric_id: %s on %s metric_group: %s' % (
                    related_metric, str(related_metric_id), base_name, str(metric_id)))

                # A NOTE ON confidence score
                # The confidence score is not stored in the DB but rather
                # calculated in real time functions/metrics/get_related_metrics
                # This is due to the fact that changing correlations on other
                # metrics in group impact this score.  If it were calculated via
                # get_cross_correlation_relationships or here, every time a
                # metric in a group had a new correlation, the confidence score
                # each every metric in the group would have to be recalculated
                # and stored.  This is a very dynamic variable.

                try:
                    result = None
                    ins = metric_group_table.insert().values(
                        metric_id=int(metric_id), related_metric_id=related_metric_id,
                        avg_coefficient=cross_correlation_data['avg_coefficient'],
                        shifted_counts=json.dumps(cross_correlation_data['shifted_counts']),
                        avg_shifted_coefficient=cross_correlation_data['avg_shifted_coefficient'],
                        timestamp=timestamp)
                    result = connection.execute(ins)
                    if result:
                        updated_metric_group = True
                        if DEBUG:
                            logger.info('debug :: related_metrics :: update_metric_group :: inserted row for related_metric_id: %s on %s metric_group: %s' % (
                                str(related_metric_id), base_name, str(metric_id)))
                    else:
                        logger.error('error :: related_metrics :: update_metric_group :: failed to insert row for related_metric_id: %s on %s metric_group: %s' % (
                            str(related_metric_id), base_name, str(metric_id)))
                except Exception as err:
                    logger.error(traceback.format_exc())
                    logger.error('error :: related_metrics :: update_metric_group :: failed to insert new related metric - %s' % str(err))

        if len(update_related_metric_ids) > 0:
            logger.info('related_metrics :: update_metric_group :: updating %s related_metric_ids to %s metric_group: %s' % (
                str(len(update_related_metric_ids)), base_name, str(metric_id)))
            for related_metric in list(cross_correlation_relationships.keys()):
                cross_correlation_data = cross_correlation_relationships[related_metric]
                related_metric_id = cross_correlation_data['metric_id']
                if related_metric_id not in update_related_metric_ids:
                    continue
                if related_metric_id == metric_id:
                    continue
                try:
                    stmt = metric_group_table.update().values(
                        avg_coefficient=cross_correlation_data['avg_coefficient'],
                        shifted_counts=json.dumps(cross_correlation_data['shifted_counts']),
                        avg_shifted_coefficient=cross_correlation_data['avg_shifted_coefficient'],
                        timestamp=timestamp).\
                        where(metric_group_table.c.metric_id == metric_id).\
                        where(metric_group_table.c.related_metric_id == related_metric_id)
                    connection.execute(stmt)
                except Exception as err:
                    logger.error(traceback.format_exc())
                    logger.error('error :: related_metrics :: update_metric_group :: failed to update metric_group_table from metric_id: %s, related_metric_id: %s - %s' % (
                        str(metric_id), str(related_metric_id), str(err)))

    if metric_group_to_be_updated and engine:
        try:
            metric_group_info_table, fail_msg, trace = metric_group_info_table_meta(skyline_app, engine)
            if fail_msg != 'metric_group_info meta reflected OK':
                logger.error('error :: related_metrics :: update_metric_group :: could not get metric_group_info_table_meta fail_msg - %s' % str(fail_msg))
            if trace != 'none':
                logger.error('error :: related_metrics :: update_metric_group :: could not get metric_group_info_table_meta trace - %s' % str(trace))
        except Exception as err:
            logger.error(traceback.format_exc())
            logger.error('error :: related_metrics :: update_metric_group :: metric_group_info_table_meta - %s' % str(err))

    related_metrics_count = 0
    if metric_group_to_be_updated:
        related_metrics_count = (len(current_related_metric_ids) + len(add_related_metric_ids)) - len(remove_related_metric_ids)

    if metric_group_info and metric_group_to_be_updated:
        # Update the timestamp in the metric_group_info and Redis
        current_updated_count = 0
        try:
            current_updated_count = metric_group_info[metric_id]['updated_count']
        except KeyError:
            current_updated_count = 0
        updated_count = current_updated_count + 1
        try:
            connection.execute(metric_group_info_table.update(
                metric_group_info_table.c.metric_id == metric_id).values(
                related_metrics=related_metrics_count, last_updated=timestamp,
                updated_count=updated_count))
        except Exception as err:
            logger.error(traceback.format_exc())
            logger.error('error :: related_metrics :: update_metric_group :: failed to update metric_group_info table record for metric_id: %s - %s' % (
                str(metric_id), str(err)))

    if not metric_group_info and metric_group_to_be_updated:
        # Insert a new metric_group_info record
        try:
            ins = metric_group_info_table.insert().values(
                metric_id=int(metric_id), related_metrics=related_metrics_count,
                last_updated=timestamp)
            result = connection.execute(ins)
            logger.info('related_metrics :: update_metric_group :: added new record to metric_groups_info table for %s' % (
                base_name))
        except Exception as err:
            logger.error(traceback.format_exc())
            logger.error('error :: related_metrics :: update_metric_group :: failed to insert metric_group_info table record - %s' % str(err))

    redis_conn_decoded = None
    try:
        redis_conn_decoded = get_redis_conn_decoded(skyline_app)
    except Exception as err:
        logger.error(traceback.format_exc())
        logger.error('error :: related_metrics :: update_metric_group :: failed to get Redis hash aet.metrics_manager.metric_names_with_ids - %s' % str(err))

    if metric_group_to_be_updated:
        try:
            connection.close()
        except Exception as err:
            logger.error(traceback.format_exc())
            logger.error('error :: related_metrics :: update_metric_group :: connection.close() failed - %s' % str(err))

        related_metric_ids = []
        for related_metric_id in list(current_related_metric_ids + add_related_metric_ids):
            if related_metric_id not in remove_related_metric_ids:
                related_metric_ids.append(related_metric_id)
        related_metrics = []
        for related_metric_id in related_metric_ids:
            related_metric = None
            try:
                related_metric = ids_with_metric_names[related_metric_id]
            except KeyError:
                related_metric = None
            if related_metric:
                related_metrics.append(related_metric)
        if related_metrics:
            try:
                redis_conn_decoded.hset('luminosity.related_metrics.metric_ids', metric_id, str(related_metric_ids))
                logger.info('related_metrics :: update_metric_group :: updated luminosity.related_metrics.metric_ids Redis hash key for %s' % (
                    base_name))
            except Exception as err:
                logger.error(traceback.format_exc())
                logger.error('error :: related_metrics :: update_metric_group :: failed to updated luminosity.related_metrics.metric_ids Redis hash key for %s - %s' % (
                    base_name, str(err)))
            try:
                redis_conn_decoded.hset('luminosity.related_metrics.metrics', base_name, str(related_metrics))
                logger.info('related_metrics :: update_metric_group :: updated luminosity.related_metrics.metrics Redis hash key for %s' % (
                    base_name))
            except Exception as err:
                logger.error(traceback.format_exc())
                logger.error('error :: related_metrics :: update_metric_group :: failed to updated luminosity.related_metrics.metrics Redis hash key for %s - %s' % (
                    base_name, str(err)))
        else:
            try:
                redis_conn_decoded.hdel('luminosity.related_metrics.metric_ids', metric_id)
                logger.info('related_metrics :: update_metric_group :: removed %s from luminosity.related_metrics.metrics Redis hash' % (
                    str(metric_id)))
            except Exception as err:
                logger.error(traceback.format_exc())
                logger.error('error :: related_metrics :: update_metric_group :: failed to remove from luminosity.related_metrics.metric_ids Redis hash key for %s - %s' % (
                    base_name, str(err)))
            try:
                redis_conn_decoded.hdel('luminosity.related_metrics.metrics', base_name)
                logger.info('related_metrics :: update_metric_group :: removed %s from luminosity.related_metrics.metrics Redis hash' % (
                    base_name))
            except Exception as err:
                logger.error(traceback.format_exc())
                logger.error('error :: related_metrics :: update_metric_group :: failed to updated luminosity.related_metrics.metrics Redis hash key for %s - %s' % (
                    base_name, str(err)))

    try:
        redis_conn_decoded.hset('luminosity.metric_group.last_updated', metric_id, timestamp)
        logger.info('related_metrics :: update_metric_group :: updating metric_group for %s id: %s - done' % (
            base_name, str(metric_id)))
    except Exception as err:
        logger.error(traceback.format_exc())
        logger.error('error :: related_metrics :: update_metric_group :: failed to get Redis hash aet.metrics_manager.metric_names_with_ids - %s' % str(err))

    if engine:
        engine_disposal(skyline_app, engine)

    return updated_metric_group
