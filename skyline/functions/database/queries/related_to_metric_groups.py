"""
Get anomalies for a metric id
"""
import logging
import traceback
from ast import literal_eval
from sqlalchemy.sql import select

from database import get_engine, engine_disposal, metric_group_table_meta
from functions.metrics.get_base_name_from_metric_id import get_base_name_from_metric_id


def related_to_metric_groups(current_skyline_app, base_name, metric_id):
    """
    Returns a dict of all the metric_groups that a metric is part of.
    """
    current_skyline_app_logger = current_skyline_app + 'Log'
    current_logger = logging.getLogger(current_skyline_app_logger)

    related_to_metric_groups_dict = {}
    related_to_metric_groups_dict['metric'] = base_name
    related_to_metric_groups_dict['metric_id'] = metric_id
    related_to_metric_groups_dict['related_to_metrics'] = {}

    try:
        engine, fail_msg, trace = get_engine(current_skyline_app)
        if fail_msg != 'got MySQL engine':
            current_logger.error('error :: related_to_metric_groups :: could not get a MySQL engine fail_msg - %s' % str(fail_msg))
        if trace != 'none':
            current_logger.error('error :: related_to_metric_groups :: could not get a MySQL engine trace - %s' % str(trace))
    except Exception as err:
        current_logger.error(traceback.format_exc())
        current_logger.error('error :: related_to_metric_groups :: could not get a MySQL engine - %s' % str(err))

    if engine:
        try:
            metric_group_table, fail_msg, trace = metric_group_table_meta(current_skyline_app, engine)
            if fail_msg != 'metric_group meta reflected OK':
                current_logger.error('error :: related_to_metric_groups :: could not get metric_group_table_meta fail_msg - %s' % str(fail_msg))
            if trace != 'none':
                current_logger.error('error :: related_to_metric_groups :: could not get metric_group_table_meta trace - %s' % str(trace))
        except Exception as err:
            current_logger.error(traceback.format_exc())
            current_logger.error('error :: related_to_metric_groups :: metric_group_table_meta - %s' % str(err))
        try:
            connection = engine.connect()
            if metric_id:
                stmt = select([metric_group_table]).where(metric_group_table.c.related_metric_id == metric_id).order_by(metric_group_table.c.avg_coefficient.desc())
            else:
                stmt = select([metric_group_table])
            results = connection.execute(stmt)
            for row in results:
                group_metric_id = row['metric_id']
                group_base_name = None
                try:
                    group_base_name = get_base_name_from_metric_id(current_skyline_app, group_metric_id)
                except Exception as err:
                    current_logger.error('error :: related_to_metric_groups :: base_name_from_metric_id failed to determine base_name from metric_id: %s - %s' % (
                        str(group_metric_id), str(err)))
                if group_base_name:
                    related_to_metric_groups_dict['related_to_metrics'][group_base_name] = dict(row)
            connection.close()
        except Exception as err:
            current_logger.error(traceback.format_exc())
            current_logger.error('error :: related_to_metric_groups :: failed to build metric_groups dict - %s' % str(err))

    if engine:
        engine_disposal(current_skyline_app, engine)

    for related_metric in list(related_to_metric_groups_dict['related_to_metrics'].keys()):
        for key in list(related_to_metric_groups_dict['related_to_metrics'][related_metric].keys()):
            if 'decimal.Decimal' in str(type(related_to_metric_groups_dict['related_to_metrics'][related_metric][key])):
                related_to_metric_groups_dict['related_to_metrics'][related_metric][key] = float(related_to_metric_groups_dict['related_to_metrics'][related_metric][key])
            if 'datetime.datetime' in str(type(related_to_metric_groups_dict['related_to_metrics'][related_metric][key])):
                related_to_metric_groups_dict['related_to_metrics'][related_metric][key] = str(related_to_metric_groups_dict['related_to_metrics'][related_metric][key])
            if key == 'shifted_counts':
                try:
                    shifted_counts_str = related_to_metric_groups_dict['related_to_metrics'][related_metric][key].decode('utf-8')
                    shifted_counts = literal_eval(shifted_counts_str)
                except AttributeError:
                    shifted_counts = related_to_metric_groups_dict['related_to_metrics'][related_metric][key]
                related_to_metric_groups_dict['related_to_metrics'][related_metric][key] = shifted_counts
        # Remap the metric_id and related_metric_id for clarity
        related_to_metric_groups_dict['related_to_metrics'][related_metric]['related_to_metric_id'] = related_to_metric_groups_dict['related_to_metrics'][related_metric]['metric_id']
        related_to_metric_groups_dict['related_to_metrics'][related_metric]['metric_id'] = metric_id
        del related_to_metric_groups_dict['related_to_metrics'][related_metric]['related_metric_id']

    return related_to_metric_groups_dict
