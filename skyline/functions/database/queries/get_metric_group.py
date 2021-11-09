"""
Get anomalies for a metric id
"""
import logging
import traceback
from ast import literal_eval
from sqlalchemy.sql import select

from database import get_engine, engine_disposal, metric_group_table_meta


def get_metric_group(current_skyline_app, metric_id=0):
    """
    Returns the metric_group table row as dict or all the metric_group table
    rows as dict if metric_id is not passed or is passed as 0.
    """
    current_skyline_app_logger = current_skyline_app + 'Log'
    current_logger = logging.getLogger(current_skyline_app_logger)

    metric_group = {}
    if metric_id:
        metric_group[metric_id] = {}

    try:
        engine, fail_msg, trace = get_engine(current_skyline_app)
        if fail_msg != 'got MySQL engine':
            current_logger.error('error :: get_metric_group :: could not get a MySQL engine fail_msg - %s' % str(fail_msg))
        if trace != 'none':
            current_logger.error('error :: get_metric_group :: could not get a MySQL engine trace - %s' % str(trace))
    except Exception as err:
        current_logger.error(traceback.format_exc())
        current_logger.error('error :: get_metric_group :: could not get a MySQL engine - %s' % str(err))

    if engine:
        try:
            metric_group_table, fail_msg, trace = metric_group_table_meta(current_skyline_app, engine)
            if fail_msg != 'metric_group meta reflected OK':
                current_logger.error('error :: get_metric_group :: could not get metric_group_table_meta fail_msg - %s' % str(fail_msg))
            if trace != 'none':
                current_logger.error('error :: get_metric_group :: could not get metric_group_table_meta trace - %s' % str(trace))
        except Exception as err:
            current_logger.error(traceback.format_exc())
            current_logger.error('error :: get_metric_group :: metric_group_table_meta - %s' % str(err))
        try:
            connection = engine.connect()
            if metric_id:
                stmt = select([metric_group_table]).where(metric_group_table.c.metric_id == metric_id).order_by(metric_group_table.c.avg_coefficient.desc())
            else:
                stmt = select([metric_group_table])
            results = connection.execute(stmt)
            for row in results:
                related_metric_id = row['related_metric_id']
                if metric_id:
                    metric_group[metric_id][related_metric_id] = dict(row)
                else:
                    p_metric_id = row['metric_id']
                    if p_metric_id not in list(metric_group.keys()):
                        metric_group[p_metric_id] = {}
                    metric_group[p_metric_id][related_metric_id] = dict(row)
            connection.close()
        except Exception as err:
            current_logger.error(traceback.format_exc())
            current_logger.error('error :: get_metric_group :: failed to build metric_group dict - %s' % str(err))

        for mi_key in list(metric_group.keys()):
            for rmi_key in list(metric_group[mi_key].keys()):
                for key in list(metric_group[mi_key][rmi_key].keys()):
                    if 'decimal.Decimal' in str(type(metric_group[mi_key][rmi_key][key])):
                        metric_group[mi_key][rmi_key][key] = float(metric_group[mi_key][rmi_key][key])
                    if 'datetime.datetime' in str(type(metric_group[mi_key][rmi_key][key])):
                        metric_group[mi_key][rmi_key][key] = str(metric_group[mi_key][rmi_key][key])
                    if key == 'shifted_counts':
                        try:
                            shifted_counts_str = metric_group[mi_key][rmi_key][key].decode('utf-8')
                            shifted_counts = literal_eval(shifted_counts_str)
                        except AttributeError:
                            shifted_counts = metric_group[mi_key][rmi_key][key]
                        metric_group[mi_key][rmi_key][key] = shifted_counts
                # Remap the metric_id and related_metric_id for clarity
                metric_group[mi_key][rmi_key]['metric_id'] = rmi_key
                metric_group[mi_key][rmi_key]['related_to_metric_id'] = mi_key
                del metric_group[mi_key][rmi_key]['related_metric_id']

    if engine:
        engine_disposal(current_skyline_app, engine)

    return metric_group
