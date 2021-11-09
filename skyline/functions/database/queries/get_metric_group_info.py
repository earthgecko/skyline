"""
Get anomalies for a metric id
"""
import logging
import traceback
from sqlalchemy.sql import select

from database import get_engine, engine_disposal, metric_group_info_table_meta
from functions.metrics.get_base_name_from_metric_id import get_base_name_from_metric_id
from functions.metrics.get_metric_ids_and_base_names import get_metric_ids_and_base_names
from matched_or_regexed_in_list import matched_or_regexed_in_list


def get_metric_group_info(current_skyline_app, metric_id=0, params={'namespaces': []}):
    """
    Returns the metrics_groups table as dict
    """
    current_skyline_app_logger = current_skyline_app + 'Log'
    current_logger = logging.getLogger(current_skyline_app_logger)

    metric_groups_info = {}

    try:
        engine, fail_msg, trace = get_engine(current_skyline_app)
        if fail_msg != 'got MySQL engine':
            current_logger.error('error :: get_metric_group_info :: could not get a MySQL engine fail_msg - %s' % str(fail_msg))
        if trace != 'none':
            current_logger.error('error :: get_metric_group_info :: could not get a MySQL engine trace - %s' % str(trace))
    except Exception as err:
        current_logger.error(traceback.format_exc())
        current_logger.error('error :: get_metric_group_info :: could not get a MySQL engine - %s' % str(err))

    if engine:
        try:
            metric_groups_info_table, fail_msg, trace = metric_group_info_table_meta(current_skyline_app, engine)
            if fail_msg != 'metric_group_info meta reflected OK':
                current_logger.error('error :: get_metric_group_info :: could not get metric_groups_info_table_meta fail_msg - %s' % str(fail_msg))
            if trace != 'none':
                current_logger.error('error :: get_metric_group_info :: could not get metric_groups_info_table_meta trace - %s' % str(trace))
        except Exception as err:
            current_logger.error(traceback.format_exc())
            current_logger.error('error :: get_metric_group_info :: metric_groups_info_table_meta - %s' % str(err))
        try:
            connection = engine.connect()
            if metric_id:
                stmt = select([metric_groups_info_table]).where(metric_groups_info_table.c.metric_id == metric_id)
            else:
                stmt = select([metric_groups_info_table])
            results = connection.execute(stmt)
            for row in results:
                group_metric_id = row['metric_id']
                if row['related_metrics'] > 0:
                    metric_groups_info[group_metric_id] = dict(row)
            connection.close()
        except Exception as err:
            current_logger.error(traceback.format_exc())
            current_logger.error('error :: get_metric_group_info :: failed to build metric_groups_info dict - %s' % str(err))

    ids_with_base_names = {}
    if not metric_id:
        try:
            ids_with_base_names = get_metric_ids_and_base_names(current_skyline_app)
        except Exception as err:
            current_logger.error(traceback.format_exc())
            current_logger.error('error :: %s :: get_metric_group_info :: get_metric_ids_and_basenames failed - %s' % (
                current_skyline_app, str(err)))

    for mi_key in list(metric_groups_info.keys()):
        base_name = None
        if ids_with_base_names:
            try:
                base_name = ids_with_base_names[int(mi_key)]
            except KeyError:
                base_name = None
        if not base_name:
            try:
                base_name = get_base_name_from_metric_id(current_skyline_app, int(mi_key))
            except Exception as err:
                current_logger.error('error :: %s :: get_metric_group_info :: get_base_name_from_metric_id failed to determine base_name for metric_id: %s - %s' % (
                    current_skyline_app, str(metric_id), str(err)))
                base_name = 'unknown'

        metric_groups_info[mi_key]['metric'] = base_name
        for key in list(metric_groups_info[mi_key].keys()):
            if 'datetime.datetime' in str(type(metric_groups_info[mi_key][key])):
                metric_groups_info[mi_key][key] = str(metric_groups_info[mi_key][key])

    # Sort by base_name
    if metric_groups_info and current_skyline_app == 'webapp':
        metric_groups_info_list = []
        metric_groups_info_dict = {}
        metric_groups_info_keys = []
        for group_metric_id in list(metric_groups_info.keys()):
            metric_group_info_data = []
            if not metric_groups_info_keys:
                for index, key in enumerate(list(metric_groups_info[group_metric_id].keys())):
                    metric_groups_info_keys.append(key)
                    if key == 'metric':
                        metric_index = index
                    if key == 'metric_id':
                        metric_id_index = index
            for key in metric_groups_info_keys:
                metric_group_info_data.append(metric_groups_info[group_metric_id][key])
            metric_groups_info_list.append(metric_group_info_data)
        if metric_groups_info_list:
            sorted_metric_groups_info_list = sorted(metric_groups_info_list, key=lambda x: x[metric_index])
            for item in sorted_metric_groups_info_list:
                group_metric_id = item[metric_id_index]
                metric_group_dict = {}
                metric_group_dict['metric_id'] = item[metric_id_index]
                metric_group_dict['metric'] = item[metric_index]
                for index, key in enumerate(metric_groups_info_keys):
                    if key in ['metric', 'metric_id']:
                        continue
                    metric_group_dict[key] = item[index]
                metric_groups_info_dict[group_metric_id] = metric_group_dict
        metric_groups_info = metric_groups_info_dict

    namespaces = []
    try:
        namespaces = params['namespaces']
    except KeyError:
        namespaces = []
    filtered_metric_groups_info = {}
    if namespaces:
        current_logger.info('%s :: get_metric_group_info :: filtering results on namespaces: %s' % (
            current_skyline_app, str(namespaces)))

        for mi_key in list(metric_groups_info.keys()):
            pattern_match = False
            try:
                pattern_match, matched_by = matched_or_regexed_in_list(current_skyline_app, metric_groups_info[mi_key]['metric'], namespaces, False)
                if pattern_match:
                    filtered_metric_groups_info[mi_key] = metric_groups_info[mi_key]
            except Exception as err:
                current_logger.error('error :: %s :: get_metric_group_info :: matched_or_regexed_in_list failed to determine if matched in namespaces: %s - %s' % (
                    current_skyline_app, str(namespaces), str(err)))
    if filtered_metric_groups_info:
        metric_groups_info = filtered_metric_groups_info.copy()

    if engine:
        engine_disposal(current_skyline_app, engine)

    return metric_groups_info
