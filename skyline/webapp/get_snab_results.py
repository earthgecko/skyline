import logging
import os
import time
import datetime
import traceback

import settings
import skyline_version

from sqlalchemy.sql import select

from database import get_engine, snab_table_meta, anomalies_table_meta
from matched_or_regexed_in_list import matched_or_regexed_in_list
from functions.metrics.get_metric_ids_and_base_names import get_metric_ids_and_base_names
from functions.plots.plot_anomalies import plot_anomalies

from skyline_functions import get_graphite_metric
from create_matplotlib_graph import create_matplotlib_graph

skyline_version = skyline_version.__absolute_version__
skyline_app = 'webapp'
skyline_app_logger = '%sLog' % skyline_app
logger = logging.getLogger(skyline_app_logger)
skyline_app_logfile = '%s/%s.log' % (settings.LOG_PATH, skyline_app)
logfile = '%s/%s.log' % (settings.LOG_PATH, skyline_app)
try:
    ENABLE_WEBAPP_DEBUG = settings.ENABLE_WEBAPP_DEBUG
except EnvironmentError as err:
    logger.error('error :: cannot determine ENABLE_WEBAPP_DEBUG from settings - %s' % str(err))
    ENABLE_WEBAPP_DEBUG = False


def get_snab_engine():

    try:
        engine, fail_msg, trace = get_engine(skyline_app)
        return engine, fail_msg, trace
    except:
        trace = traceback.format_exc()
        logger.error('%s' % trace)
        fail_msg = 'error :: get_snab_results :: failed to get MySQL engine for snab table'
        logger.error('%s' % fail_msg)
        # return None, fail_msg, trace
        raise  # to webapp to return in the UI


def snab_engine_disposal(engine):
    if engine:
        try:
            engine.dispose()
        except:
            logger.error(traceback.format_exc())
            logger.error('error :: get_snab_results :: calling engine.dispose()')
    return


# @added 20211102 - Branch #3068: SNAB
def get_snab_results(filter_on):
    """
    Update the relevant field in the snab table.

    :param filter_on: the filter_on dictionary
    :param algorithms: the algorithms dictionary
    :param algorithm_groups: the algorithm_groups dictionary
    :type filter_on: dict
    :type algorithms: dict
    :type algorithm_groups: dict
    :return: results_data
    :rtype: dict

    """

    results_data = {}
    logger.info('get_snab_results :: filter_on: %s' % (str(filter_on)))

    namespaces = []
    if filter_on['namespaces']:
        namespaces = filter_on['namespaces']

    algorithm = None
    algorithm_id = 0
    if filter_on['algorithm_id']:
        algorithm_id = filter_on['algorithm_id']
        algorithm = filter_on['algorithm']

    algorithm_group = None
    algorithm_group_id = 0
    if filter_on['algorithm_group_id']:
        algorithm_group_id = filter_on['algorithm_group_id']
        algorithm_group = filter_on['algorithm_group']

    from_timestamp = filter_on['from_timestamp']
    until_timestamp = filter_on['until_timestamp']

    result = None
    if filter_on['result']:
        result = filter_on['result']

    plot = filter_on['plot']

    logger.info('get_snab_results :: getting MySQL engine')
    try:
        engine, fail_msg, trace = get_snab_engine()
        logger.info(fail_msg)
    except Exception as err:
        trace = traceback.format_exc()
        logger.error(trace)
        logger.error('%s' % fail_msg)
        logger.error('error :: get_snab_results :: could not get a MySQL engine to get snab table - %s' % str(err))
        raise  # to webapp to return in the UI

    try:
        anomalies_table, fail_msg, trace = anomalies_table_meta(skyline_app, engine)
        if fail_msg != 'anomalies_table meta reflected OK':
            logger.error('error :: get_snab_results :: could not get a MySQL engine fail_msg - %s' % str(fail_msg))
        if trace != 'none':
            logger.error('error :: get_snab_results :: could not get a MySQL engine trace - %s' % str(trace))
    except Exception as err:
        trace = traceback.format_exc()
        logger.error(trace)
        fail_msg = 'error :: get_snab_results :: anomalies_table_meta - %s' % str(err)
        logger.error('%s' % fail_msg)
        if engine:
            snab_engine_disposal(engine)
        raise  # to webapp to return in the UI

    # Determine start and end anomaly_ids
    start_anomaly_id = 0
    try:
        connection = engine.connect()
        stmt = select([anomalies_table]).where(anomalies_table.c.anomaly_timestamp >= int(from_timestamp)).order_by(anomalies_table.c.id.asc()).limit(1)
        results = connection.execute(stmt)
        for row in results:
            start_anomaly_id = row['id']
            break
        connection.close()
    except Exception as err:
        trace = traceback.format_exc()
        logger.error(trace)
        fail_msg = 'error :: get_snab_results :: failed to determine start_anomaly_id - %s' % str(err)
        logger.error('%s' % fail_msg)
        if engine:
            try:
                connection.close()
            except:
                pass
            snab_engine_disposal(engine)
        raise  # to webapp to return in the UI
    logger.info('get_snab_results :: starting from anomaly_id: %s' % str(start_anomaly_id))

    end_anomaly_id = 0
    try:
        connection = engine.connect()
        stmt = select([anomalies_table]).where(anomalies_table.c.anomaly_timestamp <= int(until_timestamp)).order_by(anomalies_table.c.id.desc()).limit(1)
        results = connection.execute(stmt)
        for row in results:
            end_anomaly_id = row['id']
            break
        connection.close()
    except Exception as err:
        trace = traceback.format_exc()
        logger.error(trace)
        fail_msg = 'error :: get_snab_results :: failed to determine end_anomaly_id - %s' % str(err)
        logger.error('%s' % fail_msg)
        if engine:
            try:
                connection.close()
            except:
                pass
            snab_engine_disposal(engine)
        raise  # to webapp to return in the UI
    logger.info('get_snab_results :: ending with anomaly_id: %s' % str(start_anomaly_id))

    try:
        snab_table, log_msg, trace = snab_table_meta(skyline_app, engine)
        logger.info(log_msg)
        logger.info('get_snab_results :: snab_table OK')
    except:
        logger.error(traceback.format_exc())
        logger.error('error :: get_snab_results :: failed to get snab_table meta')
        if engine:
            snab_engine_disposal(engine)
        raise  # to webapp to return in the UI

    all_results = {}
    try:
        connection = engine.connect()
        stmt = select([snab_table]).\
            where(snab_table.c.anomaly_id >= start_anomaly_id).\
            where(snab_table.c.anomaly_id <= end_anomaly_id)
        if from_timestamp:
            stmt = select([snab_table]).\
                where(snab_table.c.anomaly_id >= start_anomaly_id).\
                where(snab_table.c.anomaly_id <= end_anomaly_id).\
                where(snab_table.c.snab_timestamp >= int(from_timestamp))
        if until_timestamp:
            stmt = select([snab_table]).\
                where(snab_table.c.anomaly_id >= start_anomaly_id).\
                where(snab_table.c.anomaly_id <= end_anomaly_id).\
                where(snab_table.c.snab_timestamp <= int(until_timestamp))
        if from_timestamp and until_timestamp:
            stmt = select([snab_table]).\
                where(snab_table.c.anomaly_id >= start_anomaly_id).\
                where(snab_table.c.anomaly_id <= end_anomaly_id).\
                where(snab_table.c.snab_timestamp >= int(from_timestamp)).\
                where(snab_table.c.snab_timestamp <= int(until_timestamp))
        results = connection.execute(stmt)
        for row in results:
            snab_id = row['id']
            all_results[snab_id] = dict(row)
        connection.close()
    except Exception as err:
        trace = traceback.format_exc()
        logger.error(trace)
        fail_msg = 'error :: get_snab_results :: could not determine all_results - %s' % str(err)
        logger.error(fail_msg)
        if engine:
            try:
                connection.close()
            except:
                pass
            snab_engine_disposal(engine)
        raise
    logger.info('get_snab_results :: determined %s SNAB results before filtering' % (
        str(len(all_results))))

    metric_ids_and_base_names = {}
    try:
        metric_ids_and_base_names = get_metric_ids_and_base_names(skyline_app)
    except Exception as err:
        logger.error(traceback.format_exc())
        logger.error('error :: get_snab_results :: failed to get Redis key %s - %s' % str(err))
        metric_ids_and_base_names = {}

    # Filter by result
    results_by_result = {}
    if result and result != 'all':
        for snab_id in list(all_results.keys()):
            if all_results[snab_id][result] == 1:
                results_by_result[snab_id] = all_results[snab_id]
    if results_by_result:
        all_results = dict(results_by_result)

    # Filter by algorithm
    results_by_algorithm = {}
    if algorithm_id:
        for snab_id in list(all_results.keys()):
            if all_results[snab_id]['algorithm_id'] == algorithm_id:
                results_by_algorithm[snab_id] = all_results[snab_id]
                all_results[snab_id]['algorithm'] = algorithm
    if results_by_algorithm:
        all_results = dict(results_by_algorithm)

    # Filter by algorithm_group
    results_by_algorithm_group = {}
    if algorithm_group_id:
        for snab_id in list(all_results.keys()):
            if all_results[snab_id]['algorithm_group_id'] == algorithm_group_id:
                results_by_algorithm_group[snab_id] = all_results[snab_id]
                all_results[snab_id]['algorithm_group'] = algorithm_group
    if results_by_algorithm_group:
        all_results = dict(results_by_algorithm_group)

    # Filter by namespaces
    results_by_namespaces = {}
    filtered_metrics = {}
    if namespaces:
        for metric_id in list(metric_ids_and_base_names.keys()):
            base_name = metric_ids_and_base_names[metric_id]
            try:
                pattern_match, metric_matched_by = matched_or_regexed_in_list(skyline_app, base_name, namespaces)
                if pattern_match:
                    filtered_metrics[metric_id] = base_name
            except Exception as err:
                logger.error(traceback.format_exc())
                logger.error('error :: get_snab_results :: matched_or_regexed_in_list failed - %s' % str(err))

    anomaly_ids = []
    anomalies = {}
    if not filtered_metrics:
        check_anomaly_ids = []
        for snab_id in list(all_results.keys()):
            check_anomaly_ids.append(all_results[snab_id]['anomaly_id'])
        check_metric_ids = []
        try:
            connection = engine.connect()
            stmt = select([anomalies_table], anomalies_table.c.id.in_(check_anomaly_ids))
            results = connection.execute(stmt)
            for row in results:
                anomaly_id = row['id']
                metric_id = row['metric_id']
                check_metric_ids.append(metric_id)
                anomaly_ids.append(anomaly_id)
                anomalies[anomaly_id] = dict(row)
            connection.close()
        except Exception as err:
            trace = traceback.format_exc()
            logger.error(trace)
            fail_msg = 'error :: get_snab_results :: failed to build anomaly_ids - %s' % str(err)
            logger.error('%s' % fail_msg)
            if engine:
                try:
                    connection.close()
                except:
                    pass
                snab_engine_disposal(engine)
            raise  # to webapp to return in the UI
        for metric_id in check_metric_ids:
            base_name = metric_ids_and_base_names[metric_id]
            filtered_metrics[metric_id] = base_name

    # Now determine what anomaly ids exist for the filtered metrics in the
    # all results
    metric_ids = list(filtered_metrics.keys())
    if metric_ids and not anomalies:
        try:
            connection = engine.connect()
            stmt = select([anomalies_table], anomalies_table.c.metric_id.in_(metric_ids))
            results = connection.execute(stmt)
            for row in results:
                anomaly_id = row['id']
                anomaly_ids.append(anomaly_id)
                anomalies[anomaly_id] = dict(row)
            connection.close()
        except Exception as err:
            trace = traceback.format_exc()
            logger.error(trace)
            fail_msg = 'error :: get_snab_results :: failed to build anomaly_ids - %s' % str(err)
            logger.error('%s' % fail_msg)
            if engine:
                try:
                    connection.close()
                except:
                    pass
                snab_engine_disposal(engine)
            raise  # to webapp to return in the UI
    if anomaly_ids:
        for snab_id in list(all_results.keys()):
            if all_results[snab_id]['anomaly_id'] in anomaly_ids:
                results_by_namespaces[snab_id] = all_results[snab_id]
    if results_by_namespaces:
        all_results = dict(results_by_namespaces)

    if all_results:
        for snab_id in list(all_results.keys()):
            try:
                anomaly_id = all_results[snab_id]['anomaly_id']
                metric_id = anomalies[anomaly_id]['metric_id']
                all_results[snab_id]['metric'] = metric_ids_and_base_names[metric_id]
            except Exception as err:
                logger.error(traceback.format_exc())
                logger.error('error :: get_snab_results :: failed to determine base_name - %s' % str(err))

        results_data = dict(all_results)
        del all_results

    logger.info('get_snab_results :: determined %s SNAB results' % str(len(results_data)))

    for snab_id in list(results_data.keys()):
        results_data[snab_id]['plot'] = None
        if not plot:
            continue
        timeseries = []
        try:
            anomaly_id = results_data[snab_id]['anomaly_id']
            metric = results_data[snab_id]['metric']
        except Exception as err:
            logger.error(traceback.format_exc())
            logger.error('error :: get_snab_results :: failed to determine anomaly_id and/or metric - %s' % str(err))
            continue

        graph_file = '%s/snab.%s.%s.%s.png' % (
            settings.SKYLINE_TMP_DIR, str(snab_id), metric, str(result))
        if os.path.isfile(graph_file):
            created_graph = True
            output_file = str(graph_file)
            results_data[snab_id]['plot'] = output_file
            continue
        try:
            anomaly_id = results_data[snab_id]['anomaly_id']
            metric = results_data[snab_id]['metric']
            algorithm_group = results_data[snab_id]['algorithm_group']
            anomaly_timestamp = anomalies[anomaly_id]['anomaly_timestamp']
            full_duration = anomalies[anomaly_id]['full_duration']
            graphite_from_timestamp = anomaly_timestamp - full_duration
            graphite_until_timestamp = anomaly_timestamp + 14400
            timeseries = get_graphite_metric(skyline_app, metric, graphite_from_timestamp, graphite_until_timestamp, 'list', 'object')
        except Exception as err:
            logger.error(traceback.format_exc())
            logger.error('error :: get_snab_results :: get_graphite_metric failed to get timeseries - %s' % str(err))
        output_file = None
        if timeseries:
            try:
                graph_title = '%s\nSNAB id %s - %s - %s' % (metric, str(snab_id), algorithm_group, str(result))
                created_graph, output_file = create_matplotlib_graph(skyline_app, graph_file, graph_title, timeseries, [anomaly_timestamp])
                # output_file = plot_anomalies(skyline_app, metric, timeseries, [anomaly_timestamp], graph_title, graph_file)
            except:
                logger.error(traceback.format_exc())
                logger.error('error :: get_snab_results :: plot_anomalies failed for %s snab id %s' % (metric, str(snab_id)))
        if output_file:
            results_data[snab_id]['plot'] = output_file

    if connection:
        try:
            connection.close()
        except:
            pass

    if engine:
        snab_engine_disposal(engine)
    return results_data
