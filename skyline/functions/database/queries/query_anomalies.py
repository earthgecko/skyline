"""
Get anomalies for a metric id
"""
import logging
import traceback
from sqlalchemy.sql import select

from database import (
    get_engine, engine_disposal, anomalies_table_meta,
)

# @added 20231124 - Feature #5104: boundary - external_settings
from functions.database.queries.get_apps import get_apps
from functions.database.queries.get_algorithms import get_algorithms
from functions.database.queries.get_sources import get_sources
from functions.database.queries.get_hosts import get_hosts


def get_anomalies(current_skyline_app, metric_id, params={'latest': False}):
    """
    Given a metric_id, return the anomalies for a metric or the latest one if
    latest is passed as True
    """
    current_skyline_app_logger = current_skyline_app + 'Log'
    current_logger = logging.getLogger(current_skyline_app_logger)

    anomalies = {}

    try:
        engine, fail_msg, trace = get_engine(current_skyline_app)
        if fail_msg != 'got MySQL engine':
            current_logger.error('error :: get_anomalies :: could not get a MySQL engine fail_msg - %s' % str(fail_msg))
        if trace != 'none':
            current_logger.error('error :: get_anomalies :: could not get a MySQL engine trace - %s' % str(trace))
    except Exception as err:
        current_logger.error(traceback.format_exc())
        current_logger.error('error :: get_anomalies :: could not get a MySQL engine - %s' % str(err))

    latest = False
    if params:
        try:
            latest = params['latest']
        except KeyError:
            latest = False

    if engine:
        try:
            anomalies_table, fail_msg, trace = anomalies_table_meta(current_skyline_app, engine)
            if fail_msg != 'anomalies_table meta reflected OK':
                current_logger.error('error :: get_anomalies :: could not get a MySQL engine fail_msg - %s' % str(fail_msg))
            if trace != 'none':
                current_logger.error('error :: get_anomalies :: could not get a MySQL engine trace - %s' % str(trace))
        except Exception as err:
            current_logger.error(traceback.format_exc())
            current_logger.error('error :: get_anomalies :: anomalies_table_meta - %s' % str(err))
        try:
            connection = engine.connect()
            if latest:
                stmt = select([anomalies_table]).where(anomalies_table.c.metric_id == metric_id).order_by(anomalies_table.c.id.desc()).limit(1)
            else:
                stmt = select([anomalies_table]).where(anomalies_table.c.metric_id == metric_id).order_by(anomalies_table.c.id.desc())
            results = connection.execute(stmt)
            for row in results:
                anomaly_id = row['id']
                anomalies[anomaly_id] = dict(row)
            connection.close()
        except Exception as err:
            current_logger.error(traceback.format_exc())
            current_logger.error('error :: get_anomalies :: failed to build anomalies dict - %s' % str(err))

    if engine:
        engine_disposal(current_skyline_app, engine)

    return anomalies


def get_anomalies_for_period(
        current_skyline_app, metric_ids, from_timestamp, until_timestamp):
    """
    Given a list of metric_ids and timestamps return the anomalies for period.

    :param current_skyline_app: the skyline app
    :param metric_ids: a list of metric ids (or an empty list for all metrics)
    :param from_timestamp: from
    :param until_timestamp: until
    :type current_skyline_app: str
    :type metric_ids: list
    :type from_timestamp: int
    :type until_timestamp: int
    :return: anomalies
    :rtype: dict

    """
    current_skyline_app_logger = current_skyline_app + 'Log'
    current_logger = logging.getLogger(current_skyline_app_logger)

    anomalies = {}

    try:
        engine, fail_msg, trace = get_engine(current_skyline_app)
        if fail_msg != 'got MySQL engine':
            current_logger.error('error :: get_anomalies_for_period :: could not get a MySQL engine fail_msg - %s' % str(fail_msg))
        if trace != 'none':
            current_logger.error('error :: get_anomalies_for_period :: could not get a MySQL engine trace - %s' % str(trace))
    except Exception as err:
        current_logger.error(traceback.format_exc())
        current_logger.error('error :: get_anomalies_for_period :: could not get a MySQL engine - %s' % str(err))

    if engine:
        try:
            anomalies_table, fail_msg, trace = anomalies_table_meta(current_skyline_app, engine)
            if fail_msg != 'anomalies_table meta reflected OK':
                current_logger.error('error :: get_anomalies_from_timestamp :: could not get a MySQL engine fail_msg - %s' % str(fail_msg))
            if trace != 'none':
                current_logger.error('error :: get_anomalies_from_timestamp :: could not get a MySQL engine trace - %s' % str(trace))
        except Exception as err:
            current_logger.error(traceback.format_exc())
            current_logger.error('error :: get_anomalies_from_timestamp :: anomalies_table_meta - %s' % str(err))
        try:
            connection = engine.connect()
            if metric_ids:
                stmt = select([anomalies_table], anomalies_table.c.metric_id.in_(metric_ids)).\
                    where(anomalies_table.c.anomaly_timestamp >= from_timestamp).\
                    where(anomalies_table.c.anomaly_timestamp <= until_timestamp).\
                    order_by(anomalies_table.c.id.asc())
            else:
                stmt = select([anomalies_table]).\
                    where(anomalies_table.c.anomaly_timestamp >= from_timestamp).\
                    where(anomalies_table.c.anomaly_timestamp <= until_timestamp).\
                    order_by(anomalies_table.c.id.asc())
            results = connection.execute(stmt)
            for row in results:
                anomaly_id = row['id']
                anomalies[anomaly_id] = dict(row)
            connection.close()
        except Exception as err:
            current_logger.error(traceback.format_exc())
            current_logger.error('error :: get_anomalies_for_period :: failed to build anomalies dict - %s' % str(err))

    if engine:
        engine_disposal(current_skyline_app, engine)

    # @added 20231124 - Feature #5104: boundary - external_settings
    # Convert the datapoint to a float and add app and algorithms
    if anomalies:
        try:
            anomalies = update_anomalies_with_names_for_ids(current_skyline_app, anomalies)
        except Exception as err:
            current_logger.error('error :: get_anomalies_for_period :: update_anomalies_with_names_for_ids failed - %s' % (
                err))

    return anomalies


# @added 20231003 - Feature #5092: ionosphere - create_training_data
def get_anomaly(
        current_skyline_app, anomaly_id):
    """
    Given an anomaly id, return a dict of the row in the anomalies table.
    Although there is an API method to get the details of an anomaly, it also
    returns correlations, related_events, etc.  This is a lightweight method
    that only returns the anomaly table row data.

    :param current_skyline_app: the skyline app
    :param anomaly_id: the anomaly id
    :type current_skyline_app: str
    :type anomaly_id: int
    :return: anomaly
    :rtype: dict

    """
    current_skyline_app_logger = current_skyline_app + 'Log'
    current_logger = logging.getLogger(current_skyline_app_logger)

    anomaly = {}

    try:
        engine, fail_msg, trace = get_engine(current_skyline_app)
        if fail_msg != 'got MySQL engine':
            current_logger.error('error :: functions.database.queries.query_anomalies.get_anomaly :: could not get a MySQL engine fail_msg - %s' % str(fail_msg))
        if trace != 'none':
            current_logger.error('error :: functions.database.queries.query_anomalies.get_anomaly :: could not get a MySQL engine trace - %s' % str(trace))
    except Exception as err:
        current_logger.error(traceback.format_exc())
        current_logger.error('error :: functions.database.queries.query_anomalies.get_anomaly :: could not get a MySQL engine - %s' % str(err))

    if engine:
        try:
            anomalies_table, fail_msg, trace = anomalies_table_meta(current_skyline_app, engine)
            if fail_msg != 'anomalies_table meta reflected OK':
                current_logger.error('error :: functions.database.queries.query_anomalies.get_anomaly :: could not get a MySQL engine fail_msg - %s' % str(fail_msg))
            if trace != 'none':
                current_logger.error('error :: functions.database.queries.query_anomalies.get_anomaly :: could not get a MySQL engine trace - %s' % str(trace))
        except Exception as err:
            current_logger.error(traceback.format_exc())
            current_logger.error('error :: functions.database.queries.query_anomalies.get_anomaly :: anomalies_table_meta - %s' % str(err))
        try:
            connection = engine.connect()
            stmt = select([anomalies_table]).\
                where(anomalies_table.c.id == anomaly_id).\
                order_by(anomalies_table.c.id.asc())
            results = connection.execute(stmt)
            for row in results:
                anomaly = dict(row)
                break
            connection.close()
        except Exception as err:
            current_logger.error(traceback.format_exc())
            current_logger.error('error :: functions.database.queries.query_anomalies.get_anomaly :: failed to build anomaly dict - %s' % str(err))
    if engine:
        engine_disposal(current_skyline_app, engine)

    # @added 20231124 - Feature #5104: boundary - external_settings
    # Convert the datapoint to a float and add app and algorithms
    if anomaly:
        anomaly_id = anomaly['id']
        anomalies = {
            anomaly_id: anomaly,
        }
        try:
            anomalies = update_anomalies_with_names_for_ids(current_skyline_app, anomalies)
        except Exception as err:
            current_logger.error('error :: get_anomalies_for_period :: update_anomalies_with_names_for_ids failed - %s' % (
                err))
        if anomalies:
            anomaly = anomalies[anomaly_id]
    return anomaly


# @added 20231124 - Feature #5104: boundary - external_settings
# Convert the datapoint to a float and add app and algorithms
def update_anomalies_with_names_for_ids(
        current_skyline_app, anomalies):
    """
    Given an anomaly_dict which represents anomaly DB objects, add the names for
    all the elements referenced by id, e.g. app, algorithms_run,
    triggered_algorithms etc.

    :param current_skyline_app: the skyline app
    :param anomalies: a dict of anomalies
    :type current_skyline_app: str
    :type anomalies: dict
    :return: anomalies
    :rtype: dict

    """
    current_skyline_app_logger = current_skyline_app + 'Log'
    current_logger = logging.getLogger(current_skyline_app_logger)

    apps = {}
    try:
        apps = get_apps(current_skyline_app)
    except Exception as err:
        current_logger.error('error :: update_anomalies_with_names_for_ids :: get_apps failed - %s' % (
            err))
    algorithms = {}
    all_algorithms_by_id = {}
    try:
        algorithms, all_algorithms_by_id = get_algorithms(current_skyline_app, return_all_algorithms_by_id=True)
    except Exception as err:
        current_logger.error('error :: update_anomalies_with_names_for_ids :: get_algorithms failed - %s' % err)
    all_algorithms_by_name = {}
    for id in list(all_algorithms_by_id.keys()):
        all_algorithms_by_name[all_algorithms_by_id[id]] = id

    sources = {}
    try:
        sources = get_sources(current_skyline_app)
    except Exception as err:
        current_logger.error('error :: update_anomalies_with_names_for_ids :: get_sources failed - %s' % err)
    sources_by_id = {}
    if sources:
        for source, id in sources.items():
            sources_by_id[id] = source
    hosts = {}
    try:
        hosts = get_hosts(current_skyline_app)
    except Exception as err:
        current_logger.error('error :: update_anomalies_with_names_for_ids failed - %s' % (err))
    hosts_by_id = {}
    if hosts:
        for i_host, id in hosts.items():
            hosts_by_id[id] = i_host

    for anomaly_id in list(anomalies.keys()):
        anomalous_datapoint = float(anomalies[anomaly_id]['anomalous_datapoint'])
        # Convert to float
        anomalies[anomaly_id]['anomalous_datapoint'] = anomalous_datapoint
        app_id = None
        try:
            app_id = anomalies[anomaly_id]['app_id']
        except:
            app_id = None
        app = None
        if app_id and apps:
            for key, value in apps.items():
                if value == app_id:
                    app = key
                    break
        if app:
            anomalies[anomaly_id]['app'] = app
        source = None
        source_id = None
        try:
            source_id = anomalies[anomaly_id]['source_id']
        except:
            source_id = None
        if source_id and sources_by_id:
            try:
                source = sources_by_id[source_id]
            except:
                current_logger.error('error :: failed to get source id from sources for %s' % str(source_id))
        if source:
            anomalies[anomaly_id]['source'] = source
        host = None
        host_id = None
        try:
            host_id = anomalies[anomaly_id]['host_id']
        except:
            host_id = None
        if host_id and hosts_by_id:
            try:
                host = hosts_by_id[host_id]
            except:
                current_logger.error('error :: failed to get host id from hosts for %s' % str(host_id))
        if host:
            anomalies[anomaly_id]['host'] = host

        algorithms_run_str = anomalies[anomaly_id]['algorithms_run']
        algorithm_ids_run = algorithms_run_str.split(',')
        algorithm_ids_run = [int(v) for v in algorithm_ids_run]
        algorithms_run = []
        for algorithm_id_str in algorithm_ids_run:
            algorithm_id = int(algorithm_id_str)
            algorithms_run.append(all_algorithms_by_id[algorithm_id])
        anomalies[anomaly_id]['algorithm_ids_run'] = algorithm_ids_run
        anomalies[anomaly_id]['algorithm_names_run'] = algorithms_run
        triggered_algorithms_str = anomalies[anomaly_id]['triggered_algorithms']
        triggered_algorithm_ids = triggered_algorithms_str.split(',')
        triggered_algorithm_ids = [int(v) for v in triggered_algorithm_ids]
        triggered_algorithms = []
        for algorithm_id_str in triggered_algorithm_ids:
            algorithm_id = int(algorithm_id_str)
            triggered_algorithms.append(all_algorithms_by_id[algorithm_id])
        anomalies[anomaly_id]['triggered_algorithm_ids'] = triggered_algorithm_ids
        anomalies[anomaly_id]['triggered_algorithm_names'] = triggered_algorithms

    return anomalies

