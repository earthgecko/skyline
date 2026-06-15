import logging
import os
import time
import datetime
import traceback

# @added 20230724 - Feature #5010: snab - save training_data
from ast import literal_eval

import settings
import skyline_version

# @modified 20230724 - Feature #5010: snab - save training_data
# Added Table and MetaData
from sqlalchemy import select, Table, MetaData

from database import get_engine, snab_table_meta, anomalies_table_meta
from matched_or_regexed_in_list import matched_or_regexed_in_list
# @modified 20241026 - Task #5521: webapp - update to bootstrap-5.3.3
#                      Feature #5008: webapp - snab report page
# Swap to use the database get_all_db_metric_names method rather than the Redis
# based get_metric_ids_and_base_names method which only deals active metric ids
#from functions.metrics.get_metric_ids_and_base_names import get_metric_ids_and_base_names
from functions.plots.plot_anomalies import plot_anomalies

# @modified 20230724 - Feature #5010: snab - save training_data
# Added get_redis_conn_decoded
from skyline_functions import get_graphite_metric, get_redis_conn_decoded

from create_matplotlib_graph import create_matplotlib_graph

# @added 20230713 - Feature #4994: custom_algorithm - mirages
#                   Feature #4988: Allow snab to return and save results
#                   Task #2732: Prometheus to Skyline
#                   Branch #4300: prometheus
# Handle labelled_metrics
from functions.victoriametrics.get_victoriametrics_metric import get_victoriametrics_metric

# @added 20230724 - Feature #5010: snab - save training_data
from functions.database.queries.get_algorithms import get_algorithms
from functions.database.queries.get_algorithm_groups import get_algorithm_groups

# @added 20241026 - Task #5521: webapp - update to bootstrap-5.3.3
#                   Feature #5008: webapp - snab report page
from functions.database.queries.get_all_db_metric_names import get_all_db_metric_names

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
        #connection = engine.connect()
        # @modified 20260225 - Task #5176: Migrate to sqlalchemy v2 API
        #                      Task #5628: Build v5.0.0 and test
        #stmt = select([anomalies_table]).where(anomalies_table.c.anomaly_timestamp >= int(from_timestamp)).order_by(anomalies_table.c.id.asc()).limit(1)
        stmt = select(anomalies_table).where(anomalies_table.c.anomaly_timestamp >= int(from_timestamp)).order_by(anomalies_table.c.id.asc()).limit(1)
        # @modified 20260227 - Task #5176: Migrate to sqlalchemy v2 API
        #                      Task #5628: Build v5.0.0 and test
        #results = connection.execute(stmt)
        with engine.connect() as connection:
            result = connection.execute(stmt)
            results = [dict(row._mapping) for row in result.fetchall()]
        for row in results:
            start_anomaly_id = row['id']
            break
        #connection.close()
    except Exception as err:
        trace = traceback.format_exc()
        logger.error(trace)
        fail_msg = 'error :: get_snab_results :: failed to determine start_anomaly_id - %s' % str(err)
        logger.error('%s' % fail_msg)
        if engine:
            #try:
            #    connection.close()
            #except:
            #    pass
            snab_engine_disposal(engine)
        raise  # to webapp to return in the UI
    logger.info('get_snab_results :: starting from anomaly_id: %s' % str(start_anomaly_id))

    end_anomaly_id = 0
    try:
        #connection = engine.connect()
        # @modified 20260225 - Task #5176: Migrate to sqlalchemy v2 API
        #                      Task #5628: Build v5.0.0 and test
        #stmt = select([anomalies_table]).where(anomalies_table.c.anomaly_timestamp <= int(until_timestamp)).order_by(anomalies_table.c.id.desc()).limit(1)
        stmt = select(anomalies_table).where(anomalies_table.c.anomaly_timestamp <= int(until_timestamp)).order_by(anomalies_table.c.id.desc()).limit(1)
        # @modified 20260227 - Task #5176: Migrate to sqlalchemy v2 API
        #                      Task #5628: Build v5.0.0 and test
        #results = connection.execute(stmt)
        with engine.connect() as connection:
            result = connection.execute(stmt)
            results = [dict(row._mapping) for row in result.fetchall()]
        for row in results:
            end_anomaly_id = row['id']
            break
        #connection.close()
    except Exception as err:
        trace = traceback.format_exc()
        logger.error(trace)
        fail_msg = 'error :: get_snab_results :: failed to determine end_anomaly_id - %s' % str(err)
        logger.error('%s' % fail_msg)
        if engine:
            #try:
            #    connection.close()
            #except:
            #    pass
            snab_engine_disposal(engine)
        raise  # to webapp to return in the UI
    logger.info('get_snab_results :: ending with anomaly_id: %s' % str(end_anomaly_id))

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
        #connection = engine.connect()
        # @modified 20260225 - Task #5176: Migrate to sqlalchemy v2 API
        #                      Task #5628: Build v5.0.0 and test
        #stmt = select([snab_table]).\
        stmt = select(snab_table).\
            where(snab_table.c.anomaly_id >= start_anomaly_id).\
            where(snab_table.c.anomaly_id <= end_anomaly_id)
        if from_timestamp:
            # @modified 20260225 - Task #5176: Migrate to sqlalchemy v2 API
            #                      Task #5628: Build v5.0.0 and test
            #stmt = select([snab_table]).\
            stmt = select(snab_table).\
                where(snab_table.c.anomaly_id >= start_anomaly_id).\
                where(snab_table.c.anomaly_id <= end_anomaly_id).\
                where(snab_table.c.snab_timestamp >= int(from_timestamp))
        if until_timestamp:
            # @modified 20260225 - Task #5176: Migrate to sqlalchemy v2 API
            #                      Task #5628: Build v5.0.0 and test
            #stmt = select([snab_table]).\
            stmt = select(snab_table).\
                where(snab_table.c.anomaly_id >= start_anomaly_id).\
                where(snab_table.c.anomaly_id <= end_anomaly_id).\
                where(snab_table.c.snab_timestamp <= int(until_timestamp))
        if from_timestamp and until_timestamp:
            # @modified 20260225 - Task #5176: Migrate to sqlalchemy v2 API
            #                      Task #5628: Build v5.0.0 and test
            #stmt = select([snab_table]).\
            stmt = select(snab_table).\
                where(snab_table.c.anomaly_id >= start_anomaly_id).\
                where(snab_table.c.anomaly_id <= end_anomaly_id).\
                where(snab_table.c.snab_timestamp >= int(from_timestamp)).\
                where(snab_table.c.snab_timestamp <= int(until_timestamp))
        # @modified 20260227 - Task #5176: Migrate to sqlalchemy v2 API
        #                      Task #5628: Build v5.0.0 and test
        #results = connection.execute(stmt)
        with engine.connect() as connection:
            result = connection.execute(stmt)
            results = [dict(row._mapping) for row in result.fetchall()]
        for row in results:
            snab_id = row['id']
            all_results[snab_id] = dict(row)
        #connection.close()
    except Exception as err:
        trace = traceback.format_exc()
        logger.error(trace)
        fail_msg = 'error :: get_snab_results :: could not determine all_results - %s' % str(err)
        logger.error(fail_msg)
        if engine:
            #try:
            #    connection.close()
            #except:
            #    pass
            snab_engine_disposal(engine)
        raise
    logger.info('get_snab_results :: determined %s SNAB results before filtering' % (
        str(len(all_results))))

    # @added 20241026 - Task #5521: webapp - update to bootstrap-5.3.3
    #                   Feature #5008: webapp - snab report page
    # Map ALL metric ids
    metric_names_with_ids = {}

    metric_ids_and_base_names = {}
    try:
        # @modified 20241026 - Task #5521: webapp - update to bootstrap-5.3.3
        #                      Feature #5008: webapp - snab report page
        # The get_metric_ids_and_base_names method gets the current active
        # metrics from the Redis hash, if metrics have been removed there will
        # be KeyError errors for their ids, use the get_all_db_metric_names
        # method to ensure ALL metric ids can be determined
        # metric_ids_and_base_names = get_metric_ids_and_base_names(skyline_app)
        with_ids = True
        metric_names, metric_names_with_ids = get_all_db_metric_names(skyline_app, with_ids)
    except Exception as err:
        logger.error(traceback.format_exc())
        logger.error('error :: get_snab_results :: failed to get Redis key %s - %s' % str(err))
        metric_ids_and_base_names = {}

    # @added 20241026 - Task #5521: webapp - update to bootstrap-5.3.3
    #                   Feature #5008: webapp - snab report page
    # Map ALL metric ids
    metric_ids_and_base_names = {}
    for i_base_name, mid in metric_names_with_ids.items():
        metric_ids_and_base_names[mid] = i_base_name

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
            #connection = engine.connect()
            # @modified 20260225 - Task #5176: Migrate to sqlalchemy v2 API
            #                      Task #5628: Build v5.0.0 and test
            #stmt = select([anomalies_table], anomalies_table.c.id.in_(check_anomaly_ids))
            stmt = select(anomalies_table).\
                    where(anomalies_table.c.id.in_(check_anomaly_ids))
            # @modified 20260227 - Task #5176: Migrate to sqlalchemy v2 API
            #                      Task #5628: Build v5.0.0 and test
            #results = connection.execute(stmt)
            with engine.connect() as connection:
                result = connection.execute(stmt)
                results = [dict(row._mapping) for row in result.fetchall()]

            for row in results:
                anomaly_id = row['id']
                metric_id = row['metric_id']
                check_metric_ids.append(metric_id)
                anomaly_ids.append(anomaly_id)
                anomalies[anomaly_id] = dict(row)
            #connection.close()
        except Exception as err:
            trace = traceback.format_exc()
            logger.error(trace)
            fail_msg = 'error :: get_snab_results :: failed to build anomaly_ids - %s' % str(err)
            logger.error('%s' % fail_msg)
            if engine:
                #try:
                #    connection.close()
                #except:
                #    pass
                snab_engine_disposal(engine)
            raise  # to webapp to return in the UI
        for metric_id in check_metric_ids:
            # @modified 20241026 - Task #5521: webapp - update to bootstrap-5.3.3
            # Added try except
            try:
                base_name = metric_ids_and_base_names[metric_id]
                if base_name:
                    filtered_metrics[metric_id] = base_name
            except KeyError:
                continue
    # Now determine what anomaly ids exist for the filtered metrics in the
    # all results
    metric_ids = list(filtered_metrics.keys())
    if metric_ids and not anomalies:
        try:
            #connection = engine.connect()
            # @modified 20260225 - Task #5176: Migrate to sqlalchemy v2 API
            #                      Task #5628: Build v5.0.0 and test
            #stmt = select([anomalies_table], anomalies_table.c.metric_id.in_(metric_ids))
            stmt = select(anomalies_table).\
                    where(anomalies_table.c.metric_id.in_(metric_ids))
            # @modified 20260227 - Task #5176: Migrate to sqlalchemy v2 API
            #                      Task #5628: Build v5.0.0 and test
            #results = connection.execute(stmt)
            with engine.connect() as connection:
                result = connection.execute(stmt)
                results = [dict(row._mapping) for row in result.fetchall()]
            for row in results:
                anomaly_id = row['id']
                anomaly_ids.append(anomaly_id)
                anomalies[anomaly_id] = dict(row)
            #connection.close()
        except Exception as err:
            trace = traceback.format_exc()
            logger.error(trace)
            fail_msg = 'error :: get_snab_results :: failed to build anomaly_ids - %s' % str(err)
            logger.error('%s' % fail_msg)
            if engine:
                #try:
                #    connection.close()
                #except:
                #    pass
                snab_engine_disposal(engine)
            raise  # to webapp to return in the UI
    if anomaly_ids:
        for snab_id in list(all_results.keys()):
            if all_results[snab_id]['anomaly_id'] in anomaly_ids:
                results_by_namespaces[snab_id] = all_results[snab_id]
    if results_by_namespaces:
        all_results = dict(results_by_namespaces)

    all_results_errors = []
    if all_results:
        for snab_id in list(all_results.keys()):
            try:
                anomaly_id = all_results[snab_id]['anomaly_id']
                metric_id = anomalies[anomaly_id]['metric_id']

                # @added 20241026 - Task #5521: webapp - update to bootstrap-5.3.3
                if metric_id not in metric_ids_and_base_names.keys():
                    all_results_errors.append({'base_name not found for metric_id': metric_id, 'snab_id': snab_id, 'anomaly_id': anomaly_id})
                    continue

                all_results[snab_id]['metric'] = metric_ids_and_base_names[metric_id]
                
                # @added 20230724 - add training_data page link
                labelled_metric_name = None
                if '_tenant_id="' in metric_ids_and_base_names[metric_id]:
                    labelled_metric_name = 'labelled_metrics.%s' % str(metric_id)
                all_results[snab_id]['labelled_metric'] = labelled_metric_name

            except Exception as err:
                logger.error(traceback.format_exc())
                logger.error('error :: get_snab_results :: failed to determine base_name - %s' % str(err))
                all_results_errors.append({'err': err, 'snab_id': snab_id})

        results_data = dict(all_results)
        del all_results

    if len(all_results_errors) > 0:
        logger.error('error :: get_snab_results :: %s errors reported with all_results data, all_results_errors[\'0\']: %s' % (
            str(len(all_results_errors)), str(all_results_errors[0])))

    logger.info('get_snab_results :: determined %s SNAB results' % str(len(results_data)))

    # @added 20230724 - Feature #5010: snab - save training_data
    # Added training_data page link to SNAB results table
    try:
        redis_conn_decoded = get_redis_conn_decoded(skyline_app)
    except Exception as err:
        logger.error(traceback.format_exc())
        logger.error('error :: get_snab_results :: get_redis_conn_decoded failed - %s' % (
            err))

    training_data_raw = []
    # @added 20250329 - Feature #5611: custom_algorithm_only
    # Also sync training for do_not_train instances
    training_data_raw_with_do_not_train = []
    try:
        training_data_raw_with_do_not_train = list(redis_conn_decoded.smembers('ionosphere.training_data_with_do_not_train'))
    except Exception as err:
        logger.error('error :: get_snab_results :: failed to generate a list from ionosphere.training_data_with_do_not_train Redis set, err: %s' % err)
        training_data_raw_with_do_not_train = []
    if training_data_raw_with_do_not_train:
        training_data_raw = training_data_raw_with_do_not_train

    # @modified 20250329 - Feature #5611: custom_algorithm_only
    # Only check if the do_not_train set has not been used to populate the
    # training_data_raw list
    if not training_data_raw:
        try:
            training_data_raw = list(redis_conn_decoded.smembers('ionosphere.training_data'))
        except Exception as err:
            logger.error(traceback.format_exc())
            logger.error('error :: get_snab_results :: smembers failed on ionosphere.training_data - %s' % (
                err))

    ionosphere_training_data_dict = {}
    for training_data_str in training_data_raw:
        try:
            training_data_item = literal_eval(training_data_str)
            metric = training_data_item[0]
            try:
                ionosphere_training_data_dict[metric].append(training_data_item[1])
            except:
                ionosphere_training_data_dict[metric] = []
                ionosphere_training_data_dict[metric].append(training_data_item[1])
        except Exception as err:
            logger.error('error :: get_snab_results :: failed to interpolate - %s - %s' % (
                str(training_data_str), err))
    metrics_with_training_data = list(ionosphere_training_data_dict.keys())
    algorithms = {}
    all_algorithms_by_id = {}
    try:
        algorithms, all_algorithms_by_id = get_algorithms(skyline_app, return_all_algorithms_by_id=True)
    except Exception as err:
        logger.error(traceback.format_exc())
        logger.error('error :: get_snab_results :: failed to get algorithms - %s' % str(err))
    algorithm_groups = {}
    all_algorithm_groups_by_id = {}
    try:
        algorithm_groups, all_algorithm_groups_by_id = get_algorithm_groups(skyline_app, return_all_algorithm_groups_by_id=True)
    except Exception as err:
        logger.error(traceback.format_exc())
        logger.error('error :: get_snab_results :: failed to get algorithm_groups - %s' % str(err))
    try:
        use_table_meta = MetaData()
        apps_table = Table('apps', use_table_meta, autoload_with=engine)
    except Exception as err:
        logger.error(traceback.format_exc())
        logger.error('error :: get_snab_results :: use_table Table failed on apps table - %s' % (
            err))
    apps = {}
    try:
        #connection = engine.connect()
        stmt = select(apps_table)
        #result = connection.execute(stmt)
        # @modified 20260227 - Task #5176: Migrate to sqlalchemy v2 API
        #                      Task #5628: Build v5.0.0 and test
        #result = connection.execute(stmt)
        #for row in result:
        with engine.connect() as connection:
            result = connection.execute(stmt)
            results = [dict(row._mapping) for row in result.fetchall()]
        for row in results:
            app_id = row['id']
            apps[app_id] = row['app']
        #connection.close()
    except Exception as err:
        logger.error(traceback.format_exc())
        logger.error('error :: get_snab_results :: failed to build apps - %s' % str(err))

    results_errors = []
    for snab_id in list(results_data.keys()):
        training_data_uri = None

        try:
            anomaly_timestamp = anomalies[anomaly_id]['anomaly_timestamp']
        except KeyError as err:
            results_errors.append({'anomalies error': err, 'anomaly_id': anomaly_id})
            continue
        try:
            metric = results_data[snab_id]['metric']
            anomaly_id = results_data[snab_id]['anomaly_id']
            labelled_metric_name = results_data[snab_id]['labelled_metric']
        except KeyError as err:
            results_errors.append({'results_data error': err, 'snab_id': snab_id})
            continue

        use_metric = None
        if metric in metrics_with_training_data:
            use_metric = metric
        if labelled_metric_name in metrics_with_training_data:
            use_metric = labelled_metric_name
        if use_metric:
            if anomaly_timestamp in ionosphere_training_data_dict[use_metric]:
                metric_timeseries_dir = use_metric.replace('.', '/')
                metric_training_data_dir = '%s/%s/%s' % (
                    settings.IONOSPHERE_DATA_FOLDER, str(anomaly_timestamp),
                    metric_timeseries_dir)
                if os.path.exists(metric_training_data_dir):
                    training_data_found = True
                    training_data_uri = '/ionosphere?timestamp=%s&metric=%s&requested_timestamp=%s' % (
                        str(anomaly_timestamp), use_metric, str(anomaly_timestamp))
                if not training_data_found:
                    metric_training_data_dir = '%s_saved/%s/%s' % (
                        settings.IONOSPHERE_DATA_FOLDER, str(anomaly_timestamp),
                        metric_timeseries_dir)
                    if os.path.exists(metric_training_data_dir):
                        training_data_uri = '/ionosphere?saved_training_data=true&timestamp=%s&metric=%s&requested_timestamp=%s' % (
                            str(anomaly_timestamp), use_metric, str(anomaly_timestamp))
        results_data[snab_id]['training_data_uri'] = training_data_uri
        try:
            algorithm_id = int(results_data[snab_id]['algorithm_id'])
        except:
            algorithm_id = None
        algorithm_name = None
        if algorithm_id:
            try:
                algorithm_name = str(all_algorithms_by_id[algorithm_id])
            except Exception as err:
                logger.info('warning :: get_snab_results :: failed find algorithm in all_algorithms_by_id for algorithm_id: %s - %s' % (
                    str(algorithm_id), str(err)))
        # Change algorithm_id key to algorithm and maintain order 
        results_data[snab_id] = {'algorithm' if k == 'algorithm_id' else k:v for k, v in results_data[snab_id].items()}
        results_data[snab_id]['algorithm'] = algorithm_name
        app_id = None
        try:
            app_id = int(apps[results_data[snab_id]['app_id']])
        except:
            app_id = None
        app = None
        try:
            app = str(apps[results_data[snab_id]['app_id']])
        except Exception as err:
            logger.info('warning :: get_snab_results :: failed find app in apps for app_id: %s - %s' % (
                str(algorithm_id), str(err)))
        # Change app_id key to app and maintain order 
        results_data[snab_id] = {'app' if k == 'app_id' else k:v for k, v in results_data[snab_id].items()}
        results_data[snab_id]['app'] = app
        results_data[snab_id]['algorithm_id'] = algorithm_id
        try:
            algorithm_group_id = str(results_data[snab_id]['algorithm_group_id'])
            del results_data[snab_id]['algorithm_group_id']
            results_data[snab_id]['algorithm_group_id'] = algorithm_group_id
        except:
            algorithm_group_id = None

    if len(results_errors) > 0:
        logger.error('error :: get_snab_results :: %s errors reported with results_data, results_errors[\'0\']: %s' % (
            str(len(results_errors)), str(results_errors[0])))

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
            logger.error('error :: get_snab_results :: failed to determine anomaly_id and/or metric for snab_id: %s, err: %s' % (
                str(snab_id), str(err)))
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
            try:
                algorithm_group = results_data[snab_id]['algorithm_group']
            except KeyError:
                algorithm_group = None
            anomaly_timestamp = anomalies[anomaly_id]['anomaly_timestamp']
            full_duration = anomalies[anomaly_id]['full_duration']
            graphite_from_timestamp = anomaly_timestamp - full_duration
            graphite_until_timestamp = anomaly_timestamp + 14400

            # @modified 20230713 - Feature #4994: custom_algorithm - mirages
            #                      Feature #4988: Allow snab to return and save results
            #                      Task #2732: Prometheus to Skyline
            #                      Branch #4300: prometheus
            # Handle labelled_metrics
            # timeseries = get_graphite_metric(skyline_app, metric, graphite_from_timestamp, graphite_until_timestamp, 'list', 'object')
            if metric.startswith('labelled_metrics.') or '_tenant_id="' in metric:
                timeseries = get_victoriametrics_metric(skyline_app, metric, from_timestamp, until_timestamp, 'list', 'object')
            else:
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

    #if connection:
    #    try:
    #        connection.close()
    #    except:
    #        pass

    if engine:
        snab_engine_disposal(engine)
    return results_data
