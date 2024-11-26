from __future__ import division
import logging
# @added 20230719 - Feature #5010: snab - save training_data
import time
import os

import traceback
import settings
import skyline_version

from sqlalchemy.sql import select

# @added 20230719 - Feature #5010: snab - save training_data
import requests

from database import (
    get_engine, snab_table_meta, metrics_table_meta, anomalies_table_meta)

# @added 20230804 - Feature #5010: snab - save training_data
from functions.database.queries.get_algorithms import get_algorithms

skyline_version = skyline_version.__absolute_version__
skyline_app = 'webapp'
skyline_app_logger = '%sLog' % skyline_app
logger = logging.getLogger(skyline_app_logger)
skyline_app_logfile = '%s/%s.log' % (settings.LOG_PATH, skyline_app)
logfile = '%s/%s.log' % (settings.LOG_PATH, skyline_app)
try:
    ENABLE_WEBAPP_DEBUG = settings.ENABLE_WEBAPP_DEBUG
except EnvironmentError as err:
    logger.error('error :: cannot determine ENABLE_WEBAPP_DEBUG from settings')
    ENABLE_WEBAPP_DEBUG = False


def get_snab_engine():

    try:
        engine, fail_msg, trace = get_engine(skyline_app)
        return engine, fail_msg, trace
    except:
        trace = traceback.format_exc()
        logger.error('%s' % trace)
        fail_msg = 'error :: update_snab_result :: failed to get MySQL engine for snab table'
        logger.error('%s' % fail_msg)
        # return None, fail_msg, trace
        raise  # to webapp to return in the UI


def snab_engine_disposal(engine):
    if engine:
        try:
            engine.dispose()
        except:
            logger.error(traceback.format_exc())
            logger.error('error :: update_snab_result :: calling engine.dispose()')
    return


# @modified 20230719 - Feature #5010: snab - save training_data
# Added save_training_data
# def update_snab_result(snab_id, anomaly_id, snab_result):
def update_snab_result(snab_id, anomaly_id, snab_result, save_training_data=False):
    """
    Update the relevant field in the snab table.

    :param snab_id: the snab table id
    :param anomaly_id: the anomaly id
    :param snab_result: a selected result
    :type snab_id: int
    :type anomaly_id: int
    :type result: str
    :return: snab_result_updated, base_name, anomaly_timestamp
    :rtype: tuple

    """

    snab_result_updated = False
    base_name = None
    anomaly_timestamp = None
    connection = None

    # @modified 20230719 - Feature #5010: snab - save training_data
    # Added save_training_data
    logger.info(
        'update_snab_result :: for snab id %s with anomaly id %s and result %s, save_training_data: %s' % (
            str(snab_id), str(anomaly_id), str(snab_result), str(save_training_data)))

    logger.info('getting MySQL engine')
    try:
        engine, fail_msg, trace = get_snab_engine()
        logger.info(fail_msg)
    except:
        trace = traceback.format_exc()
        logger.error(trace)
        logger.error('%s' % fail_msg)
        logger.error('error :: update_snab_result :: could not get a MySQL engine to get update snab table')
        raise  # to webapp to return in the UI
    if not engine:
        trace = 'none'
        fail_msg = 'error :: update_snab_result :: engine not obtained'
        logger.error(fail_msg)
        raise
    try:
        snab_table, log_msg, trace = snab_table_meta(skyline_app, engine)
        logger.info(log_msg)
        logger.info('snab_table OK')
    except:
        logger.error(traceback.format_exc())
        logger.error('error :: update_snab_result :: failed to get snab_table meta')
        if engine:
            snab_engine_disposal(engine)
        raise  # to webapp to return in the UI

    algorithm_id = None

    # @modified 20201004 - Task #3748: POC SNAB
    #                   Branch #3068: SNAB
    # Allow results to be changed
    determined_result = False
    existing_result = None
    try:
        if not connection:
            connection = engine.connect()
        stmt = select([snab_table]).\
            where(snab_table.c.id == snab_id)
        result = connection.execute(stmt)
        for row in result:
            tP_set = row['tP']
            if tP_set:
                existing_result = 'tP'
            fP_set = row['fP']
            if fP_set:
                existing_result = 'fP'
            tN_set = row['tN']
            if tN_set:
                existing_result = 'tN'
            fN_set = row['fN']
            if fN_set:
                existing_result = 'fN'
            unsure_set = row['unsure']
            if unsure_set:
                existing_result = 'unsure'
            determined_result = True
            # @added 20230804 - Feature #5010: snab - save training_data
            # Add algorithm to label
            algorithm_id = row['algorithm_id']

            break
        logger.info('update_snab_result :: current result values to snab id %s - tP: %s, fP: %s, tN: %s, fN: %s, unsure: %s' % (
            str(snab_id), str(tP_set), str(fP_set), str(tN_set), str(fN_set),
            str(unsure_set)))
    except:
        trace = traceback.format_exc()
        logger.error(trace)
        logger.error('error :: update_snab_result :: could not determine current result values for snab id %s' % (
            str(snab_id)))
        fail_msg = 'error :: update_snab_result :: could not determine current result values for snab id %s' % (
            str(snab_id))
        if engine:
            try:
                connection.close()
            except:
                pass
            snab_engine_disposal(engine)
        raise

    if not determined_result:
        logger.error('error :: update_snab_result :: did not determine current result values for snab id %s' % (
            str(snab_id)))
    else:
        logger.info('update_snab_result :: current existing result for snab id %s - %s' % (
            str(snab_id), str(existing_result)))

    try:
        if not connection:
            connection = engine.connect()
        if snab_result == 'tP':
            stmt = snab_table.update().\
                values(tP=1, fP=None, tN=None, fN=None, unsure=None).\
                where(snab_table.c.id == int(snab_id)).\
                where(snab_table.c.anomaly_id == int(anomaly_id))
        if snab_result == 'fP':
            stmt = snab_table.update().\
                values(tP=None, fP=1, tN=None, fN=None, unsure=None).\
                where(snab_table.c.id == int(snab_id)).\
                where(snab_table.c.anomaly_id == int(anomaly_id))
        if snab_result == 'tN':
            stmt = snab_table.update().\
                values(tP=None, fP=None, tN=1, fN=None, unsure=None).\
                where(snab_table.c.id == int(snab_id)).\
                where(snab_table.c.anomaly_id == int(anomaly_id))
        if snab_result == 'fN':
            stmt = snab_table.update().\
                values(tP=None, fP=None, tN=None, fN=1, unsure=None).\
                where(snab_table.c.id == int(snab_id)).\
                where(snab_table.c.anomaly_id == int(anomaly_id))
        if snab_result == 'unsure':
            stmt = snab_table.update().\
                values(tP=None, fP=None, tN=None, fN=None, unsure=1).\
                where(snab_table.c.id == int(snab_id)).\
                where(snab_table.c.anomaly_id == int(anomaly_id))
        if snab_result == 'NULL':
            stmt = snab_table.update().\
                values(tP=None, fP=None, tN=None, fN=None, unsure=None).\
                where(snab_table.c.id == int(snab_id)).\
                where(snab_table.c.anomaly_id == int(anomaly_id))
        connection.execute(stmt)
        snab_result_updated = True
        logger.info('update_snab_result :: updated result for snab id %s with anomaly id %s and result %s' % (
            str(snab_id), str(anomaly_id), str(snab_result)))
    except Exception as err:
        trace = traceback.format_exc()
        logger.error(trace)
        logger.error('error :: update_snab_result :: could not update result for snab id %s with anomaly id %s and result %s - %s' % (
            str(snab_id), str(anomaly_id), str(snab_result), err))
        fail_msg = 'error :: update_snab_result :: could not update result for snab id %s with anomaly id %s and result %s' % (
            str(snab_id), str(anomaly_id), str(snab_result))
        if engine:
            try:
                connection.close()
            except:
                pass
            try:
                snab_engine_disposal(engine)
            except Exception as err:
                logger.error('error :: update_snab_result :: snab_engine_disposal failed - %s' % (
                    err))
        raise

    try:
        metrics_table, log_msg, trace = metrics_table_meta(skyline_app, engine)
        logger.info(log_msg)
        logger.info('update_snab_result :: metrics_table OK')
    except:
        logger.error(traceback.format_exc())
        logger.error('error :: update_snab_result :: failed to get metrics_table meta for %s' % str(base_name))
        if engine:
            try:
                connection.close()
            except:
                pass
            snab_engine_disposal(engine)
        raise  # to webapp to return in the UI
    try:
        anomalies_table, log_msg, trace = anomalies_table_meta(skyline_app, engine)
        logger.info(log_msg)
        logger.info('update_snab_result :: anomalies_table OK')
    except:
        logger.error(traceback.format_exc())
        logger.error('error :: update_snab_result :: failed to get anomalies_table meta')
        if engine:
            try:
                connection.close()
            except:
                pass
            snab_engine_disposal(engine)
        raise  # to webapp to return in the UI
    metric_id = None
    try:
        if not connection:
            connection = engine.connect()
        stmt = select([anomalies_table]).\
            where(anomalies_table.c.id == anomaly_id)
        result = connection.execute(stmt)
        for row in result:
            metric_id = row['metric_id']
            anomaly_timestamp = row['anomaly_timestamp']
            break
        logger.info('update_snab_result :: determined anomaly_timestamp %s for metric id %s for anomaly id %s' % (
            str(anomaly_timestamp), str(metric_id), str(anomaly_id)))
    except:
        trace = traceback.format_exc()
        logger.error(trace)
        fail_msg = 'error :: update_snab_result :: could not determine anomaly timestamp or metric id from DB for anomaly id %s' % (
            str(anomaly_id))
        logger.error('%s' % fail_msg)
        if engine:
            try:
                connection.close()
            except:
                pass
            snab_engine_disposal(engine)
        raise  # to webapp to return in the UI
    if metric_id:
        try:
            if not connection:
                connection = engine.connect()
            stmt = select([metrics_table]).where(metrics_table.c.id == int(metric_id))
            result = connection.execute(stmt)
            for row in result:
                base_name = row['metric']
        except:
            trace = traceback.format_exc()
            logger.error(trace)
            fail_msg = 'error :: could not determine metric id from metrics table'
            if engine:
                try:
                    connection.close()
                except:
                    pass
                snab_engine_disposal(engine)
            raise

    if connection:
        try:
            connection.close()
        except:
            pass

    if engine:
        snab_engine_disposal(engine)

    # @added 20230719 - Feature #5010: snab - save training_data
    # Allow snab to save training_data that is trained through the UI (not via
    # the slack link) when the user evaluates the instance as fP or fN. These
    # are the states that which the algorithm is failing at and saving the
    # training_data gives the user (me) data to pattern with. Instances
    # evaluated as tP or tN will not be saved because the algorithm is working
    # as desired in these instances and therefore no data in required for
    # patterning with.
    saved_training_data = False

    # @added 20230924 - Feature #4988: Allow snab to return and save results
    # Save all evaluated data.  The easiest way is just to save all evaluated
    # results.  This will result in approx 820KB (1.6MB when the training_data
    # is loaded with graphs) per evalaution.  All evaluations are required to
    # reproduce the results and/or add evaluations from different parties.
    # So base it on user input and settings.SNAB_SAVE_ALL_EVALUATED_TRAINING_DATA
    user_specified_training = False
    try:
        save_all_evaluated_training_data = settings.SNAB_SAVE_ALL_EVALUATED_TRAINING_DATA
    except: 
        save_all_evaluated_training_data = False
    logger.info('update_snab_result :: save_all_evaluated_training_data: %s' % (
        str(save_all_evaluated_training_data)))
    do_save_training = save_all_evaluated_training_data
    if save_training_data and snab_result in ['fP', 'fN', 'unsure']:
        do_save_training = True
    use_base_name = str(base_name)
    if '_tenant_id=' in use_base_name:
        use_base_name = 'labelled_metrics.%s' % str(metric_id)
    saved_training_data_dir = '%s_saved' % settings.IONOSPHERE_DATA_FOLDER
    timeseries_dir = use_base_name.replace('.', '/')
    saved_training_data_dir = '%s_saved/%s/%s' % (
        settings.IONOSPHERE_DATA_FOLDER, str(anomaly_timestamp),
        timeseries_dir)
    algorithms = {}
    if not save_training_data and do_save_training:
        if os.path.exists(saved_training_data_dir):
            logger.info('update_snab_result :: training_data already saved')
            do_save_training = False
        else:
            all_algorithms_by_id = {}
            algorithms_with_id = {}
            try:
                algorithms, all_algorithms_by_id = get_algorithms(skyline_app, return_all_algorithms_by_id=True)
            except Exception as err:
                logger.error('error :: update_snab_result :: get_algorithms failed - %s' % (err))
            for key, value in all_algorithms_by_id.items():
                algorithms_with_id[value] = key
            current_snab_algos = []
            for snab_app in list(settings.SNAB_CHECKS.keys()):
                for analysis_mode in list(settings.SNAB_CHECKS[snab_app].keys()):
                    for test_algo in list(settings.SNAB_CHECKS[snab_app][analysis_mode].keys()):
                        current_snab_algos.append(test_algo)
            current_snab_algo_ids = []
            for snab_algo in list(set(current_snab_algos)):
                try:
                    current_snab_algo_ids.append(algorithms_with_id[snab_algo])
                except:
                    pass
            # Only save the training_data if the SNAB testing algorithm is
            # evaluated, not the normal algo evaluation to ensure that the
            # training_data label reflects the evaluation of the SNAB algo.
            if algorithm_id and current_snab_algo_ids:
                if not algorithm_id in current_snab_algo_ids:
                    do_save_training = False

    # @modified 20230924 - Feature #4988: Allow snab to return and save results
    # if save_training_data and snab_result in ['fP', 'fN', 'unsure']:
    if do_save_training:
        logger.info('update_snab_result :: saving training_data')
        human_date = time.strftime('%Y%m%d%H%M%S', time.localtime(int(time.time())))
        use_base_name = str(base_name)
        if '_tenant_id=' in use_base_name:
            use_base_name = 'labelled_metrics.%s' % str(metric_id)
        saved_td_label = '%s.snab.%s.%s.%s.%s' % (
            human_date, snab_result, str(anomaly_timestamp), str(metric_id),
            str(anomaly_id))

        # @added 20230804 - Feature #5010: snab - save training_data
        # Add algorithm to label
        if algorithm_id:
            if not algorithms:
                try:
                    algorithms, all_algorithms_by_id = get_algorithms(skyline_app, return_all_algorithms_by_id=True)
                except Exception as err:
                    logger.error(traceback.format_exc())
                    logger.error('error :: update_snab_result :: get_algorithms failed - %s' % (
                        err))
            algorithm = None
            try:
                algorithm = all_algorithms_by_id[algorithm_id]
            except:
                algorithm = None
            if algorithm:
                saved_td_label = '%s.snab.%s.%s.%s.%s.%s' % (
                    human_date, snab_result, algorithm, str(anomaly_timestamp),
                    str(metric_id), str(anomaly_id))

        url = '%s/ionosphere?save_training_data=true&saved_td_label=%s&timestamp=%s&metric=%s&requested_timestamp=%s&format=json' % (
            settings.SKYLINE_URL, saved_td_label, str(anomaly_timestamp),
            use_base_name, str(anomaly_timestamp))
        user = None
        password = None
        if settings.WEBAPP_AUTH_ENABLED:
            user = str(settings.WEBAPP_AUTH_USER)
            password = str(settings.WEBAPP_AUTH_USER_PASSWORD)
        # Handle self signed certificate
        verify_ssl = True
        try:
            running_on_docker = settings.DOCKER
        except:
            running_on_docker = False
        if running_on_docker:
            verify_ssl = False
        try:
            overall_verify_ssl = settings.VERIFY_SSL
        except:
            overall_verify_ssl = True
        if not overall_verify_ssl:
            verify_ssl = False
        try:
            if user and password:
                r = requests.get(url, timeout=60, auth=(user, password), verify=verify_ssl)
            else:
                r = requests.get(url, timeout=60, verify=verify_ssl)
            if r.status_code == 200:
                logger.info('update_snab_result :: saved training_data')
                saved_training_data = '%s/ionosphere?saved_training_data=true&metric=%s&timestamp=%s' % (
                    settings.SKYLINE_URL, use_base_name, str(anomaly_timestamp))
        except Exception as err:
            logger.error(traceback.format_exc())
            logger.error('error :: update_snab_result :: failed get training to save graphs from %s - %s' % (
                str(url), err))

    # @modified 20230719 - Feature #5010: snab - save training_data
    # return snab_result_updated, base_name, anomaly_timestamp, existing_result
    return snab_result_updated, base_name, anomaly_timestamp, existing_result, saved_training_data


# @added 20230721 - Feature #5008: webapp - snab report page
def get_snab_algorithms(algorithms):
    snab_algorithms = {}
    algorithms_by_id = {}
    for algo in list(algorithms.keys()):
        algo_id = algorithms[algo]
        algorithms_by_id[algo_id] = algo

    logger.info('get_snab_algorithms :: getting MySQL engine')
    
    try:
        engine, fail_msg, trace = get_snab_engine()
        logger.info(fail_msg)
    except:
        trace = traceback.format_exc()
        logger.error(trace)
        logger.error('%s' % fail_msg)
        logger.error('error :: get_snab_algorithms :: could not get a MySQL engine to get update snab table')
        raise  # to webapp to return in the UI
    if not engine:
        trace = 'none'
        fail_msg = 'error :: get_snab_algorithms :: engine not obtained'
        logger.error(fail_msg)
        raise
    try:
        snab_table, log_msg, trace = snab_table_meta(skyline_app, engine)
        logger.info(log_msg)
        logger.info('get_snab_algorithms :: snab_table OK')
    except:
        logger.error(traceback.format_exc())
        logger.error('error :: get_snab_algorithms :: failed to get snab_table meta')
        if engine:
            snab_engine_disposal(engine)
        raise  # to webapp to return in the UI

    warnings = []
    try:
        connection = engine.connect()
        # Only include algorithms with an algorithm_id as the three-sigma (or
        # current primary algorithm has no id)
        stmt = select([snab_table.c.algorithm_id]).\
            where(snab_table.c.algorithm_id > 0)
        result = connection.execute(stmt)
        for row in result:
            try:
                algorithm_id = row['algorithm_id']
                algorithm = algorithms_by_id[algorithm_id]
                snab_algorithms[algorithm_id] = algorithm
            except Exception as err:
                warnings.append([str(dict(row)), err])
        try:
            connection.close()
        except:
            pass
    except:
        logger.error(traceback.format_exc())
        logger.error('error :: get_snab_algorithms :: to query snab table')
        if engine:
            snab_engine_disposal(engine)
        raise  # to webapp to return in the UI
    if warnings:
        logger.info('warning :: get_snab_algorithms :: failed to determine algorithm from some ids, last 3 warnings - %s' % (
            str(warnings[-3:])))
    if engine:
        snab_engine_disposal(engine)

    return snab_algorithms