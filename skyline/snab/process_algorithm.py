"""
process_algorithm.py
"""
import copy
import glob
import gzip
import logging
import os
import shutil
import traceback

from ast import literal_eval

import pandas as pd
from sqlalchemy.sql import select
from sqlalchemy import or_

from database import (
    get_engine, snab_table_meta, metrics_table_meta, anomalies_table_meta)
from functions.database.queries.get_algorithms import get_algorithms
from functions.database.queries.get_all_db_metric_names import get_all_db_metric_names
from functions.database.queries.get_snab_result import get_snab_result
from functions.database.queries.insert_new_algorithm import insert_new_algorithm
from functions.database.queries.insert_snab_results_algorithms import insert_snab_results_algorithms
from functions.database.queries.query_anomalies import get_anomaly
from functions.metrics.get_base_name_from_metric_id import get_base_name_from_metric_id
from functions.timeseries.determine_data_frequency import determine_data_frequency
from functions.timeseries.downsample import downsample_timeseries
from functions.timeseries.strictly_increasing_monotonicity import strictly_increasing_monotonicity
from functions.timeseries.load_timeseries_csv import load_timeseries_csv
from settings import SLACK_OPTS, IONOSPHERE_DATA_FOLDER, IONOSPHERE_PROFILES_FOLDER
from slack_functions import slack_post_message
from skyline_functions import (
    get_redis_conn_decoded, get_graphite_metric, nonNegativeDerivative, mkdir_p)
# @added 20250121 - Feature #5588: snab.process_algorithm
from functions.plots.plot_timeseries import plot_timeseries
# @added 20250128 - Feature #5588: snab.process_algorithm
from functions.timeseries.load_timeseries_json import load_timeseries_json

skyline_app = 'snab'
skyline_app_logger = '%sLog' % skyline_app
logger = logging.getLogger(skyline_app_logger)

# @added 20251006 - Feature #5588: snab.process_algorithm
# THIS PROCESS is a reprocess algorithm process, not a process new algorithm
# process as was found when testing. It will reprocess snab_ids and update
# results for the original algorithm in the DB.
# @modified 20260809 Feature #5588: snab.process_algorithm
# IT NOW ONLY processes new algorithms and does not update the results of the
# original snab algorithm, which was a bug

# @added 20250107 - Feature #5588: snab.process_algorithm
def get_process_algorithm_work(number_to_get):
    """
    Return entries from the snab.process_algorithm_work Redis set as
    sorted_snab_work.


    """

    function_str = 'snab.process_algorithm.get_process_algorithm_work'
    sorted_snab_work = []
    try:
        redis_conn_decoded = get_redis_conn_decoded(skyline_app)
    except Exception as err:
        logger.error('error :: %s :: get_redis_conn_decoded failed for, err: %s' % (
            function_str, err))
    try:
        sorted_snab_work = list(redis_conn_decoded.smembers('snab.process_algorithm_work'))
    except Exception as err:
        logger.error('error :: %s :: smembers failed on snab.process_algorithm_work, err: %s' % (
            function_str, err))
    if sorted_snab_work:
        logger.info('%s :: %s work items snab.process_algorithm_work' % (
            function_str, str(len(sorted_snab_work))))
        if number_to_get == 1:
            sorted_snab_work = [literal_eval(sorted_snab_work[0])]
        else:
            for item in sorted_snab_work[0:(number_to_get - 1)]:
                sorted_snab_work.append(literal_eval(item))
        logger.info('%s :: adding %s work items to sorted_snab_work' % (
            function_str, str(len(sorted_snab_work))))
        for item in sorted_snab_work:
            try:
                redis_conn_decoded.srem('snab.process_algorithm_work', str(item))
            except Exception as err:
                logger.error('error :: %s :: srem failed on snab.process_algorithm_work for %s, err: %s' % (
                    function_str, str(item), err))

    return sorted_snab_work


# @added 20250107 - Feature #5588: snab.process_algorithm
def add_algorithm_work():

    function_str = 'snab.process_algorithm.add_algorithm_work'
    process_algorithm_work = {}
    try:
        redis_conn_decoded = get_redis_conn_decoded(skyline_app)
    except Exception as err:
        logger.error('error :: %s :: get_redis_conn_decoded failed for, err: %s' % (
            function_str, err))
    snab_process_algorithm_request = {}
    try:
        snab_process_algorithm_request = redis_conn_decoded.get('webapp.snab.process_algorithm.request')
    except Exception as err:
        logger.error('error :: %s :: get failed for webapp.snab.process_algorithm.request, err: %s' % (
            function_str, err))
    snab_process_algorithm = {}
    if snab_process_algorithm_request:
        try:
            snab_process_algorithm = literal_eval(snab_process_algorithm_request)
        except Exception as err:
            logger.error('error :: %s :: literal_eval failed on webapp.snab.process_algorithm.request data, err: %s' % (
                function_str, err))
    if not snab_process_algorithm:
        return process_algorithm_work

    logger.info('%s :: processing snab_process_algorithm: %s' % (
        function_str, str(snab_process_algorithm)))

    try:
        redis_conn_decoded.delete('webapp.snab.process_algorithm.request')
    except Exception as err:
        logger.error('error :: %s :: delete failed on webapp.snab.process_algorithm.request, err: %s' % (
            function_str, err))

    try:
        algorithm = snab_process_algorithm['algorithm']
        algorithm_source = snab_process_algorithm['algorithm_source']
        algorithm_parameters = snab_process_algorithm['algorithm_parameters']
        if isinstance(algorithm_parameters, str):
            try:
                algorithm_parameters = literal_eval(algorithm_parameters)
            except Exception as err:
                logger.error('error :: %s :: literal_eval failed on algorithm_parameters data, err: %s' % (
                    function_str, err))
        snab_algorithm_group_id = int(snab_process_algorithm['snab_algorithm_group_id'])
        limit = int(snab_process_algorithm['limit'])
        added_at = int(snab_process_algorithm['added_at'])
    except Exception as err:
        logger.error('error :: %s :: failed to determine item from snab_process_algorithm, err: %s' % (
            function_str, err))
        return process_algorithm_work

    # @added 20250128 - Feature #5588: snab.process_algorithm
    data_only = False
    try:
       data_only = snab_process_algorithm['data_only']
    except:
        data_only = False
    snab_dataset_id = None
    try:
        snab_dataset_id = snab_process_algorithm['snab_dataset_id']
    except KeyError:
        snab_dataset_id = None

    try:
        slack_enabled = len(SLACK_OPTS['bot_user_oauth_access_token'])
    except:
        slack_enabled = False
    if slack_enabled:
        try:
            alert_slack_channel = None
            slack_message = '*Skyline - NOTICE* - snab_process_algorithm called for %s with on snab_algorithm_group_id: %s evaluations with limit: %s' % (
                str(algorithm), str(snab_algorithm_group_id), str(limit))
            slack_post = slack_post_message(skyline_app, alert_slack_channel, None, slack_message)
            logger.info('%s :: posted notice to slack - %s' % (
                function_str, slack_message))
        except Exception as err:
            logger.error('error :: %s :: slack_post_message failed - %s' % (
                function_str, err))

    algorithm_id = 0
    algorithm_group_id = 0
    try:
        algorithm_id, algorithm_group_id = insert_new_algorithm(skyline_app, algorithm)
    except Exception as err:
        logger.error('error :: %s :: insert_new_algorithm failed on algorithm: %s, err: %s' % (
            function_str, algorithm, err))

    try:
        engine, fail_msg, trace = get_engine(skyline_app)
    except Exception as err:
        logger.error('error :: %s :: get_engine failed, err: %s' % (
            function_str, err))
        return process_algorithm_work

    try:
        snab_table, log_msg, trace = snab_table_meta(skyline_app, engine)
    except Exception as err:
        logger.error('error :: %s :: failed to get snab_table meta, err: %s' % (
            function_str, err))
        if engine:
            try:
                engine.dispose()
            except Exception as err2:
                logger.error('error :: %s :: calling engine.dispose(), err: %s' % (
                    function_str, err2))
        return process_algorithm_work

    warnings = []
    existing_algorithm_snab_ids = []
    try:
        #connection = engine.connect()
        # @modified 20260225 - Task #5176: Migrate to sqlalchemy v2 API
        #                      Task #5628: Build v5.0.0 and test
        #stmt = select([snab_table.c.id]).where((snab_table.c.algorithm_id == algorithm_id))
        stmt = select(snab_table.c.id).where(snab_table.c.algorithm_id == algorithm_id)

        # @modified 20260227 - Task #5176: Migrate to sqlalchemy v2 API
        #                      Task #5628: Build v5.0.0 and test
        #result = connection.execute(stmt)
        #for row in result:
        with engine.connect() as connection:
            result = connection.execute(stmt)
            results = [dict(row._mapping) for row in result.fetchall()]
        for row in results:
            try:
                snab_id = row['id']
                existing_algorithm_snab_ids.append(snab_id)
            except Exception as err:
                warnings.append([str(dict(row)), err])
        #try:
        #    connection.close()
        #except:
        #    pass
    except Exception as err:
        logger.error('error :: %s :: failed querying snab_table data, err: %s' % (
            function_str, err))

    snab_evaluations_dict = {}
    warnings = []
    skipped = []
    try:
        #connection = engine.connect()
        # @modified 20260225 - Task #5176: Migrate to sqlalchemy v2 API
        #                      Task #5628: Build v5.0.0 and test
        #stmt = select([snab_table]).\
        stmt = select(snab_table).\
            where(
                (snab_table.c.algorithm_group_id == snab_algorithm_group_id) &
                or_(
                    snab_table.c.tP.isnot(None),
                    snab_table.c.fP.isnot(None),
                    snab_table.c.tN.isnot(None),
                    snab_table.c.fN.isnot(None)
                )
            )
        # @modified 20260227 - Task #5176: Migrate to sqlalchemy v2 API
        #                      Task #5628: Build v5.0.0 and test
        #result = connection.execute(stmt)
        #for row in result:
        with engine.connect() as connection:
            result = connection.execute(stmt)
            results = [dict(row._mapping) for row in result.fetchall()]
        for row in results:
            try:
                snab_id = row['id']
                if snab_id in existing_algorithm_snab_ids:
                    continue
                row_dict = dict(row)
                # Verify all states are None because the above query at times
                # returns rows that have no evaluation.
                states = ['tP', 'fP', 'tN', 'fN']
                all_Nones = True
                for state in states:
                    if row_dict[state] is not None:
                        all_Nones = False
                if all_Nones:
                    skipped.append(snab_id)
                    continue
                snab_evaluations_dict[snab_id] = dict(row)
            except Exception as err:
                warnings.append([str(dict(row)), err])
        #try:
        #    connection.close()
        #except:
        #    pass
    except Exception as err:
        logger.error('error :: %s :: failed querying snab_table data, err: %s' % (
            function_str, err))
        if engine:
            try:
                engine.dispose()
            except Exception as err2:
                logger.error('error :: %s :: calling engine.dispose(), err: %s' % (
                    function_str, err2))
        return process_algorithm_work
    if skipped:
        logger.info('%s :: skipped %s snab_ids where evaluations where all None' % (
            function_str, str(len(skipped))))

    if engine:
        try:
            engine.dispose()
        except Exception as err:
            logger.error('error :: %s :: calling engine.dispose() after query, err: %s' % (
                function_str, err))

    if algorithm_source == 'default':
        algorithm_source = '/opt/skyline/github/skyline/skyline/custom_algorithms/%s.py' % algorithm
    if not os.path.isfile(algorithm_source):
        skyline_algorithm_source = '/opt/skyline/github/skyline/skyline/custom_algorithms/skyline_%s.py' % algorithm
        if not os.path.isfile(skyline_algorithm_source):
            logger.error('error :: %s :: the algorithm_source was not found at %s or %s' % (
                function_str, algorithm_source, skyline_algorithm_source))
            return process_algorithm_work
        else:
            algorithm_source = skyline_algorithm_source

    metric_ids_with_base_names = {}
    try:
        metric_ids_with_base_names = redis_conn_decoded.hgetall('aet.metrics_manager.ids_with_metric_names')
    except Exception as err:
        logger.error('error :: %s :: hgetall failed on aet.metrics_manager.ids_with_metric_names, err: %s' % (
            function_str, err))
    all_fetched_metric_ids = []
    if metric_ids_with_base_names:
        metric_ids_with_base_names = {int(k): v for k, v in metric_ids_with_base_names.items()}
        all_fetched_metric_ids = set(list(metric_ids_with_base_names.keys()))
    with_ids = True
    try:
        metric_names, metric_names_with_ids = get_all_db_metric_names(skyline_app, with_ids)
    except Exception as err:
        logger.error('error :: %s :: get_all_active_db_metric_names failed, err: %s' % (
            function_str, err))
    for metric, metric_id in metric_names_with_ids.items():
        if int(metric_id) not in all_fetched_metric_ids:
            metric_ids_with_base_names[int(metric_id)] = metric

    known_algorithms = {}
    try:
        known_algorithms = get_algorithms(skyline_app)
        # known_algorithms dict example
        # {'histogram_bins': 1, 'first_hour_average': 2, ..., 'irregular_unstable': 253}
    except Exception as err:
        logger.error('error :: %s :: get_algorithms failed - %s' % (
            function_str, str(err)))

    added_anomaly_ids = []
    redis_set_data = []
    for snab_id, snab_dict in snab_evaluations_dict.items():
        if snab_dict['algorithm_group_id'] != snab_algorithm_group_id:
            continue
        if limit:
            if len(redis_set_data) == limit:
                logger.info('%s :: limiting to %s work items' % (
                    function_str, str(limit)))
                break
        work_dict = {}
        anomaly_id = 0
        try:
            anomaly_id = int(snab_dict['anomaly_id'])
        except Exception as err:
            logger.error('error :: %s :: failed to determine anomaly_id for snab_id: %s, err: %s' % (
                function_str, str(snab_id), err))
            continue

        # Early versions of SNAB data added multiple evaluations for an
        # algorithm at times.
        if anomaly_id in added_anomaly_ids:
            continue

        snab_results = {}
        try:
            snab_results = get_snab_result(skyline_app, anomaly_id)
        except Exception as err:
            logger.error('error :: %s :: get_snab_result failed for anomaly_id %s - %s' % (
                function_str, str(anomaly_id), str(err)))

        algorithm_id = 0
        try:
            algorithm_id = known_algorithms[algorithm]
        except KeyError:
            logger.info('warning :: %s :: %s not found in known_algorithms' % (
                function_str, algorithm))
            algorithm_id = 0
        snab_results_ids = list(snab_results.keys())
        for i_snab_id in snab_results_ids:
            if snab_results[i_snab_id]['algorithm_group_id'] != snab_algorithm_group_id:
                del snab_results[i_snab_id]
                continue
            states = ['tP', 'fP', 'tN', 'fN']
            all_Nones = True
            for state in states:
                if snab_results[i_snab_id][state] is not None:
                    all_Nones = False
            if all_Nones:
                del snab_results[i_snab_id]
                continue

        if len(snab_results) > 1:
            logger.error('error :: %s :: snab_results has more than one entry, snab_results: %s' % (
                function_str, str(snab_results)))
        snab_result = {}
        if snab_results:
            snab_results_id = list(snab_results.keys())[0]
            snab_result = copy.deepcopy(snab_results[snab_results_id])

        anomaly = {}
        try:
            anomaly = get_anomaly(skyline_app, anomaly_id)
        except Exception as err:
            logger.error('error :: %s :: get_anomaly failed with anomaly_id: %s, err: %s' % (
                function_str, str(anomaly_id), err))
            continue

        try:
            # Discard any snab evaluations that are not from Mirage as they were
            # based on Redis data
            if anomaly['full_duration'] < (86400 * 5):
                continue
            metric_id = anomaly['metric_id']
            metric = None
            try:
                metric = metric_ids_with_base_names[metric_id]
            except:
                metric = None
            if not metric:
                try:
                    metric = get_base_name_from_metric_id(skyline_app, metric_id)
                except Exception as err:
                    logger.error('error :: %s :: get_base_name_from_metric_id failed to determine base_name for metric_id: %s, err: %s' % (
                        function_str, str(metric_id), str(err)))
                    continue
                if metric:
                    metric_ids_with_base_names[metric_id] = metric
            work_dict['snab_algorithm_group_id'] = snab_algorithm_group_id
            work_dict['original_snab_result'] = copy.deepcopy(snab_result)
            work_dict['metric_id'] = metric_id
            work_dict['metric'] = metric
            work_dict['labelled_metric_name'] = None
            if '_tenant_id=' in metric:
                # Any SNAB evaluations made on labelled_metrics are skipped as
                # it is probable that the data that was analysed has been pruned
                continue
            work_dict['full_duration'] = int(anomaly['full_duration'])
            work_dict['anomalous'] = None
            work_dict['anomaly_id'] = anomaly_id
            work_dict['timestamp'] = int(anomaly['anomaly_timestamp'])
            work_dict['original_anomaly_timestamp'] = int(anomaly['anomaly_timestamp'])
            work_dict['value'] = int(anomaly['anomalous_datapoint'])
            work_dict['full_duration'] = int(anomaly['full_duration'])
            work_dict['snab_only'] = True
            work_dict['snab_process_algorithm'] = True
            work_dict['source'] = 'webapp'
            work_dict['alert_slack_channel'] = None
            if algorithm_id:
                work_dict['algorithm_id'] = algorithm_id
            if algorithm_group_id:
                work_dict['algorithm_group_id'] = algorithm_group_id
            work_dict['algorithm'] = algorithm
            work_dict['algorithm_source'] = algorithm_source
            work_dict['algorithm_parameters'] = copy.deepcopy(algorithm_parameters)
            work_dict['max_execution_time'] = 50
            work_dict['debug_logging'] = True
            work_dict['anomaly_data'] = None
            work_dict['context'] = 'testing'
            work_dict['processed'] = False
            work_dict['analysed'] = False
            # @added 20250128 - Feature #5588: snab.process_algorithm
            work_dict['data_only'] = data_only
            work_dict['snab_dataset_id'] = snab_dataset_id

        except Exception as err:
            logger.error('error :: %s :: failed to interpolate work_dict items for determine base_name for snab_id: %s, err: %s' % (
                function_str, str(snab_id), str(err)))
            break
        redis_set_data.append(str(work_dict))
        added_anomaly_ids.append(anomaly_id)
        process_algorithm_work[snab_id] = work_dict
    logger.info('%s :: adding %s work items to snab.process_algorithm_work' % (
        function_str, str(len(redis_set_data))))
    added = 0
    try:
        added = redis_conn_decoded.sadd('snab.process_algorithm_work', *set(redis_set_data))
    except Exception as err:
        logger.error('error :: %s :: failed to sadd to snab.process_algorithm_work, err: %s' % (
            function_str, str(err)))
        return {}
    if slack_enabled:
        try:
            alert_slack_channel = None
            slack_message = '*Skyline - NOTICE* - snab_process_algorithm added %s evalauted anomalies to analysis with %s' % (
                str(added), str(algorithm))
            slack_post = slack_post_message(skyline_app, alert_slack_channel, None, slack_message)
            if slack_post:
                logger.info('%s :: posted notice to slack - %s' % (
                    function_str, slack_message))
        except Exception as err:
            logger.error('error :: %s :: slack_post_message failed - %s' % (
                function_str, err))

    return process_algorithm_work


# @added 20250121 - Feature #5588: snab.process_algorithm
def create_graph_png(graph_image, timeseries, check_details):
    function_str = 'snab.process_algorithm.create_graph_png'
    plotted = False
    output_file = None
    if not os.path.isfile(graph_image):
        metric = str(check_details['metric'])
        title = title = '%s.%s.%s' % (
            str(check_details['metric_id']),
            str(check_details['anomaly_id']),
            str(check_details['original_anomaly_timestamp']))
        line_color = 'blue'
        if 'original_snab_result' in check_details:
            try:
                original_snab_result = check_details['original_snab_result']
                if original_snab_result['tP']:
                    line_color = 'red'
                if original_snab_result['fN']:
                    line_color = 'red'
            except:
                pass
        plot_parameters = {
            'title': title, 'line_color': line_color, 'bg_color': 'white',
            'figsize': (7, 3), 'use_label': title, 'linewidth': 0.5,
            'plot_legend': False,
        }
        try:
            plotted, output_file = plot_timeseries(
                    skyline_app, metric, timeseries, graph_image,
                    plot_parameters=plot_parameters)
            if output_file:
                logger.info('%s :: created graph png: %s' % (
                    function_str, graph_image))
        except Exception as err:
            logger.error('error :: %s :: failed to create graph png: %s, err: %s' % (
                function_str, graph_image, err))
        return plotted

# @added 20250121 - Feature #5588: snab.process_algorithm
def make_label_file(csv_file, check_details):
    function_str = 'snab.process_algorithm.make_label_file'
    label = None
    label_file = None
    if 'original_snab_result' in check_details:
        try:
            original_snab_result = check_details['original_snab_result']
            if original_snab_result['tP']:
                label = 'anomaly'
            if original_snab_result['fN']:
                label = 'normal'
            if original_snab_result['fP']:
                label = 'normal'
            if original_snab_result['fN']:
                label = 'anomaly'
        except:
            pass
        if label:
            label_file = '%s/%s.txt' % (os.path.dirname(csv_file), label)
            with open(label_file, 'a') as f:
                f.write('True')
            os.chmod(label_file, mode=0o644)
    return label_file

# @added 20250128 - Feature #5588: snab.process_algorithm
def make_snab_dataset(csv_file, snab_dataset_id):
    function_str = 'snab.process_algorithm.make_snab_dataset'
    csv_dir = os.path.dirname(csv_file)
    replacement_dirs = 'snab_datasets/%s' % str(snab_dataset_id)
    snab_dataset_anomaly_data_dir = csv_dir.replace('evaluated_datasets', replacement_dirs)
    # Already in snab_datasets folder
    copied = 0
    if replacement_dirs in csv_file:
        return copied
    if not os.path.isdir(snab_dataset_anomaly_data_dir):
        mkdir_p(snab_dataset_anomaly_data_dir)
    data_files = []
    try:
        glob_path = '%s/*.*' % csv_dir
        data_files = glob.glob(glob_path)
    except Exception as err:
        logger.error('error :: %s :: glob failed on %s, err: %s' % (
            function_str, str(glob_path), err))
    for i_file in data_files:
        try:
            shutil.copy(i_file, snab_dataset_anomaly_data_dir)
            copied += 1
        except Exception as err:
            logger.error('error :: %s :: shutil failed to copy %s to %s, err: %s' % (
                function_str, str(i_file), snab_dataset_anomaly_data_dir,
                err))
    return copied

def get_snab_process_algorithm_data(anomaly_data, check_details):
    """
    Create the SNAB evaluate_datasets data if it does not exist.  Return the
    anomaly_data path and filename and the timeseries

    """
    function_str = 'snab.process_algorithm.create_snab_process_algorithm_data'
    anomaly_data_file = None
    timeseries = []
    is_strictly_increasing_monotonically = False

    csv_file = str(anomaly_data)
    if anomaly_data.endswith('.gz'):
        csv_file = anomaly_data.replace('.gz', '')
    graph_image = csv_file.replace('.csv', '.png')
    # @added 20250128 - Feature #5588: snab.process_algorithm
    snab_dataset_id = None
    try:
        snab_dataset_id = check_details['snab_dataset_id']
    except KeyError:
        snab_dataset_id = None

    if os.path.isfile(anomaly_data):
        if anomaly_data.endswith('.csv') or anomaly_data.endswith('.csv.gz'):
            try:
                timeseries = load_timeseries_csv(skyline_app, anomaly_data)
            except Exception as err:
                logger.error('error :: %s :: load_timeseries_csv failed to load timeseries from %s, err: %s' % (
                    function_str, anomaly_data, str(err)))
            # @added 20250121 - Feature #5588: snab.process_algorithm
            if not os.path.isfile(graph_image):
                try:
                    plotted = create_graph_png(graph_image, timeseries, check_details)
                except Exception as err:
                    logger.error('error :: %s :: create_graph_png failed for graph_image: %s, err: %s' % (
                        function_str, graph_image, str(err)))
        # @added 20250128 - Feature #5588: snab.process_algorithm
        try:
            label_file = make_label_file(csv_file, check_details)
        except Exception as err:
            logger.error('error :: %s :: make_label_file failed, err: %s' % (
                function_str, str(err)))
        # @added 20250128 - Feature #5588: snab.process_algorithm
        if snab_dataset_id:
            try:
                copied = make_snab_dataset(csv_file, snab_dataset_id)
            except Exception as err:
                logger.error('error :: %s :: make_snab_dataset failed for snab_dataset_id: %s with csv_file: %s, err: %s' % (
                    function_str, str(snab_dataset_id), csv_file, str(err)))

        return anomaly_data, timeseries

    metric = str(check_details['metric'])
    until_timestamp = int(check_details['original_anomaly_timestamp'])

    # Check if ionosphere data exists
    metric = str(check_details['metric'])
    until_timestamp = int(check_details['original_anomaly_timestamp'])
    labelled_metric_name = None
    try:
        labelled_metric_name = check_details['labelled_metric_name']
    except:
        labelled_metric_name = None
    metric_name = str(metric)
    if labelled_metric_name:
        metric_name = str(metric)
    timeseries_dir = metric_name.replace('.', '/')
    features_profile_dir = '%s/%s/%s' % (
        IONOSPHERE_PROFILES_FOLDER, timeseries_dir, str(until_timestamp))
    # /opt/skyline/ionosphere/features_profiles/stats/server-2/procs/waiting/1689226561/
    fp_downsampled_json_file = '%s/%s.downsampled.json' % (features_profile_dir, metric_name)
    if os.path.isfile(fp_downsampled_json_file):
        try:
            timeseries = load_timeseries_json(skyline_app, fp_downsampled_json_file)
            logger.info('%s :: using: fp_downsampled_json_file: %s' % (
                function_str, fp_downsampled_json_file))
        except Exception as err:
            logger.error('error :: %s :: load_timeseries_json failed on %s, err: %s' % (
                function_str, fp_downsampled_json_file, err))
    fp_json_file = '%s/%s.json' % (features_profile_dir, metric_name)
    if os.path.isfile(fp_json_file) and not timeseries:
        try:
            timeseries = load_timeseries_json(skyline_app, fp_json_file)
            logger.info('%s :: using: fp_json_file: %s' % (
                function_str, fp_json_file))
        except Exception as err:
            logger.error('error :: %s :: load_timeseries_json failed on %s, err: %s' % (
                function_str, fp_json_file, err))

    # /opt/skyline/ionosphere/data_saved/1696342249/stats/server-2/eth0/txPackets/stats.server-2.eth0.txPackets.txt
    ionosphere_data_saved_dir = '%s_saved/%s/%s' % (
        IONOSPHERE_DATA_FOLDER,
        str(until_timestamp), timeseries_dir)
    data_saved_downsampled_json_file = '%s/%s.downsampled.json' % (ionosphere_data_saved_dir, metric_name)
    if os.path.isfile(data_saved_downsampled_json_file) and not timeseries:
        try:
            timeseries = load_timeseries_json(skyline_app, data_saved_downsampled_json_file)
            logger.info('%s :: using: data_saved_downsampled_json_file: %s' % (
                function_str, data_saved_downsampled_json_file))
        except Exception as err:
            logger.error('error :: %s :: load_timeseries_json failed on %s, err: %s' % (
                function_str, data_saved_downsampled_json_file, err))
    data_saved_json_file = '%s/%s.json' % (ionosphere_data_saved_dir, metric_name)
    if os.path.isfile(data_saved_json_file) and not timeseries:
        try:
            timeseries = load_timeseries_json(skyline_app, data_saved_json_file)
            logger.info('%s :: using data_saved_json_file: %s' % (
                function_str, data_saved_json_file))
        except Exception as err:
            logger.error('error :: %s :: load_timeseries_json failed on %s, err: %s' % (
                function_str, data_saved_json_file, err))

    full_duration = int(check_details['full_duration'])
    from_timestamp = until_timestamp - full_duration

    if not timeseries:
        try:
            timeseries = get_graphite_metric(
                skyline_app, metric, from_timestamp, until_timestamp, 'list',
                'object')
            logger.info('%s :: using data from Graphite for %s' % (
                function_str, metric))
        except Exception as err:
            logger.error('error :: %s :: get_graphite_metric failed to get time series metric: %s, from_timestamp: %s, until_timestamp: %s, err: %s' % (
                function_str, metric, str(from_timestamp), str(until_timestamp),
                str(err)))
    if timeseries:
        # @added 20250121 - Feature #5588: snab.process_algorithm
        # Check monotonicity as the metric may no longer exist and will not have
        # had nonNegativeDerivate applied
        try:
            is_strictly_increasing_monotonically = strictly_increasing_monotonicity(timeseries)
        except Exception as err:
            logger.error('error :: %s :: is_strictly_increasing_monotonically failed on time series metric: %s, from_timestamp: %s, until_timestamp: %s, err: %s' % (
                function_str, metric, str(from_timestamp), str(until_timestamp),
                str(err)))
        if is_strictly_increasing_monotonically:
            try:
                timeseries = nonNegativeDerivative(timeseries)
            except Exception as err:
                logger.error('error :: %s :: nonNegativeDerivative failed on time series metric: %s, from_timestamp: %s, until_timestamp: %s, err: %s' % (
                    function_str, metric, str(from_timestamp), str(until_timestamp),
                    str(err)))

        if len(timeseries) < 800:
            logger.info('%s :: insufficient data in time series for metric: %s, len(timeseries): %s' % (
                function_str, metric, len(timeseries)))
            timeseries = []
            return anomaly_data, timeseries
        if len(timeseries) > 2000:
            resolution = 0
            try:
                resolution = determine_data_frequency(skyline_app, timeseries, False)
            except Exception as err:
                logger.error('error :: %s :: determine_data_frequency failed, err: %s' % (
                    function_str, err))
                logger.error('error :: %s :: determine_data_frequency failed on timeseries for metric: %s, from_timestamp: %s, until_timestamp: %s, err: %s' % (
                    function_str, metric, str(from_timestamp), str(until_timestamp),
                    str(err)))
            if resolution < 600:
                logger.info('%s :: downsampling timeseries data from %s to 600 second resolution for metric: %s' % (
                    function_str, str(resolution), metric))
                downsampled_timeseries = []
                try:
                    downsampled_timeseries = downsample_timeseries(skyline_app, timeseries, resolution, 600, 'mean', 'end')
                except Exception as err:
                    logger.error('error :: %s :: determine_data_frequency failed on timeseries for metric: %s, from_timestamp: %s, until_timestamp: %s, err: %s' % (
                        function_str, metric, str(from_timestamp), str(until_timestamp),
                        str(err)))
                if downsampled_timeseries:
                    logger.info('%s :: downsampled from %s to %s data points for metric: %s' % (
                        function_str, str(len(timeseries)), str(len(downsampled_timeseries)),
                        metric))
                    timeseries = list(downsampled_timeseries)

        try:
            df = pd.DataFrame(timeseries, columns=['timestamp','value'])
            df.to_csv(csv_file, index=False)
            logger.info('%s :: created %s for metric: %s' % (
                function_str, csv_file, metric))
        except Exception as err:
            logger.error('error :: %s :: failed to write timeseries to %s for metric: %s, err: %s' % (
                function_str, csv_file, metric, err))
            return anomaly_data, timeseries
        if os.path.isfile(csv_file):
            anomaly_data_file = str(csv_file)
            anomaly_data_gz = '%s.gz' % csv_file
            try:
                f_in = open(csv_file)
                f_out = gzip.open(anomaly_data_gz, 'wt')
                f_out.writelines(f_in)
                f_out.close()
                f_in.close()
                os.chmod(anomaly_data_gz, mode=0o644)
            except Exception as err:
                logger.error('error :: %s :: failed to gzip %s, err: %s' % (
                    function_str, anomaly_data, err))
            if os.path.isfile(anomaly_data_gz):
                logger.info('gzipped - %s' % anomaly_data_gz)
                anomaly_data_file = str(anomaly_data_gz)
                if os.path.isfile(csv_file):
                    try:
                        os.remove(csv_file)
                    except OSError:
                        pass

        # @added 20250121 - Feature #5588: snab.process_algorithm
        # Add label and graph
        try:
            label_file = make_label_file(csv_file, check_details)
        except Exception as err:
            logger.error('error :: %s :: make_label_file failed, err: %s' % (
                function_str, str(err)))
        if not os.path.isfile(graph_image):
            try:
                plotted = create_graph_png(graph_image, timeseries, check_details)
                if plotted:
                    logger.info('%s :: created graph_image: %s' % (
                        function_str, graph_image))
            except Exception as err:
                logger.error('error :: %s :: create_graph_png failed for graph_image: %s, err: %s' % (
                    function_str, graph_image, str(err)))

        # @added 20250128 - Feature #5588: snab.process_algorithm
        if snab_dataset_id:
            try:
                copied = make_snab_dataset(csv_file, snab_dataset_id)
            except Exception as err:
                logger.error('error :: %s :: make_snab_dataset failed for snab_dataset_id: %s with csv_file: %s, err: %s' % (
                    function_str, str(snab_dataset_id), csv_file, str(err)))

    return anomaly_data_file, timeseries

def update_snab_result(
        snab_id, anomaly_id, anomalous, original_snab_result, data_relabelled):
    """
    Update the relevant field in the snab table.

    :param snab_id: the snab table id
    :param anomaly_id: the anomaly id
    :param snab_result: a selected result
    :param original_snab_result: the original snab_result dictionary
    :type snab_id: int
    :type anomaly_id: int
    :type result: str
    :type original_snab_result: dict
    :return: snab_result_updated
    :rtype: boolean

    """
    function_str = 'update_snab_result'
    snab_result_updated = False
    original_result = None
    original_anomalous = None
    original_snab_id = None

    logger.info('update_snab_result :: for snab id: %s, anomaly id: %s, anomalous: %s' % (
            str(snab_id), str(anomaly_id), str(anomalous)))
    try:
        original_snab_id = original_snab_result['id']
        if original_snab_result['tP']:
            original_result = 'tP'
            original_anomalous = True
        if original_snab_result['fP']:
            original_result = 'fP'
            original_anomalous = False
        if original_snab_result['tN']:
            original_result = 'tN'
            original_anomalous = False
        if original_snab_result['fN']:
            original_result = 'fN'
            original_anomalous = True
    except Exception as err:
        logger.error('error :: update_snab_result :: failed to determine values from original_snab_result, err: %s' % err)
    logger.info('update_snab_result :: snab original_result: %s' % str(original_result))

    if data_relabelled:
        changed = True
        if original_anomalous and data_relabelled == 'anomaly':
            # Do nothing
            changed = False
        if not original_anomalous and data_relabelled == 'normal':
            # Do nothing
            changed = False
        if original_anomalous and data_relabelled == 'normal':
            original_anomalous = False
            original_result = 'tN'
        if not original_anomalous and data_relabelled == 'anomaly':
            original_anomalous = True
            original_result = 'tP'

    algorithm_result = None
    if original_anomalous is not None:
        if original_result == 'tP' and anomalous:
            algorithm_result = 'tP'
        if original_result == 'tP' and not anomalous:
            algorithm_result = 'fN'
        if original_result == 'fP' and anomalous:
            algorithm_result = 'fP'
        if original_result == 'fP' and not anomalous:
            algorithm_result = 'tN'
        if original_result == 'tN' and anomalous:
            algorithm_result = 'fP'
        if original_result == 'tN' and not anomalous:
            algorithm_result = 'tN'
        if original_result == 'fN' and anomalous:
            algorithm_result = 'tP'
        if original_result == 'fN' and not anomalous:
            algorithm_result = 'fN'
    logger.info('update_snab_result :: algorithm_result: %s' % str(algorithm_result))
    if algorithm_result is None:
        logger.error('error :: update_snab_result :: failed to determine algorithm_result for snab_id: %s' % (
            str(snab_id)))
        return snab_result_updated

    try:
        engine, fail_msg, trace = get_engine(skyline_app)
    except Exception as err:
        logger.error('error :: %s :: get_engine failed, err: %s' % (
            function_str, err))
        return snab_result_updated

    try:
        snab_table, log_msg, trace = snab_table_meta(skyline_app, engine)
    except Exception as err:
        logger.error('error :: %s :: failed to get snab_table meta, err: %s' % (
            function_str, err))
        if engine:
            try:
                engine.dispose()
            except Exception as err2:
                logger.error('error :: %s :: calling engine.dispose(), err: %s' % (
                    function_str, err2))
        return snab_result_updated

    snab_result = str(algorithm_result)
    try:
        #connection = engine.connect()
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
        if snab_result == 'NULL':
            stmt = snab_table.update().\
                values(tP=None, fP=None, tN=None, fN=None, unsure=None).\
                where(snab_table.c.id == int(snab_id)).\
                where(snab_table.c.anomaly_id == int(anomaly_id))

        # @modified 20260227 - Task #5176: Migrate to sqlalchemy v2 API
        #                      Task #5628: Build v5.0.0 and test
        #connection.execute(stmt)
        #connection.close()
        with engine.begin() as connection:
            connection.execute(stmt)

        snab_result_updated = True
        logger.info('update_snab_result :: updated result for snab id %s with anomaly id %s and result %s' % (
            str(snab_id), str(anomaly_id), str(snab_result)))
    except Exception as err:
        trace = traceback.format_exc()
        logger.error(trace)
        logger.error('error :: update_snab_result :: could not update result for snab id %s with anomaly id %s and result %s, err: %s' % (
            str(snab_id), str(anomaly_id), str(snab_result), err))
    if engine:
        try:
            engine.dispose()
        except Exception as err:
            logger.error('error :: update_snab_result :: engine.disposal failed, err: %s' % (
                err))
    return snab_result_updated
