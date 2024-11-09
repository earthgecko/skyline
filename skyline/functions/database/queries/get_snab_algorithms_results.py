"""
get_snab_algorithms_results.py
"""
import logging
import traceback

from sqlalchemy import select

import settings
from database import get_engine, engine_disposal, snab_results_algorithms_table_meta
from functions.database.queries.get_algorithms import get_algorithms


# @added 20230802 - Feature #5038: snab_results_algorithms
#                   Feature #5042: Add save_training_data link options to SNAB slack results message
#                   Feature #4988: Allow snab to return and save results
def get_snab_algorithms_results(current_skyline_app, snab_id=0, snab_ids=[]):
    """
    Returns a dict of the snab_algorithms_results for check.
    """
    function_str = 'database.queries.get_snab_results_algorithms'

    current_skyline_app_logger = current_skyline_app + 'Log'
    current_logger = logging.getLogger(current_skyline_app_logger)

    algorithms_results = {}

    algorithms = {}
    all_algorithms_by_id = {}
    try:
        algorithms, all_algorithms_by_id = get_algorithms(current_skyline_app, return_all_algorithms_by_id=True)
    except Exception as err:
        current_logger.error('error :: %s :: get_algorithms failed - %s' % (function_str, err))
        return algorithms_results
    all_algorithms_by_name = {}
    for id in list(all_algorithms_by_id.keys()):
        all_algorithms_by_name[all_algorithms_by_id[id]] = id

    try:
        engine, fail_msg, trace = get_engine(current_skyline_app)
    except Exception as err:
        current_logger.error('error :: %s :: could not get a MySQL engine - %s' % (function_str, err))
        return algorithms_results

    try:
        snab_results_algorithms_table, fail_msg, trace = snab_results_algorithms_table_meta(current_skyline_app, engine)
    except Exception as err:
        current_logger.error('error :: %s :: failed to get snab_results_algorithms_table meta - %s' % (
            function_str, err))
        if engine:
            engine_disposal(current_skyline_app, engine)
        return algorithms_results

    consensus_achieved = False
    if snab_id:
        try:
            algorithms_results['snab_id'] = snab_id
            algorithms_results['algorithms_results'] = {}
            connection = engine.connect()
            stmt = select(snab_results_algorithms_table).where(snab_results_algorithms_table.c.snab_id == snab_id)
            result = connection.execute(stmt)
            for row in result:
                algorithms_results_id = row['id']
                algorithm_id = row['algorithm_id']
                if row['consensus_achieved']:
                    consensus_achieved = row['consensus_achieved']
                algorithms_results['algorithms_results'][algorithms_results_id] = {
                    'algorithm_group_id': row['algorithm_group_id'],
                    'algorithm_id': algorithm_id,
                    'algorithm': all_algorithms_by_id[algorithm_id],
                    # Coerce Decimal to float for json
                    'anomalyScore': float(row['anomalyScore']),
                    'runtime': float(row['runtime']),
                    'consensus_achieved': consensus_achieved,
                }
            connection.close()
            if consensus_achieved:
                consensus_achieved_str = consensus_achieved
                consensus_achieved_ids = consensus_achieved_str.split(',')
                consensus_achieved = []
                for id in consensus_achieved_ids:
                    consensus_achieved.append(all_algorithms_by_id[int(float(id))])
                consensus_achieved = sorted(consensus_achieved)
            algorithms_results['consensus_achieved'] = consensus_achieved
        except Exception as err:
            current_logger.error(traceback.format_exc())
            current_logger.error('error :: get_snab_algorithms_results :: failed to build snab_algorithms_results for snab_id: %s - %s' % (
                str(snab_id), str(err)))

    if snab_ids:
        all_sql_results = {}
        try:
            current_logger.info('get_snab_algorithms_results :: querying snab_algorithms_results for %s snab_ids' % (
                str(len(snab_ids))))
            connection = engine.connect()
            # @modified 20230922 - 
            # Improve query performance because the WHERE IN query is inefficient
            # stmt = select([snab_results_algorithms_table], snab_results_algorithms_table.c.snab_id.in_(snab_ids))
            stmt = select([snab_results_algorithms_table]).\
                where(snab_results_algorithms_table.c.snab_id >= min(snab_ids)).\
                where(snab_results_algorithms_table.c.snab_id <= max(snab_ids))
            result = connection.execute(stmt)
            current_logger.info('get_snab_algorithms_results :: queried snab_algorithms_results')
            done_count = 0
            for row in result:
                # @modified 20230922 - 
                # Improve query performance because the WHERE IN query is inefficient
                all_sql_results[row['id']] = dict(row)
            connection.close()

            # @added 20230922 - 
            # Improve query performance because the WHERE IN query is inefficient
            snab_ids_set = set(snab_ids)
            current_logger.info('get_snab_algorithms_results :: got %s results, before filtering on %s snab_ids' % (
                str(len(all_sql_results)), str(len(snab_ids_set))))
            for key, value in all_sql_results.items():
                # membership of the list is slower than membership of a set
                # if all_sql_results[key]['anomaly_id'] in anomaly_ids:
                if not value['snab_id'] in snab_ids_set:
                    continue
                row = value

                # if not done_count % 1000:
                #     current_logger.info('get_snab_algorithms_results :: %s snab_ids processed' % (
                #         str(done_count)))
                # done_count += 1

                algorithms_results_id = row['id']
                snab_id = row['snab_id']
                if snab_id not in algorithms_results:
                    algorithms_results[snab_id] = {}
                    algorithms_results[snab_id]['algorithms_results'] = {}
                algorithm_group_id = row['algorithm_group_id']
                algorithms_results[snab_id]['algorithm_group_id'] = algorithm_group_id
                algorithm_id = row['algorithm_id']
                algorithms_results[snab_id]['algorithm_id'] = algorithm_id
                algorithm = all_algorithms_by_id[algorithm_id]
                algorithms_results[snab_id]['algorithm'] = algorithm

                consensus_achieved = False
                if row['consensus_achieved']:
                    consensus_achieved = row['consensus_achieved']
                algorithms_results[snab_id]['algorithms_results'][algorithms_results_id] = {
                    'algorithm_group_id': algorithm_group_id,
                    'algorithm_id': algorithm_id,
                    'algorithm': algorithm,
                    # Coerce Decimal to float for json
                    'anomalyScore': float(row['anomalyScore']),
                    'runtime': float(row['runtime']),
                    'consensus_achieved': consensus_achieved,
                }
                if consensus_achieved:
                    consensus_achieved_str = consensus_achieved
                    consensus_achieved_ids = consensus_achieved_str.split(',')
                    consensus_achieved = []
                    for id in consensus_achieved_ids:
                        consensus_achieved.append(all_algorithms_by_id[int(float(id))])
                    consensus_achieved = sorted(consensus_achieved)
                algorithms_results[snab_id]['consensus_achieved'] = consensus_achieved
            # connection.close()
            current_logger.info('get_snab_algorithms_results :: %s algorithms_results determined' % (
                str(len(algorithms_results))))
        except Exception as err:
            current_logger.error(traceback.format_exc())
            current_logger.error('error :: get_snab_algorithms_results :: failed to build snab_algorithms_results for %s snab_ids - %s' % (
                str(len(snab_ids)), str(err)))

    if engine:
        engine_disposal(current_skyline_app, engine)

    return algorithms_results
