"""
insert_snab_results_algorithms.py
"""
import logging
import traceback

from sqlalchemy import select

import settings
from database import get_engine, engine_disposal, snab_results_algorithms_table_meta


# @added 20230801 - Feature #5038: snab_results_algorithms
#                   Feature #4988: Allow snab to return and save results
def insert_snab_results_algorithms(current_skyline_app, data):
    """
    Insert a new algorithm record into the snab_results_algorithms table
    """

    function_str = 'database.queries.insert_snab_results_algorithms'

    new_row = {}

    current_skyline_app_logger = current_skyline_app + 'Log'
    current_logger = logging.getLogger(current_skyline_app_logger)

    try:
        engine, fail_msg, trace = get_engine(current_skyline_app)
    except Exception as err:
        current_logger.error('error :: %s :: could not get a MySQL engine - %s' % (function_str, err))
        return new_row

    try:
        snab_results_algorithms_table, fail_msg, trace = snab_results_algorithms_table_meta(current_skyline_app, engine)
    except Exception as err:
        current_logger.error('error :: %s :: failed to get snab_results_algorithms_table meta - %s' % (
            function_str, err))
        if engine:
            engine_disposal(current_skyline_app, engine)
        return new_row

    try:
        #connection = engine.connect()
        if data['consensus_achieved']:
            ins = snab_results_algorithms_table.insert().values(
                snab_id=data['snab_id'],
                algorithm_group_id=data['algorithm_group_id'],
                algorithm_id=data['algorithm_id'],
                anomalyScore=data['anomalyScore'],
                runtime=float(data['runtime']),
                consensus_achieved=data['consensus_achieved'])
        else:
            ins = snab_results_algorithms_table.insert().values(
                snab_id=data['snab_id'],
                algorithm_group_id=data['algorithm_group_id'],
                algorithm_id=data['algorithm_id'],
                anomalyScore=data['anomalyScore'],
                runtime=float(data['runtime']))

        # @modified 20260226 - Task #5176: Migrate to sqlalchemy v2 API
        #                      Task #5628: Build v5.0.0 and test
        #result = connection.execute(ins)
        #connection.close()
        #new_row = dict(data)
        with engine.begin() as connection:
            result = connection.execute(ins)
            _row = result.fetchone()
            new_row = dict(_row._mapping) if _row is not None else None

    except Exception as err:
        current_logger.error('error :: %s :: could not insert into snab_results_algorithms_table: %s - %s' % (
            function_str, str(data), err))
        if engine:
            engine_disposal(current_skyline_app, engine)
        return new_row

    if engine:
        engine_disposal(current_skyline_app, engine)
    if new_row:
        current_logger.info('%s :: inserted new snab_results_algorithms row with: %s' % (
            function_str, str(new_row)))
    else:
        current_logger.error('error :: %s :: failed to inserted snab_results_algorithms row with: %s' % (
            function_str, str(data)))

    return new_row
