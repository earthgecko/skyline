"""
insert_new_algorithm.py
"""
import logging
import traceback

from sqlalchemy import select

import settings
from database import get_engine, engine_disposal, algorithms_table_meta


# @added 20230728 - Feature #5038: snab_results_algorithms
#                   Feature #4988: Allow snab to return and save results
def insert_new_algorithm(current_skyline_app, algorithm):
    """
    Insert a new algorithm into the algorithms table
    """

    function_str = 'database.queries.insert_new_algorithm'

    new_algorithm_id = 0

    current_skyline_app_logger = current_skyline_app + 'Log'
    current_logger = logging.getLogger(current_skyline_app_logger)

    try:
        engine, fail_msg, trace = get_engine(current_skyline_app)
    except Exception as err:
        current_logger.error('error :: %s :: could not get a MySQL engine - %s' % (function_str, err))
        return new_algorithm_id

    try:
        algorithms_table, fail_msg, trace = algorithms_table_meta(current_skyline_app, engine)
    except Exception as err:
        current_logger.error('error :: %s :: failed to get algorithms_table meta - %s' % (
            function_str, err))
        if engine:
            engine_disposal(current_skyline_app, engine)
        return 0

    # Check the algorithm is not in the table
    try:
        connection = engine.connect()
        stmt = select(algorithms_table).where(algorithms_table.c.algorithm == algorithm)
        result = connection.execute(stmt)
        for row in result:
            new_algorithm_id = row['id']
            break
        connection.close()
    except Exception as err:
        current_logger.error(traceback.format_exc())
        # @modified 20241106 - Task #5526: Build v5.0.0 and upgrade deps
        # bandit interpreting as SQL because of the term update
        # B608:hardcoded_sql_expressions] Possible SQL injection vector through string-based query construction.
        current_logger.error('error :: %s :: failed to select algorithm from algorithms - %s' % (
            function_str, str(err)))  # nosec B608

    if new_algorithm_id:
        if engine:
            engine_disposal(current_skyline_app, engine)
        current_logger.info('%s :: algorithm: %s already exists with id: %s' % (
            function_str, str(algorithm), str(new_algorithm_id)))
        return new_algorithm_id

    try:
        connection = engine.connect()
        ins = algorithms_table.insert().values(algorithm=algorithm)
        result = connection.execute(ins)
        connection.close()
        new_algorithm_id = result.inserted_primary_key[0]
    except Exception as err:
        current_logger.error('error :: %s :: could not insert algorithm: %s into DB - %s' % (
            function_str, str(algorithm), err))
        if engine:
            engine_disposal(current_skyline_app, engine)
        return 0

    if engine:
        engine_disposal(current_skyline_app, engine)
    if new_algorithm_id:
        current_logger.info('%s :: inserted new algorithm with id: %s, algorithm: %s' % (
            function_str, str(new_algorithm_id), str(algorithm)))
    else:
        current_logger.error('error :: %s :: failed to inserted new algorithm, no id returned, algorithm: %s' % (
            function_str, str(algorithm)))

    return new_algorithm_id
