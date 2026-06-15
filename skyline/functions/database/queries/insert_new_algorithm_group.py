"""
insert_new_algorithm.py
"""
import logging
import traceback

from sqlalchemy import select

import settings
from database import get_engine, engine_disposal, algorithm_groups_table_meta


# @added 20250112 - Feature #5588: snab.process_algorithm
#                   Feature #5038: snab_results_algorithms
#                   Feature #4988: Allow snab to return and save results
def insert_new_algorithm_group(current_skyline_app, algorithm):
    """
    Insert a new algorithm_group into the algorithm_groups table
    """

    function_str = 'database.queries.insert_new_algorithm_group'

    new_algorithm_group_id = 0

    current_skyline_app_logger = current_skyline_app + 'Log'
    current_logger = logging.getLogger(current_skyline_app_logger)

    try:
        engine, fail_msg, trace = get_engine(current_skyline_app)
    except Exception as err:
        current_logger.error('error :: %s :: could not get a MySQL engine - %s' % (function_str, err))
        return new_algorithm_group_id

    try:
        algorithm_groups_table, fail_msg, trace = algorithm_groups_table_meta(current_skyline_app, engine)
    except Exception as err:
        current_logger.error('error :: %s :: failed to get algorithm_groups_table meta - %s' % (
            function_str, err))
        if engine:
            engine_disposal(current_skyline_app, engine)
        return 0

    if algorithm.startswith('skyline_'):
        algorithm = algorithm.lstrip('skyline_')

    # Check the algorithm_group is not in the table
    try:
        #connection = engine.connect()
        stmt = select(algorithm_groups_table).where(algorithm_groups_table.c.algorithm_group == algorithm)
        # @modified 20260226 - Task #5176: Migrate to sqlalchemy v2 API
        #                      Task #5628: Build v5.0.0 and test
        #result = connection.execute(stmt)
        #for row in result:
        with engine.connect() as connection:
            result = connection.execute(stmt)
            results = [dict(row._mapping) for row in result.fetchall()]
        for row in results:
            new_algorithm_group_id = row['id']
            break
        #connection.close()
    except Exception as err:
        current_logger.error(traceback.format_exc())
        current_logger.error('error :: %s :: failed to select algorithm_group from algorithm_groups - %s' % (
            function_str, str(err)))  # nosec B608

    if new_algorithm_group_id:
        if engine:
            engine_disposal(current_skyline_app, engine)
        current_logger.info('%s :: algorithm: %s already exists with id: %s' % (
            function_str, str(algorithm), str(new_algorithm_group_id)))
        return new_algorithm_group_id

    try:
        #connection = engine.connect()
        ins = algorithm_groups_table.insert().values(algorithm_group=algorithm)

        # @modified 20260226 - Task #5176: Migrate to sqlalchemy v2 API
        #                      Task #5628: Build v5.0.0 and test
        #result = connection.execute(ins)
        #connection.close()
        #new_algorithm_group_id = result.inserted_primary_key[0]
        with engine.begin() as connection:
            result = connection.execute(ins)
            new_algorithm_group_id = result.inserted_primary_key[0]

    except Exception as err:
        current_logger.error('error :: %s :: could not insert algorithm: %s into algorithm_groups table, err: %s' % (
            function_str, str(algorithm), err))
        if engine:
            engine_disposal(current_skyline_app, engine)
        return 0

    if engine:
        engine_disposal(current_skyline_app, engine)
    if new_algorithm_group_id:
        current_logger.info('%s :: inserted new algorithm_group with id: %s, algorithm: %s' % (
            function_str, str(new_algorithm_group_id), str(algorithm)))
    else:
        current_logger.error('error :: %s :: failed to inserted new algorithm_group, no id returned, algorithm: %s' % (
            function_str, str(algorithm)))

    return new_algorithm_group_id
