"""
Get algorithms
"""
import logging
import traceback

from database import get_engine, engine_disposal


# @added 20211001 - Feature #4264: luminosity - cross_correlation_relationships
def get_algorithm_groups(current_skyline_app):
    """
    Returns a dict of algorithm_groups and their ids.
    """
    current_skyline_app_logger = current_skyline_app + 'Log'
    current_logger = logging.getLogger(current_skyline_app_logger)

    algorithm_groups = {}
    try:
        engine, fail_msg, trace = get_engine(current_skyline_app)
        if fail_msg != 'got MySQL engine':
            current_logger.error('error :: get_algorithm_groups :: could not get a MySQL engine fail_msg - %s' % str(fail_msg))
        if trace != 'none':
            current_logger.error('error :: get_algorithm_groups :: could not get a MySQL engine trace - %s' % str(trace))
    except Exception as err:
        current_logger.error(traceback.format_exc())
        current_logger.error('error :: get_algorithm_groups :: could not get a MySQL engine - %s' % str(err))

    algorithm_groups_list = []
    if engine:
        try:
            connection = engine.connect()
            stmt = 'SELECT DISTINCT(algorithm_group) FROM algorithm_groups'
            result = connection.execute(stmt)
            for row in result:
                algorithm_groups_list.append(row['algorithm_group'])
            connection.close()
        except Exception as err:
            current_logger.error(traceback.format_exc())
            current_logger.error('error :: get_algorithm_groups :: failed to build algorithm_groups_list - %s' % str(err))
    if algorithm_groups_list:
        try:
            connection = engine.connect()
            for algorithm_group in algorithm_groups_list:
                stmt = 'SELECT id FROM algorithm_groups WHERE algorithm_group=\'%s\'' % algorithm_group
                result = connection.execute(stmt)
                for row in result:
                    algorithm_groups[algorithm_group] = row['id']
                    break
            connection.close()
        except Exception as err:
            current_logger.error(traceback.format_exc())
            current_logger.error('error :: get_algorithm_groups :: failed to build algorithm_groups - %s' % str(err))

    if engine:
        engine_disposal(current_skyline_app, engine)

    return algorithm_groups
