"""
Get algorithms
"""
import logging
import traceback

from database import get_engine, engine_disposal


# @added 20211001 - Feature #4264: luminosity - cross_correlation_relationships
def get_algorithms(current_skyline_app):
    """
    Returns a dict of algorithms and their ids.
    """
    current_skyline_app_logger = current_skyline_app + 'Log'
    current_logger = logging.getLogger(current_skyline_app_logger)

    algorithms = {}
    try:
        engine, fail_msg, trace = get_engine(current_skyline_app)
        if fail_msg != 'got MySQL engine':
            current_logger.error('error :: get_algorithms :: could not get a MySQL engine fail_msg - %s' % str(fail_msg))
        if trace != 'none':
            current_logger.error('error :: get_algorithms :: could not get a MySQL engine trace - %s' % str(trace))
    except Exception as err:
        current_logger.error(traceback.format_exc())
        current_logger.error('error :: get_algorithms :: could not get a MySQL engine - %s' % str(err))

    algorithms_list = []
    if engine:
        try:
            connection = engine.connect()
            stmt = 'SELECT DISTINCT(algorithm) FROM algorithms'
            result = connection.execute(stmt)
            for row in result:
                algorithms_list.append(row['algorithm'])
            connection.close()
        except Exception as err:
            current_logger.error(traceback.format_exc())
            current_logger.error('error :: get_algorithms :: failed to build algorithms_list - %s' % str(err))
    if algorithms_list:
        try:
            connection = engine.connect()
            for algorithm in algorithms_list:
                stmt = 'SELECT id FROM algorithms WHERE algorithm=\'%s\'' % algorithm
                result = connection.execute(stmt)
                for row in result:
                    algorithms[algorithm] = row['id']
                    break
            connection.close()
        except Exception as err:
            current_logger.error(traceback.format_exc())
            current_logger.error('error :: get_algorithms :: failed to build algorithms - %s' % str(err))

    if engine:
        engine_disposal(current_skyline_app, engine)

    return algorithms
