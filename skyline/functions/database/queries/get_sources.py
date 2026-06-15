"""
get_sources.py
"""
import logging
import traceback

# @added 20230106 - Task #4022: Move mysql_select calls to SQLAlchemy
#                   Task #4778: v4.0.0 - update dependencies
# @modified 20260227 - Task #5176: Migrate to sqlalchemy v2 API
# Added text
from sqlalchemy import select, Table, MetaData, text

from database import get_engine, engine_disposal


# @added 20230107 - Task #4022: Move mysql_select calls to SQLAlchemy
#                   Task #4778: v4.0.0 - update dependencies
# Use sqlalchemy rather than string-based query construction
def get_sources(current_skyline_app):
    """
    Returns a dict of sources and their ids.
    """
    current_skyline_app_logger = current_skyline_app + 'Log'
    current_logger = logging.getLogger(current_skyline_app_logger)

    sources = {}
    try:
        engine, fail_msg, trace = get_engine(current_skyline_app)
        if fail_msg != 'got MySQL engine':
            current_logger.error('error :: get_sources :: could not get a MySQL engine fail_msg - %s' % str(fail_msg))
        if trace != 'none':
            current_logger.error('error :: get_sources :: could not get a MySQL engine trace - %s' % str(trace))
    except Exception as err:
        current_logger.error(traceback.format_exc())
        current_logger.error('error :: get_sources :: could not get a MySQL engine - %s' % str(err))

    sources_list = []
    if engine:
        try:
            #connection = engine.connect()
            stmt = text('SELECT DISTINCT(source) FROM sources')
            # @modified 20260226 - Task #5176: Migrate to sqlalchemy v2 API
            #                      Task #5628: Build v5.0.0 and test
            #result = connection.execute(stmt)
            #for row in result:
            with engine.connect() as connection:
                result = connection.execute(stmt)
                results = [dict(row._mapping) for row in result.fetchall()]
            for row in results:
                sources_list.append(row['source'])
            #connection.close()
        except Exception as err:
            current_logger.error(traceback.format_exc())
            current_logger.error('error :: get_sources :: failed to build sources_list - %s' % str(err))
    if sources_list:
        try:
            try:
                use_table_meta = MetaData()
                use_table = Table('sources', use_table_meta, autoload_with=engine)
            except Exception as err:
                current_logger.error(traceback.format_exc())
                current_logger.error('error :: get_sources :: use_table Table failed sources table - %s' % (
                    err))

            # @modified 20260226 - Task #5176: Migrate to sqlalchemy v2 API
            #                      Task #5628: Build v5.0.0 and test
            #connection = engine.connect()
            with engine.connect() as connection:
                for source in sources_list:
                    stmt = select(use_table.c.id).where(use_table.c.source == source)
                    # @modified 20260226 - Task #5176: Migrate to sqlalchemy v2 API
                    #                      Task #5628: Build v5.0.0 and test
                    #result = connection.execute(stmt)
                    #for row in result:
                    with engine.connect() as connection:
                        result = connection.execute(stmt)
                        results = [dict(row._mapping) for row in result.fetchall()]
                    for row in results:
                        sources[source] = row['id']
                        break
            #connection.close()
        except Exception as err:
            current_logger.error(traceback.format_exc())
            current_logger.error('error :: get_sources :: failed to build sources - %s' % str(err))

    if engine:
        engine_disposal(current_skyline_app, engine)

    return sources
