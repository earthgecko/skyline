"""
get_hosts.py
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
def get_hosts(current_skyline_app):
    """
    Returns a dict of hosts and their ids.
    """
    current_skyline_app_logger = current_skyline_app + 'Log'
    current_logger = logging.getLogger(current_skyline_app_logger)

    hosts = {}
    try:
        engine, fail_msg, trace = get_engine(current_skyline_app)
        if fail_msg != 'got MySQL engine':
            current_logger.error('error :: get_hosts :: could not get a MySQL engine fail_msg - %s' % str(fail_msg))
        if trace != 'none':
            current_logger.error('error :: get_hosts :: could not get a MySQL engine trace - %s' % str(trace))
    except Exception as err:
        current_logger.error(traceback.format_exc())
        current_logger.error('error :: get_hosts :: could not get a MySQL engine - %s' % str(err))

    hosts_list = []
    if engine:
        try:
            #connection = engine.connect()
            stmt = text('SELECT DISTINCT(host) FROM hosts')

            # @modified 20260226 - Task #5176: Migrate to sqlalchemy v2 API
            #                      Task #5628: Build v5.0.0 and test
            #result = connection.execute(stmt)
            #for row in result:
            with engine.connect() as connection:
                result = connection.execute(stmt)
                results = [dict(row._mapping) for row in result.fetchall()]
            for row in results:
                hosts_list.append(row['host'])
            #connection.close()
        except Exception as err:
            current_logger.error(traceback.format_exc())
            current_logger.error('error :: get_hosts :: failed to build hosts_list - %s' % str(err))
    if hosts_list:
        try:
            try:
                use_table_meta = MetaData()
                use_table = Table('hosts', use_table_meta, autoload_with=engine)
            except Exception as err:
                current_logger.error(traceback.format_exc())
                current_logger.error('error :: get_hosts :: use_table Table failed hosts table - %s' % (
                    err))
            # @modified 20260226 - Task #5176: Migrate to sqlalchemy v2 API
            #                      Task #5628: Build v5.0.0 and test
            #connection = engine.connect()
            with engine.connect() as connection:
                for host in hosts_list:
                    stmt = select(use_table.c.id).where(use_table.c.host == host)
                    result = connection.execute(stmt)
                    results = [dict(row._mapping) for row in result.fetchall()]
                    for row in results:
                        hosts[host] = row['id']
                        break
            #connection.close()
        except Exception as err:
            current_logger.error(traceback.format_exc())
            current_logger.error('error :: get_hosts :: failed to build hosts - %s' % str(err))

    if engine:
        engine_disposal(current_skyline_app, engine)

    return hosts
