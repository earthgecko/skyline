"""
get_apps.py
"""
import logging
import traceback

# @added 20230106 - Task #4022: Move mysql_select calls to SQLAlchemy
#                   Task #4778: v4.0.0 - update dependencies
from sqlalchemy import select, Table, MetaData

from database import get_engine, engine_disposal


# @added 20221219 - Feature #4658: ionosphere.learn_repetitive_patterns
def get_apps(current_skyline_app):
    """
    Returns a dict of apps and their ids.
    """
    current_skyline_app_logger = current_skyline_app + 'Log'
    current_logger = logging.getLogger(current_skyline_app_logger)

    apps = {}
    try:
        engine, fail_msg, trace = get_engine(current_skyline_app)
        if fail_msg != 'got MySQL engine':
            current_logger.error('error :: get_apps :: could not get a MySQL engine fail_msg - %s' % str(fail_msg))
        if trace != 'none':
            current_logger.error('error :: get_apps :: could not get a MySQL engine trace - %s' % str(trace))
    except Exception as err:
        current_logger.error(traceback.format_exc())
        current_logger.error('error :: get_apps :: could not get a MySQL engine - %s' % str(err))

    apps_list = []
    if engine:
        try:
            connection = engine.connect()
            stmt = 'SELECT DISTINCT(app) FROM apps'
            result = connection.execute(stmt)
            for row in result:
                apps_list.append(row['app'])
            connection.close()
        except Exception as err:
            current_logger.error(traceback.format_exc())
            current_logger.error('error :: get_apps :: failed to build apps_list - %s' % str(err))
    if apps_list:
        try:

            # @added 20230106 - Task #4022: Move mysql_select calls to SQLAlchemy
            #                   Task #4778: v4.0.0 - update dependencies
            # Use the MetaData autoload rather than string-based query construction
            try:
                use_table_meta = MetaData()
                use_table = Table('apps', use_table_meta, autoload=True, autoload_with=engine)
            except Exception as err:
                current_logger.error(traceback.format_exc())
                current_logger.error('error :: get_apps :: use_table Table failed on apps table - %s' % (
                    err))

            connection = engine.connect()
            for app in apps_list:

                # @modified 20230106  - Task #4022: Move mysql_select calls to SQLAlchemy
                # stmt = 'SELECT id FROM apps WHERE app=\'%s\'' % app
                stmt = select(use_table.c.id).where(use_table.c.app == app)

                result = connection.execute(stmt)
                for row in result:
                    apps[app] = row['id']
                    break
            connection.close()
        except Exception as err:
            current_logger.error(traceback.format_exc())
            current_logger.error('error :: get_apps :: failed to build apps - %s' % str(err))

    if engine:
        engine_disposal(current_skyline_app, engine)

    return apps
