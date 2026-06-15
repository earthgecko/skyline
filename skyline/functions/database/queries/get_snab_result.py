"""
get_snab_result.py
"""
import logging
import traceback

from sqlalchemy import select, Table, MetaData

from database import get_engine, engine_disposal


# @added 20230707 - Feature #4988: Allow snab to return and save results
def get_snab_result(current_skyline_app, anomaly_id):
    """
    Returns a dict of the snab results for an anomaly.
    """
    current_skyline_app_logger = current_skyline_app + 'Log'
    current_logger = logging.getLogger(current_skyline_app_logger)

    snab_result = {}
    try:
        engine, fail_msg, trace = get_engine(current_skyline_app)
        if fail_msg != 'got MySQL engine':
            current_logger.error('error :: get_snab_result :: could not get a MySQL engine fail_msg - %s' % str(fail_msg))
        if trace != 'none':
            current_logger.error('error :: get_snab_result :: could not get a MySQL engine trace - %s' % str(trace))
    except Exception as err:
        current_logger.error(traceback.format_exc())
        current_logger.error('error :: get_snab_result :: could not get a MySQL engine - %s' % str(err))

    try:
        use_table_meta = MetaData()
        use_table = Table('snab', use_table_meta, autoload_with=engine)
    except Exception as err:
        current_logger.error(traceback.format_exc())
        current_logger.error('error :: get_snab_result :: use_table Table failed on snab table - %s' % (
            err))

    try:
        use_table_meta = MetaData()
        apps_table = Table('apps', use_table_meta, autoload_with=engine)
    except Exception as err:
        current_logger.error(traceback.format_exc())
        current_logger.error('error :: get_snab_result :: use_table Table failed on apps table - %s' % (
            err))
    apps = {}
    try:
        #connection = engine.connect()
        stmt = select(apps_table)
        # @modified 20260226 - Task #5176: Migrate to sqlalchemy v2 API
        #                      Task #5628: Build v5.0.0 and test
        #result = connection.execute(stmt)
        #for row in result:
        with engine.connect() as connection:
            result = connection.execute(stmt)
            results = [dict(row._mapping) for row in result.fetchall()]
        for row in results:
            app_id = row['id']
            apps[app_id] = row['app']
        #connection.close()
    except Exception as err:
        current_logger.error(traceback.format_exc())
        current_logger.error('error :: get_snab_result :: failed to build apps - %s' % str(err))

    try:
        #connection = engine.connect()
        stmt = select(use_table).where(use_table.c.anomaly_id == int(anomaly_id))
        # @modified 20260226 - Task #5176: Migrate to sqlalchemy v2 API
        #                      Task #5628: Build v5.0.0 and test
        #result = connection.execute(stmt)
        #for row in result:
        with engine.connect() as connection:
            result = connection.execute(stmt)
            results = [dict(row._mapping) for row in result.fetchall()]
        for row in results:
            snab_id = row['id']
            snab_app_id = row['app_id']
            # Coerce Decimal to float for json
            # @modified 20250121 - Feature #5588: snab.process_algorithm
            # Handle None
            try:
                anomalyScore = float(row['anomalyScore'])
            except:
                anomalyScore = None
            runtime = float(row['runtime'])
            slack_thread_ts = float(row['slack_thread_ts'])
            snab_app = None
            for app_id in list(apps.keys()):
                if snab_app_id == app_id:
                   snab_app = apps[app_id]
                   break
            snab_result[snab_id] = dict(row)
            snab_result[snab_id]['app'] = snab_app
            snab_result[snab_id]['anomalyScore'] = anomalyScore
            snab_result[snab_id]['runtime'] = runtime
            snab_result[snab_id]['slack_thread_ts'] = slack_thread_ts
        #connection.close()
    except Exception as err:
        current_logger.error(traceback.format_exc())
        current_logger.error('error :: get_snab_result :: failed to build snab_result - %s' % str(err))

    if engine:
        engine_disposal(current_skyline_app, engine)

    return snab_result
