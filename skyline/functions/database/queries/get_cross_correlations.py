"""
Get anomalies for a metric id
"""
import logging
import traceback
from sqlalchemy.sql import select

from database import get_engine, engine_disposal, luminosity_table_meta


def get_cross_correlations(current_skyline_app, anomaly_ids):
    """
    Given a list of anomaly ids, return the cross correlations
    """
    current_skyline_app_logger = current_skyline_app + 'Log'
    current_logger = logging.getLogger(current_skyline_app_logger)

    cross_correlations = {}

    try:
        engine, fail_msg, trace = get_engine(current_skyline_app)
        if fail_msg != 'got MySQL engine':
            current_logger.error('error :: get_cross_correlations :: could not get a MySQL engine fail_msg - %s' % str(fail_msg))
        if trace != 'none':
            current_logger.error('error :: get_cross_correlations :: could not get a MySQL engine trace - %s' % str(trace))
    except Exception as err:
        current_logger.error(traceback.format_exc())
        current_logger.error('error :: get_cross_correlations :: could not get a MySQL engine - %s' % str(err))

    if engine:
        try:
            luminosity_table, fail_msg, trace = luminosity_table_meta(current_skyline_app, engine)
            if fail_msg != 'luminosity_table meta reflected OK':
                current_logger.error('error :: get_cross_correlations :: could not get luminosity_table_meta fail_msg - %s' % str(fail_msg))
            if trace != 'none':
                current_logger.error('error :: get_cross_correlations :: could not get luminosity_table_meta trace - %s' % str(trace))
        except Exception as err:
            current_logger.error(traceback.format_exc())
            current_logger.error('error :: get_cross_correlations :: luminosity_table_meta - %s' % str(err))
        try:
            #connection = engine.connect()
            # @modified 20260225 - Task #5176: Migrate to sqlalchemy v2 API
            #                      Task #5628: Build v5.0.0 and test
            #stmt = select([luminosity_table], luminosity_table.c.id.in_(anomaly_ids))
            stmt = select(luminosity_table).\
                    where(luminosity_table.c.id.in_(anomaly_ids))

            # @modified 20260226 - Task #5176: Migrate to sqlalchemy v2 API
            #                      Task #5628: Build v5.0.0 and test
            #results = connection.execute(stmt)
            with engine.connect() as connection:
                result = connection.execute(stmt)
                results = [dict(row._mapping) for row in result.fetchall()]
            for row in results:
                anomaly_id = row['id']
                if anomaly_id not in list(cross_correlations.keys()):
                    cross_correlations[anomaly_id] = {}
                metric_id = row['metric_id']
                cross_correlations[anomaly_id][metric_id] = dict(row)
            #connection.close()
        except Exception as err:
            current_logger.error(traceback.format_exc())
            current_logger.error('error :: get_cross_correlations :: failed to build cross_correlations dict - %s' % str(err))

    if engine:
        engine_disposal(current_skyline_app, engine)

    return cross_correlations
