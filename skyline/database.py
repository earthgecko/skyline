# coding=utf-8
import logging
import traceback
from sqlalchemy import (
    create_engine, Column, Table, Integer, String, MetaData, DateTime)
from sqlalchemy.dialects.mysql import DOUBLE, FLOAT, TINYINT, VARCHAR, FLOAT

import settings


def get_engine(current_skyline_app):
    '''
    # @added 20161209 - Branch #922: ionosphere
    #                   Task #1658: Patterning Skyline Ionosphere
    # Use SQLAlchemy, mysql.connector is still around but starting the
    # move to SQLAlchemy now that all the webapp Ionosphere SQLAlchemy patterns
    # work
    '''
    try:
        engine = create_engine(
            'mysql+mysqlconnector://%s:%s@%s:%s/%s' % (
                settings.PANORAMA_DBUSER, settings.PANORAMA_DBUSERPASS,
                settings.PANORAMA_DBHOST, str(settings.PANORAMA_DBPORT),
                settings.PANORAMA_DATABASE))
        return engine, 'got MySQL engine', 'none'
    except:
        trace = traceback.format_exc()
        current_skyline_app_logger = current_skyline_app + 'Log'
        current_logger = logging.getLogger(current_skyline_app_logger)
        current_logger.error(trace)
        fail_msg = 'error :: could not create MySQL engine'
        current_logger.error('%s' % fail_msg)
        return None, fail_msg, trace


def ionosphere_table_meta(current_skyline_app, engine):

    current_skyline_app_logger = current_skyline_app + 'Log'
    current_logger = logging.getLogger(current_skyline_app_logger)

    # Create the ionosphere table MetaData
    try:
        ionosphere_meta = MetaData()
    # @modified 20161209 - Branch #922: ionosphere
    #                      Task #1658: Patterning Skyline Ionosphere
    # HOWEVER:
    # Create the ionosphere table MetaData, however this the only table not
    # currently being reflected from the database.  Due to the possibility of
    # SQL update statements with updates, etc it is probably better to stick
    # with a single source of truth schema in the skyline.sql rather than having
    # to micro manage the schema in Python/SQLAlchemy code too and you cannot
    # add COMMENTS with SQLAlchemy table creation.  However z_ts_<metric_id> and
    # z_fp_<metric_id> tables ARE and WILL continue to ONLY be described in
    # Python/SQLAlchemy in skyline/webapp/ionosphere_backend.py, otherwise
    # Skyline would probably be running peewee like pydsn.  However peewee
    # cannot create tables per se and I was tired of raw mysql.connector so...
    # as of 20161209184400 - thank you SQLAlchemy, reflecting is the better
    # option.  However it means if you are in here, see the skyline.sql for the
    # source of truth
    #    ionosphere_table = Table(
    #        'ionosphere', ionosphere_meta,
    #        Column('id', Integer, primary_key=True),
    #        Column('metric_id', Integer, nullable=False, key='metric_id'),
    #        Column('enabled', TINYINT(), nullable=True),
    #        Column('tsfresh_version', VARCHAR(12), nullable=True),
    #        Column('calc_time', FLOAT, nullable=True),
    #        Column('features_count', Integer, nullable=True),
    #        Column('features_sum', DOUBLE, nullable=True),
    #        Column('deleted', Integer, nullable=True),
    #        Column('matched_count', Integer, nullable=True),
    #        Column('last_matched', Integer, nullable=True),
    #        Column('created_timestamp', DateTime()),
    #        mysql_charset='utf8',
    #        mysql_key_block_size='255',
    #        mysql_engine='MyISAM')
    #    ionosphere_table.create(engine, checkfirst=True)
    #    return ionosphere_table, 'ionosphere_table meta OK', 'none'
        ionosphere_table = Table('ionosphere', ionosphere_meta, autoload=True, autoload_with=engine)
        return ionosphere_table, 'ionosphere_table meta reflected OK', 'none'
    except:
        trace = traceback.format_exc()
        current_logger.error('%s' % trace)
        fail_msg = 'error :: failed to create table - ionosphere'
        current_logger.error('%s' % fail_msg)
        return False, fail_msg, trace


def metrics_table_meta(current_skyline_app, engine):

    current_skyline_app_logger = current_skyline_app + 'Log'
    current_logger = logging.getLogger(current_skyline_app_logger)

    # Create the metrics table MetaData
    try:
        metrics_meta = MetaData()
        metrics_table = Table('metrics', metrics_meta, autoload=True, autoload_with=engine)
        return metrics_table, 'metrics_table meta reflected OK', 'none'
    except:
        trace = traceback.format_exc()
        current_logger.error('%s' % trace)
        fail_msg = 'error :: failed to reflect the metrics table meta'
        current_logger.error('%s' % fail_msg)
        return False, fail_msg, trace


def anomalies_table_meta(current_skyline_app, engine):

    current_skyline_app_logger = current_skyline_app + 'Log'
    current_logger = logging.getLogger(current_skyline_app_logger)

    # Create the anomalies table MetaData
    try:
        anomalies_meta = MetaData()
        anomalies_table = Table('anomalies', anomalies_meta, autoload=True, autoload_with=engine)
        return anomalies_table, 'anomalies_table meta reflected OK', 'none'
    except:
        trace = traceback.format_exc()
        current_logger.error('%s' % trace)
        fail_msg = 'error :: failed to reflect the anomalies table meta'
        current_logger.error('%s' % fail_msg)
        return False, fail_msg, trace


# @added 20170107 - Feature #1844: ionosphere_matched DB table
#                   Branch #922: ionosphere
#                   Task #1658: Patterning Skyline Ionosphere
# This table will allow for each not anomalous match that Ionosphere records to
# be reviewed.  It could get big and perhaps should be optional

def ionosphere_matched_table_meta(current_skyline_app, engine):

    current_skyline_app_logger = current_skyline_app + 'Log'
    current_logger = logging.getLogger(current_skyline_app_logger)

    # Create the ionosphere_matched table MetaData
    try:
        ionosphere_matched_meta = MetaData()
        ionosphere_matched_table = Table('ionosphere_matched', ionosphere_matched_meta, autoload=True, autoload_with=engine)
        return anomalies_table, 'ionosphere_matched_table meta reflected OK', 'none'
    except:
        trace = traceback.format_exc()
        current_logger.error('%s' % trace)
        fail_msg = 'error :: failed to reflect the ionosphere_matched table meta'
        current_logger.error('%s' % fail_msg)
        return False, fail_msg, trace
