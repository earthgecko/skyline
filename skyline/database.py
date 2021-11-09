# coding=utf-8
import logging
import traceback
from sqlalchemy import (
    create_engine, Column, Table, Integer, String, MetaData, DateTime)
from sqlalchemy.dialects.mysql import DOUBLE, FLOAT, TINYINT, VARCHAR, SMALLINT

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


# @added 20210420  - Task #4022: Move mysql_select calls to SQLAlchemy
# Add a global engine_disposal method
def engine_disposal(current_skyline_app, engine):
    if engine:
        try:
            engine.dispose()
        except:
            current_skyline_app_logger = current_skyline_app + 'Log'
            current_logger = logging.getLogger(current_skyline_app_logger)
            current_logger.error(traceback.format_exc())
            current_logger.error('error :: engine_disposal :: calling engine.dispose()')
    return


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
        return ionosphere_matched_table, 'ionosphere_matched_table meta reflected OK', 'none'
    except:
        trace = traceback.format_exc()
        current_logger.error('%s' % trace)
        fail_msg = 'error :: failed to reflect the ionosphere_matched table meta'
        current_logger.error('%s' % fail_msg)
        return False, fail_msg, trace


# @added 20170305 - Feature #1960: ionosphere_layers
def ionosphere_layers_table_meta(current_skyline_app, engine):

    current_skyline_app_logger = current_skyline_app + 'Log'
    current_logger = logging.getLogger(current_skyline_app_logger)

    # Create the ionosphere_layers table MetaData
    try:
        ionosphere_layers_meta = MetaData()
        ionosphere_layers_table = Table('ionosphere_layers', ionosphere_layers_meta, autoload=True, autoload_with=engine)
        return ionosphere_layers_table, 'ionosphere_layers_table meta reflected OK', 'none'
    except:
        trace = traceback.format_exc()
        current_logger.error('%s' % trace)
        fail_msg = 'error :: failed to reflect the ionosphere_layers table meta'
        current_logger.error('%s' % fail_msg)
        return False, fail_msg, trace


def layers_algorithms_table_meta(current_skyline_app, engine):

    current_skyline_app_logger = current_skyline_app + 'Log'
    current_logger = logging.getLogger(current_skyline_app_logger)

    # Create the layers_algorithms table MetaData
    try:
        layers_algorithms_meta = MetaData()
        layers_algorithms_table = Table('layers_algorithms', layers_algorithms_meta, autoload=True, autoload_with=engine)
        return layers_algorithms_table, 'layers_algorithms_table meta reflected OK', 'none'
    except:
        trace = traceback.format_exc()
        current_logger.error('%s' % trace)
        fail_msg = 'error :: failed to reflect the layers_algorithms table meta'
        current_logger.error('%s' % fail_msg)
        return False, fail_msg, trace


def ionosphere_layers_matched_table_meta(current_skyline_app, engine):

    current_skyline_app_logger = current_skyline_app + 'Log'
    current_logger = logging.getLogger(current_skyline_app_logger)

    # Create the ionosphere_layers_matched table MetaData
    try:
        ionosphere_layers_matched_meta = MetaData()
        ionosphere_layers_matched_table = Table('ionosphere_layers_matched', ionosphere_layers_matched_meta, autoload=True, autoload_with=engine)
        return ionosphere_layers_matched_table, 'ionosphere_layers_matched_table meta reflected OK', 'none'
    except:
        trace = traceback.format_exc()
        current_logger.error('%s' % trace)
        fail_msg = 'error :: failed to reflect the ionosphere_layers_matched table meta'
        current_logger.error('%s' % fail_msg)
        return False, fail_msg, trace


# @added 20180414 - Branch #2270: luminosity
def luminosity_table_meta(current_skyline_app, engine):

    current_skyline_app_logger = current_skyline_app + 'Log'
    current_logger = logging.getLogger(current_skyline_app_logger)

    # Create the luminosity table MetaData
    try:
        luminosity_meta = MetaData()
        luminosity_table = Table('luminosity', luminosity_meta, autoload=True, autoload_with=engine)
        return luminosity_table, 'luminosity_table meta reflected OK', 'none'
    except:
        trace = traceback.format_exc()
        current_logger.error('%s' % trace)
        fail_msg = 'error :: failed to reflect the luminosity table meta'
        current_logger.error('%s' % fail_msg)
        return False, fail_msg, trace


# @added 20200928 - Task #3748: POC SNAB
#                   Branch #3068: SNAB
def snab_table_meta(current_skyline_app, engine):

    current_skyline_app_logger = current_skyline_app + 'Log'
    current_logger = logging.getLogger(current_skyline_app_logger)

    # Create the snab table MetaData
    try:
        snab_meta = MetaData()
        snab_table = Table('snab', snab_meta, autoload=True, autoload_with=engine)
        return snab_table, 'snab_table meta reflected OK', 'none'
    except:
        trace = traceback.format_exc()
        current_logger.error('%s' % trace)
        fail_msg = 'error :: failed to reflect the snab table meta'
        current_logger.error('%s' % fail_msg)
        return False, fail_msg, trace


# @added 20210412 - Feature #4014: Ionosphere - inference
#                   Branch #3590: inference
def motifs_matched_table_meta(current_skyline_app, engine):

    current_skyline_app_logger = current_skyline_app + 'Log'
    current_logger = logging.getLogger(current_skyline_app_logger)

    # Create the motifs_matched table MetaData
    try:
        motifs_matched_meta = MetaData()
        motifs_matched_table = Table('motifs_matched', motifs_matched_meta, autoload=True, autoload_with=engine)
        return motifs_matched_table, 'snab_table meta reflected OK', 'none'
    except:
        trace = traceback.format_exc()
        current_logger.error('%s' % trace)
        fail_msg = 'error :: failed to reflect the motifs_matched table meta'
        current_logger.error('%s' % fail_msg)
        return False, fail_msg, trace


# @added 20210414 - Feature #4014: Ionosphere - inference
#                   Branch #3590: inference
def not_anomalous_motifs_table_meta(current_skyline_app, engine):

    current_skyline_app_logger = current_skyline_app + 'Log'
    current_logger = logging.getLogger(current_skyline_app_logger)

    # Create the not_anomalous_motifs table MetaData
    try:
        not_anomalous_motifs_meta = MetaData()
        not_anomalous_motifs_table = Table('not_anomalous_motifs', not_anomalous_motifs_meta, autoload=True, autoload_with=engine)
        return not_anomalous_motifs_table, 'not_anomalous_motifs meta reflected OK', 'none'
    except:
        trace = traceback.format_exc()
        current_logger.error('%s' % trace)
        fail_msg = 'error :: failed to reflect the not_anomalous_motifs table meta'
        current_logger.error('%s' % fail_msg)
        return False, fail_msg, trace


# @added 20210805 - Feature #4164: luminosity - cloudbursts
def cloudburst_table_meta(current_skyline_app, engine):

    current_skyline_app_logger = current_skyline_app + 'Log'
    current_logger = logging.getLogger(current_skyline_app_logger)

    # Create the cloudburst table MetaData
    try:
        cloudburst_meta = MetaData()
        cloudburst_table = Table('cloudburst', cloudburst_meta, autoload=True, autoload_with=engine)
        return cloudburst_table, 'cloudburst meta reflected OK', 'none'
    except:
        trace = traceback.format_exc()
        current_logger.error('%s' % trace)
        fail_msg = 'error :: failed to reflect the cloudburst table meta'
        current_logger.error('%s' % fail_msg)
        return False, fail_msg, trace


# @added 20210805 - Feature #4164: luminosity - cloudbursts
def cloudbursts_table_meta(current_skyline_app, engine):

    current_skyline_app_logger = current_skyline_app + 'Log'
    current_logger = logging.getLogger(current_skyline_app_logger)

    # Create the cloudbursts table MetaData
    try:
        cloudbursts_meta = MetaData()
        cloudbursts_table = Table('cloudbursts', cloudbursts_meta, autoload=True, autoload_with=engine)
        return cloudbursts_table, 'cloudbursts meta reflected OK', 'none'
    except:
        trace = traceback.format_exc()
        current_logger.error('%s' % trace)
        fail_msg = 'error :: failed to reflect the cloudbursts table meta'
        current_logger.error('%s' % fail_msg)
        return False, fail_msg, trace


# @added 20210929 - Feature #4264: luminosity - cross_correlation_relationships
def metric_group_table_meta(current_skyline_app, engine):

    current_skyline_app_logger = current_skyline_app + 'Log'
    current_logger = logging.getLogger(current_skyline_app_logger)

    # Create the metric_group table MetaData
    try:
        metric_group_meta = MetaData()
        metric_group_table = Table('metric_group', metric_group_meta, autoload=True, autoload_with=engine)
        return metric_group_table, 'metric_group meta reflected OK', 'none'
    except:
        trace = traceback.format_exc()
        current_logger.error('%s' % trace)
        fail_msg = 'error :: failed to reflect the metric_group table meta'
        current_logger.error('%s' % fail_msg)
        return False, fail_msg, trace


# @added 20210929 - Feature #4264: luminosity - cross_correlation_relationships
def metric_group_info_table_meta(current_skyline_app, engine):

    current_skyline_app_logger = current_skyline_app + 'Log'
    current_logger = logging.getLogger(current_skyline_app_logger)

    # Create the metric_group_info table MetaData
    try:
        metric_group_info_meta = MetaData()
        metric_group_info_table = Table('metric_group_info', metric_group_info_meta, autoload=True, autoload_with=engine)
        return metric_group_info_table, 'metric_group_info meta reflected OK', 'none'
    except:
        trace = traceback.format_exc()
        current_logger.error('%s' % trace)
        fail_msg = 'error :: failed to reflect the metric_group_info table meta'
        current_logger.error('%s' % fail_msg)
        return False, fail_msg, trace
