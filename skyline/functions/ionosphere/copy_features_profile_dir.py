"""
copy_features_profile.py
"""
import logging
import traceback
from os import path
import shutil
import glob
from ast import literal_eval
from decimal import Decimal
import datetime
import copy

from sqlalchemy import (
    Column, Table, Integer, MetaData)
from sqlalchemy.dialects.mysql import DOUBLE
# @modified 20260227 - Task #5176: Migrate to sqlalchemy v2 API
# Added insert
from sqlalchemy.sql import select, insert


import settings
from skyline_functions import mkdir_p, write_data_to_file

from functions.database.queries.get_ionosphere_fp_row import get_ionosphere_fp_db_row
from functions.metrics.get_base_name_from_metric_id import get_base_name_from_metric_id
from database import (
    get_engine, engine_disposal, ionosphere_table_meta, 
    alias_features_profile_table_meta,
)



from functions.database.queries.get_ionosphere_fp_row_from_match_id import get_ionosphere_fp_row_from_match_id
from functions.database.queries.fp_timeseries import get_db_fp_timeseries
from functions.database.queries.get_motifs_matched_row import get_motifs_matched_row
from functions.ionosphere.get_fp_motif import get_fp_motif
from functions.metrics.get_base_name_from_metric_id import get_base_name_from_metric_id
from functions.timeseries.determine_data_frequency import determine_data_frequency
from functions.pandas.timeseries_to_datetime_indexed_df import timeseries_to_datetime_indexed_df
from skyline_functions import get_graphite_metric, get_redis_conn_decoded
# @added 20220801 - Task #2732: Prometheus to Skyline
#                   Branch #4300: prometheus
# Handle labelled_metric name added metric id
from functions.victoriametrics.get_victoriametrics_metric import get_victoriametrics_metric
# @added 20250122 - Feature #5592: tenant_id column in DB tables
from functions.metrics.get_tenant_id import get_tenant_id


# @added 20241011 - Feature #5481: ionosphere.copy_features_profile
#                   Feature #5479: ionosphere.alias_features_profile
def copy_features_profile(current_skyline_app, alias_fp_dict):
    """
    Return dictionary of the newly copied features profile

    :param current_skyline_app: the app calling the function
    :param alias_fp_dict: the alias_fp_dict from create_alias_fps_for_metrics.py, e.g
        alias_fp_dict = {
            'fp_id': fp_id,
            'metric_id': alias_metric_id,
            'metric': alias_metric,
            'original_metric_id': original_metric_id,
            'original_metric': original_metric,
            'exists': alias_exists,
            'label': label,
            'user_id': user_id,
            'alias_id':alias_fp_id
        }
    :type current_skyline_app: str
    :type alias_fp_dict: dict
    :return: fp_created_via_copy
    :rtype: dict

    """

    current_skyline_app_logger = current_skyline_app + 'Log'
    current_logger = logging.getLogger(current_skyline_app_logger)
    function_str = 'functions.ionosphere.copy_features_profile'

    current_logger.info('%s :: copying features profile for alias_fp_dict: %s' % (
        function_str, str(alias_fp_dict)))

    fp_created_via_copy = {}
    try:
        fp_id = alias_fp_dict['fp_id']
        target_metric_id = alias_fp_dict['metric_id']
        original_metric_id = alias_fp_dict['original_metric_id']
        label = alias_fp_dict['label']
        user_id = alias_fp_dict['user_id']
        alias_fp_id = alias_fp_dict['alias_id']

    except Exception as err:
        current_logger.error('error :: %s :: failed to determine key from alias_fp_dict: %s, err: %s' % (
            function_str, str(alias_fp_dict), err))
        if current_skyline_app == 'webapp':
            # Raise to webapp
            raise
        return fp_created_via_copy

    fp_id_row = {}
    try:
        fp_id = alias_fp_dict['fp_id']
        fp_id_row = get_ionosphere_fp_db_row(current_skyline_app, fp_id)
        """
        {'id': 10340,
        'metric_id': 3364,
        'full_duration': 604800,
        'anomaly_timestamp': 1678333800,
        'enabled': 1,
        'tsfresh_version': '0.19.1',
        'calc_time': 3.73059,
        'features_count': 210,
        'features_sum': Decimal('42465.7191241570'),
        'deleted': None,
        'matched_count': 48,
        'last_matched': 1693558788,
        'created_timestamp': datetime.datetime(2023, 3, 9, 7, 25, 10),
        'last_checked': 1704727086,
        'checked_count': 141,
        'parent_id': 0,
        'generation': 0,
        'motif_matched_count': 22,
        'motif_last_matched': 1701327816,
        'motif_last_checked': 1727827552,
        'motif_checked_count': 268,
        'validated': 1,
        'layers_id': 0,
        'echo_fp': 0,
        'user_id': 2,
        'label': 'none',
        'alias_id': 0}
        """
    except Exception as err:
        current_logger.error('error :: %s :: failed to get_ionosphere_fp_db_row for fp_id: %s, err: %s' % (
            function_str, str(fp_id), err))
        if current_skyline_app == 'webapp':
            # Raise to webapp
            raise
        return fp_created_via_copy

    if len(fp_id_row) < 20:
        current_logger.error('error :: %s :: failed to get_ionosphere_fp_db_row for fp_id: %s' % (
            function_str, str(fp_id)))
        if current_skyline_app == 'webapp':
            # Raise to webapp
            raise
        return fp_created_via_copy

    # Get base_name
    original_base_name = None
    try:
        metric_id = fp_id_row['metric_id']
        original_base_name = get_base_name_from_metric_id(current_skyline_app, metric_id)
    except Exception as err:
        current_logger.error('error :: %s :: failed to get_base_name_from_metric_id for metric_id: %s, err: %s' % (
            function_str, str(fp_id_row['metric_id']), err))
        if current_skyline_app == 'webapp':
            # Raise to webapp
            raise
        return fp_created_via_copy
    use_base_name = str(original_base_name)
    if '_tenant_id=' in original_base_name:
        use_base_name = 'labelled_metrics.%s' % str(metric_id)
    fp_timestamp = fp_id_row['anomaly_timestamp']
    metric_timeseries_dir = use_base_name.replace('.', '/')
    src_features_profile_dir = '%s/%s' % (
        settings.IONOSPHERE_PROFILES_FOLDER, metric_timeseries_dir)
    src_features_profile_dir = '%s/%s' % (
        src_features_profile_dir, str(fp_timestamp))

    target_base_name = None
    try:
        target_base_name = get_base_name_from_metric_id(current_skyline_app, target_metric_id)
    except Exception as err:
        current_logger.error('error :: %s :: failed to get_base_name_from_metric_id for metric_id: %s, err: %s' % (
            function_str, str(target_metric_id), err))
        if current_skyline_app == 'webapp':
            # Raise to webapp
            raise
        return fp_created_via_copy
    use_target_base_name = str(target_base_name)
    if '_tenant_id=' in target_base_name:
        use_target_base_name = 'labelled_metrics.%s' % str(target_metric_id)
    target_metric_timeseries_dir = use_target_base_name.replace('.', '/')
    dest_features_profile_dir = '%s/%s' % (
        settings.IONOSPHERE_PROFILES_FOLDER, target_metric_timeseries_dir)
    dest_features_profile_dir = '%s/%s' % (
        dest_features_profile_dir, str(fp_timestamp))

    # Determine that the features_profile assets exist



    try:
        engine, fail_msg, trace = get_engine(current_skyline_app)
    except Exception as err:
        trace = traceback.format_exc()
        current_logger.error(trace)
        fail_msg = 'error :: %s :: could not get a MySQL engine - %s' % (function_str, err)
        current_logger.error('%s' % fail_msg)
        if engine:
            engine_disposal(current_skyline_app, engine)
        if current_skyline_app == 'webapp':
            # Raise to webapp
            raise
        return fp_created_via_copy
    try:
        ionosphere_table, log_msg, trace = ionosphere_table_meta(current_skyline_app, engine)
    except Exception as err:
        current_logger.error(traceback.format_exc())
        current_logger.error('error :: %s :: failed to get ionosphere_table meta, err: %s' % (
            function_str, err))
        if engine:
            engine_disposal(engine)
        if current_skyline_app == 'webapp':
            # Raise to webapp
            raise
        return fp_created_via_copy
    try:
        alias_features_profile_table, log_msg, trace = alias_features_profile_table_meta(current_skyline_app, engine)
    except Exception as err:
        current_logger.error(traceback.format_exc())
        current_logger.error('error :: %s :: failed to get alias_features_profile_table meta, err: %s' % (
            function_str, err))
        if engine:
            engine_disposal(engine)
        if current_skyline_app == 'webapp':
            # Raise to webapp
            raise
        return fp_created_via_copy

    # @added 20250122 - Feature #5592: tenant_id column in DB tables
    tenant_id = 0
    try:
        tenant_id = get_tenant_id(current_skyline_app, metric_id=metric_id, base_name=None, log=False)
    except Exception as err:
        current_logger.error('error :: create_features_profile :: get_tenant_id failed, err: %s' % (
            err))
        tenant_id = 0

    # Create DB ionosphere record
    new_fp_id = False
    try:
        #connection = engine.connect()
        ins = ionosphere_table.insert().values(
            metric_id=int(target_metric_id),
            # @added 20250122 - Feature #5592: tenant_id column in DB tables
            tenant_id=tenant_id,
            full_duration=int(fp_id_row['full_duration']),
            anomaly_timestamp=int(fp_id_row['anomaly_timestamp']),
            enabled=1, tsfresh_version=fp_id_row['tsfresh_version'],
            calc_time=fp_id_row['calc_time'],
            features_count=fp_id_row['features_count'],
            features_sum=fp_id_row['features_sum'],
            parent_id=fp_id_row['id'],
            generation=fp_id_row['generation'],
            validated=fp_id_row['validated'],
            echo_fp=fp_id_row['echo_fp'],
            #created_timestamp=db_created_timestamp,
            user_id=user_id, label=label, alias_id=alias_fp_id)

        # @modified 20260226 - Task #5176: Migrate to sqlalchemy v2 API
        #                      Task #5628: Build v5.0.0 and test
        #result = connection.execute(ins)
        #connection.close()
        #new_fp_id = result.inserted_primary_key[0]
        with engine.begin() as connection:
            result = connection.execute(ins)
            new_fp_id = int(result.rowcount)

        current_logger.info('create_features_profile :: new ionosphere fp_id: %s' % str(new_fp_id))
    except Exception as err:
        trace = traceback.format_exc()
        current_logger.error('%s' % trace)
        fail_msg = 'error :: create_features_profile :: failed to insert a new record into the ionosphere table for %s, err: %s' % (
            target_base_name, err)
        current_logger.error('%s' % fail_msg)
        if engine:
            current_logger.info('create_features_profile :: disposing of any engine')
            engine_disposal(current_skyline_app, engine)
        if current_skyline_app == 'webapp':
            # Raise to webbapp to provide traceback to user in UI
            raise
        return fp_created_via_copy

####

    # Create z_fp_<metric_id> table
    fp_table_created = False
    fp_table_name = 'z_fp_%s' % str(target_metric_id)
    try:
        fp_meta = MetaData()
        fp_metric_table = Table(
            fp_table_name, fp_meta,
            Column('id', Integer, primary_key=True),
            Column('fp_id', Integer, nullable=False, key='fp_id'),
            Column('feature_id', Integer, nullable=False),
            Column('value', DOUBLE(), nullable=True),
            mysql_charset='utf8',
            mysql_engine='InnoDB')
        fp_metric_table.create(engine, checkfirst=True)
        fp_table_created = True
    except:
        trace = traceback.format_exc()
        current_logger.error('%s' % trace)
        fail_msg = 'error :: create_features_profile :: failed to create table - %s' % fp_table_name
        current_logger.error('%s' % fail_msg)
        # if context == 'training' or context == 'features_profile':
        if context in ['training', 'features_profile']:
            # @added 20170806 - Bug #2130: MySQL - Aborted_clients
            # Added missing disposal
            if engine:
                fp_create_engine_disposal(current_skyline_app, engine)
            # Raise to webbapp I believe to provide traceback to user in UI
            raise
        current_logger.info('create_features_profile :: %s - automated so the table should exists continuing' % context)

    if not fp_table_created:
        trace = 'none'
        fail_msg = 'error :: create_features_profile :: failed to determine True for create table - %s' % fp_table_name
        current_logger.error('%s' % fail_msg)
        # if context == 'training' or context == 'features_profile':
        if context in ['training', 'features_profile']:
            # @added 20170806 - Bug #2130: MySQL - Aborted_clients
            # Added missing disposal
            if engine:
                fp_create_engine_disposal(current_skyline_app, engine)
            # Raise to webbapp I believe to provide traceback to user in UI
            raise ValueError(fail_msg)
        current_logger.info('create_features_profile :: %s - automated so the table should exists continuing' % context)

    # Insert features and values
    insert_statement = []
    for fname_id, f_value in features_data:
        insert_statement.append({'fp_id': new_fp_id, 'feature_id': fname_id, 'value': f_value},)
    # if insert_statement == []:
    if not insert_statement:
        trace = traceback.format_exc()
        current_logger.error('%s' % trace)
        fail_msg = 'error :: create_features_profile :: empty insert_statement for %s inserts' % fp_table_name
        current_logger.error('%s' % fail_msg)
        # raise
    # else:
        # feature_count = sum(1 for x in a if isinstance(x, insert_statement))
        # current_logger.info(
        #     'fp_id - %s - %s feature values in insert_statement for %s ' %
        #     (str(feature_count), str(new_fp_id), fp_table_name))
        # feature_count = sum(1 for x in a if isinstance(x, insert_statement))
        # current_logger.info(
        #     'fp_id - %s - feature values in insert_statement for %s ' %
        #     (str(new_fp_id), fp_table_name))

    try:
        # @modified 20260227 - Task #5176: Migrate to sqlalchemy v2 API
        #                      Task #5628: Build v5.0.0 and test
        #connection = engine.connect()
        #connection.execute(fp_metric_table.insert(), insert_statement)
        #connection.close()
        with engine.begin() as connection:
            connection.execute(insert(fp_metric_table), insert_statement)

        current_logger.info('create_features_profile :: fp_id - %s - feature values inserted into %s' % (str(new_fp_id), fp_table_name))
    except:
        trace = traceback.format_exc()
        current_logger.error('%s' % trace)
        fail_msg = 'error :: create_features_profile :: failed to insert a feature values into %s' % fp_table_name
        current_logger.error('%s' % fail_msg)
        # if context == 'training' or context == 'features_profile':
        if context in ['training', 'features_profile']:
            # @added 20170806 - Bug #2130: MySQL - Aborted_clients
            # Added missing disposal
            if engine:
                fp_create_engine_disposal(current_skyline_app, engine)
            # Raise to webbapp I believe to provide traceback to user in UI
            raise
        current_logger.info('create_features_profile :: %s - automated so the table should exists continuing' % context)

    # Create metric ts table if not exists ts_<metric_id>
    # Create z_ts_<metric_id> table
    # @modified 20170121 - Feature #1854: Ionosphere learn - generations
    # TODO Adding the option to not save timeseries to DB, as default?
    # ts_table_created = False
    ts_table_name = 'z_ts_%s' % str(metrics_id)
    try:
        ts_meta = MetaData()
        # @modified 20161222 - Task #1812: z_fp table type
        # Changed to InnoDB from MyISAM as no files open issues and MyISAM clean
        # up, there can be LOTS of file_per_table z_fp_ tables/files without
        # the MyISAM issues.  z_fp_ tables are mostly read and will be shuffled
        # in the table cache as required.
        ts_metric_table = Table(
            ts_table_name, ts_meta,
            Column('id', Integer, primary_key=True),
            Column('fp_id', Integer, nullable=False, key='fp_id'),
            Column('timestamp', Integer, nullable=False),
            Column('value', DOUBLE(), nullable=True),
            mysql_charset='utf8',
            # @modified 20180324 - Bug #2340: MySQL key_block_size
            #                      MySQL key_block_size #45
            # Removed as under MySQL 5.7 breaks
            # mysql_key_block_size='255',
            mysql_engine='InnoDB')
        ts_metric_table.create(engine, checkfirst=True)
        # ts_table_created = True
        current_logger.info('create_features_profile :: metric ts table created OK - %s' % (ts_table_name))
    except:
        trace = traceback.format_exc()
        current_logger.error('%s' % trace)
        fail_msg = 'error :: create_features_profile :: failed to create table - %s' % ts_table_name
        current_logger.error('%s' % fail_msg)
        # if context == 'training' or context == 'features_profile':
        if context in ['training', 'features_profile']:
            # @added 20170806 - Bug #2130: MySQL - Aborted_clients
            # Added missing disposal
            if engine:
                fp_create_engine_disposal(current_skyline_app, engine)
            # Raise to webbapp I believe to provide traceback to user in UI
            raise
        current_logger.info('create_features_profile :: %s - automated so the table should exists continuing' % context)

    # Insert timeseries that the features profile was created from
    raw_timeseries = []
    anomaly_json = '%s/%s.json' % (metric_training_data_dir, use_base_name)

    # @added 20190327 - Feature #2484: FULL_DURATION feature profiles
    # if context == 'ionosphere_echo' or context == 'ionosphere_echo_check':
    if context in ['ionosphere_echo', 'ionosphere_echo_check']:
        i_full_duration_in_hours = int(settings.FULL_DURATION / 60 / 60)
        anomaly_json = '%s/%s.mirage.redis.%sh.json' % (metric_training_data_dir, use_base_name, str(i_full_duration_in_hours))

    if path.isfile(anomaly_json):
        current_logger.info('create_features_profile :: metric anomaly json found OK - %s' % (anomaly_json))
        try:
            # Read the timeseries json file
            with open(anomaly_json, 'r') as f:
                raw_timeseries = f.read()
        except:
            trace = traceback.format_exc()
            current_logger.error(trace)
            fail_msg = 'error :: create_features_profile :: failed to read timeseries data from %s' % anomaly_json
            current_logger.error('%s' % (fail_msg))
            fail_msg = 'error: failed to read timeseries data from %s' % anomaly_json
            # end = timer()
            # if context == 'training' or context == 'features_profile':
            if context in ['training', 'features_profile']:
                # @added 20170806 - Bug #2130: MySQL - Aborted_clients
                # Added missing disposal
                if engine:
                    fp_create_engine_disposal(current_skyline_app, engine)
                # Raise to webbapp I believe to provide traceback to user in UI
                raise
    else:
        trace = 'none'
        fail_msg = 'error: file not found - %s' % (anomaly_json)
        current_logger.error(fail_msg)
        # raise

    # Convert the timeseries to csv
    timeseries_array_str = str(raw_timeseries).replace('(', '[').replace(')', ']')
    # @added 20250403 - Task #5591: get_victoriametrics_metric - switch from query_range to export
    if 'nan' in timeseries_array_str:
        try:
            timeseries_array_str = str(timeseries_array_str).replace('nan', 'None').replace('NaN', 'None')
        except Exception as err:
            current_logger.error('error :: failed to replace nan with None, err: %s' % (
                err))

    del raw_timeseries
    timeseries = literal_eval(timeseries_array_str)

    datapoints = timeseries
    validated_timeseries = []
    for datapoint in datapoints:
        try:
            new_datapoint = [str(int(datapoint[0])), float(datapoint[1])]
            validated_timeseries.append(new_datapoint)
        # @modified 20170913 - Task #2160: Test skyline with bandit
        # Added "nosec" to exclude from bandit tests
        except:  # nosec
            continue

    del timeseries
    del timeseries_array_str
    del datapoints

    insert_statement = []
    for ts, value in validated_timeseries:
        insert_statement.append({'fp_id': new_fp_id, 'timestamp': ts, 'value': value},)
    try:
        connection = engine.connect()
        connection.execute(ts_metric_table.insert(), insert_statement)
        connection.close()
        current_logger.info('create_features_profile :: fp_id - %s - timeseries inserted into %s' % (str(new_fp_id), ts_table_name))
    except:
        trace = traceback.format_exc()
        current_logger.error('%s' % trace)
        fail_msg = 'error :: create_features_profile :: failed to insert the timeseries into %s' % ts_table_name
        current_logger.error('%s' % fail_msg)
        # if context == 'training' or context == 'features_profile':
        if context in ['training', 'features_profile']:
            # @added 20170806 - Bug #2130: MySQL - Aborted_clients
            # Added missing disposal
            if engine:
                fp_create_engine_disposal(current_skyline_app, engine)
            raise
        current_logger.info('create_features_profile :: %s - automated so the table should exist continuing' % context)


####


    # Copy data from training data dir to features_profiles dir
    if not path.isdir(dest_features_profile_dir):
        mkdir_p(dest_features_profile_dir)

    if path.isdir(dest_features_profile_dir):
        current_logger.info('create_features_profile :: fp_id - %s - features profile dir created - %s' % (str(new_fp_id), dest_features_profile_dir))
        # src_files = os.listdir(src)
        # for file_name in src_files:
        #    full_file_name = path.join(src, file_name)
        #    if (path.isfile(full_file_name)):
        #        shutil.copy(full_file_name, dest)

        data_files = []
        try:
            glob_path = '%s/*.*' % src_features_profile_dir
            data_files = glob.glob(glob_path)
        except:
            trace = traceback.format_exc()
            current_logger.error('%s' % trace)
            current_logger.error('error :: create_features_profile :: glob - fp_id - %s - training data not copied to %s' % (str(new_fp_id), dest_features_profile_dir))

        for i_file in data_files:
            try:
                shutil.copy(i_file, dest_features_profile_dir)
                current_logger.info('create_features_profile :: fp_id - %s - training data copied - %s' % (str(new_fp_id), i_file))
            except shutil.Error as e:
                trace = traceback.format_exc()
                current_logger.error('%s' % trace)
                current_logger.error('error :: create_features_profile :: shutil error - fp_id - %s - training data not copied to %s' % (str(new_fp_id), ts_features_profile_dir))
                current_logger.error('error :: create_features_profile :: %s' % (e))
            # Any error saying that the directory doesn't exist
            except OSError as e:
                trace = traceback.format_exc()
                current_logger.error('%s' % trace)
                current_logger.error('error :: create_features_profile :: OSError error - fp_id - %s - training data not copied to %s' % (str(new_fp_id), ts_features_profile_dir))
                current_logger.error('error :: create_features_profile :: %s' % (e))
        current_logger.info('create_features_profile :: fp_id - %s - training data copied to %s' % (str(new_fp_id), ts_features_profile_dir))


    return fp_created_via_copy
