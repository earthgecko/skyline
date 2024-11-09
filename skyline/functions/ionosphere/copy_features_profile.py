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
from time import time
import datetime
import copy

from sqlalchemy import (
    Column, Table, Integer, MetaData)
from sqlalchemy.dialects.mysql import DOUBLE
from sqlalchemy.sql import select

import settings
from skyline_functions import mkdir_p, write_data_to_file

from functions.database.queries.get_ionosphere_fp_row import get_ionosphere_fp_db_row
from functions.metrics.get_base_name_from_metric_id import get_base_name_from_metric_id
from functions.database.queries.fp_timeseries import get_db_fp_timeseries
from database import (
    get_engine, engine_disposal, ionosphere_table_meta, 
    alias_features_profile_table_meta,
)

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
            'alias_id': alias_fp_id
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
        original_fp_id = int(fp_id)
        fp_id_row = get_ionosphere_fp_db_row(current_skyline_app, original_fp_id)
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
        original_metric_id = int(metric_id)
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

    # Copy the features_profile file resources
    data_files = []
    try:
        glob_path = '%s/*.*' % src_features_profile_dir
        data_files = glob.glob(glob_path)
    except Exception as err:
        current_logger.error('error :: %s :: glob.glob failed on src_features_profile_dir: %s for metric_id: %s, err: %s' % (
            function_str, src_features_profile_dir, str(metric_id), err))
        if current_skyline_app == 'webapp':
            # Raise to webapp
            raise
        return fp_created_via_copy
    if not path.isdir(dest_features_profile_dir):
        mkdir_p(dest_features_profile_dir)
    for i_file in data_files:
        try:
            src_filename = path.basename(i_file)
            dest_filename = src_filename.replace(use_base_name, use_target_base_name)
            dest_file = '%s/%s' % (dest_features_profile_dir, dest_filename)
            # This is idempotent and will copy and replace if the files already
            # exist
            shutil.copy(i_file, dest_file)
            current_logger.info('%s :: training data copied - %s to %s' % (
                function_str, i_file, dest_file))
        except shutil.Error as err:
            current_logger.error('error :: %s :: shutil err: %s' % (function_str, err))
        # Any error saying that the directory doesn't exist
        except OSError as os_err:
            current_logger.error('error :: %s :: OSError err: %s' % (function_str, os_err))

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

    # Create DB ionosphere record
    new_fp_id = False
    try:
        connection = engine.connect()
        ins = ionosphere_table.insert().values(
            metric_id=int(target_metric_id),
            full_duration=int(fp_id_row['full_duration']),
            anomaly_timestamp=int(fp_id_row['anomaly_timestamp']),
            enabled=1, tsfresh_version=fp_id_row['tsfresh_version'],
            calc_time=fp_id_row['calc_time'],
            features_count=fp_id_row['features_count'],
            features_sum=float(fp_id_row['features_sum']),
            parent_id=fp_id_row['id'],
            generation=fp_id_row['generation'],
            validated=fp_id_row['validated'],
            echo_fp=fp_id_row['echo_fp'],
            #created_timestamp=db_created_timestamp,
            user_id=user_id, label=label, alias_id=alias_fp_id)
        result = connection.execute(ins)
        connection.close()
        new_fp_id = result.inserted_primary_key[0]
        current_logger.info('%s :: copied to new ionosphere fp_id: %s' % (function_str, str(new_fp_id)))
    except Exception as err:
        trace = traceback.format_exc()
        current_logger.error('%s' % trace)
        fail_msg = 'error :: %s :: failed to insert a new record into the ionosphere table for %s, err: %s' % (
            function_str, target_base_name, err)
        current_logger.error('%s' % fail_msg)
        if engine:
            current_logger.info('%s :: disposing of any engine' % function_str)
            engine_disposal(current_skyline_app, engine)
        if current_skyline_app == 'webapp':
            # Raise to webbapp to provide traceback to user in UI
            raise
        return fp_created_via_copy

    # Create z_fp_<target_metric_id> table if it does not exists
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
    except Exception as err:
        trace = traceback.format_exc()
        current_logger.error('%s' % trace)
        fail_msg = 'error :: %s :: failed to create table - %s, err: %s' % (
            function_str, fp_table_name, err)
        current_logger.error('%s' % fail_msg)
        # if context == 'training' or context == 'features_profile':
        if current_skyline_app == 'webapp':
            if engine:
                engine_disposal(current_skyline_app, engine)
            # Raise to webbapp I believe to provide traceback to user in UI
            raise
        return fp_created_via_copy

    if not fp_table_created:
        trace = 'none'
        fail_msg = 'error :: %s :: failed to determine True for create table - %s' % (
            function_str, fp_table_name)
        current_logger.error('%s' % fail_msg)
        if current_skyline_app == 'webapp':
            if engine:
                engine_disposal(current_skyline_app, engine)
            # Raise to webbapp I believe to provide traceback to user in UI
            raise
        return fp_created_via_copy

    # Get features for the fp from the original z_fp_<metric_id> table
    metric_fp_table = 'z_fp_%s' % str(original_metric_id)
    fp_features = []
    try:
        use_table_meta = MetaData()
        use_table = Table(metric_fp_table, use_table_meta, autoload=True, autoload_with=engine)
    except Exception as err:
        current_logger.error(traceback.format_exc())
        current_logger.error('error :: %s :: use_table Table failed on %s table, err: %s' % (
            function_str, metric_fp_table, err))
    try:
        stmt = select([use_table.c.feature_id, use_table.c.value]).where(use_table.c.fp_id == original_fp_id)
        connection = engine.connect()
        for row in engine.execute(stmt):
            fp_feature_id = int(row['feature_id'])
            fp_value = float(row['value'])
            fp_features.append([fp_feature_id, fp_value])
        connection.close()
        current_logger.info('%s :: determined %s features for original_fp_id: %s' % (
            function_str, str(len(fp_features)), str(original_fp_id)))
    except Exception as err:
        current_logger.error(traceback.format_exc())
        current_logger.error('error :: %s :: use_table Table failed on %s table, err: %s' % (
            function_str, metric_fp_table, err))
    if not fp_features:
        trace = 'none'
        fail_msg = 'error :: %s :: failed to fp_features from the DB for original_fp_id: %s' % (
            function_str, str(original_fp_id))
        current_logger.error('%s' % fail_msg)
        if current_skyline_app == 'webapp':
            if engine:
                engine_disposal(current_skyline_app, engine)
            # Raise to webbapp to provide traceback to user in UI
            raise
        return fp_created_via_copy

    # Insert features and values
    insert_statement = []
    for fname_id, f_value in fp_features:
        insert_statement.append({'fp_id': new_fp_id, 'feature_id': fname_id, 'value': f_value},)
    if not insert_statement:
        current_logger.error('%s' % trace)
        fail_msg = 'error :: %s :: empty insert_statement for %s inserts' % (
            function_str, fp_table_name)
        current_logger.error('%s' % fail_msg)
        if current_skyline_app == 'webapp':
            if engine:
                engine_disposal(current_skyline_app, engine)
            # Raise to webbapp to provide traceback to user in UI
            raise
        return fp_created_via_copy
    try:
        connection = engine.connect()
        connection.execute(fp_metric_table.insert(), insert_statement)
        connection.close()
        current_logger.info('%s :: fp_id - %s - feature values inserted into %s' % (
            function_str, str(new_fp_id), fp_table_name))
    except Exception as err:
        trace = traceback.format_exc()
        current_logger.error('%s' % trace)
        fail_msg = 'error :: %s :: failed to insert a feature values into %s, err:  %s' % (
            function_str, fp_table_name, err)
        current_logger.error('%s' % fail_msg)
        if current_skyline_app == 'webapp':
            if engine:
                engine_disposal(current_skyline_app, engine)
            # Raise to webbapp to provide traceback to user in UI
            raise
        return fp_created_via_copy

    # Create z_ts_<target_metric_id> table if it does not exists
    ts_table_name = 'z_ts_%s' % str(target_metric_id)
    try:
        ts_meta = MetaData()
        ts_metric_table = Table(
            ts_table_name, ts_meta,
            Column('id', Integer, primary_key=True),
            Column('fp_id', Integer, nullable=False, key='fp_id'),
            Column('timestamp', Integer, nullable=False),
            Column('value', DOUBLE(), nullable=True),
            mysql_charset='utf8',
            mysql_engine='InnoDB')
        ts_metric_table.create(engine, checkfirst=True)
        current_logger.info('%s :: metric ts table created OK - %s' % (
            function_str, ts_table_name))
    except Exception as err:
        trace = traceback.format_exc()
        current_logger.error('%s' % trace)
        fail_msg = 'error :: %s :: failed to create table - %s, err: %s' % (
            function_str, ts_table_name, err)
        current_logger.error('%s' % fail_msg)
        if current_skyline_app == 'webapp':
            if engine:
                engine_disposal(current_skyline_app, engine)
            # Raise to webbapp to provide traceback to user in UI
            raise
        return fp_created_via_copy

    original_fp_id_metric_timeseries = []
    try:
        original_fp_id_metric_timeseries = get_db_fp_timeseries(current_skyline_app, original_metric_id, original_fp_id)
        current_logger.info('%s :: determined %s values for the original_fp_id time series %s' % (
            function_str, str(len(original_fp_id_metric_timeseries)), str(original_fp_id)))
    except Exception as err:
        current_logger.error(traceback.format_exc())
        current_logger.error('error :: %s :: get_db_fp_timeseries failed to get time series for original_metric_id: %s, original_fp_id: %s, err: %s' % (
            function_str, str(original_metric_id), str(original_fp_id), err))
        if current_skyline_app == 'webapp':
            if engine:
                engine_disposal(current_skyline_app, engine)
            # Raise to webbapp to provide traceback to user in UI
            raise
        return fp_created_via_copy

    # Insert original time series for the new copied features profile
    insert_statement = []
    for ts, value in original_fp_id_metric_timeseries:
        insert_statement.append({'fp_id': new_fp_id, 'timestamp': ts, 'value': value},)
    try:
        connection = engine.connect()
        connection.execute(ts_metric_table.insert(), insert_statement)
        connection.close()
        current_logger.info('%s :: fp_id: %s, timeseries inserted into %s' % (
            function_str, str(new_fp_id), ts_table_name))
    except Exception as err:
        trace = traceback.format_exc()
        current_logger.error('%s' % trace)
        fail_msg = 'error :: %s :: failed to insert the timeseries into %s, err: %s' % (
            function_str, ts_table_name, err)
        current_logger.error('%s' % fail_msg)
        if current_skyline_app == 'webapp':
            if engine:
                engine_disposal(current_skyline_app, engine)
            # Raise to webbapp to provide traceback to user in UI
            raise
        return fp_created_via_copy

    features_profile_created_file = '%s/%s.%s.fp.created.txt' % (
        dest_features_profile_dir, str(fp_timestamp), use_target_base_name)
    if path.isfile(features_profile_created_file):
        # Read the created file and replace the fp_id
        with open(features_profile_created_file, 'r') as f:
            fp_created_str = f.read()
        fp_created_list = literal_eval(fp_created_str)
        fp_created_list[0] = new_fp_id
        fp_created_list[1] = int(time())
        data = str(fp_created_list)
        write_data_to_file(current_skyline_app, features_profile_created_file, 'w', data)

    try:
        fp_created_via_copy = get_ionosphere_fp_db_row(current_skyline_app, new_fp_id)
        # Coerce features_sum from Decimal to a float
        try:
            features_sum = float(fp_created_via_copy['features_sum'])
            fp_created_via_copy['features_sum'] = features_sum
        except:
            pass
        # Coerce created_timestamp from datetime to a str
        try:
            created_date_str = str(fp_created_via_copy['created_timestamp'])
            fp_created_via_copy['created_timestamp'] = created_date_str
        except:
            pass
    except Exception as err:
        trace = traceback.format_exc()
        current_logger.error('%s' % trace)
        fail_msg = 'error :: %s :: get_ionosphere_fp_db_row failed to get data for new copied fp: %s, err: %s' % (
            function_str, new_fp_id, err)
        current_logger.error('%s' % fail_msg)

    if engine:
        engine_disposal(current_skyline_app, engine)

    return fp_created_via_copy
