"""
This script creates any missing Ionosphere `z_fp_` and `z_ts_` tables that may be
missing from the database using the raw data in the IONOSPHERE_DATA_FOLDER if
there is any.

Run the script with your relevant paths and Python version for example:

.. code-block:: bash

    # BACKUP the skyline MySQL database FIRST

    PYTHON_MAJOR_VERSION="2.7"
    PYTHON_VIRTUALENV_DIR="/opt/python_virtualenv"
    PROJECT="skyline-py2712"

    cd "${PYTHON_VIRTUALENV_DIR}/projects/${PROJECT}"
    source bin/activate
    cd /opt/skyline/github/skyline
    python${PYTHON_MAJOR_VERSION} skyline/tsfresh_features/autobuild_features_profile_tables.py
    deactivate

"""

import os
import sys
import csv
from ast import literal_eval

import traceback

import sqlalchemy
from sqlalchemy import (
    create_engine, Column, Table, Integer, MetaData)
from sqlalchemy.dialects.mysql import DOUBLE
from sqlalchemy.sql import select

current_dir = os.path.dirname(os.path.realpath(__file__))
parent_dir = os.path.join(os.path.dirname(os.path.realpath(current_dir)))
root_dir = os.path.join(os.path.dirname(os.path.realpath(parent_dir)))

skyline_dir = parent_dir
sys.path.append(skyline_dir)

# @modified 20191115 - Branch #3262: py3
# This prevents flake8 E402 - module level import not at top of file
if True:
    import settings
    # @added 20191029 - Task #3302: Handle csv.reader in py3
    #                   Branch #3262: py3
#    from skyline_functions import read_csv

    from tsfresh_feature_names import TSFRESH_FEATURES
    from database import (ionosphere_table_meta, metrics_table_meta)

if __name__ == '__main__':

    tables_created = 0

    def get_engine():
        try:
            engine = create_engine(
                'mysql+mysqlconnector://%s:%s@%s:%s/%s' % (
                    settings.PANORAMA_DBUSER, settings.PANORAMA_DBUSERPASS,
                    settings.PANORAMA_DBHOST, str(settings.PANORAMA_DBPORT),
                    settings.PANORAMA_DATABASE))
            return engine, 'got MySQL engine', 'none'
        except:
            print('error :: DB engine not obtained')
            print(traceback.format_exc())
            return None

    try:
        engine, log_msg, trace = get_engine()
    except:
        print(traceback.format_exc())

    if not engine:
        print('error :: engine not obtained to get fp_ids')

    try:
        ionosphere_table, log_msg, trace = ionosphere_table_meta('autobuild', engine)
    except:
        print(traceback.format_exc())

    # get all fp ids and metric ids from ionosphere table
    fps_data = []
    try:
        connection = engine.connect()
        stmt = select([ionosphere_table]).where(ionosphere_table.c.metric_id > 0)
        result = connection.execute(stmt)
        for row in result:
            if row['enabled'] != 1:
                continue
            if row['deleted'] == 1:
                continue
            fp_id = row['id']
            metric_id = row['metric_id']
            anomaly_timestamp = row['anomaly_timestamp']
            fps_data.append([int(fp_id), int(metric_id), int(anomaly_timestamp)])
        connection.close()
    except:
        print('Failed to get ionosphere data from the database')

    try:
        metrics_table, log_msg, trace = metrics_table_meta('autobuild', engine)
    except:
        print(traceback.format_exc())

    # get all fp ids and metric ids from ionosphere table
    ionosphere_metrics_data = []
    try:
        connection = engine.connect()
        stmt = select([metrics_table]).where(metrics_table.c.ionosphere_enabled > 0)
        result = connection.execute(stmt)
        for row in result:
            metric_id = row['id']
            metric = row['metric']
            ionosphere_metrics_data.append([int(metric_id), str(metric)])
        connection.close()
    except:
        print('Failed to get metrics data from the database')

    create_tables_for = []
    for fp_id, metric_id, anomaly_timestamp in fps_data:
        z_fp_table_name = 'z_fp_%s' % str(metric_id)
        if not engine.dialect.has_table(engine, z_fp_table_name):  # If table don't exist, Create.
            metric = None
            for metrics_id, metric_name in ionosphere_metrics_data:
                if metric_id == metrics_id:
                    metric = metric_name
            create_tables_for.append([int(fp_id), int(metric_id), str(metric), int(anomaly_timestamp)])

    create_tables_for_has_data = []
    for fp_id, metrics_id, metric, anomaly_timestamp in create_tables_for:
        metric_dir_namespace = metric.replace('.', '/')
        fp_dir = '%s/%s/%s' % (settings.IONOSPHERE_PROFILES_FOLDER, metric_dir_namespace, str(anomaly_timestamp))
        has_ts_json = False
        has_features_transposed = False
        anomaly_json = '%s/%s.json' % (fp_dir, metric)
        if os.path.isfile(anomaly_json):
            has_ts_json = True
        features_file = '%s/%s.tsfresh.input.csv.features.transposed.csv' % (fp_dir, metric)
        if os.path.isfile(features_file):
            has_features_transposed = True
        create_tables = False
        if has_ts_json and has_features_transposed:
            create_tables = True

        if not create_tables:
            continue

        # @added 20191029 - Task #3302: Handle csv.reader in py3
        #                      Branch #3262: py3
        try:
            python_version
        except:
            from sys import version_info
            python_version = int(version_info[0])
        if python_version == 3:
            try:
                codecs
            except:
                import codecs

        features_data = []

        with open(features_file, 'rb') as fr:
            # @modified 20191029 - Task #3302: Handle csv.reader in py3
            #                      Branch #3262: py3
            # reader = csv.reader(fr, delimiter=',')
            if python_version == 2:
                reader = csv.reader(fr, delimiter=',')
            else:
                reader = csv.reader(codecs.iterdecode(fr, 'utf-8'), delimiter=',')

            for i, line in enumerate(reader):
                feature_name_item = False
                fname_id = False
                f_value = False
                feature_name = str(line[0])
                feature_name_item = filter(
                    lambda x: x[1] == feature_name, TSFRESH_FEATURES)
                if feature_name_item:
                    feature_name_id = feature_name_item[0]
                if feature_name_item:
                    feature_name_list = feature_name_item[0]
                    fname_id = int(feature_name_list[0])
                f_value = str(line[1])
                if fname_id and f_value:
                    features_data.append([fname_id, f_value])

        fp_table_name = 'z_fp_%s' % str(metrics_id)
        if not engine.dialect.has_table(engine, fp_table_name):  # If table don't exist, Create.
            # Create z_fp_<metric_id> table
            fp_table_created = False
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
                tables_created += 1
            except:
                print(traceback.format_exc())
                print('error :: create_features_profile :: failed to create table - %s' % fp_table_name)

            if not fp_table_created:
                continue

            # Insert features and values
            insert_statement = []
            for fname_id, f_value in features_data:
                insert_statement.append({'fp_id': fp_id, 'feature_id': fname_id, 'value': f_value},)
            if insert_statement == []:
                print(traceback.format_exc())
                print('error :: create_features_profile :: empty insert_statement for %s inserts' % fp_table_name)
                continue

            try:
                connection = engine.connect()
                connection.execute(fp_metric_table.insert(), insert_statement)
                connection.close()
                print('create_features_profile :: fp_id - %s - feature values inserted into %s' % (str(fp_id), fp_table_name))
            except:
                print(traceback.format_exc())
                print('error :: create_features_profile :: failed to insert a feature values into %s' % fp_table_name)
                continue

        ts_table_name = 'z_ts_%s' % str(metrics_id)
        if not engine.dialect.has_table(engine, ts_table_name):  # If table don't exist, Create.
            # Create metric ts table if not exists ts_<metric_id>
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
                # ts_table_created = True
                print('create_features_profile :: metric ts table created OK - %s' % (ts_table_name))
                tables_created += 1
            except:
                print(traceback.format_exc())
                print('error :: create_features_profile :: failed to create table - %s' % ts_table_name)
                continue

            # Insert timeseries that the features profile was created from
            raw_timeseries = []
            try:
                # Read the timeseries json file
                with open(anomaly_json, 'r') as f:
                    raw_timeseries = f.read()
            except:
                print(traceback.format_exc())
                print('error :: create_features_profile :: failed to read timeseries data from %s' % anomaly_json)
                continue

            # Convert the timeseries to csv
            timeseries_array_str = str(raw_timeseries).replace('(', '[').replace(')', ']')
            timeseries = literal_eval(timeseries_array_str)

            datapoints = timeseries
            validated_timeseries = []
            for datapoint in datapoints:
                try:
                    new_datapoint = [str(int(datapoint[0])), float(datapoint[1])]
                    validated_timeseries.append(new_datapoint)
                # @modified 20170913 - Task #2160: Test skyline with bandit
                # Added nosec to exclude from bandit tests
                except:  # nosec
                    continue

            insert_statement = []
            for ts, value in validated_timeseries:
                insert_statement.append({'fp_id': fp_id, 'timestamp': ts, 'value': value},)
            try:
                connection = engine.connect()
                connection.execute(ts_metric_table.insert(), insert_statement)
                connection.close()
                print('create_features_profile :: fp_id - %s - timeseries inserted into %s' % (str(fp_id), ts_table_name))
            except:
                print(traceback.format_exc())
                print('error :: create_features_profile :: failed to insert the timeseries into %s' % ts_table_name)

    print('info :: %s tables created' % str(tables_created))
