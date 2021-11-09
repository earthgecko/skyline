import logging
import os
import traceback

import pandas as pd
from sqlalchemy.sql import select
from adtk.visualization import plot

import settings
from functions.database.queries.get_cloudburst_row import get_cloudburst_row
from functions.graphite.get_metrics_timeseries import get_metrics_timeseries
from database import get_engine, engine_disposal, cloudburst_table_meta

skyline_app = 'webapp'
skyline_app_logger = '%sLog' % skyline_app
logger = logging.getLogger(skyline_app_logger)
skyline_app_logfile = '%s/%s.log' % (settings.LOG_PATH, skyline_app)
logfile = '%s/%s.log' % (settings.LOG_PATH, skyline_app)


def get_cloudburst_plot(cloudburst_id, base_name, shift, all_in_period=False):
    """
    Create a plot of the cloudburst and return the path and filename

    :param cloudburst_id: the cloudburt id
    :param base_name: the name of the metric
    :param shift: the number of indice to shift the plot
    :type cloudburst_id: int
    :type base_name: str
    :type shift: int
    :return: path and file
    :rtype:  str

    """

    function_str = 'get_cloudburst_plot'

    logger.info(
        'get_cloudburst_plot - cloudburst_id: %s, base_name: %s' % (
            str(cloudburst_id), str(base_name)))

    save_to_file = '%s/cloudburst_id.%s.%s.shift.%s.png' % (
        settings.SKYLINE_TMP_DIR, str(cloudburst_id), base_name, str(shift))
    if all_in_period:
        save_to_file = '%s/cloudburst_id.%s.all.%s.shift.%s.png' % (
            settings.SKYLINE_TMP_DIR, str(cloudburst_id), base_name, str(shift))

    cloudburst_dict = {}
    try:
        cloudburst_dict = get_cloudburst_row(skyline_app, cloudburst_id)
    except Exception as err:
        logger.error(traceback.format_exc())
        logger.error('error :: %s :: get_cloudburst_row failed - %s' % (
            function_str, err))
        raise

    if not cloudburst_dict:
        logger.error('error :: %s :: no cloudburst_dict - %s' % function_str)
        return None, None

    if os.path.isfile(save_to_file):
        return cloudburst_dict, save_to_file

    try:
        from_timestamp = cloudburst_dict['from_timestamp']
        until_timestamp = from_timestamp + cloudburst_dict['full_duration']
        resolution = cloudburst_dict['resolution']
    except Exception as err:
        logger.error(traceback.format_exc())
        logger.error('error :: %s :: failed parse values from cloudburst_dict - %s' % (
            function_str, err))
        raise

    metrics_functions = {}
    metrics_functions[base_name] = {}
    metrics_functions[base_name]['functions'] = None

    if resolution > 60:
        resolution_minutes = int(resolution / 60)
        summarize_intervalString = '%smin' % str(resolution_minutes)
        summarize_func = 'median'
        metrics_functions[base_name]['functions'] = {'summarize': {'intervalString': summarize_intervalString, 'func': summarize_func}}

    try:
        metrics_timeseries = get_metrics_timeseries(skyline_app, metrics_functions, from_timestamp, until_timestamp, log=False)
    except Exception as err:
        logger.error(traceback.format_exc())
        logger.error('error :: %s :: get_metrics_timeseries failed - %s' % (
            function_str, err))
        raise

    try:
        timeseries = metrics_timeseries[base_name]['timeseries']
        timeseries_length = len(timeseries)
        timeseries = timeseries[1:(timeseries_length - 2)]
    except Exception as err:
        logger.error(traceback.format_exc())
        logger.error('error :: %s :: failed to determine timeseries - %s' % (
            function_str, err))
        raise

    anomalies_in_period = []
    if all_in_period:
        try:
            engine, fail_msg, trace = get_engine(skyline_app)
        except Exception as err:
            trace = traceback.format_exc()
            logger.error(trace)
            fail_msg = 'error :: %s :: could not get a MySQL engine - %s' % (function_str, err)
            logger.error('%s' % fail_msg)
            if engine:
                engine_disposal(skyline_app, engine)
            raise
        try:
            cloudburst_table, log_msg, trace = cloudburst_table_meta(skyline_app, engine)
        except Exception as err:
            logger.error(traceback.format_exc())
            logger.error('error :: %s :: failed to get cloudburst_table meta for cloudburst id %s - %s' % (
                function_str, str(cloudburst_id), err))
            if engine:
                engine_disposal(engine)
            raise
        try:
            connection = engine.connect()
            stmt = select([cloudburst_table]).\
                where(cloudburst_table.c.metric_id == cloudburst_dict['metric_id']).\
                where(cloudburst_table.c.timestamp >= from_timestamp).\
                where(cloudburst_table.c.timestamp <= until_timestamp).\
                where(cloudburst_table.c.id != cloudburst_id)
            result = connection.execute(stmt)
            for row in result:
                anomalies_in_period.append([row['timestamp'], row['end']])
            connection.close()
        except Exception as err:
            logger.error(traceback.format_exc())
            logger.error('error :: %s :: could not get cloudburst row for cloudburst id %s - %s' % (
                function_str, str(cloudburst_id), err))
            if engine:
                engine_disposal(engine)
            raise
        if engine:
            engine_disposal(skyline_app, engine)

    anomalies = []
    if anomalies_in_period:
        logger.info(
            'get_cloudburst_plot - adding %s all_in_period anomalies to cloudburst plot' % (
                str(len(anomalies_in_period))))
        for period_anomalies in anomalies_in_period:
            new_anomalies = [item for item in timeseries if int(item[0]) >= period_anomalies[0] and int(item[0]) <= period_anomalies[1]]
            if new_anomalies:
                anomalies = anomalies + new_anomalies
    try:
        cloudburst_anomalies = [item for item in timeseries if int(item[0]) >= cloudburst_dict['timestamp'] and int(item[0]) <= cloudburst_dict['end']]
        anomalies = anomalies + cloudburst_anomalies
        df = pd.DataFrame(timeseries, columns=['date', 'value'])
        df['date'] = pd.to_datetime(df['date'], unit='s')
        datetime_index = pd.DatetimeIndex(df['date'].values)
        df = df.set_index(datetime_index)
        df.drop('date', axis=1, inplace=True)
        anomalies_data = []
        # @modified 20210831
        # Align periods
        # anomaly_timestamps = [int(item[0]) for item in anomalies]
        # anomaly_timestamps = [(int(item[0]) + (resolution * 2)) for item in anomalies]
        # anomaly_timestamps = [(int(item[0]) + (resolution * 6)) for item in anomalies]
        # anomaly_timestamps = [(int(item[0]) + (resolution * 4)) for item in anomalies]
        # anomaly_timestamps = [(int(item[0]) + (resolution * 3)) for item in anomalies]
        anomaly_timestamps = [(int(item[0]) + (resolution * shift)) for item in anomalies]
        for item in timeseries:
            if int(item[0]) in anomaly_timestamps:
                anomalies_data.append(1)
            else:
                anomalies_data.append(0)
        df['anomalies'] = anomalies_data
        title = '%s\ncloudburst id: %s' % (base_name, str(cloudburst_id))
        if all_in_period:
            title = '%s (all in period)' % title
        plot(df['value'], anomaly=df['anomalies'], anomaly_color='red', title=title, save_to_file=save_to_file)
    except Exception as err:
        logger.error(traceback.format_exc())
        logger.error('error :: %s :: failed to plot cloudburst - %s' % (
            function_str, err))
        raise

    if not os.path.isfile(save_to_file):
        return cloudburst_dict, None

    return cloudburst_dict, save_to_file
