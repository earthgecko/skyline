from __future__ import division
import logging
import os
from time import time
import operator
import re
from sys import version_info

import traceback
import mysql.connector
from mysql.connector import errorcode

from sqlalchemy.sql import select

import numpy as np
import scipy

import settings

from database import (
    get_engine, ionosphere_layers_table_meta, layers_algorithms_table_meta,
    ionosphere_layers_matched_table_meta)

skyline_app = 'ionosphere'
skyline_app_logger = '%sLog' % skyline_app
logger = logging.getLogger(skyline_app_logger)
skyline_app_logfile = '%s/%s.log' % (settings.LOG_PATH, skyline_app)

python_version = int(version_info[0])

this_host = str(os.uname()[1])

# Converting one settings variable into a local variable, just because it is a
# long string otherwise.
try:
    ENABLE_IONOSPHERE_DEBUG = settings.ENABLE_IONOSPHERE_DEBUG
except:
    logger.error('error :: layers :: cannot determine ENABLE_IONOSPHERE_DEBUG from settings' % skyline_app)
    ENABLE_IONOSPHERE_DEBUG = False

try:
    SERVER_METRIC_PATH = '.%s' % settings.SERVER_METRICS_NAME
    if SERVER_METRIC_PATH == '.':
        SERVER_METRIC_PATH = ''
except:
    SERVER_METRIC_PATH = ''

try:
    learn_full_duration = int(settings.IONOSPHERE_LEARN_DEFAULT_FULL_DURATION_DAYS) * 86400
except:
    learn_full_duration = 86400 * 30  # 2592000

context = 'ionosphere_layers'


def run_layer_algorithms(base_name, layers_id, timeseries):

    """
    Called by :class:`~skyline.skyline.Ionosphere.spawn_layers_process` to
    evaluate anomalies against a custom layers boundary algorithm.

    :param metric: the metric base_name
    :type metric: str
    :return: True or False
    :rtype: boolean

    """
    logger = logging.getLogger(skyline_app_logger)
    logger.info('layers :: layers_id - %s' % str(layers_id))

    def layers_get_an_engine():

        try:
            engine, fail_msg, trace = get_engine(skyline_app)
            return engine, fail_msg, trace
        except:
            trace = traceback.format_exc()
            logger.error('%s' % trace)
            fail_msg = 'error :: layers :: get_an_engine :: failed to get MySQL engine'
            logger.error('%s' % fail_msg)
            return None, fail_msg, trace

    def layers_engine_disposal(engine):
        try:
            if engine:
                try:
                    engine.dispose()
                    logger.info('layers :: MySQL engine disposed of')
                    return True
                except:
                    logger.error(traceback.format_exc())
                    logger.error('error :: calling engine.dispose()')
            else:
                logger.info('layers :: no MySQL engine to dispose of')
                return True
        except:
            return False
        return False

    engine = None
    try:
        engine, log_msg, trace = layers_get_an_engine()
    except:
        logger.error(traceback.format_exc())
        logger.error('error :: could not get a MySQL engine for layers_algorithms for layers_id %s - %s' % (str(layers_id), base_name))
        return False

    if not engine:
        logger.error('error :: engine not obtained for layers_algorithms_table for layers_id %s - %s' % (str(layers_id), base_name))
        return False

    try:
        layers_algorithms_table, log_msg, trace = layers_algorithms_table_meta(skyline_app, engine)
        logger.info(log_msg)
        logger.info('layers_algorithms_table OK')
    except:
        logger.error(traceback.format_exc())
        logger.error('error :: failed to get layers_algorithms_table meta for layers_id %s - %s' % (str(layers_id), base_name))
        if engine:
            layers_engine_disposal(engine)
        return False

    layers_algorithms_result = None
    try:
        connection = engine.connect()
        stmt = select([layers_algorithms_table]).where(layers_algorithms_table.c.layer_id == int(layers_id))
        layers_algorithms_result = connection.execute(stmt)
        connection.close()
        # @modified 20170308 - Feature #1960: ionosphere_layers
        # Not currently used
        # layer_algorithms_details_object = layers_algorithms_result
    except:
        logger.error(traceback.format_exc())
        logger.error('error :: failed to get layers_algorithms for layers_id %s - %s' % (str(layers_id), base_name))
        if engine:
            layers_engine_disposal(engine)
        return False

    layer_active = True
    es_layer = False
    f1_layer = False
    f2_layer = False
    # @added 20170616 - Feature #2048: D1 ionosphere layer
    d1_layer = False

    # @modified 20170307 - Feature #1960: ionosphere_layers
    # Use except on everything, remember how fast Skyline can iterate
    try:
        for row in layers_algorithms_result:
            current_fp_id = row['fp_id']
            current_metric_id = row['metric_id']
            layer = row['layer']
            if layer == 'D':
                d_condition = row['condition']
                d_boundary_limit = float(row['layer_boundary'])
            # @added 20170616 - Feature #2048: D1 ionosphere layer
            if layer == 'D1':
                d1_condition = row['condition']
                if str(d1_condition) != 'none':
                    d1_boundary_limit = float(row['layer_boundary'])
                    d1_boundary_times = row['times_in_row']
                    d1_layer = layer_active
            if layer == 'E':
                e_condition = row['condition']
                e_boundary_limit = float(row['layer_boundary'])
                e_boundary_times = row['times_in_row']
            if layer == 'Es':
                es_condition = row['condition']
                es_day = row['layer_boundary']
                es_layer = layer_active
            if layer == 'F1':
                f1_from_time = row['layer_boundary']
                f1_layer = layer_active
            if layer == 'F2':
                f2_until_time = row['layer_boundary']
                f2_layer = layer_active
    except:
        logger.error(traceback.format_exc())
        logger.error('error :: failed iterate layers_algorithms_result for layers_id %s - %s' % (str(layers_id), base_name))
        if engine:
            layers_engine_disposal(engine)
        return False

    # Update ionosphere_layers checked_count
    checked_timestamp = int(time())
    try:
        ionosphere_layers_table, log_msg, trace = ionosphere_layers_table_meta(skyline_app, engine)
        logger.info(log_msg)
        logger.info('ionosphere_layers_table OK')
    except:
        logger.error(traceback.format_exc())
        logger.error('error :: failed to get ionosphere_layers_table meta for layers_id %s - %s' % (str(layers_id), base_name))
        if engine:
            layers_engine_disposal(engine)
        return False
    try:
        connection = engine.connect()
        connection.execute(
            ionosphere_layers_table.update(
                ionosphere_layers_table.c.id == layers_id).
            values(check_count=ionosphere_layers_table.c.check_count + 1,
                   last_checked=checked_timestamp))
        connection.close()
        logger.info('updated check_count for %s' % str(layers_id))
    except:
        logger.error(traceback.format_exc())
        logger.error('error :: could not update check_count and last_checked for %s ' % str(layers_id))
        if engine:
            layers_engine_disposal(engine)
        return False

    not_anomalous = False
    autoaggregate = False
    autoaggregate_value = 0

    # Determine if the namespace is to be aggregated using the Boundary settings
    if settings.BOUNDARY_AUTOAGGRERATION:
        # @modified 20170307 - Feature #1960: ionosphere_layers
        # Use except on everything, remember how fast Skyline can iterate
        try:
            for autoaggregate_metric in settings.BOUNDARY_AUTOAGGRERATION_METRICS:
                autoaggregate = False
                autoaggregate_value = 0
                CHECK_MATCH_PATTERN = autoaggregate_metric[0]
                check_match_pattern = re.compile(CHECK_MATCH_PATTERN)
                pattern_match = check_match_pattern.match(base_name)
                if pattern_match:
                    autoaggregate = True
                    autoaggregate_value = autoaggregate_metric[1]
                    break
        except:
            logger.error(traceback.format_exc())
            logger.error('error :: could not determine Boundary autoaggregation settings for %s ' % str(layers_id))
            if engine:
                layers_engine_disposal(engine)
            return False

    try:
        int_end_timestamp = int(timeseries[-1][0])
        last_hour = int_end_timestamp - 3600
        last_timestamp = int_end_timestamp
        start_timestamp = last_hour
    except:
        logger.error(traceback.format_exc())
        logger.error('error :: could not determine timeseries variables for %s ' % str(layers_id))
        if engine:
            layers_engine_disposal(engine)
        return False

    use_timeseries = timeseries
    if autoaggregate:
        logger.info('layers :: aggregating timeseries at %s seconds' % str(autoaggregate_value))
        aggregated_timeseries = []
        # @modified 20170307 - Feature #1960: ionosphere_layers
        # Use except on everything, remember how fast Skyline can iterate
        try:
            next_timestamp = last_timestamp - int(autoaggregate_value)
            logger.info('layers :: aggregating from %s to %s' % (str(start_timestamp), str(int_end_timestamp)))
        except:
            logger.error(traceback.format_exc())
            logger.error('error :: could not determine timeseries variables for autoaggregation for %s ' % str(layers_id))
            if engine:
                layers_engine_disposal(engine)
            return False

        valid_timestamps = False
        try:
            valid_timeseries = int_end_timestamp - start_timestamp
            if valid_timeseries == 3600:
                valid_timestamps = True
        except:
            logger.error(traceback.format_exc())
            logger.error('error :: layers :: aggregating error - not valid_timeseries for layers_id %s - %s' % (str(layers_id), base_name))
            if engine:
                layers_engine_disposal(engine)
            return False

        if valid_timestamps:
            try:
                # Check sane variables otherwise we can just hang here in a while loop
                while int(next_timestamp) > int(start_timestamp):
                    value = np.sum(scipy.array([int(x[1]) for x in timeseries if x[0] <= last_timestamp and x[0] > next_timestamp]))
                    aggregated_timeseries += ((last_timestamp, value),)
                    last_timestamp = next_timestamp
                    next_timestamp = last_timestamp - autoaggregate_value
                aggregated_timeseries.reverse()
                use_timeseries = aggregated_timeseries
            except:
                logger.error(traceback.format_exc())
                logger.error('error :: layers :: error creating aggregated_timeseries for layers_id %s - %s' % (str(layers_id), base_name))
                if engine:
                    layers_engine_disposal(engine)
                return False

    timeseries = use_timeseries

    # @modified 20170307 - Feature #1960: ionosphere_layers
    # Use except on everything, remember how fast Skyline can iterate
    try:
        last_datapoint = timeseries[-1][1]
    except:
        logger.error(traceback.format_exc())
        logger.error('error :: layers :: invalid timeseries for layers_id %s - %s' % (str(layers_id), base_name))
        if engine:
            layers_engine_disposal(engine)
        return False
    try:
        int_end_timestamp = int(timeseries[-1][0])
        last_hour = int_end_timestamp - 3600
        last_timestamp = int_end_timestamp
        start_timestamp = last_hour
    except:
        logger.error(traceback.format_exc())
        logger.error('error :: could not determine timeseries variables from the use_timeseries for %s ' % str(layers_id))
        if engine:
            layers_engine_disposal(engine)
        return False

    # Thanks to Matthew Flaschen http://stackoverflow.com/users/47773/matthew-flaschen
    # for his operator op_func pattern at http://stackoverflow.com/a/2983144, it
    # it is a funky pattern :)
    ops = {'<': operator.le,
           '>': operator.ge,
           '==': operator.eq,
           '!=': operator.ne,
           '<=': operator.le,
           '>=': operator.ge}

    # D layer
    # @modified 20170307 - Feature #1960: ionosphere_layers
    # Use except on everything, remember how fast Skyline can iterate
    try:
        op_func = ops[d_condition]
        op_func_result = op_func(last_datapoint, d_boundary_limit)
        if op_func_result:
            if engine:
                layers_engine_disposal(engine)
            logger.info(
                'layers :: discarding as the last value %s in the timeseries matches D layer boundary %s %s' % (
                    str(last_datapoint), str(d_condition),
                    str(d_boundary_limit)))
            return False
    except:
        logger.error(traceback.format_exc())
        logger.error('error :: layers :: invalid D layer op_func for layers_id %s - %s' % (str(layers_id), base_name))
        if engine:
            layers_engine_disposal(engine)
        return False

    # @added 20170616 - Feature #2048: D1 ionosphere layer
    if d1_layer:
        try:
            op_func = ops[d1_condition]
            count = 0
            while count < d1_boundary_times:
                count += 1
                if count == 1:
                    understandable_message_str = 'the last and latest value in the timeseries'
                if count == 2:
                    understandable_message_str = 'the 2nd last value in the timeseries'
                if count == 3:
                    understandable_message_str = 'the 3rd last value in the timeseries'
                if count >= 4:
                    understandable_message_str = 'the %sth last value in the timeseries' % str(count)
                value = float(timeseries[-count][1])
                op_func_result = op_func(value, e_boundary_limit)
                if op_func_result:
                    if engine:
                        layers_engine_disposal(engine)
                    logger.info('layers :: %s was %s and breaches the D1 layer boundary of %s %s' % (
                        str(understandable_message_str), str(value),
                        str(d1_condition), str(d1_boundary_limit)))
                    return False
        except:
            logger.error(traceback.format_exc())
            logger.error('error :: layers :: invalid D1 layer op_func for layers_id %s - %s' % (str(layers_id), base_name))
            if engine:
                layers_engine_disposal(engine)
            return False

    # E layer
    # @modified 20170314 - Feature #1960: ionosphere_layers
    # Changed condition so the correct method to not "unset"
    # e_layer_matched = True
    e_layer_matched = False
    # @modified 20170307 - Feature #1960: ionosphere_layers
    # Use except on everything, remember how fast Skyline can iterate
    try:
        op_func = ops[e_condition]
        count = 0
        while count < e_boundary_times:
            count += 1
            if count == 1:
                understandable_message_str = 'the last and latest value in the timeseries'
            if count == 2:
                understandable_message_str = 'the 2nd last value in the timeseries'
            if count == 3:
                understandable_message_str = 'the 3rd last value in the timeseries'
            if count >= 4:
                understandable_message_str = 'the %sth last value in the timeseries' % str(count)
            value = float(timeseries[-count][1])
            op_func_result = op_func(value, e_boundary_limit)
            if not op_func_result:
                logger.info('layers :: %s was %s and breaches the E layer boundary of %s %s' % (
                    str(understandable_message_str), str(value),
                    str(e_condition), str(e_boundary_limit)))
            # @modified 20170314 - Feature #1960: ionosphere_layers
            # Do not override the condition
                # e_layer_matched = False
            else:
                e_layer_matched = True
                logger.info('layers :: %s was %s and matches the E layer boundary of %s as not anomalous' % (
                    str(understandable_message_str), str(value),
                    str(e_boundary_limit)))
                break

    except:
        logger.error(traceback.format_exc())
        logger.error('error :: layers :: invalid E layer op_func for layers_id %s - %s' % (str(layers_id), base_name))
        if engine:
            layers_engine_disposal(engine)
        return False

    if es_layer:
        logger.info('layers :: Es layer not implemented yet - cannot evaluate es_day %s and es_condition %s' % (str(es_day), str(es_condition)))
    if f1_layer:
        logger.info('layers :: F1 layer not implemented yet - cannot evaluate f1_from_time %s' % str(f1_from_time))
    if f2_layer:
        logger.info('layers :: F2 layer not implemented yet - cannot evaluate f2_until_time %s' % str(f2_until_time))

    if not e_layer_matched:
        if engine:
            layers_engine_disposal(engine)
        logger.info('layers :: returning False not_anomalous breached E layer')
        return False
    else:
        not_anomalous = True

    if not_anomalous:
        try:
            connection = engine.connect()
            connection.execute(
                ionosphere_layers_table.update(
                    ionosphere_layers_table.c.id == layers_id).
                values(matched_count=ionosphere_layers_table.c.matched_count + 1,
                       last_matched=checked_timestamp))
            connection.close()
            logger.info('layers :: updated matched_count for %s' % str(layers_id))
        except:
            logger.error(traceback.format_exc())
            logger.error('error :: layers :: could not update matched_count and last_matched for %s ' % str(layers_id))
            if engine:
                layers_engine_disposal(engine)
            return False

        try:
            ionosphere_layers_matched_table, log_msg, trace = ionosphere_layers_matched_table_meta(skyline_app, engine)
            logger.info(log_msg)
            logger.info('layers :: ionosphere_layers_matched_table OK')
        except:
            logger.error(traceback.format_exc())
            logger.error('error :: layers :: failed to get ionosphere_layers_matched_table meta for %s' % base_name)
            if engine:
                layers_engine_disposal(engine)
            return False

        try:
            connection = engine.connect()
            ins = ionosphere_layers_matched_table.insert().values(
                layer_id=int(layers_id),
                fp_id=int(current_fp_id),
                metric_id=int(current_metric_id),
                anomaly_timestamp=int(last_timestamp),
                anomalous_datapoint=float(last_datapoint),
                full_duration=int(settings.FULL_DURATION))
            result = connection.execute(ins)
            connection.close()
            new_matched_id = result.inserted_primary_key[0]
            logger.info('layers :: new ionosphere_layers_matched id: %s' % str(new_matched_id))
        except:
            logger.error(traceback.format_exc())
            logger.error(
                'error :: layers :: could not update ionosphere_layers_matched for %s with with timestamp %s' % (
                    str(layers_id), str(last_timestamp)))
            if engine:
                layers_engine_disposal(engine)
            return False

    # @added 20170306 - Feature #1964: ionosphere_layers - namespace_matches
    # to be considered

    if engine:
        layers_engine_disposal(engine)

    return not_anomalous
