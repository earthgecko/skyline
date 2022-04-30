"""
get_matched_timeseries.py
"""
import logging
import traceback
from os import path
from ast import literal_eval

import numpy as np

import settings
from functions.database.queries.get_ionosphere_fp_row_from_match_id import get_ionosphere_fp_row_from_match_id
from functions.database.queries.fp_timeseries import get_db_fp_timeseries
from functions.database.queries.get_motifs_matched_row import get_motifs_matched_row
from functions.ionosphere.get_fp_motif import get_fp_motif
from functions.metrics.get_base_name_from_metric_id import get_base_name_from_metric_id
from functions.timeseries.determine_data_frequency import determine_data_frequency
from functions.pandas.timeseries_to_datetime_indexed_df import timeseries_to_datetime_indexed_df
from skyline_functions import get_graphite_metric


# @added 20220317 - Feature #4540: Plot matched timeseries
#                   Feature #4014: Ionosphere - inference
def get_matched_timeseries(current_skyline_app, match_id, layers_match_id):
    """
    Return dictionary of trained and matched timeseries as a list e.g.

    matched_timeseries = {
        'matched': {}
        'match_id': match_id,
        'layers_match_id': layers_match_id,
        'metric': 'metric_name',
        'trained': [[ts, value], [ts, value], ..., [ts, value]],
        'matched': [[ts, value], [ts, value], ..., [ts, value]],
    }

    :param current_skyline_app: the app calling the function
    :param match_id: the Ionosphere match id
    :param layers_match_id: the Ionosphere layers match id
    :type current_skyline_app: str
    :type match_id: int
    :type layer_match_id: int
    :return: dictionary of timeseries
    :rtype: dict

    """

    current_skyline_app_logger = current_skyline_app + 'Log'
    current_logger = logging.getLogger(current_skyline_app_logger)
    function_str = 'functions.ionosphere.get_matched_timeseries'

    current_logger.info('%s :: match_id: %s, layer_match_id: %s' % (
        function_str, str(match_id), str(layers_match_id)))

    matched_timeseries = {}

    matched = {}
    matched['match'] = {}
    try:
        matched = get_ionosphere_fp_row_from_match_id(current_skyline_app, match_id, layers_match_id)
    except Exception as err:
        current_logger.error(traceback.format_exc())
        current_logger.error('error :: %s :: get_ionosphere_fp_row_from_match_id failed - %s' % (function_str, err))
        if current_skyline_app == 'webapp':
            # Raise to webapp
            raise
        return matched_timeseries

    if not matched['match']:
        current_logger.warning('warning :: %s :: no match found' % (function_str))
        return matched_timeseries

    try:
        fp_id = matched['fp']['id']
        metric_id = matched['fp']['metric_id']
    except Exception as err:
        current_logger.error(traceback.format_exc())
        current_logger.error('error :: %s :: failed to determine fp and metric ids from matched dict - %s' % (
            function_str, err))
        if current_skyline_app == 'webapp':
            # Raise to webapp
            raise
        return matched_timeseries

    matched_timeseries = matched.copy()

    # Get the fp timeseries
    fp_timeseries = []
    try:
        fp_timeseries = get_db_fp_timeseries(current_skyline_app, metric_id, fp_id)
        matched['fp']['timeseries'] = fp_timeseries
    except Exception as err:
        current_logger.error(traceback.format_exc())
        current_logger.error('error :: %s :: get_db_fp_timeseries failed for fp id %s - %s' % (
            function_str, str(fp_id), err))
        if current_skyline_app == 'webapp':
            # Raise to webapp
            raise
        return matched_timeseries

    motifs_matched_id = 0
    fp_motif = {}
    if match_id:
        try:
            motifs_matched_id = matched['match']['motifs_matched_id']
            current_logger.info('motifs_matched_id: %s', motifs_matched_id)
        except KeyError:
            motifs_matched_id = 0
        except Exception as err:
            current_logger.error(traceback.format_exc())
            current_logger.error('error :: %s :: failed to determine matched[\'motifs_matched_id\'] - %s' % (
                function_str, err))
    if motifs_matched_id:
        try:
            motifs_matched_row = get_motifs_matched_row(current_skyline_app, motifs_matched_id)
            matched_timeseries['motif_matched'] = motifs_matched_row
        except Exception as err:
            current_logger.error(traceback.format_exc())
            current_logger.error('error :: %s :: get_motifs_matched_row failed for motif_matched id %s - %s' % (
                function_str, str(motifs_matched_id), err))
        if motifs_matched_row:
            try:
                fp_motif = get_fp_motif(current_skyline_app, motifs_matched_id, motifs_matched_row, fp_timeseries)
                matched_timeseries['fp_motif'] = fp_motif
            except Exception as err:
                current_logger.error(traceback.format_exc())
                current_logger.error('error :: %s :: get_fp_motif failed for motifs_matched id %s - %s' % (
                    function_str, str(motifs_matched_id), err))

    if not motifs_matched_id:
        matched_timeseries['fp_timeseries'] = fp_timeseries

    try:
        metric = get_base_name_from_metric_id(current_skyline_app, metric_id)
    except Exception as err:
        current_logger.error(traceback.format_exc())
        current_logger.error('error :: %s :: get_base_name_from_metric_id failed for metric id %s - %s' % (
            function_str, str(metric_id), err))

    matched_timeseries['metric'] = metric

    # Create the matched timeseries
    match_timeseries = []
    fp_full_duration = matched['fp']['full_duration']
    metric_timestamp = matched['match']['metric_timestamp']

    # Check if training data exists to get the match timeseries from
    try:
        metric_dir = metric.replace('.', '/')
        metric_training_dir = '%s/%s/%s' % (
            settings.IONOSPHERE_DATA_FOLDER, str(metric_timestamp), metric_dir)
        timeseries_json = '%s/%s.json' % (metric_training_dir, metric)
        full_duration_in_hours_int = int(settings.FULL_DURATION / 60 / 60)
        full_duration_timeseries_json = '%s/%s.mirage.redis.%sh.json' % (
            metric_training_dir, metric, str(full_duration_in_hours_int))
        if fp_full_duration == settings.FULL_DURATION:
            timeseries_json_file = full_duration_timeseries_json
        else:
            timeseries_json_file = timeseries_json
        match_timeseries = []
        if path.isfile(timeseries_json_file):
            try:
                with open((timeseries_json_file), 'r') as f:
                    raw_timeseries = f.read()
                timeseries_array_str = str(raw_timeseries).replace('(', '[').replace(')', ']')
                del raw_timeseries
                match_timeseries = literal_eval(timeseries_array_str)
                del timeseries_array_str
            except Exception as err:
                current_logger.error(traceback.format_exc())
                current_logger.error('error :: %s :: failed to get timeseries from %s - %s' % (
                    function_str, str(timeseries_json_file), err))
    except Exception as err:
        current_logger.error(traceback.format_exc())
        current_logger.error('error :: %s :: failed to determine if there is a training timeseries - %s' % (
            function_str, err))

    # Get it from Graphite
    if not match_timeseries:
        try:
            current_logger.info('%s :: getting data from graphite for %s from: %s, until: %s' % (
                function_str, metric, str((metric_timestamp - fp_full_duration)),
                str(metric_timestamp)))
            match_timeseries = get_graphite_metric(
                current_skyline_app, metric, (metric_timestamp - fp_full_duration),
                metric_timestamp, 'list', 'object')
        except Exception as err:
            current_logger.error(traceback.format_exc())
            current_logger.error('error :: %s :: get_graphite_metric failed for %s - %s' % (
                function_str, str(metric), err))

    fp_resolution = 0
    try:
        fp_resolution = determine_data_frequency(current_skyline_app, fp_timeseries, False)
    except Exception as err:
        current_logger.error(traceback.format_exc())
        current_logger.error('error :: %s :: determine_data_frequency failed to determine resolution of fp_timeseries - %s' % (
            function_str, err))

    match_resolution = 0
    try:
        match_resolution = determine_data_frequency(current_skyline_app, match_timeseries, False)
    except Exception as err:
        current_logger.error(traceback.format_exc())
        current_logger.error('error :: %s :: determine_data_frequency failed to determine resolution of match_timeseries - %s' % (
            function_str, err))

    scale_to = 0
    # if match_resolution > fp_resolution:
    if match_resolution < fp_resolution:
        # Scale the match to seconds
        scale_to = match_resolution / fp_resolution
        current_logger.info('%s :: scaling match_timeseries to value / %s because match_resolution: %s, fp_resolution: %s' % (
            function_str, str(scale_to), str(match_resolution),
            str(fp_resolution)))
        scaled_match_timeseries = []
        for ts, value in match_timeseries:
            scaled_value = value / scale_to
            scaled_match_timeseries.append([ts, scaled_value])
        match_timeseries = scaled_match_timeseries

    if match_resolution > fp_resolution:
        current_logger.info('%s :: resampling fp_timeseries to match_resolution: %s' % (
            function_str, str(match_resolution)))
        resample_ts = list(fp_timeseries)
        if fp_motif:
            resample_ts = list(fp_motif['fp_motif_timeseries'])
        df = timeseries_to_datetime_indexed_df(current_skyline_app, resample_ts, False)
        T = '%sT' % str(int(match_resolution / 60))
        resample_df = df.resample(T).mean()
        fp_motif_timeseries = list(zip(resample_df.index.astype(np.int64) // 10**9, resample_df['value'].to_list()))

    if motifs_matched_id:
        if scale_to:
            matched_timeseries['matched_fp_timeseries'] = fp_motif_timeseries
        else:
            matched_timeseries['matched_fp_timeseries'] = fp_motif['fp_motif_timeseries']
        fp_motif_length = len(matched_timeseries['matched_fp_timeseries'])
        matched_timeseries['timeseries'] = match_timeseries[-fp_motif_length:]
    else:
        # Ensure the fp_timeseries is the same length as the matched timeseries
        matched_timeseries['matched_fp_timeseries'] = fp_timeseries[-(len(match_timeseries)):]
        matched_timeseries['timeseries'] = match_timeseries

    # Ensure timesereis are same length
    matched_fp_timeseries_len = len(matched_timeseries['matched_fp_timeseries'])
    matched_timeseries_len = len(matched_timeseries['timeseries'])

    if matched_fp_timeseries_len > matched_timeseries_len:
        matched_timeseries['matched_fp_timeseries'] = matched_timeseries['matched_fp_timeseries'][-matched_timeseries_len:]
    if matched_fp_timeseries_len < matched_timeseries_len:
        matched_timeseries['timeseries'] = matched_timeseries['timeseries'][-matched_fp_timeseries_len:]

    # matched_timeseries['matched_fp_timeseries'] = fp_timeseries[-(len(match_timeseries)):]
    # matched_timeseries['timeseries'] = match_timeseries

    return matched_timeseries
