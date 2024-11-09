"""
motif_annihilation.py
"""
import logging
import traceback
import sys
import os
import copy
import json
import zlib
from ast import literal_eval
from datetime import datetime, timezone
from multiprocessing import Process
from os import kill, getpid
from time import time, sleep
from threading import Thread

import warnings

import matplotlib.pyplot as plt
import matplotlib
import matplotlib.style as mplstyle
from matplotlib.pylab import rcParams
from matplotlib.dates import DateFormatter
import numpy as np
import stumpy

sys.path.insert(0, '/opt/skyline/github/skyline/skyline')
import settings
from skyline_functions import (
    get_redis_conn_decoded, get_graphite_metric, write_data_to_file)
from features_profile import calculate_features_profile
from functions.victoriametrics.get_victoriametrics_metric import get_victoriametrics_metric
from ionosphere_functions import create_features_profile
from functions.numpy.percent_different import get_percent_different
from functions.skyline.is_skyline_busy import is_skyline_busy
from functions.timeseries.determine_data_frequency import determine_data_frequency
from functions.timeseries.downsample import downsample_timeseries
from functions.timeseries_predictions.fft_extrapolation import fft_extrapolation
from functions.wind.wind_return_results import wind_return_results
from functions.wind.wind_submit_job import wind_submit_job

warnings.filterwarnings('ignore')
mplstyle.use('fast')
np.warnings = warnings

skyline_app = 'ionosphere'
skyline_app_logger = '%sLog' % skyline_app
logger = logging.getLogger(skyline_app_logger)
skyline_app_logfile = '%s/%s.log' % (settings.LOG_PATH, skyline_app)
skyline_app_loglock = '%s.lock' % skyline_app_logfile
skyline_app_logwait = '%s.wait' % skyline_app_logfile

this_host = str(os.uname()[1])

app_module = 'motif_annihilation'

try:
    SERVER_METRIC_PATH = '.%s' % settings.SERVER_METRICS_NAME
    if SERVER_METRIC_PATH == '.':
        SERVER_METRIC_PATH = ''
except:
    SERVER_METRIC_PATH = ''

try:
    IONOSPHERE_EXTERNAL_MOTIF_ANNIHILATION_HOSTS_DICT = settings.IONOSPHERE_EXTERNAL_MOTIF_ANNIHILATION_HOSTS_DICT
except:
    IONOSPHERE_EXTERNAL_MOTIF_ANNIHILATION_HOSTS_DICT = {}

verify_ssl = True
try:
    running_on_docker = settings.DOCKER
except:
    running_on_docker = False
if running_on_docker:
    verify_ssl = False
try:
    overall_verify_ssl = settings.VERIFY_SSL
except:
    overall_verify_ssl = True
if not overall_verify_ssl:
    verify_ssl = False
user = None
password = None
if settings.WEBAPP_AUTH_ENABLED:
    user = str(settings.WEBAPP_AUTH_USER)
    password = str(settings.WEBAPP_AUTH_USER_PASSWORD)


class MotifAnnihilation(Thread):
    """
    The MotifAnnihilation class which controls the snab thread and spawned
    threads.

    """

    def __init__(self, parent_pid):
        """
        Initialize the MotifAnnihilation
        """
        super().__init__()
        self.daemon = True
        self.redis_conn_decoded = get_redis_conn_decoded(skyline_app)
        self.daemon = True
        self.parent_pid = parent_pid
        self.current_pid = getpid()

    def check_if_parent_is_alive(self):
        """
        Self explanatory
        """
        try:
            kill(self.current_pid, 0)
            kill(self.parent_pid, 0)
        except:
            logger.warning('warning :: parent or current process dead')
            sys.exit(0)


    def spin_motif_annihilation(self, work_dict):
        """
        Assign motif annihilation work item to process.

        :param self: the self object
        :type self: object
        :param work_dict: a work dict from the
            ionosphere.find_repetitive_patterns.motif_annihilation.work Redis
            hash
        :type work_dict: dict
        :return: None
        :rtype: None
        
        """
        function_str = 'spin_motif_annihilation'

        logger.info('%s :: %s :: processing work' % (app_module, function_str))

        def remove_hash_key(hash_key, anomaly_id, work_host, source_host):
            try:
                removed = self.redis_conn_decoded.hdel('ionosphere.find_repetitive_patterns.motif_annihilation.work', hash_key)
                if removed:
                    logger.info('%s :: remove_hash_key :: removed %s from ionosphere.find_repetitive_patterns.motif_annihilation.work Redis hash' % (
                        app_module, hash_key))
            except Exception as err:
                logger.error('error :: %s :: remove_hash_key :: failed to remove %s from ionosphere.find_repetitive_patterns.motif_annihilation.work Redis hash, err: %s' % (
                    app_module, hash_key, err))
            if source_host == this_host:
                done_key = 'ionosphere.find_repetitive_patterns.motif_annihilation.%s' % str(anomaly_id)
            else:
                done_key = 'ionosphere.find_repetitive_patterns.motif_annihilation.%s.%s' % (
                    source_host, str(anomaly_id))
            try:
                self.redis_conn_decoded.setex(done_key, 14400, int(time()))
            except Exception as err:
                logger.error('error :: %s :: remove_hash_key :: failed to setex %s Redis key, err: %s' % (
                    app_module, done_key, err))
            return

        def candidate_timeseries(anomaly_timeseries, pw4_timeseries):
            """
            Given a 7 day anomaly time series and the time series for the
            previous 4 weeks before that as pw4_timeseries, check to see if the
            time series min_y_padded and max_y_padded are found in the
            pw4_timeseries at least 3 times otherwise there is no point
            running annihilation.

            param anomaly_timeseries: the anomaly_timeseries
            param pw4_timeseries: the pw4_timeseries

            """
            candidate = True
            candidate_dict = {}
            range_padding = 10
            min_found = 0
            max_found = 0
            try:
                min_y = min([v for t, v in anomaly_timeseries])
                max_y = max([v for t, v in anomaly_timeseries])
                max_min_y = float(min_y)
                min_max_y = float(max_y)
                if min_y > 0:
                    # min_min_y = min_y - ((min_y / 100) * range_padding)
                    max_min_y = min_y + ((min_y / 100) * range_padding)
                if max_y > 0:
                    min_max_y = max_y - ((max_y / 100) * range_padding)
                    # max_max_y = max_y + ((max_y / 100) * range_padding)
                min_found = len([v for t, v in pw4_timeseries if v <= max_min_y])
                candidate_dict['min_y'] = {'value': min_y, 'max_min_y': max_min_y, 'min_found': min_found}
                if min_found < 3:
                    candidate = False
                max_found = len([v for t, v in pw4_timeseries if v >= min_max_y])
                candidate_dict['max_y'] = {'value': max_y, 'min_max_y': min_max_y, 'max_found': max_found}
            except Exception as err:
                logger.error(traceback.format_exc())
                logger.error('error :: %s :: candidate_timeseries :: failed to determine if candidate, err: %s' % (
                    app_module, err))
            if max_found < 3:
                candidate = False
            return candidate, candidate_dict


        def annihilation_motifs(batch_size, max_distance, max_area_percent_diff, timeseries):
            """
            Given a time series, take every batch_size window subsequences and
            search for similar patterns within the given time series.  Do this
            for every window and record a motif as common if >= 3 similar motifs
            are found in the given time series.

            :param batch_size: the window size of the batches (motif size)
            :param max_distance: the max_distance under which a motif is considered similar
            :param max_area_percent_diff: the max_area_percent_diff under which a motif is
                considered similar
            :param timeseries: the time series, pw4_timeseries
            :type batch_size: int
            :type max_distance: float
            :type max_area_percent_diff: float
            :type timeseries: list
            :return: motif_analysis
            :rtype: dict
            
            """
            function_str = 'annihilation_motifs'
            range_padding = 10
            dev_null = None
            motif_analysis = {}

            start_all = time()
            start_batch_size = time()
            motif_analysis[batch_size] = {}
            motif_analysis[batch_size]['motifs'] = {}

            motifs_found = []
            similar_motifs = 0
            distance_motifs = 0

            index_count = len(timeseries)

            # Only record motifs that represent a set of a single value
            # once
            single_value_motifs_found = []

            subsequences = 0
            last_motif_match_index = None
            last_batch_index = None
            motif_indices = []
            total_subsequences = len(list(range(0, ((len(timeseries) - 1) - batch_size))))
            logger.info('%s :: %s :: batch_size: %s, number of subsequences: %s' % (
                app_module, function_str, str(batch_size), str(total_subsequences)))
            for batch_index in range(0, ((len(timeseries) - 1) - batch_size)):
                motif_analysis[batch_size]['motifs'][batch_index] = {}

                subsequences += 1

                if not subsequences % 200:
                    logger.info('%s :: %s :: processed %s subsequences of %s total subsequences, motifs_found: %s, in %s seconds' % (
                        app_module, function_str, str(subsequences), str(total_subsequences),
                        str(len(list(motif_analysis[batch_size]['motifs'].keys()))),
                        (time() - start_batch_size)))

                if last_motif_match_index is not None:
                    # Do not consider any subsequence if they are within
                    # 3600 seconds of the last motif found, meaning
                    # only record the first motif in an hour period
                    if batch_index < (last_motif_match_index + 6):
                        del motif_analysis[batch_size]['motifs'][batch_index]
                        continue

                # Only consider 3 motifs per hour
                if last_batch_index is not None:
                    if batch_index < (last_batch_index + 2):
                        del motif_analysis[batch_size]['motifs'][batch_index]
                        continue
                    # Do not consider any subsequence 2 periods behind or ahead
                    if batch_index in list(range((last_batch_index - batch_size), (last_batch_index + batch_size))):
                        del motif_analysis[batch_size]['motifs'][batch_index]
                        continue

                motifs_found = []
                similar_motifs = 0
                distance_motifs = 0

                pred_timeseries = []
                subsequence = timeseries[batch_index:(batch_index + batch_size)]
                # There is no requirement to pad the subsequence with fft_extrapolations
                # if len(subsequence) < batch_size:
                #     pred_timeseries = fft_extrapolation('ionosphere', timeseries, n_predict=batch_size, log=False)
                #    subsequence = timeseries[batch_index:(batch_index + batch_size)]

                if len(subsequence) < batch_size:
                    break

                dataset = [float(item[1]) for item in subsequence]

                # Determine if the motif is a sequence of a single value
                single_value_motif = False
                if len(set(dataset)) == 1:
                    sequence_value = dataset[0]
                    single_value_motif = True
                    if sequence_value in single_value_motifs_found:
                        # Already recorded
                        del motif_analysis[batch_size]['motifs'][batch_index]
                        continue
                last_batch_index = int(batch_index)

                max_y = max(dataset)
                min_y = min(dataset)

                # Use appropriate padding for the range of the data
                use_range_padding = ((max_y - min_y) / 100) * range_padding
                if max_y < 30:
                    use_range_padding = 2.0
                range_total = max_y - min_y
                modify_use_range = False
                if max_y < 120:
                    if range_padding < 20:
                        range_padding = 20
                        modify_use_range = True
                if max_y < 30:
                    if range_padding < 33:
                        range_padding = 33
                        modify_use_range = True
                if range_total < 30:
                    if range_padding < 33:
                        range_padding = 33
                        modify_use_range = True
                if range_total < 5:
                    if range_padding < 200:
                        range_padding = 200
                        modify_use_range = True
                if modify_use_range: 
                    use_range_padding = (range_total / 100) * range_padding
                if min_y > 0 and (min_y - use_range_padding) > 0:
                    min_y_padded = min_y - use_range_padding
                else:
                    min_y_padded = min_y
                max_y_padded = max_y + use_range_padding
                if min_y_padded == max_y_padded:
                    min_y_padded = min_y_padded - ((min_y_padded / 100) * range_padding)
                    max_y_padded = max_y_padded + ((max_y_padded / 100) * range_padding)

                mass_times = []

                count = 0

                index_count = len(timeseries)
                relate_dataset = [float(item[1]) for item in timeseries]
                if pred_timeseries:
                    relate_dataset = [float(item[1]) for item in pred_timeseries]

                current_best_indices = []
                current_best_dists = []

                try:
                    start_mass = time()
                    match_results = stumpy.match(
                        dataset,
                        relate_dataset,
                        max_distance=max_distance,
                        max_matches=200
                    )
                    end_mass = time()
                    mass_times.append((end_mass - start_mass))
                    match_results_list = match_results.tolist()
                    if len(match_results_list) == 0:
                        continue
                    current_best_indices = [item[1] for item in match_results_list]
                    current_best_dists = [item[0] for item in match_results_list]
                except Exception as err:
                    logger.error('error :: %s :: %s :: stumpy.match error: %s' % (
                        app_module, function_str, str(err)))
                    continue

                for index, best_dist in enumerate(current_best_dists):
                    try:
                        try:
                            # Coerce np int to int for json outputs
                            motif = [int(batch_index), int(current_best_indices[index]), float(best_dist), subsequence]
                        except Exception as err:
                            dev_null = err
                            motif = []
                            del dev_null
                            continue

                        # Skip fft predictions if there are any
                        if current_best_indices[index] > (index_count - 1):
                            continue

                        if best_dist > float(max_distance):
                            continue
                        else:
                            if motif:
                                count += 1
                                motifs_found.append(motif)
                    except Exception as err:
                        logger.error('error :: %s :: %s :: could not determine is if %s timeseries at index %s was a match, err: %s' % (
                            app_module, function_str, str(batch_index),
                            str(current_best_indices[index]), err))
                        continue

                # Patterns are sorted
                sorted_motifs = []
                motifs_found_in_indices = []
                if motifs_found:
                    sorted_motifs = sorted(motifs_found, key=lambda x: x[2])
                    for item in sorted_motifs:
                        motifs_found_in_indices.append(item[0])

                for motif in sorted_motifs:
                    try:
                        add_match = False
                        all_in_range = False

                        # Coerce np int to int for json outputs
                        motif_id = int(motif[0])
                        best_index = int(motif[1])
                        best_dist = float(motif[2])

                        if batch_index == best_index:
                            continue

                        # Skip any fft_predictions
                        if best_index > (index_count - 1):
                            continue

                        # @added 20210414 - Feature #4014: Ionosphere - inference
                        #                   Branch #3590: inference
                        # Store the not anomalous motifs
                        motif_sequence = motif[3]

                        match_type = 'not_similar_enough'

                        # @added 20240105 - Bug #5196: Ionosphere - inference - consider current_best_indices 0 as valid
                        #                   Feature #4014: Ionosphere - inference
                        #                   Task #5178: Build and test skyline v4.1.0
                        # Declare default outside the below if not add_match block
                        motif_area = None
                        subsequence_motif_area = None
                        percent_different = None

                        #full_relate_timeseries = timeseries
                        # full_relate_dataset = [float(item[1]) for item in full_relate_timeseries]
                        relate_timeseries = [item for index, item in enumerate(timeseries) if index >= best_index and index < (best_index + int(batch_size))]
                        relate_dataset = [item[1] for item in relate_timeseries]

                        # Determine if the motif is a sequence of a single value and already matched
                        if len(set(relate_dataset)) == 1:
                            motif_sequence_value = relate_dataset[0]
                            if motif_sequence_value in single_value_motifs_found:
                                # motif already recorded
                                continue

                        if not add_match:
                            all_in_range = True
                            for value in relate_dataset:
                                if value < min_y_padded:
                                    all_in_range = False
                                    break
                                if value > max_y_padded:
                                    all_in_range = False
                                    break
                            if all_in_range:
                                related_max_y = max(relate_dataset)
                                if related_max_y < (max_y - range_padding):
                                    all_in_range = False
                                if related_max_y > (max_y + range_padding):
                                    all_in_range = False
                                related_min_y = min(relate_dataset)
                                if related_min_y < (min_y - range_padding):
                                    all_in_range = False
                                if related_min_y > (min_y + range_padding):
                                    all_in_range = False
                            if all_in_range:
                                add_match = True
                                match_type = 'all_in_range'
                                similar_motifs += 1

                            if not all_in_range:
                                continue

                            # Compute the area using the composite trapezoidal rule
                            motif_area = None
                            subsequence_motif_area = None
                            percent_different = None
                            try:
                                batch_size_dataset = [float(item[1]) for item in motif_sequence]
                                y_motif = np.array(batch_size_dataset)
                                motif_area = np.trapz(y_motif, dx=1)
                            except Exception as err:
                                logger.error('error :: %s :: %s :: failed to get motif_area with np.trapz - %s' % (
                                    app_module, function_str ,err))
                            try:
                                y_subsequence_motif = np.array(relate_dataset)
                                subsequence_motif_area = np.trapz(y_subsequence_motif, dx=1)
                            except Exception as err:
                                logger.error('error :: %s :: %s :: failed to get subseqence_motif_area with np.trapz - %s' % (
                                    app_module, function_str ,err))
                            # Determine the percentage difference (as a
                            # positive value) of the areas under the
                            # curves.
                            if motif_area and subsequence_motif_area:
                                percent_different = get_percent_different(subsequence_motif_area, motif_area, True)
                                if percent_different > max_area_percent_diff:
                                    if add_match:
                                        add_match = False
                                if percent_different <= max_area_percent_diff:
                                    if best_dist < max_distance:
                                        add_match = True
                                        match_type = 'distance and area'
                                        distance_motifs += 1
                            if best_dist > max_distance:
                                add_match = False
                                match_type = 'distance not within range'

                        if add_match:
                            motif_indices.append(batch_index)
                            if percent_different is None:
                                percent_different = 0.0
                            try:
                                motif_id = '%s-%s' % (str(batch_index), str(best_index))
                                motif_analysis[batch_size]['motifs'][batch_index][motif_id] = {}
                                motif_analysis[batch_size]['motifs'][batch_index][motif_id]['subsequence_start_index'] = int(batch_index)
                                motif_analysis[batch_size]['motifs'][batch_index][motif_id]['match_sequence_start_index'] = int(best_index)
                                motif_analysis[batch_size]['motifs'][batch_index][motif_id]['distance'] = float(best_dist)
                                motif_analysis[batch_size]['motifs'][batch_index][motif_id]['size'] = int(batch_size)
                                motif_analysis[batch_size]['motifs'][batch_index][motif_id]['max_distance'] = float(max_distance)
                                motif_analysis[batch_size]['motifs'][batch_index][motif_id]['motif_area'] = float(motif_area)
                                motif_analysis[batch_size]['motifs'][batch_index][motif_id]['subsequence_motif_area'] = float(subsequence_motif_area)
                                motif_analysis[batch_size]['motifs'][batch_index][motif_id]['area_percent_diff'] = float(percent_different)
                                motif_analysis[batch_size]['motifs'][batch_index][motif_id]['max_area_percent_diff'] = float(max_area_percent_diff)
                                motif_analysis[batch_size]['motifs'][batch_index][motif_id]['match_type'] = match_type
                            except Exception as err:
                                logger.error('error :: %s :: %s :: processing motif and adding at index: %s, err: %s' % (
                                    app_module, function_str, str(motif[0]), str(err)))
                                continue        
                    except Exception as err:
                        logger.error('error :: %s :: %s :: processing motif: %s, err: %s' % (
                            app_module, function_str, str(motif), str(err)))
                        continue
                if len(motif_analysis[batch_size]['motifs'][batch_index].keys()) < 3:
                    del motif_analysis[batch_size]['motifs'][batch_index]
                else:
                    last_motif_match_index = batch_index
                    if single_value_motif:
                        single_value_motifs_found.append(sequence_value)
            logger.info('%s :: %s :: completed in %s seconds' % (
                app_module, function_str, (time() - start_all)))
            return motif_analysis


        def motif_annihilated_timeseries_indices(
            motif_index, motif_timeseries, timeseries, known_annihilated_timeseries_indices=[], debug=False,
            use_predictions=True, max_distance=1.6):

            function_str = 'motif_annihilated_timeseries_indices'
            annihilated_timeseries_indices = set()
            motifs_matched = {}
            range_padding = 10
            max_area_percent_diff = 20.0
            batch_size = len(motif_timeseries)

            start_all = time()

            if len(motif_timeseries) == 0:
                return annihilated_timeseries_indices, motifs_matched

            # start_index_datestr = datetime.utcfromtimestamp(motif_timeseries[0][0]).strftime('%Y-%m-%d %H:%M:%S')
            start_index_datestr = datetime.fromtimestamp(motif_timeseries[0][0], timezone.utc).strftime('%Y-%m-%d %H:%M:%S')

            index_count = len(timeseries)
            motifs_found = []
            similar_motifs = 0
            distance_motifs = 0

            subsequence = list(motif_timeseries)
            dataset = [float(item[1]) for item in motif_timeseries]
            max_y = max(dataset)
            min_y = min(dataset)

            use_range_padding = ((max_y - min_y) / 100) * range_padding
            if max_y < 30:
                use_range_padding = 2.0
            range_total = max_y - min_y
            modify_use_range = False
            if max_y < 120:
                if range_padding < 20:
                    range_padding = 20
                    modify_use_range = True
            if max_y < 30:
                if range_padding < 33:
                    range_padding = 33
                    modify_use_range = True
            if range_total < 30:
                if range_padding < 33:
                    range_padding = 33
                    modify_use_range = True
            if range_total < 5:
                if range_padding < 200:
                    range_padding = 200
                    modify_use_range = True
            if modify_use_range: 
                use_range_padding = (range_total / 100) * range_padding
            if min_y > 0 and (min_y - use_range_padding) > 0:
                min_y_padded = min_y - use_range_padding
            else:
                min_y_padded = min_y        
            max_y_padded = max_y + use_range_padding
            if min_y_padded == max_y_padded:
                min_y_padded = min_y_padded - ((min_y_padded / 100) * range_padding)
                max_y_padded = max_y_padded + ((max_y_padded / 100) * range_padding)

            if min_y == 0:
                min_y_padded = min_y
            if max_y == 0:
                max_y_padded = max_y

            mass_times = []

            count = 0

            relate_dataset = [float(item[1]) for item in timeseries]
            if use_predictions:
                pred_timeseries = fft_extrapolation('ionosphere', timeseries, n_predict=batch_size, log=False)
                relate_dataset = [float(item[1]) for item in pred_timeseries]

            current_best_indices = []
            current_best_dists = []

            if batch_size >= 3:
                try:
                    start_mass = time()
                    use_top_matches = len(relate_dataset) + batch_size
                    match_results = stumpy.match(
                        dataset,
                        relate_dataset,
                        max_distance=max_distance,
                        max_matches=int(use_top_matches)
                    )
                    end_mass = time()
                    mass_times.append((end_mass - start_mass))
                    match_results_list = match_results.tolist()
                    if len(match_results_list) == 0:
                        return annihilated_timeseries_indices, motifs_matched
                    current_best_indices = [item[1] for item in match_results_list]
                    current_best_dists = [item[0] for item in match_results_list]
                except Exception as err:
                    logger.error('error :: %s :: %s :: stumpy.match error: %s' % (
                        app_module, function_str, str(err)))
                    return annihilated_timeseries_indices, motifs_matched

            if not current_best_indices:
                return annihilated_timeseries_indices, motifs_matched

            errors = []
            for index, best_dist in enumerate(current_best_dists):
                try:
                    try:
                        # Coerce np int to int for json outputs
                        motif = [int(current_best_indices[index]), int(current_best_indices[index]), float(best_dist), subsequence]
                    except Exception as err:
                        errors.append([index, float(best_dist), err])
                        motif = []
                    # skip fft predictions
                    if current_best_indices[index] > (index_count - 1):
                        continue

                    # if list(best_indices)[0] and best_dists:
                    # If it is greater than 1.0 it is not similar
                    # if best_dist.real > 1.0:
                    # if best_dist.real > IONOSPHERE_INFERENCE_MASS_TS_MAX_DISTANCE:
                    if best_dist > float(max_distance):
                        continue
                    else:
                        if motif:
                            count += 1
                            motifs_found.append(motif)
                except Exception as err:
                    logger.error('error :: %s :: %s :: could not determine is if %s timeseries at index %s was a match, err: %s' % (
                        app_module, function_str, str(index),
                        str(current_best_indices[index]), err))
                    continue
            if errors:
                logger.error('error :: %s :: %s :: %s errors encountered creating motifs_found, sample err: %s' % (
                    app_module, function_str, str(len(errors)), str(errors[0])))

            # Patterns are sorted
            sorted_motifs = []
            motifs_found_in_indices = []
            if motifs_found:
                sorted_motifs = sorted(motifs_found, key=lambda x: x[2])
                for item in sorted_motifs:
                    motifs_found_in_indices.append(item[0])

            for motif in sorted_motifs:
                try:
                    add_match = False
                    all_in_range = False

                    # Coerce np int to int for json outputs
                    source_index = int(motif[0])
                    best_index = int(motif[1])
                    best_dist = float(motif[2])

                    # skip fft predictions
                    if best_index > (index_count - 1):
                        continue

                    motif_sequence = motif[3]
                    match_type = 'not_similar_enough'

                    motif_area = None
                    subsequence_motif_area = None
                    percent_different = None

                    full_relate_timeseries = list(timeseries)
                    relate_timeseries = [item for index, item in enumerate(full_relate_timeseries) if index >= best_index and index < (best_index + int(batch_size))]
                    relate_dataset = [item[1] for item in relate_timeseries]

                    if not add_match:
                        all_in_range = True
                        for value in relate_dataset:
                            if value < min_y_padded:
                                all_in_range = False
                                break
                            if value > max_y_padded:
                                all_in_range = False
                                break
                        if all_in_range:
                            related_max_y = max(relate_dataset)
                            if related_max_y < (max_y - range_padding):
                                all_in_range = False
                            if related_max_y > (max_y + range_padding):
                                all_in_range = False
                            related_min_y = min(relate_dataset)
                            if related_min_y < (min_y - range_padding):
                                all_in_range = False
                            if related_min_y > (min_y + range_padding):
                                all_in_range = False
                        if all_in_range:
                            add_match = True
                            match_type = 'all_in_range'
                            similar_motifs += 1

                        # Compute the area using the composite trapezoidal rule
                        motif_area = None
                        subsequence_motif_area = None
                        percent_different = None
                        try:
                            batch_size_dataset = [float(item[1]) for item in motif_sequence]
                            y_motif = np.array(batch_size_dataset)
                            motif_area = np.trapz(y_motif, dx=1)
                        except Exception as err:
                            logger.error('error :: %s :: %s :: failed to get motif_area with np.trapz - %s' % (
                                app_module, function_str ,err))
                        try:
                            y_subsequence_motif = np.array(relate_dataset)
                            subsequence_motif_area = np.trapz(y_subsequence_motif, dx=1)
                        except Exception as err:
                            logger.error('error :: %s :: %s :: failed to get subseqence_motif_area with np.trapz - %s' % (
                                app_module, function_str ,err))

                        # Determine the percentage difference (as a
                        # positive value) of the areas under the
                        # curves.
                        if motif_area and subsequence_motif_area:
                            percent_different = get_percent_different(subsequence_motif_area, motif_area, True)
                            if percent_different > max_area_percent_diff:
                                if add_match:
                                    add_match = False
                            if percent_different <= max_area_percent_diff:
                                if best_dist < max_distance:
                                    add_match = True
                                    match_type = 'distance and area'
                                    distance_motifs += 1
                        if best_dist > max_distance:
                            add_match = False
                            match_type = 'distance not within range'

                    if add_match:
                        for annihilated_index in list(range(best_index, (best_index + batch_size))):
                            annihilated_timeseries_indices.add(annihilated_index)
                        if best_index not in motifs_matched.keys():
                            motifs_matched[source_index] = {}
                        if percent_different is None:
                            percent_different = 0.0
                        try:
                            # Coerce to int and float for json outputs
                            motifs_matched[source_index][best_index] = {
                                'subsequence_start_index': int(motif_index),
                                'match_sequence_start_index': int(best_index),
                                'distance': float(best_dist),
                                'size': int(batch_size),
                                'max_distance': float(max_distance),
                                'motif_area': float(motif_area),
                                'subsequence_motif_area': float(subsequence_motif_area),
                                'area_percent_diff':  float(percent_different),
                                'max_area_percent_diff': float(max_area_percent_diff),
                                'match_type': match_type,  
                            }
                        except Exception as err:
                            logger.error('error :: %s :: %s :: processing motif and adding at index: %s, err: %s' % (
                                app_module, function_str, str(motif[0]), err))

                except Exception as err:
                    logger.error('error :: %s :: %s :: processing motif: %s, err: %s' % (
                        app_module, function_str, str(motif), err))
                    continue

            if debug:
                if len(annihilated_timeseries_indices) == 0:
                    logger.debug('debug :: %s :: %s :: motif_index: %s, batch_size: %s, %s, len(match_results_list): %s, motifs annihilated: %s data points, new_annihilations: 0, completed in %s seconds' % (
                        app_module, function_str, str(motif_index), str(batch_size),
                        start_index_datestr, str(len(match_results_list)),
                        str(len(annihilated_timeseries_indices)), (time() - start_all)))
                else:
                    new_annihilations = [a_i for a_i in annihilated_timeseries_indices if a_i not in known_annihilated_timeseries_indices]
                    logger.debug('debug :: %s :: %s :: motif_index: %s, batch_size: %s, %s, len(match_results_list): %s, motifs annihilated: %s data points, new_annihilations: %s, completed in %s seconds' % (
                        app_module, function_str, str(motif_index), str(batch_size),
                        start_index_datestr, str(len(match_results_list)),
                        str(len(annihilated_timeseries_indices)),
                        len(new_annihilations), (time() - start_all)))

            return annihilated_timeseries_indices, motifs_matched


        def annihilate_timeseries(batch_size, anomaly_timeseries, annihilated_timeseries_indices, pw5_timeseries):
            """
            Given a batch_size, an anomly_timeseries,
            the annihilated_timeseries_indices and a pw5_timeseries annihilate
            the passed indices from the anomaly_timeseries, if any indices
            remain in the anomaly_timeseries and the remaining motifs in the
            anomaly_timeseries that are < batch_size in size are then considered
            per data point (plus its delta) as micro motifs and the
            pw5_timeseries is searched to determine the number of similar value
            data points exists and if >= 4 then the pw5_timeseries is searched
            to determine if similar deltas exists and if >=4 exist, the micro
            motif is annihilated from the anomaly_timeseries.  The
            pw5_timeseries and not the pw4_timeseries is required to be used to
            ensure the boundary indices and deltas between the pw4_timeseries
            and the anomaly_timeseries can be calculated if required.

            :param batch_size: the window size of the batches (motif size)
            :param anomaly_timeseries: the max_distance under which a motif is considered similar
            :param annihilated_timeseries_indices: the max_area_percent_diff under which a motif is
                considered similar
            :param pw5_timeseries: the pw5_timeseries
            :type batch_size: int
            :type max_distance: float
            :type max_area_percent_diff: float
            :type timeseries: list
            :return: motif_analysis
            :rtype: dict
            
            """
            function_str = 'annihilate_timeseries'

            annihilated_anomaly_timeseries = []
            annihilated_count = 0
            unannihilated_anomaly_timeseries = []
            for index, item in enumerate(anomaly_timeseries):
                t = item[0]
                v = item[1]
                if index in annihilated_timeseries_indices:
                    v = -1
                    annihilated_count += 1
                else:
                    unannihilated_anomaly_timeseries.append([t, v])
                annihilated_anomaly_timeseries.append([t, v])
            unannihilated_dict = {}
            annihilated_unannihilated_dict = {}

            # Handle if there are multiple data points not annihilated
            # and they are all the same value, normally 0
            same_value = []
            if unannihilated_anomaly_timeseries:
                same_value = list(set([v for t, v in unannihilated_anomaly_timeseries]))

            # If the remaining unannihilated motifs (even if a single data point) are
            # less than batch_size consider for micro motif annihilation
            max_item_size = 0
            # Determine the max size of remaining unannihilated motifs (even if a
            # single data point)
            last_unannihilated_timestamp = None
            last_unannihilated_motif = []
            for item in unannihilated_anomaly_timeseries:
                if not last_unannihilated_timestamp:
                    last_unannihilated_timestamp = item[0]
                    last_unannihilated_motif.append(item)
                    continue
                if not last_unannihilated_motif:
                    last_unannihilated_motif = [item]
                if item[0] > (last_unannihilated_timestamp + 600):
                    if last_unannihilated_motif:
                        last_unannihilated_motif_size = len(last_unannihilated_motif)
                        if last_unannihilated_motif_size > max_item_size:
                            max_item_size = int(last_unannihilated_motif_size)
                    last_unannihilated_motif = []
                else:
                    last_unannihilated_motif.append(item)
                last_unannihilated_timestamp = item[0]
            if last_unannihilated_motif:
                last_unannihilated_motif_size = len(last_unannihilated_motif)
                if last_unannihilated_motif_size > max_item_size:
                    max_item_size = int(last_unannihilated_motif_size)

            # If the timeseries was not annihilated and less than batch_size
            # indices remain then attempt to annihilate each index based on
            # the value occurring >= 4 times in the pw5_timeseries AND the
            # delta of the index value from the previous value occurring at
            # least 4 times as well.  This delta check ensures that the
            # the check is similar to a micro motif, not just value but
            # range too.
            if annihilated_count < len(anomaly_timeseries) or len(same_value) == 1 or max_item_size < (batch_size * 2):
                unannihilated_index_count = len(anomaly_timeseries) - annihilated_count
                logger.info('%s :: %s :: the anomaly timeseries was not annihilated by common motifs and %s indices remain' % (
                    app_module, function_str, str(unannihilated_index_count)))

                if annihilated_count >= (len(anomaly_timeseries) - (batch_size - 1)) or len(same_value) == 1 or max_item_size < (batch_size * 2):
                    for index, item in enumerate(anomaly_timeseries):
                        if index not in annihilated_timeseries_indices:
                            pw5_index = None
                            delta = 0
                            if index != 0:
                                delta = anomaly_timeseries[index][1] - anomaly_timeseries[(index - 1)][1]
                            else:
                                try:
                                    ts = item[0]
                                    pw5_index = [i_index for i_index, i_item in enumerate(pw5_timeseries) if i_item[0] == ts][0]
                                    pw5_value = pw5_timeseries[(pw5_index - 1)][1]
                                    delta = anomaly_timeseries[index][1] - pw5_value
                                except:
                                    delta = anomaly_timeseries[(index + 1)][1] - anomaly_timeseries[index][1]
                            unannihilated_dict[index] = {'timestamp': item[0], 'value': item[1], 'delta': delta}
                            if pw5_index:
                                # These are used to calculate the delta from
                                unannihilated_dict[index]['pw5_index'] = pw5_index - 1
                                unannihilated_dict[index]['pw5_value'] = pw5_value
            annihilated_unannihilated_count = 0
            annihilated_unannihilated_dict = {}
            # micro motif annihilation
            for key, data in unannihilated_dict.items():
                annihilated_unannihilated_dict[key] = data
                annihilated_unannihilated_dict[key]['annihilated'] = False
                value = data['value']
                range_padding = 10
                if value == 0:
                    min_y = float(value)
                    min_value = min([v for t, v in anomaly_timeseries if v > 0])
                    max_y = min_value
                    if min_value:
                        max_y = min_value - ((min_value / 100) * range_padding)
                else:
                    min_y = value - ((value / 100) * range_padding)
                    max_y = value + ((value / 100) * range_padding)
                annihilated_unannihilated_dict[key]['min_y'] = min_y
                annihilated_unannihilated_dict[key]['max_y'] = max_y
                found_values = [[index, item] for index, item in enumerate(pw5_timeseries) if item[1] >= min_y and item[1] <= max_y]
                annihilated_unannihilated_dict[key]['value_found_count'] = len(found_values)
                # If at least 4 similar values are not found, move on and done not check
                # if the delta is common
                if len(found_values) < 4:
                    continue
                # If at least 4 similar values were found, check to determine if the
                # delta is common
                delta_values = []
                for index, item in enumerate(pw5_timeseries):
                    if index == 0:
                        last_value = item[1]
                        delta_values.append(0)
                        continue
                    delta = item[1] - last_value
                    delta_values.append(delta)
                    last_value = item[1]

                delta_value = data['delta']
                if delta_value == 0:
                    min_delta = float(delta_value)
                    min_value = min([v for t, v in anomaly_timeseries if v > 0])
                    max_delta = min_value
                    if min_value:
                        max_delta = min_value - ((min_value / 100) * range_padding)
                else:
                    min_delta = delta_value - ((delta_value / 100) * range_padding)
                    max_delta = delta_value + ((delta_value / 100) * range_padding)
                annihilated_unannihilated_dict[key]['min_delta'] = min_delta
                annihilated_unannihilated_dict[key]['max_delta'] = max_delta
                if min_delta < 0 and max_delta < 0:
                    found_deltas = [delta for delta in delta_values if delta > max_delta and delta < min_delta]
                else:
                    found_deltas = [delta for delta in delta_values if delta > min_delta and delta < max_delta]
                annihilated_unannihilated_dict[key]['delta_found_count'] = len(found_deltas)
                # If at least 4 similar values and at least 4 similar deltas were found
                # class the micro motif as common and annihilate the data point
                if len(found_deltas) >= 4:
                    match_index = found_values[0][0]
                    annihilated_timeseries_indices.append(match_index)
                    annihilated_anomaly_timeseries[key] = [data['timestamp'], -1]
                    annihilated_count += 1
                    annihilated_unannihilated_count += 1
                    annihilated_unannihilated_dict[key]['annihilated'] = True
            if len(unannihilated_dict) > 0:
                logger.info('%s :: %s :: annihilated_unannihilated_dict: %s' % (
                    app_module, function_str, str(annihilated_unannihilated_dict)))
                logger.info('%s :: %s :: mirco motifs annihilated: %s of the remaining %s unannihilated indices' % (
                    app_module, function_str, str(annihilated_unannihilated_count),
                    str(len(unannihilated_dict))))
            logger.info('%s :: %s :: annihilated: %s indices of the %s indices in the anomaly_timeseries' % (
                app_module, function_str, str(annihilated_count), str(len(anomaly_timeseries))))
            return annihilated_anomaly_timeseries, annihilated_count, unannihilated_dict, annihilated_unannihilated_dict


        def plot_match(
            metric, subsequence_index, subsequence, match_subsequence,
            output_file, plot_annotations={}):
            try:
                matplotlib.rcParams['path.simplify_threshold'] = 1.0

                subsequence_values = [item[1] for item in subsequence]

                # Plot match
                rcParams['figure.figsize'] = 10, 4
                # Improve matplotlib render performance
                rcParams['path.simplify_threshold'] = 1.0
                plt.style.use('fast')
                fig = plt.figure(frameon=False)
                ax = fig.add_subplot(111)
                derived_timeseries = False
                pw5_timeseries = False
                context = 'motif_annihilation - ANNIHILATED anomalous time series\n%s' % metric
                if 'match_type' in plot_annotations:
                    if plot_annotations['match_type'] == 'NOT SIMILAR':
                        context = 'NOT ANNIHILATED'
                if 'derived_timeseries' in plot_annotations:
                    context = 'Derived time series from best matched common motifs from previous 4 weeks\n%s' % metric
                    derived_timeseries = True
                if 'pw5_timeseries' in plot_annotations:
                    context = '5 weeks\n%s' % metric
                    pw5_timeseries = True

                if not derived_timeseries:
                    try:
                        match_subsequence_values = [item[1] for item in match_subsequence]
                    except Exception as err:
                        logger.error('error :: %s :: plot_match :: match_subsequence_values, err: %s' % (
                            app_module, err))
                        return False

                graph_title = '%s' % (context)
                ax.set_title(graph_title, fontsize='medium')
                if hasattr(ax, 'set_facecolor'):
                    ax.set_facecolor('white')
                else:
                    ax.set_axis_bgcolor('white')
                # datetimes = [datetime.utcfromtimestamp(int(item[0])) for item in subsequence]
                datetimes = [datetime.fromtimestamp(int(item[0]), timezone.utc) for item in subsequence]
                plt.xticks(rotation=0, horizontalalignment='center')
                xfmt = DateFormatter('%m/%d %H')

                plt.gca().xaxis.set_major_formatter(xfmt)
                ax.xaxis.set_major_formatter(xfmt)

                not_anomalous_label = 'derived similar time series from common motifs'
                not_anomalous_line_color = 'green'
                if 'match_type' in plot_annotations:
                    if plot_annotations['match_type'] == 'NOT SIMILAR':
                        not_anomalous_line_color = 'orange'

                if pw5_timeseries:
                    not_anomalous_label = 'annihilated anomaly time series'
                    not_anomalous_line_color = 'green'

                if not derived_timeseries:
                    ax.plot(
                        datetimes, match_subsequence_values, label=not_anomalous_label,
                        color=not_anomalous_line_color, lw=1, linestyle='solid',
                        zorder=4)
                ax.tick_params(axis='both', labelsize='small')
                matched_label = 'annihilated anomalous pattern'
                if pw5_timeseries:
                    matched_label = 'previous 4 weeks from which common motifs were derived'

                if derived_timeseries:
                    matched_label = 'derived best matches from common motifs from the previous 4 weeks'
                    ax.plot(datetimes, subsequence_values, lw=1, label=matched_label, color='blue', ls='--')
                else:
                    ax.plot(datetimes, subsequence_values, lw=1, label=matched_label, color='blue', ls='--', zorder=3, alpha=0.5)
                ax.get_yaxis().get_major_formatter().set_useOffset(False)
                ax.get_yaxis().get_major_formatter().set_scientific(False)
                box = ax.get_position()
                ax.set_position([box.x0, box.y0 + box.height * 0.1,
                                box.width, box.height * 0.9])
                ax.legend(loc='upper center', bbox_to_anchor=(0.5, -0.1),
                        fancybox=True, shadow=True, ncol=2, fontsize='small')
                plt.rc('lines', lw=1, color='black')
                plt.grid(True)
                ax.grid(visible=True, which='both', axis='both', color='lightgray',
                        linestyle='solid', alpha=0.5, linewidth=0.6)

                if hasattr(ax, 'set_facecolor'):
                    ax.set_facecolor('white')
                else:
                    ax.set_axis_bgcolor('white')
                rcParams['xtick.direction'] = 'out'
                rcParams['ytick.direction'] = 'out'
                ax.margins(y=.02, x=.03)
                text_x = datetimes[0]
                text_y = max(subsequence_values) - ((max(subsequence_values) / 10) * 1)
                ax_text = 'data points: %s' % str(len(subsequence_values))
                if plot_annotations and not derived_timeseries and not pw5_timeseries:
                    for key, value in plot_annotations.items():
                        ax_text = '%s\n%s: %s' % (ax_text, key, str(value))
                ax_text_color = 'green'
                ax_text_color = not_anomalous_line_color
                ax.text(text_x, text_y, ax_text, size=8, color=ax_text_color, alpha=0.7)
                plt.savefig(output_file, format='png')
                fig.clf()
                plt.close(fig)
                return output_file
            except Exception as err:
                logger.error('error :: %s :: plot_match :: plot failed, err: %s' % (
                    app_module, err))
                return False


        def plot_annihilation(
                metric, batch_size, anomaly_timeseries, matched_motifs,
                pw4_timeseries, output_file):
            function_str = 'plot_annihilation'
            plotted_files = []
            anomaly_timeseries_indices = [index for index, item in enumerate(anomaly_timeseries)]
            anomaly_timeseries_matches = {}
            for i in anomaly_timeseries_indices:
                anomaly_timeseries_matches[i] = {}
                for motif_index in list(matched_motifs.keys()):
                    # Handle ints coerced into str when json has been used
                    motif_index_int = motif_index
                    if isinstance(motif_index, str):
                        motif_index_int = int(motif_index)
                    for ii in list(matched_motifs[motif_index].keys()):
                        try:
                            motif_match_indices = list(matched_motifs[motif_index][ii].keys())
                            for mmi in motif_match_indices:
                                mmi_int = int(mmi)
                                if i in list(range(mmi_int, (mmi_int + batch_size))):
                                    anomaly_timeseries_matches[i][motif_index_int] = copy.deepcopy(matched_motifs[motif_index][ii][mmi])
                        except:
                            pass
                if len(anomaly_timeseries_matches[i]) == 0:
                    del anomaly_timeseries_matches[i]
            logger.info('%s :: %s :: len(anomaly_timeseries): %s' % (
                app_module, function_str, str(len(anomaly_timeseries))))
            logger.info('%s :: %s :: len(anomaly_timeseries_matches): %s' % (
                app_module, function_str, str(len(anomaly_timeseries_matches))))
            best_matches = []
            no_best_matches = []
            for index in list(anomaly_timeseries_matches.keys()):
                best_match = None
                best_matches_dict = {}
                best_dist = None
                best_area = None
                best_total = None
                dists = []
                areas = []
                totals = []
                batch_size = None
                for i in list(anomaly_timeseries_matches[index].keys()):
                    area_percent_diff = anomaly_timeseries_matches[index][i]['area_percent_diff']
                    if not isinstance(area_percent_diff, float):
                        area_percent_diff = 100.0
                    dist = anomaly_timeseries_matches[index][i]['distance']
                    if not isinstance(dist, float):
                        dist = 100.0
                    total = (dist + area_percent_diff)
                    size = anomaly_timeseries_matches[index][i]['size']
                    best_matches_dict[i] = [dist, area_percent_diff, total, size]
                    dists.append(dist)
                    areas.append(area_percent_diff)
                    totals.append(total)
                if dists:
                    best_dist = sorted(dists)[0]
                if areas:
                    best_area = sorted(areas)[0]
                if totals:
                    best_total = sorted(totals)[0]
                for i_index, item in best_matches_dict.items():
                    dist = item[0]
                    area = item[1]
                    total = item[2]
                    size = item[3]
                    if dist == 100.0 and area == 100.0:
                        continue
                    if dist == best_dist and area == best_area:
                        best_match = i_index
                        break
                if best_match is None:
                    for i_index, item in best_matches_dict.items():
                        total = item[2]
                        if total == 200.0:
                            continue
                        if total == best_total:
                            best_match = i_index
                            size = item[3]
                            break
                if best_match is None:
                    no_best_matches.append(anomaly_timeseries_matches[index])
                if best_match:
                    size = best_matches_dict[best_match][3]
                timestamps = [int(t) for t, v in anomaly_timeseries[index:(index + size)]]
                if size == 1:
                    timestamps = [int(anomaly_timeseries[index][0])]
                best_match_values = []
                if best_match:
                    best_match_values = [v for t, v in pw4_timeseries[best_match:(best_match + size)]]
                    if size == 1:
                        # micro motif
                        best_match_values = [anomaly_timeseries[index][1]]
                for i_index, t in enumerate(timestamps):
                    try:
                        best_matches.append([int(t), int(best_match_values[i_index])])
                    except:
                        continue
            best_matches_timestamps = [int(t) for t, v in best_matches]
            best_matches_timestamps_dict = {}
            for index, item in enumerate(best_matches):
                t = int(item[0])
                best_matches_timestamps_dict[t] = index
            missing = []
            for index, item in enumerate(anomaly_timeseries):
                if int(item[0]) not in best_matches_timestamps:
                    missing.append([index, int(item[0]), item])
            logger.info('%s :: %s :: missing count: %s' % (
                app_module, function_str, str(len(missing))))
            if len(no_best_matches) > 0:
                logger.info('%s :: %s :: no_best_matches: %s' % (
                app_module, function_str, str(len(no_best_matches))))
            new_best_matches = []
            for index, item in enumerate(anomaly_timeseries):
                t = int(item[0])
                if t not in best_matches_timestamps:
                    new_best_matches.append(anomaly_timeseries[index])
                else:
                    t_index = best_matches_timestamps_dict[t]
                    new_best_matches.append(best_matches[t_index])
            try:
                plotted_file = plot_match(metric, 0, anomaly_timeseries, new_best_matches, output_file)
                if os.path.isfile(plotted_file):
                    plotted_files.append(plotted_file)
            except Exception as err:
                logger.error('error :: %s :: %s :: plot_match failed to plot match, err: %s' % (
                    app_module, function_str, err))
            try:
                output_file = output_file.replace('motif_annihilation', 'motif_annihilation.derived')
                plotted_file = plot_match(metric, 0, new_best_matches, [], output_file, plot_annotations={'derived_timeseries': True})
                if os.path.isfile(plotted_file):
                    plotted_files.append(plotted_file)
            except Exception as err:
                logger.error('error :: %s :: %s :: plot_match failed to plot derived time series, err: %s' % (
                    app_module, function_str, err))
            # Plot pw5_timeseries
            pw5_timeseries = list(pw4_timeseries)
            for item in anomaly_timeseries:
                pw5_timeseries.append(item)
            metric_timestamp = anomaly_timeseries[-1][0]
            pw4_timeseries_padded = [[t, v] if t < (metric_timestamp - (86400 * 7)) else [t, np.nan] for t, v in pw5_timeseries]
            padded_anomaly_timeseries = [[t, v] if t > (metric_timestamp - (86400 * 7)) else [t, np.nan] for t, v in pw5_timeseries]
            try:
                output_file = output_file.replace('motif_annihilation.derived', 'motif_annihilation.pw5_timeseries')
                plotted_file = plot_match(metric, 0, pw4_timeseries_padded, padded_anomaly_timeseries, output_file, plot_annotations={'pw5_timeseries': True})
                if os.path.isfile(plotted_file):
                    plotted_files.append(plotted_file)
            except Exception as err:
                logger.error('error :: %s :: %s :: plot_match failed to plot derived time series, err: %s' % (
                    app_module, function_str, err))

            return plotted_files


        def determine_external_analysis_host(metric, external_motif_annihilation_hosts):
            try:
                external_hosts = list(external_motif_annihilation_hosts.keys())
                number_of_external_hosts = len(external_hosts)
                if number_of_external_hosts == 1:
                    return external_hosts[0]
                external_analysis_host = None
                metric_as_bytes = str(metric).encode()
                value = zlib.adler32(metric_as_bytes)
                for index, external_host in enumerate(external_hosts):
                    modulo_result = value % number_of_external_hosts
                    if modulo_result == index:
                        external_analysis_host = external_host
                        break
            except:
                external_analysis_host = None
            return external_analysis_host

        def return_wind_results(results_url, work_dict, results):
            function_str = 'return_wind_results'
            try:
                logger.info('%s :: %s :: returning results to %s' % (
                    app_module, function_str, results_url))
                wind_results = {}
                work_dict['work_host'] = this_host
                work_dict['results'] = results
                try:
                    wind_results = wind_return_results(skyline_app, work_dict)
                except Exception as err:
                    logger.error('error :: %s :: %s :: wind_return_results failed, err: %s' % (
                        app_module, function_str, err))
                if wind_results:
                    logger.info('%s :: %s :: results_url responded with to %s' % (
                        app_module, function_str, str(wind_results['status_code'])))
            except Exception as err:
                logger.error('error :: %s :: %s :: return_wind_results failed, err: %s' % (
                    app_module, function_str, err))
            return

        learn = False
        metric = None
        results = {}
        anomaly_timeseries_annihilated = False
        source_host = str(this_host)
        work_host = str(this_host)

        hash_key = None
        try:
            metric_id = work_dict['metric_id']
            metric = work_dict['metric']
            anomaly_id = work_dict['anomaly_id']
            metric_timestamp = work_dict['anomaly_timestamp']
            hash_key = work_dict['hash_key']
        except Exception as err:
            logger.error('error :: %s :: %s :: failed to determine details from work_dict, err: %s' % (
                app_module, function_str, err))
            remove_hash_key(hash_key, 0, work_host, source_host)
            return

        logger.info('%s :: %s :: processing %s' % (
            app_module, function_str, str(hash_key)))

        # Allow for a Skyline node to do work for another Skyline node and post
        # the results back to the originating Skyline node using wind
        external_results = {}
        work_host = None
        try:
            if 'results' in work_dict.keys():
                external_results = work_dict['results']
            if external_results:
                if 'work_host' in work_dict.keys():
                    work_host = work_dict['work_host']
        except Exception as err:
            logger.error('error :: %s :: %s :: failed to determine results from work_dict, err: %s' % (
                app_module, function_str, err))
        if external_results:
            logger.info('%s :: %s :: processing results from sent from %s' % (
                app_module, function_str, str(work_host)))

        results_url = None
        if not external_results:
            # Accept work
            try:
                if 'results_url' in work_dict.keys():
                    results_url = work_dict['results_url']
                    source_host = work_dict['source_host']
            except Exception as err:
                logger.error('error :: %s :: %s :: failed to determine details from work_dict, err: %s' % (
                    app_module, function_str, err))

        if source_host == this_host:
            done_key = 'ionosphere.find_repetitive_patterns.motif_annihilation.%s' % str(anomaly_id)
        else:
            done_key = 'ionosphere.find_repetitive_patterns.motif_annihilation.%s.%s' % (
                source_host, str(anomaly_id))

        if not results_url and not external_results:
            done_key_exists = False
            try:
                done_key_exists = self.redis_conn_decoded.exists(done_key)
            except Exception as err:
                logger.error('error :: %s :: remove_hash_key :: exists failed on %s Redis key, err: %s' % (
                    app_module, done_key, err))
            if done_key_exists:
                logger.info('%s :: %s :: work already done for anomaly_id: %s, removing nothing to do' % (
                    app_module, function_str, str(anomaly_id)))
                remove_hash_key(hash_key, anomaly_id, work_host, source_host)
                return

        local_metric = True
        if results_url:
            local_metric = False

        use_metric = str(metric)
        if '_tenant_id="' in metric:
            use_metric = 'labelled_metrics.%s' % str(metric_id)
         
        # Only process metric anomalies that have a training data dir
        if local_metric:
            metric_timeseries_dir = use_metric.replace('.', '/')
            metric_training_data_dir = '%s/%s/%s' % (
                settings.IONOSPHERE_DATA_FOLDER, metric_timestamp,
                metric_timeseries_dir)
            if not os.path.exists(metric_training_data_dir):
                logger.info('%s :: %s :: nothing to do, no metric_training_data_dir: %s' % (
                    app_module, function_str, metric_training_data_dir))
                remove_hash_key(hash_key, anomaly_id, work_host, source_host)
                return

        pw5_timeseries = []

        if external_results or results_url:
            try:
                pw5_timeseries = work_dict['pw5_timeseries']
            except Exception as err:
                logger.error('error :: %s :: %s :: failed to determine pw5_timeseries from external results work_dict, err: %s' % (
                    app_module, function_str, err))
                remove_hash_key(hash_key, anomaly_id, work_host, source_host)
                return

        if not pw5_timeseries and not external_results:
            try:
                if '_tenant_id="' not in metric:
                    pw5_timeseries = get_graphite_metric(
                        skyline_app, metric, (metric_timestamp - ((86400 * 7) * 5)),
                        (metric_timestamp + 3600), 'list', 'object')
                else:
                    pw5_timeseries = get_victoriametrics_metric(
                        skyline_app, metric, (metric_timestamp - ((86400 * 7) * 5)),
                        (metric_timestamp + 3600), 'list', 'object')
            except Exception as err:
                logger.error('error :: %s :: %s :: failed to get pw5_timeseries, err: %s' % (
                    app_module, function_str, err))
            if pw5_timeseries:
                logger.info('%s :: %s :: pw5_timeseries was surfaced with %s data points' % (
                    app_module, function_str, str(len(pw5_timeseries))))
                downsampled_timeseries = []
                resolution = 0
                try:
                    resolution = determine_data_frequency(skyline_app, pw5_timeseries, False)
                except Exception as err:
                    logger.error('error :: %s :: %s :: determine_data_frequency failed, err: %s' % (
                        app_module, function_str, err))
                logger.info('%s :: %s :: pw5_timeseries, resolution: %s' % (
                    app_module, function_str, str(resolution)))
                if resolution < 600:
                    logger.info('%s :: %s :: downsampling pw5_timeseries resolution to 600' % (
                        app_module, function_str))
                    try:
                        downsampled_timeseries = downsample_timeseries(skyline_app, pw5_timeseries, resolution, 600, 'mean', 'end')
                    except Exception as err:
                        logger.error('error :: %s :: %s :: downsample_timeseries failed, err: %s' % (
                            app_module, function_str, err))
                if downsampled_timeseries:
                    pw5_timeseries = list(downsampled_timeseries)

        if not pw5_timeseries:
            logger.info('%s :: %s :: no pw5_timeseries was surfaced, nothing to do' % (
                app_module, function_str))
            remove_hash_key(hash_key, anomaly_id, work_host, source_host)
            return

        # Outsource the analysis to an external Skyline node
        external_analysis_host = None
        if IONOSPHERE_EXTERNAL_MOTIF_ANNIHILATION_HOSTS_DICT and not results_url:
            # Map the metric to a host
            try:
                external_analysis_host = determine_external_analysis_host(metric, IONOSPHERE_EXTERNAL_MOTIF_ANNIHILATION_HOSTS_DICT)
            except Exception as err:
                logger.error('error :: %s :: %s :: determine_external_analysis_host failed, err: %s' % (
                    app_module, function_str, err))
        if external_results:
            external_analysis_host = None
        if external_analysis_host:
            logger.info('%s :: %s :: sending job to %s' % (
                app_module, function_str, external_analysis_host))
            submitted_job_response_dict = {}
            try:
                work_dict['source_host'] = this_host
                work_dict['app'] = skyline_app
                work_dict['job'] = 'motif_annihilation'
                work_dict['redis_work_hash'] = 'ionosphere.find_repetitive_patterns.motif_annihilation.work'
                work_dict['redis_work_hash_key'] = hash_key
                work_dict['data'] = {
                    'metric_id': metric_id, 'metric': metric,
                    'anomaly_id': anomaly_id, 'anomaly_timestamp': metric_timestamp
                }
                results_url = '%s/wind' % settings.SKYLINE_URL
                work_dict['results_url'] = results_url
                work_dict['results'] = None
                work_dict['pw5_timeseries'] = pw5_timeseries
                work_url = '%s/wind' % IONOSPHERE_EXTERNAL_MOTIF_ANNIHILATION_HOSTS_DICT[external_analysis_host]['url']
                work_dict['work_url'] = work_url
                if 'auth_user' in IONOSPHERE_EXTERNAL_MOTIF_ANNIHILATION_HOSTS_DICT[external_analysis_host].keys():
                    work_dict['auth_user'] = IONOSPHERE_EXTERNAL_MOTIF_ANNIHILATION_HOSTS_DICT[external_analysis_host]['auth_user']
                if 'auth_password' in IONOSPHERE_EXTERNAL_MOTIF_ANNIHILATION_HOSTS_DICT[external_analysis_host].keys():
                    work_dict['auth_password'] = IONOSPHERE_EXTERNAL_MOTIF_ANNIHILATION_HOSTS_DICT[external_analysis_host]['auth_password']
                if user:
                    work_dict['results_url_user'] = user
                if password:
                    work_dict['results_url_password'] = password
                work_dict['results_url'] = results_url

                try:
                    submitted_job_response_dict = wind_submit_job(skyline_app, work_dict)
                except Exception as err:
                    logger.error('error :: %s :: %s :: wind_submit_job failed, err: %s' % (
                        app_module, function_str, err))
            except Exception as err:
                logger.error('error :: %s :: %s :: error encountered populating work_dict to use for wind_submit_job, err: %s' % (
                    app_module, function_str, err))
            if submitted_job_response_dict:
                logger.info('%s :: %s :: submitted job to external_analysis_host and got status_code: %s' % (
                    app_module, function_str, str(submitted_job_response_dict['status_code'])))
            remove_hash_key(hash_key, anomaly_id, work_host, source_host)
            return

        original_pw5_timeseries = [item for item in pw5_timeseries if item[0] <= metric_timestamp]
        original_anomaly_timeseries = [item for item in pw5_timeseries if item[0] >= (metric_timestamp - (86400 * 7)) and item[0] <= metric_timestamp]
        original_anomaly_timeseries_plus_3600 = [item for item in pw5_timeseries if item[0] >= (metric_timestamp - (86400 * 7)) and item[0] <= (metric_timestamp + 3600)]
        original_pw4_timeseries = [item for item in pw5_timeseries if item[0] >= (metric_timestamp - ((86400 * 7) * 5)) and item[0] <= (metric_timestamp - (86400 * 7))]

        if results_url:
            results['work_host'] = this_host
            results['anomaly_timeseries_annihilated'] = anomaly_timeseries_annihilated
            results['learn'] = learn
            results['anomaly_timeseries'] = original_anomaly_timeseries
            results['anomaly_timeseries_plus_3600'] = original_anomaly_timeseries_plus_3600
            results['pw4_timeseries'] = original_pw4_timeseries

        converted = []
        for datapoint in original_pw5_timeseries:
            try:
                new_datapoint = [int(float(datapoint[0])), float(datapoint[1])]
                converted.append(new_datapoint)
            except:
                continue
        minmax_timeseries = []
        if converted:
            minmax_values = [x[1] for x in converted]
            x_np = np.asarray(minmax_values)
            # Min-Max scaling
            if len(x_np) > 0:
                np_minmax = (x_np - x_np.min()) / (x_np.max() - x_np.min())
                for (ts, v) in zip(converted, np_minmax):
                    minmax_timeseries.append([ts[0], float(v)])

        anomaly_timeseries = [item for item in minmax_timeseries if item[0] >= (metric_timestamp - (86400 * 7)) and item[0] <= metric_timestamp]
        anomaly_timeseries_plus_3600 = [item for item in minmax_timeseries if item[0] >= (metric_timestamp - (86400 * 7)) and item[0] <= (metric_timestamp + 3600)]
        pw4_timeseries = [item for item in minmax_timeseries if item[0] >= (metric_timestamp - ((86400 * 7) * 5)) and item[0] <= (metric_timestamp - (86400 * 7))]
        pw5_timeseries = list(minmax_timeseries)

        batch_size = 6
        max_distance = 1.6
        max_area_percent_diff = 20.0
        candidate = False
        candidate_dict = {}
        if not external_results:
            candidate, candidate_dict = candidate_timeseries(anomaly_timeseries, pw4_timeseries)
            logger.info('%s :: %s :: candidate: %s, candidate_dict: %s' % (
                app_module, function_str, str(candidate), str(candidate_dict)))
        if results_url:
            results['candidate'] = candidate
            results['candidate_dict'] = candidate_dict

        if external_results:
            try:
                candidate = external_results['candidate']
            except Exception as err:
                logger.error('error :: %s :: %s :: failed to get candidate from result, err: %s' % (
                    app_module, function_str, err))
            try:
                candidate_dict = external_results['candidate_dict']
            except Exception as err:
                logger.error('error :: %s :: %s :: failed to get candidate_dict from result, err: %s' % (
                    app_module, function_str, err))

        if not candidate:
            logger.info('%s :: %s :: not checking for annihilation motifs because the min or max values were not found at least 4 times in the pw4_timeseries' % (
                app_module, function_str))
            if results_url:
                try:
                    return_wind_results(results_url, work_dict, results)
                except Exception as err:
                    logger.error('error :: %s :: %s :: return_wind_results failed, err: %s' % (
                        app_module, function_str, err))
            remove_hash_key(hash_key, anomaly_id, work_host, source_host)
            return

        start_all = time()
        motif_analysis = {}

        if external_results:
            try:
                motif_analysis = external_results['motif_analysis']
                batch_size_str = str(batch_size)
                logger.info('%s :: %s :: %s annihilation motifs found in external_results' % (
                    app_module, function_str, str(len(list(motif_analysis[batch_size_str]['motifs'].keys())))))
                # Coerce batch_size str into int
                motif_analysis[batch_size] = copy.deepcopy(motif_analysis[batch_size_str])
                del motif_analysis[batch_size_str]
            except Exception as err:
                logger.error('error :: %s :: %s :: failed to get motif_analysis from external_results, err: %s' % (
                    app_module, function_str, err))

        if not external_results and not motif_analysis:
            try:
                motif_analysis = annihilation_motifs(batch_size, max_distance, max_area_percent_diff, pw4_timeseries)
            except Exception as err:
                logger.error('error :: %s :: %s :: annihilation_motifs failed, err: %s' % (
                    app_module, function_str, err))
        if results_url:
            results['motif_analysis'] = motif_analysis

        if not len(motif_analysis) > 0:
            if results_url:
                try:
                    return_wind_results(results_url, work_dict, results)
                except Exception as err:
                    logger.error('error :: %s :: %s :: return_wind_results failed, err: %s' % (
                        app_module, function_str, err))
            remove_hash_key(hash_key, anomaly_id, work_host, source_host)
            return

        logger.info('%s :: %s :: %s annihilation motifs found' % (
            app_module, function_str, str(len(list(motif_analysis[batch_size]['motifs'].keys())))))

        annihilated_timeseries_indices = []
        matched_motifs = {}

        if external_results:
            try:
                annihilated_timeseries_indices = external_results['annihilated_timeseries_indices']
                logger.info('%s :: %s :: %s annihilated_timeseries_indices found in external_results' % (
                    app_module, function_str, str(len(annihilated_timeseries_indices))))
            except Exception as err:
                logger.error('error :: %s :: %s :: failed to get annihilated_timeseries_indices from external_results, err: %s' % (
                    app_module, function_str, err))
            try:
                matched_motifs = external_results['matched_motifs']
                logger.info('%s :: %s :: %s matched_motifs found in external_results' % (
                    app_module, function_str, str(len(matched_motifs))))
            except Exception as err:
                logger.error('error :: %s :: %s :: failed to get matched_motifs from external_results, err: %s' % (
                    app_module, function_str, err))
                logger.info('%s :: %s :: nothing to process from external_results' % (
                    app_module, function_str))
                remove_hash_key(hash_key, anomaly_id, work_host, source_host)
                return

        if not external_results:
            done = 0
            start_annihilation = time()
            anomaly_timeseries_indices = [index for index, item in enumerate(anomaly_timeseries)]
            for motif_id in list(motif_analysis[batch_size]['motifs'].keys()):
                done += 1
                motif_timeseries = pw4_timeseries[motif_id:(motif_id + batch_size)]
                motif_annihilated_timeseries_indices_set = []
                try:
                    motif_annihilated_timeseries_indices_set, motifs_matched = motif_annihilated_timeseries_indices(motif_id, motif_timeseries, anomaly_timeseries_plus_3600, known_annihilated_timeseries_indices=annihilated_timeseries_indices, debug=False)
                    matched_motifs[motif_id] = motifs_matched
                except Exception as err:
                    logger.error(traceback.format_exc())
                    logger.error('error :: %s :: %s :: motif_annihilated_timeseries_indices failed on motif_id: %s, err: %s' % (
                        app_module, function_str, str(motif_id), err))
                    continue
                for i in list(motif_annihilated_timeseries_indices_set):
                    annihilated_timeseries_indices.append(i)
                annihilated_timeseries_indices = sorted(list(set(annihilated_timeseries_indices)))
                if annihilated_timeseries_indices == anomaly_timeseries_indices:
                    logger.info('%s :: %s :: timeseries annihilated after %s motifs compared of %s motifs' % (
                        app_module, function_str, str(done),
                        str(len(list(motif_analysis[batch_size]['motifs'].keys())))))
                    break
            logger.info('%s :: %s :: annihilated %s indices in %s seconds' % (
                app_module, function_str, str(len(annihilated_timeseries_indices)),
                str(time() - start_annihilation)))
            if results_url:
                results['annihilated_timeseries_indices'] = annihilated_timeseries_indices
                results['matched_motifs'] = dict(matched_motifs)

        annihilated_anomaly_timeseries = []
        annihilated_count = 0
        unannihilated_dict = {}
        annihilated_unannihilated_dict = {}

        if external_results:
            external_results_error = False
            try:
                annihilated_anomaly_timeseries = external_results['annihilated_anomaly_timeseries']
                logger.info('%s :: %s :: %s annihilated_anomaly_timeseries found in external_results' % (
                    app_module, function_str, str(len(annihilated_anomaly_timeseries))))
            except Exception as err:
                logger.error('error :: %s :: %s :: failed to get annihilated_timeseries_indices from external_results, err: %s' % (
                    app_module, function_str, err))
                external_results_error = True
            try:
                annihilated_count = external_results['annihilated_count']
                logger.info('%s :: %s :: annihilated_count: %s, found in external_results' % (
                    app_module, function_str, str(annihilated_count)))
            except Exception as err:
                logger.error('error :: %s :: %s :: failed to get annihilated_count from external_results, err: %s' % (
                    app_module, function_str, err))
                external_results_error = True
            try:
                unannihilated_dict = external_results['unannihilated_dict']
                logger.info('%s :: %s :: %s items in unannihilated_dict found in external_results' % (
                    app_module, function_str, str(len(unannihilated_dict))))
            except Exception as err:
                logger.error('error :: %s :: %s :: failed to get unannihilated_dict from external_results, err: %s' % (
                    app_module, function_str, err))
                external_results_error = True
            try:
                annihilated_unannihilated_dict = external_results['annihilated_unannihilated_dict']
                logger.info('%s :: %s :: %s items annihilated_annihilated_unannihilated_dict found in external_results' % (
                    app_module, function_str, str(len(annihilated_unannihilated_dict))))
            except Exception as err:
                logger.error('error :: %s :: %s :: failed to get annihilated_unannihilated_dict from external_results, err: %s' % (
                    app_module, function_str, err))
                external_results_error = True
            if external_results_error:
                logger.info('%s :: %s :: missing data from external_results, not processing' % (
                    app_module, function_str))
                remove_hash_key(hash_key, anomaly_id, work_host, source_host)
                return

        if not external_results:
            try:
                # The pw5_timeseries is used here to allow for values and deltas to
                # be determine across the boundaries of the pw4_timeseries and the
                # anomaly_timeseries if they are required
                annihilated_anomaly_timeseries, annihilated_count, unannihilated_dict, annihilated_unannihilated_dict = annihilate_timeseries(batch_size, anomaly_timeseries, annihilated_timeseries_indices, pw5_timeseries)
            except Exception as err:
                logger.error(traceback.format_exc())
                logger.error('error :: %s :: %s :: annihilate_timeseries failed, err: %s' % (
                    app_module, function_str, err))
            # If there are micro motif annihilations add them to the matched_motifs
            if annihilated_unannihilated_dict and matched_motifs:
                micro_motifs_key = list(matched_motifs.keys())[-1] + batch_size
                matched_motifs[micro_motifs_key] = {}
                for key, item in annihilated_unannihilated_dict.items():
                    data = {
                        'subsequence_start_index': int(key),
                        'match_sequence_start_index': int(key),
                        'distance': 0.0,
                        'size': 1,
                        'max_distance': float(max_distance),
                        'motif_area': 0.0,
                        'subsequence_motif_area': 0.0,
                        'area_percent_diff': 0.0,
                        'max_area_percent_diff': float(max_area_percent_diff),
                        'match_type': 'micro_motif'
                    }
                    matched_motifs[micro_motifs_key][key] = {key: data}
            if results_url:
                results['matched_motifs'] = dict(matched_motifs)
                results['annihilated_anomaly_timeseries'] = annihilated_anomaly_timeseries
                results['annihilated_count'] = annihilated_count
                results['unannihilated_dict'] = unannihilated_dict
                results['annihilated_unannihilated_dict'] = annihilated_unannihilated_dict
        if annihilated_count == len(anomaly_timeseries):
            learn = True
            anomaly_timeseries_annihilated = True
            logger.info('%s :: %s :: ANNILIHILATED - the anomaly timeseries was annihilated by common motifs, good to LEARN' % (
                app_module, function_str))
            if results_url:
                results['learn'] = learn
                results['anomaly_timeseries_annihilated'] = anomaly_timeseries_annihilated

        annihilation_process_runtime = time() - start_all
        logger.info('%s :: %s :: the annihilation process took %s seconds' % (
            app_module, function_str, str(annihilation_process_runtime)))

        # If this is a wind job then all analysis work is done at this stage so
        # results are now returned
        if results_url:
            results['annihilation_process_runtime'] = annihilation_process_runtime            
            try:
                return_wind_results(results_url, work_dict, results)
            except Exception as err:
                logger.error('error :: %s :: %s :: return_wind_results failed, err: %s' % (
                    app_module, function_str, err))
            remove_hash_key(hash_key, anomaly_id, work_host, source_host)
            return

        if not learn:
            logger.info('%s :: %s :: the anomaly timeseries could not be annihilated by common motifs, not valid to LEARN' % (
                app_module, function_str))
            remove_hash_key(hash_key, anomaly_id, work_host, source_host)
            return

        # LEARN
        # Save the resources that were learnt from
        pw5_json_file = '%s/%s.motif_annihilation.pw5_timeseries.json' % (metric_training_data_dir, use_metric)
        if not os.path.isfile(pw5_json_file):
            timeseries_json = str(original_pw5_timeseries).replace('[', '(').replace(']', ')')
            try:
                write_data_to_file(skyline_app, pw5_json_file, 'w', timeseries_json)
                logger.info('%s :: %s :: added pw5_timeseries json file :: %s' % (
                    app_module, function_str, pw5_json_file))
            except Exception as err:
                logger.error('error :: %s :: %s :: failed to add %s, err: %s' % (
                    app_module, function_str, pw5_json_file, err))
        matched_motifs_file = '%s/%s.motif_annihilation.matched_motifs.json' % (metric_training_data_dir, use_metric)
        if not os.path.isfile(matched_motifs_file):
            try:
                with open(matched_motifs_file, 'w') as fh:
                    json.dump(matched_motifs, fh)
                logger.info('%s :: %s :: added matched_motifs json file :: %s' % (
                    app_module, function_str, matched_motifs_file))
            except Exception as err:
                logger.error('error :: %s :: %s :: failed to add %s, err: %s' % (
                    app_module, function_str, matched_motifs_file, err))
        annihilation_plot_file = '%s/%s.motif_annihilation.png' % (metric_training_data_dir, use_metric)
        plotted_files = []
        try:
            plotted_files = plot_annihilation(use_metric, batch_size, original_anomaly_timeseries, matched_motifs, original_pw4_timeseries, annihilation_plot_file)
            logger.info('%s :: %s :: plot_annihilation created %s plots' % (
                app_module, function_str, str(len(plotted_files))))
        except Exception as err:
            logger.error('error :: %s :: %s :: plot_annihilation failed to add %s, err: %s' % (
                app_module, function_str, annihilation_plot_file, err))
        if plotted_files:
            for plotted_file in plotted_files:
                logger.info('%s :: %s :: added plot :: %s' % (
                    app_module, function_str, plotted_file))

        # CREATE FEATURES_PROFILE
        logger.info('%s :: %s :: creating LEARNT features profile for %s, anomaly_timestamp: %s' % (
            app_module, function_str, metric, str(metric_timestamp)))

        fp_in_successful = None
        create_context = 'training_data'
        features_file = '%s/%s.tsfresh.input.csv.features.transposed.csv' % (metric_training_data_dir, use_metric)
        if not os.path.isfile(features_file):
            logger.info('%s :: %s :: extracting features' % (app_module, function_str))
            try:
                fp_csv, successful, fp_exists, fp_id, log_msg, traceback_format_exc, f_calc = calculate_features_profile(skyline_app, metric_timestamp, metric, 'training_data')
            except Exception as err:
                logger.error('error :: %s :: calculate_features_profile failed for metric: %s, timestamp: %s - %s' % (
                    function_str, metric, str(metric_timestamp), err))
        try:
            fp_learn = False
            learn_parent_id = 0
            generation = 2
            ionosphere_job = 'motif_annihilation'
            slack_ionosphere_job = 'motif_annihilation'
            user_id = 1
            label = 'LEARNT - motif_annihilation'
            fp_id, fp_in_successful, fp_exists, fail_msg, traceback_format_exc = create_features_profile(skyline_app, metric_timestamp, metric, create_context, ionosphere_job, learn_parent_id, generation, fp_learn, slack_ionosphere_job, user_id, label)
            if fp_exists:
                logger.warning('warning :: %s :: %s :: failed to create a features profile for %s, %s as an fp already exists' % (
                    app_module, function_str, metric, str(metric_timestamp)))
        except Exception as err:
            logger.error(traceback.format_exc())
            logger.error('error :: %s :: %s :: failed to create a features profile for %s, %s, err: %s' % (
                app_module, function_str, metric, str(metric_timestamp), err))
        if fp_in_successful is False:
            logger.error(traceback_format_exc)
            logger.error(fail_msg)
            logger.error('error :: %s :: %s :: failed to create a features profile for %s, %s' % (
                app_module, function_str, metric, str(metric_timestamp)))
        if fp_id:
            logger.info('%s :: %s :: created fp_id: : %s' % (app_module, function_str, str(fp_id)))

        if external_results:
            done_key = 'ionosphere.find_repetitive_patterns.motif_annihilation.%s' % str(anomaly_id)
            done_key_exists = False
            try:
                done_key_exists = self.redis_conn_decoded.exists(done_key)
            except Exception as err:
                logger.error('error :: %s :: remove_hash_key :: exists failed on %s Redis key, err: %s' % (
                    app_module, done_key, err))
            if done_key_exists:
                logger.info('%s :: %s :: work already done for anomaly_id: %s, removing nothing to do' % (
                    app_module, function_str, str(anomaly_id)))
                remove_hash_key(hash_key, anomaly_id, work_host, source_host)
                return

        remove_hash_key(hash_key, anomaly_id, work_host, source_host)

        return


    def run(self):
        """
        - Called when the process intializes.

        - Determine if Redis is up and discover checks to run.

        - Assign check to run.

        - Wait for the processes to finish.

        """

        # Log management to prevent overwriting
        # Allow the bin/<skyline_app>.d to manage the log
        now = time()
        log_wait_for = now + 5
        while now < log_wait_for:
            if os.path.isfile(skyline_app_loglock):
                sleep(.1)
                now = time()
            else:
                now = log_wait_for + 1

        logger.info('starting %s motif_annihilation' % skyline_app)
        if os.path.isfile(skyline_app_loglock):
            logger.error('error - bin/%s.d log management seems to have failed, continuing' % skyline_app)

        while True:
            now = time()
            # Make sure Redis is up
            try:
                self.redis_conn_decoded.ping()
            except:
                logger.error(traceback.format_exc())
                logger.error('error :: cannot connect to redis at socket path %s' % settings.REDIS_SOCKET_PATH)
                sleep(10)

            # Determine if any suitable metric anomalies are to be processed
            work_dict = {}
            while True:

                current_timestamp = int(time())

                # Determine if there are any anomalies to process.  These are
                # added by ionosphere.find_repetitive_patterns which does all
                # the work to determine what metrics are including in learning,
                # etc.  All metrics added to this hash are valid for learning.
                motif_annihilation_work = {}
                try:
                    motif_annihilation_work = self.redis_conn_decoded.hgetall('ionosphere.find_repetitive_patterns.motif_annihilation.work')
                except Exception as err:
                    logger.error('error :: could not query Redis hash ionosphere.motif_annihilation.work, err: %s' % err)
                if len(motif_annihilation_work) == 0:
                    logger.info('%s :: no work, sleeping for 60 seconds' % app_module)
                    sleep(59)
                    continue

                skyline_busy = False
                if not IONOSPHERE_EXTERNAL_MOTIF_ANNIHILATION_HOSTS_DICT:
                    try:
                        skyline_busy = is_skyline_busy(self, skyline_app)
                    except Exception as err:
                        logger.error('error :: %s :: is_skyline_busy failed, err: %s' % (
                            app_module, err))

                if skyline_busy:
                    logger.info('%s :: skyline is busy - sleeping for 60 seconds' % app_module)
                    sleep(59)
                    continue

                work_timestamp_keys = list(sorted(list(motif_annihilation_work.keys())))
                work_timestamp_key_to_process = None
                for work_timestamp_key in work_timestamp_keys:
                    if (int(float(work_timestamp_key)) + 4200) < current_timestamp:
                        # @added 20240910 - Feature #5318: motif_annihilation
                        # Remove old keys
                        if int(float(work_timestamp_key)) < (current_timestamp - (3600 * 3)):
                            try:
                                self.redis_conn_decoded.hdel('ionosphere.find_repetitive_patterns.motif_annihilation.work', work_timestamp_key)
                                logger.info('%s :: hdel old key %s from ionosphere.find_repetitive_patterns.motif_annihilation.work Redis hash' % (
                                    app_module, work_timestamp_key))
                                continue
                            except Exception as err:
                                logger.error('error :: %s :: failed to hdel old key %s from ionosphere.find_repetitive_patterns.motif_annihilation.work Redis hash, err: %s' % (
                                    app_module, work_timestamp_key, err))

                        continue
                    else:
                        work_timestamp_key_to_process = work_timestamp_key

                if not work_timestamp_key_to_process:
                    logger.info('%s :: no work ready to do done - sleeping for 60 seconds' % app_module)
                    sleep(59)
                    continue

                work_timestamp_key = work_timestamp_key_to_process
                try:
                    work_dict = literal_eval(motif_annihilation_work[work_timestamp_key])
                except Exception as err:
                    logger.error('error :: %s :: literal_eval failed on motif_annihilation_work[%s], err: %s' % (
                        app_module, work_timestamp_key, err))
                    # @added 20240910 - Feature #5318: motif_annihilation
                    if 'malformed node or string' in str(err):
                        try:
                            self.redis_conn_decoded.hdel('ionosphere.find_repetitive_patterns.motif_annihilation.work', work_timestamp_key)
                        except Exception as err:
                            logger.error('error :: %s :: failed to hdel literal_eval fail key %s from ionosphere.find_repetitive_patterns.motif_annihilation.work Redis hash, err: %s' % (
                                app_module, work_timestamp_key, err))

                if work_dict:
                    work_dict['hash_key'] = work_timestamp_key
                    break

            remove_work = False
            if work_dict:
                log_work_dict = dict(work_dict)
                log_work_dict['pw5_timeseries'] = 'pw5_timeseries redacted for log'
                if 'results' in log_work_dict.keys():
                    if isinstance(log_work_dict['results'], dict):
                        if len(log_work_dict['results']) > 0:
                            log_work_dict['results'] = 'results redacted for log'
                if 'auth_password' in log_work_dict.keys():
                    # @modified 20241106 - Task #5526: Build v5.0.0 and upgrade deps
                    # bandit - B105:hardcoded_password_string - Possible hardcoded password
                    log_work_dict['auth_password'] = 'redacted for log'  # nosec B105
                if 'results_url_password' in log_work_dict.keys():
                    log_work_dict['results_url_password'] = 'redacted for log'  # nosec B105
                logger.info('%s :: processing work_dict: %s' % (app_module, str(log_work_dict)))
                del log_work_dict
                # Spawn process
                pids = []
                spawned_pids = []
                pid_count = 0
                p = Process(target=self.spin_motif_annihilation, args=(work_dict,))
                pids.append(p)
                pid_count += 1
                logger.info('%s :: starting spin_motif_annihilation' % app_module)
                p.start()
                spawned_pids.append(p.pid)

                # Self monitor processes and terminate if any spin_snab_process
                # that has run for longer than 58 seconds
                p_starts = time()
                while time() - p_starts <= 1200:
                    if any(p.is_alive() for p in pids):
                        # Just to avoid hogging the CPU
                        sleep(.1)
                    else:
                        # All the processes are done, break now.
                        time_to_run = time() - p_starts
                        logger.info('%s :: spin_motif_annihilation completed in %.2f seconds' % (
                            app_module, time_to_run))
                        break
                else:
                    # We only enter this if we didn't 'break' above.
                    logger.info('%s :: timed out, killing all spin_motif_annihilation process' % app_module)
                    for p in pids:
                        p.terminate()
                        remove_work = True
                for p in pids:
                    if p.is_alive():
                        logger.info('%s :: stopping spin_motif_annihilation - %s' % (app_module, str(p.is_alive())))
                        killing_pid = p.pid
                        logger.info('%s :: kill spin_motif_annihilation with pid: %s' % (app_module, str(killing_pid)))
                        p.terminate()
                        logger.info('%s :: killed spin_motif_annihilation process with pid: %s' % (app_module, str(killing_pid)))
                        remove_work = True
                if remove_work:
                    logger.info('%s :: removing failed work key %s from ionosphere.find_repetitive_patterns.motif_annihilation.work' % (
                        app_module, work_timestamp_key))
                    try:
                        self.redis_conn_decoded.hdel('ionosphere.find_repetitive_patterns.motif_annihilation.work', work_timestamp_key)
                    except Exception as err:
                        logger.error('error :: %s :: failed to remove failed work key %s from ionosphere.find_repetitive_patterns.motif_annihilation.work, err: %s' % (
                            app_module, work_timestamp_key, err))
