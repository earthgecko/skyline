from __future__ import division
import logging
from os import path
# import string
import time
from ast import literal_eval
from timeit import default_timer as timer
import traceback

from flask import request

import settings
import skyline_version
from skyline_functions import (
    mkdir_p, write_data_to_file,
    mysql_select,
    mirage_load_metric_vars,
)
# @added 20210414 - Feature #4014: Ionosphere - inference
#                   Branch #3590: inference
from motif_plots import plot_motif_match
from motif_match_types import motif_match_types_dict

from functions.memcache.get_fp_timeseries import get_fp_timeseries
from ionosphere.common_functions import get_metrics_db_object
from functions.database.queries.get_ionosphere_fp_row import get_ionosphere_fp_db_row
from functions.numpy.percent_different import get_percent_different

skyline_version = skyline_version.__absolute_version__
skyline_app = 'webapp'
skyline_app_logger = '%sLog' % skyline_app
logger = logging.getLogger(skyline_app_logger)
skyline_app_logfile = '%s/%s.log' % (settings.LOG_PATH, skyline_app)
logfile = '%s/%s.log' % (settings.LOG_PATH, skyline_app)


# @added 20210415 - Feature #4014: Ionosphere - inference
# @modified 20210417 - Feature #4014: Ionosphere - inference
# Allow the user to define the batch_size per similarity search
# @modified 20210425 - Feature #4014: Ionosphere - inference
# Added max_area_percent_diff for computing the area under the curve
def on_demand_motif_analysis(
        metric, timestamp, similarity, batch_size, top_matches, max_distance,
        range_padding, max_area_percent_diff):
    """
    Process a motif similarity search on demand
    """
    import numpy as np
    import mass_ts as mts

    logger = logging.getLogger(skyline_app_logger)
    dev_null = None
    function_str = 'on_demand_motif_analysis'
    logger.info('%s :: with parameters :: metric: %s, timestamp: %s, similarity: %s, batch_size:%s, top_matches: %s, max_distance: %s, range_padding: %s, max_area_percent_diff: %s' % (
        function_str, str(metric), str(timestamp), str(similarity),
        str(batch_size), str(top_matches), str(max_distance),
        str(range_padding), str(max_area_percent_diff)))
    trace = 'none'
    fail_msg = 'none'

    start = time.time()
    start_timer = timer()
    metric_vars_dict = {}
    metric_id = 0
    fp_ids = []
    timeseries = []
    not_similar_enough_sample = 0
    not_similar_motifs = 0
    similar_motifs = 0
    exact_motifs = 0
    distance_motifs = 0
    motifs_found = []
    find_exact_matches_run = False
    exact_matches_found = []
    fps_timeseries = {}
    # A motif_analysis dict to add to and return
    motif_analysis = {}
    motif_analysis[metric] = {}
    motif_analysis[metric]['timestamp'] = int(timestamp)
    motif_analysis[metric]['started'] = start
    motif_analysis[metric]['motifs'] = {}
    motif_analysis[metric]['exact_motifs'] = exact_motifs
    motif_analysis[metric]['similar_motifs'] = similar_motifs
    motif_analysis[metric]['not_similar_motifs'] = not_similar_motifs
    motif_analysis[metric]['not_similar_enough_sample'] = not_similar_enough_sample
    # @added 20210417 - Feature #4014: Ionosphere - inference
    # Allow the user to define the batch_size per similarity search
    motif_analysis[metric]['batch_size'] = int(batch_size)
    motif_analysis[metric]['top_matches'] = int(top_matches)
    motif_analysis[metric]['max_distance'] = float(max_distance)
    # @added 20210425 - Feature #4014: Ionosphere - inference
    # Added max_area_percent_diff for computing the area under the curve
    motif_analysis[metric]['max_area_percent_diff'] = float(max_area_percent_diff)

    fps_checked_for_motifs = []

    metric_dir = metric.replace('.', '/')
    metric_timeseries_dir = '%s/%s/%s' % (
        settings.IONOSPHERE_DATA_FOLDER, str(timestamp), metric_dir)

    # @added 20210418 - Feature #4014: Ionosphere - inference
    # Allow for the similarity search on saved_training_data
    if 'saved_training_data' in request.args:
        saved_training_data_str = request.args.get('saved_training_data', 'false')
        if saved_training_data_str == 'true':
            saved_metric_timeseries_dir = '%s_saved/%s/%s' % (
                settings.IONOSPHERE_DATA_FOLDER, str(timestamp), metric_dir)
            if path.exists(saved_metric_timeseries_dir):
                metric_timeseries_dir = saved_metric_timeseries_dir
                logger.info('%s :: using saved training_data dir - %s' % (function_str, saved_metric_timeseries_dir))

    metric_vars_file = '%s/%s.txt' % (metric_timeseries_dir, metric)
    timeseries_json = '%s/%s.json' % (metric_timeseries_dir, metric)
    full_duration_in_hours = int(settings.FULL_DURATION / 60 / 60)
    full_duration_timeseries_json = '%s/%s.mirage.redis.%sh.json' % (
        metric_timeseries_dir, metric, str(full_duration_in_hours))
    try:
        metric_vars_dict = mirage_load_metric_vars(skyline_app, metric_vars_file, True)
    except Exception as e:
        logger.error('error :: inference :: failed to load metric variables from check file - %s - %s' % (
            metric_vars_file, e))
    if not metric_vars_dict:
        motif_analysis[metric]['status'] = 'error'
        motif_analysis[metric]['reason'] = 'could not load training data variables'
        return motif_analysis

    full_duration = metric_vars_dict['metric_vars']['full_duration']

    # Determine the metric details from the database
    metric_id = 0
    metric_db_object = {}
    try:
        metric_db_object = get_metrics_db_object(metric)
    except Exception as e:
        logger.error('error :: %s :: failed to get_metrics_db_object - %s' % (function_str, e))
    try:
        metric_id = int(metric_db_object['id'])
    except Exception as e:
        logger.error('error :: %s :: failed to determine metric_id from metric_db_object %s - %s' % (
            function_str, str(metric_db_object), e))
        metric_id = 0
    if not metric_id:
        logger.error('error :: %s :: failed to get metric id for %s from the database' % (function_str, str(metric)))
        fail_msg = 'failed to get metric id'
        motif_analysis[metric]['status'] = 'error'
        motif_analysis[metric]['reason'] = 'could not determine metric id'
        return motif_analysis, fail_msg, trace

    # @modified 20210419 - Feature #4014: Ionosphere - inference
    # Create a unique dir for each batch_size max_distance
    # motif_images_dir = '%s/motifs' % metric_timeseries_dir
    motif_images_dir = '%s/motifs/batch_size.%s/top_matches.%s/max_distance.%s' % (
        metric_timeseries_dir, str(batch_size), str(top_matches), str(max_distance))

    if not path.exists(motif_images_dir):
        # provision motifs image resources
        mkdir_p(motif_images_dir)

    full_durations = [full_duration]
    if path.isfile(full_duration_timeseries_json):
        full_durations = [full_duration, settings.FULL_DURATION]
    logger.info('%s :: full_durations - %s' % (function_str, str(full_durations)))

    # Loop through analysis per full_duration
    for full_duration in full_durations:
        start_full_duration = timer()
        fp_ids = []
        try:
            query = 'SELECT id,last_matched from ionosphere WHERE metric_id=%s AND full_duration=%s AND enabled=1 ORDER BY last_matched DESC' % (
                str(metric_id), str(full_duration))
            results = mysql_select(skyline_app, query)
            for row in results:
                fp_ids.append(int(row[0]))
        except Exception as e:
            logger.error('error :: %s :: failed to get fp ids via mysql_select from %s - %s' % (function_str, metric, e))

        logger.info('%s :: metric_id: %s, full_duration: %s, fp_ids: %s' % (
            function_str, (metric_id), str(full_duration), str(fp_ids)))

        if not fp_ids:
            continue

        # Now there are known fps, load the timeseries
        if full_duration == settings.FULL_DURATION:
            timeseries_json_file = full_duration_timeseries_json
        else:
            timeseries_json_file = timeseries_json
        try:
            with open((timeseries_json_file), 'r') as f:
                raw_timeseries = f.read()
            timeseries_array_str = str(raw_timeseries).replace('(', '[').replace(')', ']')
            del raw_timeseries
            timeseries = literal_eval(timeseries_array_str)
            del timeseries_array_str
        except Exception as e:
            logger.error('error :: %s :: failed to load timeseries for %s from %s - %s' % (
                function_str, metric, timeseries_json_file, e))
            continue

        anomalous_timeseries_subsequence = []
        for timestamp_float, value in timeseries[-int(batch_size):]:
            anomalous_timeseries_subsequence.append([int(timestamp_float), value])

        logger.info('%s :: looking for motif in trained fps of full_duration: %s' % (function_str, (full_duration)))
        dataset = [float(item[1]) for item in anomalous_timeseries_subsequence]

        max_y = max(dataset)
        min_y = min(dataset)

        # full_y_range = max_y - min_y

        # range_padding_percent = range_padding
        # This was just a test that did not have the desired results
        # if full_y_range < 10:
        #     range_padding_percent = 35
        # if full_y_range < 5:
        #     range_padding_percent = 75
        # if full_y_range < 2:
        #    range_padding_percent = 100

        use_range_padding = ((max_y - min_y) / 100) * range_padding
        if min_y > 0 and (min_y - use_range_padding) > 0:
            min_y_padded = min_y - use_range_padding
        else:
            min_y_padded = min_y
        max_y_padded = max_y + use_range_padding
        if min_y_padded == max_y_padded:
            min_y_padded = min_y_padded - ((min_y_padded / 100) * range_padding)
            max_y_padded = max_y_padded + ((max_y_padded / 100) * range_padding)

        # anomalous_ts = np.array(dataset)
        anomalous_ts = dataset

        mass2_batch_times = []
        exact_match_times = []

        nan = np.array([np.nan])
        nanj = complex(0.0, float('nan'))
        empty_dists = np.array(nan + nanj)

        # plotted = False
        count = 0

        # fp_ids = [fp_id for index, fp_id in enumerate(fp_ids) if index == 0]

        # motifs_found = []
        # exact_matches_found = []
        # fps_timeseries = {}

        for fp_id in fp_ids:
            if (time.time() - start) >= 20:
                break
            # Attempt to surface the fp timeseries from memcache and/or db
            # @modified 20210424 - Feature #4014: Ionosphere - inference
            #                      Task #4030: refactoring
            fp_timeseries = None
            try:
                fp_timeseries = get_fp_timeseries(skyline_app, metric_id, fp_id)
            except Exception as e:
                logger.error('inference :: did not get fp timeseries with get_fp_timeseries(%s, %s, %s) - %s' % (
                    skyline_app, str(metric_id), str(fp_id), e))
            if not fp_timeseries:
                continue

            relate_dataset = [float(item[1]) for item in fp_timeseries]

            fps_timeseries[fp_id] = fp_timeseries

            current_best_indices = []
            current_best_dists = []
            best_indices = None
            best_dists = None

            try:
                logger.info('%s :: running mts.mass2_batch fp_id: %s, full_duration: %s, batch_size: %s, top_matches: %s, max_distance: %s, motif_size: %s' % (
                    function_str, str(fp_id), str(full_duration),
                    str(batch_size), str(top_matches), str(max_distance),
                    str(len(anomalous_ts))))

                # @added 20210418 - Feature #4014: Ionosphere - inference
                # Handle top_matches being greater than possible kth that can be found
                # mts.mass2_batch error: kth(=50) out of bounds (16)
                use_top_matches = int(top_matches)
                if (len(fp_timeseries) / int(batch_size)) <= int(top_matches):
                    use_top_matches = round(len(fp_timeseries) / int(batch_size)) - 1
                    if use_top_matches == 2:
                        use_top_matches = 1
                    logger.info('%s :: adjusting top_matches to %s (the maximum possible top - 1) as kth(=%s) will be out of bounds mts.mass2_batch' % (
                        function_str, str(use_top_matches), str(top_matches)))

                start_mass2_batch = timer()
                # @modified 20210418 - Feature #4014: Ionosphere - inference
                # Handle top_matches being greater than possible kth that can be found
                # best_indices, best_dists = mts.mass2_batch(relate_dataset, anomalous_ts, batch_size=int(batch_size), top_matches=int(top_matches))
                best_indices, best_dists = mts.mass2_batch(relate_dataset, anomalous_ts, batch_size=int(batch_size), top_matches=int(use_top_matches))
                end_mass2_batch = timer()
                mass2_batch_times.append((end_mass2_batch - start_mass2_batch))
                current_best_indices = best_indices.tolist()
                current_best_dists = best_dists.tolist()

                # @added 20210412 - Feature #4014: Ionosphere - inference
                #                   Branch #3590: inference
                # Add fp_id to fps_checked_for_motifs to enable ionosphere to update the
                # motif related columns in the ionosphere database table
                fps_checked_for_motifs.append(fp_id)
            except Exception as e:
                logger.error('error :: %s :: %s mts.mass2_batch error: %s' % (
                    function_str, (fp_id), str(e)))
                continue

            try:
                if str(list(best_dists)) == str(list(empty_dists)):
                    logger.info('%s :: mts.mass2_batch no similar motif from fp id %s - best_dists: %s' % (
                        function_str, (fp_id), str(list(best_dists))))
                    continue
            except Exception as e:
                dev_null = e

            if not current_best_indices[0]:
                continue
            # if list(best_indices)[0] != anomalous_index:
            #     continue
            # If the best_dists is > 1 they are not very similar
            # if list(best_dists)[0].real > 1.0:
            #     continue
            # if list(best_indices)[0] and best_dists:
            for index, best_dist in enumerate(current_best_dists):
                try:
                    motif_added = False
                    """
                    Note: mass_ts finds similar motifs NOT the same motif, the same motif
                    will result in the best_dists being a nan+nanj
                    So it is DIYed
                    """
                    try:
                        # @modified 20210414 - Feature #4014: Ionosphere - inference
                        #                      Branch #3590: inference
                        # Store the not anomalous motifs
                        # motif = [fp_id, current_best_indices[index], best_dist.real]
                        motif = [fp_id, current_best_indices[index], best_dist.real, anomalous_timeseries_subsequence, full_duration]
                    except Exception as e:
                        dev_null = e
                        motif = []

                    # if list(best_indices)[0] and best_dists:
                    # If it is greater than 1.0 it is not similar
                    # if best_dist.real > 1.0:
                    # if best_dist.real > IONOSPHERE_INFERENCE_MASS_TS_MAX_DISTANCE:
                    if best_dist.real > float(max_distance):
                        continue
                    else:
                        if motif:
                            count += 1
                            motifs_found.append(motif)
                            motif_added = True
                    if not motif_added:
                        if best_dist == nanj:
                            count += 1
                            motifs_found.append(motif)
                            motif_added = True
                    if not motif_added:
                        if str(best_dist) == 'nan+nanj':
                            count += 1
                            motifs_found.append([fp_id, current_best_indices[index], 0.0, anomalous_timeseries_subsequence, full_duration])
                            motif_added = True
                    if not motif_added:
                        if best_dist == empty_dists:
                            count += 1
                            motifs_found.append(motif)
                            motif_added = True
                except Exception as e:
                    logger.error(traceback.format_exc())
                    logger.error('error :: %s :: could not determine is if fp id %s timeseries at index %s was a match - %s' % (
                        function_str, str(fp_id), str(current_best_indices[index]), e))
                    continue

            # FIND EXACT MATCHES
            # Seeing as I cannot reproduce finding nan+nanj which represents an
            # exact match with mts.mass2_batch, do it DIY style - iterate the
            # timeseries and create a batch_size subsequence for every index and
            # compare the values to the anomalous_ts for an exact match.
            # This takes ~0.024850 seconds on a timeseries with 10079 datapoints
            # @modified 20210418 - Feature #4014: Ionosphere - inference
            # However fiding exact matches can add ~2.5 seconds on 90 minute
            # batch_size and with a proproptionally scaled max_distance of say 15
            # finding an exact match in a longer sequence is less important,
            # the greater the batch_size the most likely greater the variability
            # and the chance of an exact match decreases.  So save 2.5 seconds.
            # UNLESS
            # At a 5 (to 10) batch_size and max_distance of 1.0 an exact match
            # can be found. Exact matches are quite frequent and sometimes with
            # such little variability, similar matchs may not be found.
            # Therefore find find_exact_matches has its place.  MASS
            # A CAVEAT here is that boring metrics and that change and have a
            # low variability even at a larger batch_size could also benefit and
            # possibly achieve better accruracy from the use of find_exact_matches
            # as they can be shapelets resemble a batch_size 5 shapelet.
            # It would perhaps be possible to use one or more of the features
            # profile tsfresh values to identify these types of shapelets, if
            # you knew which feature/s were most descriptive of this type of
            # shapelet, e.g. 'value__skewness': 3.079477685394873, etc (maybe)
            # However I predict that this method will perform worst on these
            # types of shapelets.
            # find_exact_matches = False
            # exact matches can be found in batch sizes of 500 and similar not
            # So actually always run it.
            find_exact_matches = True
            find_exact_matches_run = True

            if int(batch_size) < 10:
                find_exact_matches = True
                find_exact_matches_run = True

            if find_exact_matches:
                try:
                    start_exact_match = timer()
                    indexed_relate_dataset = []
                    for index, item in enumerate(relate_dataset):
                        indexed_relate_dataset.append([index, item])
                    last_index = indexed_relate_dataset[-1][0]
                    current_index = 0
                    while current_index < last_index:
                        subsequence = [value for index, value in indexed_relate_dataset[current_index:(current_index + int(batch_size))]]
                        if subsequence == anomalous_ts:
                            exact_matches_found.append([fp_id, current_index, 0.0, anomalous_timeseries_subsequence, full_duration])
                            motifs_found.append([fp_id, current_index, 0.0, anomalous_timeseries_subsequence, full_duration])
                        current_index += 1
                    end_exact_match = timer()
                    exact_match_times.append((end_exact_match - start_exact_match))
                except Exception as e:
                    logger.error(traceback.format_exc())
                    logger.error('error :: %s :: could not determine it any exact matches could be found in fp id %s timeseries - %s' % (
                        function_str, str(fp_id), e))

        logger.info('%s :: mts.mass2_batch runs on %s fps of full_duration %s in %.6f seconds' % (
            function_str, str(len(mass2_batch_times)), str(full_duration), sum(mass2_batch_times)))
        if find_exact_matches_run:
            logger.info('%s :: exact_match runs on %s fps of full_duration %s in %.6f seconds' % (
                function_str, str(len(exact_match_times)), str(full_duration), sum(exact_match_times)))
        end_full_duration = timer()
        logger.info('%s :: analysed %s fps of full_duration %s in %.6f seconds' % (
            function_str, str(len(fp_ids)), str(full_duration), (end_full_duration - start_full_duration)))

        # Patterns are sorted
        sorted_motifs = []
        motifs_found_in_fps = []
        if motifs_found:
            sorted_motifs = sorted(motifs_found, key=lambda x: x[2])
            for item in sorted_motifs:
                motifs_found_in_fps.append(item[0])
        logger.info('%s :: %s motifs found' % (
            function_str, str(len(sorted_motifs))))

        for motif in sorted_motifs:
            if (time.time() - start) >= 25:
                break
            try:
                add_match = False
                all_in_range = False

                fp_id = motif[0]
                best_index = motif[1]
                best_dist = motif[2]

                # @added 20210414 - Feature #4014: Ionosphere - inference
                #                   Branch #3590: inference
                # Store the not anomalous motifs
                motif_sequence = motif[3]

                motif_full_duration = motif[4]

                match_type = 'not_similar_enough'

                if motif in exact_matches_found:
                    add_match = True
                    match_type = 'exact'
                    all_in_range = True
                    exact_motifs += 1
                full_relate_timeseries = fps_timeseries[fp_id]
                # full_relate_dataset = [float(item[1]) for item in full_relate_timeseries]
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
                        logger.info('%s :: ALL IN RANGE - all_in_range: %s, motif: %s' % (
                            function_str, str(all_in_range), str(relate_dataset[0:2])))
                        add_match = True
                        match_type = 'all_in_range'
                        similar_motifs += 1

                    # @added 20210425 - Feature #4014: Ionosphere - inference
                    # Compute the area using the composite trapezoidal rule.
                    motif_area = None
                    fp_motif_area = None
                    percent_different = None
                    try:
                        batch_size_dataset = [float(item[1]) for item in motif_sequence]
                        y_motif = np.array(batch_size_dataset)
                        motif_area = np.trapz(y_motif, dx=1)
                    except Exception as e:
                        logger.error('error :: %s :: failed to get motif_area with np.trapz - %s' % (
                            function_str, e))
                    try:
                        y_fp_motif = np.array(relate_dataset)
                        fp_motif_area = np.trapz(y_fp_motif, dx=1)
                    except Exception as e:
                        logger.error('error :: %s :: failed to get fp_motif_area with np.trapz - %s' % (
                            function_str, e))
                    # Determine the percentage difference (as a
                    # positive value) of the areas under the
                    # curves.
                    if motif_area and fp_motif_area:
                        percent_different = get_percent_different(fp_motif_area, motif_area, True)
                        if percent_different > max_area_percent_diff:
                            if add_match:
                                logger.info('%s :: AREA TOO DIFFERENT - not adding all_in_range match' % (
                                    function_str))
                                add_match = False
                            # BUT ...
                            if best_dist < 3 and not add_match:
                                logger.info('%s :: DISTANCE VERY SIMILAR - adding match even though area_percent_diff is greater than max_area_percent_diff because best_dist: %s' % (
                                    function_str, str(best_dist)))
                                add_match = True
                                match_type = 'distance'
                                distance_motifs += 1

                if similarity == 'all':
                    if not add_match:
                        not_similar_motifs += 1
                        if not_similar_enough_sample >= 10:
                            continue
                        not_similar_enough_sample += 1
                        add_match = True
                        match_type = 'not_similar_enough'

                if add_match:
                    generation = 0
                    fp_id_row = None
                    try:
                        fp_id_row = get_ionosphere_fp_db_row(skyline_app, int(fp_id))
                    except Exception as e:
                        logger.error('error :: %s :: failed to get_ionosphere_fp_db_row for fp_id %s - %s' % (
                            function_str, str(fp_id), e))
                    if fp_id_row:
                        try:
                            generation = fp_id_row['generation']
                        except Exception as e:
                            logger.error('error :: %s :: failed to generation from fp_id_row for fp_id %s - %s' % (
                                function_str, str(fp_id), e))
                    if generation == 0:
                        generation_str = 'trained'
                    else:
                        generation_str = 'LEARNT'
                    motif_match_types = motif_match_types_dict()
                    type_id = motif_match_types[match_type]

                    motif_id = '%s-%s' % (str(fp_id), str(best_index))
                    motif_analysis[metric]['motifs'][motif_id] = {}
                    motif_analysis[metric]['motifs'][motif_id]['metric_id'] = metric_id
                    motif_analysis[metric]['motifs'][motif_id]['fp_id'] = fp_id
                    motif_analysis[metric]['motifs'][motif_id]['generation'] = generation
                    motif_analysis[metric]['motifs'][motif_id]['index'] = best_index
                    motif_analysis[metric]['motifs'][motif_id]['distance'] = best_dist
                    motif_analysis[metric]['motifs'][motif_id]['size'] = int(batch_size)
                    motif_analysis[metric]['motifs'][motif_id]['max_distance'] = float(max_distance)
                    motif_analysis[metric]['motifs'][motif_id]['timestamp'] = timestamp
                    motif_analysis[metric]['motifs'][motif_id]['type_id'] = type_id
                    motif_analysis[metric]['motifs'][motif_id]['type'] = match_type
                    motif_analysis[metric]['motifs'][motif_id]['full_duration'] = motif_full_duration
                    # @added 20210414 - Feature #4014: Ionosphere - inference
                    #                   Branch #3590: inference
                    # Store the not anomalous motifs
                    motif_analysis[metric]['motifs'][motif_id]['motif_timeseries'] = anomalous_timeseries_subsequence
                    motif_analysis[metric]['motifs'][motif_id]['motif_sequence'] = motif_sequence
                    not_anomalous_timestamp = int(anomalous_timeseries_subsequence[-1][0])
                    graph_period_seconds = not_anomalous_timestamp - int(anomalous_timeseries_subsequence[0][0])
                    motif_analysis[metric]['motifs'][motif_id]['motif_period_seconds'] = graph_period_seconds
                    motif_analysis[metric]['motifs'][motif_id]['motif_period_minutes'] = round(graph_period_seconds / 60)

                    motif_analysis[metric]['motifs'][motif_id]['image'] = None

                    motif_analysis[metric]['motifs'][motif_id]['motif_area'] = motif_area
                    motif_analysis[metric]['motifs'][motif_id]['fp_motif_area'] = fp_motif_area
                    motif_analysis[metric]['motifs'][motif_id]['area_percent_diff'] = percent_different
                    motif_analysis[metric]['motifs'][motif_id]['max_area_percent_diff'] = max_area_percent_diff

                    if (time.time() - start) >= 25:
                        continue

                    graph_image_file = '%s/motif.%s.%s.%s.with_max_distance.%s.png' % (
                        motif_images_dir, motif_id, match_type, str(batch_size),
                        str(max_distance))
                    plotted_image = False
                    on_demand_motif_analysis = True
                    if not path.isfile(graph_image_file):
                        plotted_image, plotted_image_file = plot_motif_match(
                            skyline_app, metric, timestamp, fp_id, full_duration,
                            generation_str, motif_id, best_index, int(batch_size),
                            best_dist, type_id, relate_dataset,
                            anomalous_timeseries_subsequence, graph_image_file,
                            on_demand_motif_analysis)
                    else:
                        plotted_image = True
                        logger.info('%s :: plot already exists - %s' % (function_str, str(graph_image_file)))
                    if plotted_image:
                        motif_analysis[metric]['motifs'][motif_id]['image'] = graph_image_file
                    else:
                        logger.error('failed to plot motif match plot')
                        graph_image_file = None
            except Exception as e:
                logger.error(traceback.format_exc())
                logger.error('error :: inference :: with fp id %s proceesing motif at index: %s - %s' % (
                    str(fp_id), str(motif[0]), str(e)))
                continue
    end_timer = timer()
    motif_analysis[metric]['fps_checked'] = fps_checked_for_motifs
    motif_analysis[metric]['exact_motifs'] = exact_motifs
    motif_analysis[metric]['similar_motifs'] = similar_motifs
    motif_analysis[metric]['distance_motifs'] = distance_motifs
    motif_analysis[metric]['not_similar_motifs'] = not_similar_motifs
    motif_analysis[metric]['not_similar_enough_sample'] = not_similar_enough_sample

    motif_analysis_file = '%s/motif.analysis.similarity_%s.batch_size_%s.top_matches_%s.max_distance_%s.dict' % (
        motif_images_dir, similarity, str(batch_size), str(top_matches),
        str(max_distance))
    try:
        write_data_to_file(skyline_app, motif_analysis_file, 'w', str(motif_analysis))
    except Exception as e:
        trace = traceback.format_exc()
        logger.error('%s' % trace)
        fail_msg = '%s :: error :: failed to write motif_analysis_file - %s' % (function_str, motif_analysis_file)
        logger.error('%s' % fail_msg)
        dev_null = e

    motif_ids = list(motif_analysis[metric]['motifs'].keys())
    logger.info('%s :: %s motif matches found, %s fps where checked and motifs plotted in %.6f seconds for %s' % (
        function_str, str(len(motif_ids)), str(len(fps_checked_for_motifs)), (end_timer - start_timer), metric))
    if dev_null:
        del dev_null
    return motif_analysis, fail_msg, trace
