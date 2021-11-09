from __future__ import division
import logging
import os
import sys
from timeit import default_timer as timer
import traceback
import operator
import warnings

import numpy as np
import mass_ts as mts

# import matplotlib.image as mpimg
# %matplotlib inline
if True:
    sys.path.insert(0, '/opt/skyline/github/skyline/skyline')
    import settings
    from functions.timeseries.determine_data_frequency import determine_data_frequency
    from motif_match_types import motif_match_types_dict

warnings.filterwarnings('ignore')

skyline_app = 'luminosity'
skyline_app_logger = '%sLog' % skyline_app
logger = logging.getLogger(skyline_app_logger)
skyline_app_logfile = '%s/%s.log' % (settings.LOG_PATH, skyline_app)

python_version = int(sys.version_info[0])

this_host = str(os.uname()[1])

try:
    SINGLE_MATCH = settings.IONOSPHERE_INFERENCE_MOTIFS_SINGLE_MATCH
except Exception as e:
    logger.warning('warn :: functions.luminosity.find_cloudburst_motifs :: cannot determine IONOSPHERE_INFERENCE_MOTIFS_SINGLE_MATCH from settings - %s' % e)
    SINGLE_MATCH = True

try:
    IONOSPHERE_INFERENCE_MOTIFS_SETTINGS = settings.IONOSPHERE_INFERENCE_MOTIFS_SETTINGS.copy()
except Exception as e:
    logger.warning('warn :: functions.luminosity.find_cloudburst_motifs :: cannot determine IONOSPHERE_INFERENCE_MOTIFS_SETTINGS from settings - %s' % e)
    IONOSPHERE_INFERENCE_MOTIFS_SETTINGS = {}

try:
    IONOSPHERE_INFERENCE_MOTIFS_TOP_MATCHES = settings.IONOSPHERE_INFERENCE_MOTIFS_TOP_MATCHES
except Exception as e:
    logger.warning('warn :: functions.luminosity.find_cloudburst_motifs :: cannot determine IONOSPHERE_INFERENCE_MOTIFS_TOP_MATCHES from settings - %s' % e)
    IONOSPHERE_INFERENCE_MOTIFS_TOP_MATCHES = 20.0

try:
    IONOSPHERE_INFERENCE_MASS_TS_MAX_DISTANCE = settings.IONOSPHERE_INFERENCE_MASS_TS_MAX_DISTANCE
except Exception as e:
    logger.warning('warn :: functions.luminosity.find_cloudburst_motifs :: cannot determine IONOSPHERE_INFERENCE_MASS_TS_MAX_DISTANCE from settings - %s' % e)
    IONOSPHERE_INFERENCE_MASS_TS_MAX_DISTANCE = 20.0

context = 'inference_inference'


def find_cloudburst_motifs(metric, snippet, timeseries, print_output=False):
    """
    Takes a snippet of timeseries, creates motifs at batch_sizes from the
    snippet and searches for those motifs in the given timeseries, returns a
    matched_motifs dict and a timeseries_matched dict

    """
    logger = logging.getLogger(skyline_app_logger)
    child_process_pid = os.getpid()
    logger.info('functions.luminosity.find_cloudburst_motifs :: running for process_pid - %s for %s' % (
        str(child_process_pid), metric))

    start = timer()
    debug_logging = False
    timeseries_matched = {}
    timeseries_matched[metric] = {}
    matched_motifs = {}
    motifs_found = []
    dev_null = None

    for item in timeseries:
        timestamp = int(item[0])
        timeseries_matched[metric][timestamp] = {}
        timeseries_matched[metric][timestamp]['motif_matches'] = {}

    mass2_batch_times = []
    mass3_times = []
    exact_match_times = []

    nan = np.array([np.nan])
    nanj = complex(0.0, float('nan'))
    empty_dists = np.array(nan + nanj)

    motifs_found = []
    exact_matches_found = []

    motif_match_types = motif_match_types_dict()

    start_full_duration = timer()

    # metric_resolution = determine_data_frequency(skyline_app, timeseries, False)
    logger.info('functions.luminosity.find_cloudburst_motifs :: looking for similar motifs in timeseries of length: %s' % str(len(timeseries)))

    exact_match_times = []

#    relate_dataset = [float(item[1]) for item in fp_timeseries]
    relate_dataset = [float(item[1]) for item in timeseries]

    # namespace_key = 'default_inference_batch_sizes'
    # for batch_size in list(IONOSPHERE_INFERENCE_MOTIFS_SETTINGS[namespace_key].keys()):
    # for batch_size in [len(snippet)]:
    batch_size = len(snippet)
    if print_output:
        print('functions.luminosity.find_cloudburst_motifs :: checking %s at batch_size: %s' % (
            metric, str(batch_size)))

    # @added 20210423 - Feature #4014: Ionosphere - inference
    # The convenience mass2_batch method will not work to find
    # top matches if the number of top_matches to be found are
    # greater than the number of indices in which a match can be
    # found.  In these cases such as trying to find the:
    # batch_size: 1440, top_matches: 50, max_distance: 30, snippet_length: 1451
    # even setting the top_matches to 1 will result in
    # mass2_batch throwing the error:
    # mts.mass2_batch error: kth(=1) out of bounds (1)
    # So use mass3 as appropriate.
    use_mass3 = False
    use_mass2_batch = True
    n = len(snippet)
    indices = list(range(0, n - batch_size + 1, batch_size))
    # mass2_batch default is 3 so if there are less than 3
    # indices in which the best macthes can be found, use mass3
    if len(indices) < 3:
        use_mass3 = True
        use_mass2_batch = False
        logger.info('functions.luminosity.find_cloudburst_motifs :: batch_size: %s, snippet length: %s, len(indices) < 3, using mass3' % (
            str(batch_size), str(n)))
        if print_output:
            print('functions.luminosity.find_cloudburst_motifs :: batch_size: %s, snippet length: %s, len(indices) < 3, using mass3' % (
                str(batch_size), str(n)))

    top_matches = 1
    max_distance = 1.8
    find_exact_matches = True

    # if use_mass2_batch:
    logger.info('functions.luminosity.find_cloudburst_motifs :: analysis run - metric: %s, batch_size: %s, top_matches: %s, max_distance: %s, snippet_length: %s' % (
        str(metric), str(batch_size), str(top_matches),
        str(max_distance), str(len(snippet))))
    if print_output:
        print('functions.luminosity.find_cloudburst_motifs :: analysis run - metric: %s, batch_size: %s, top_matches: %s, max_distance: %s, snippet_length: %s' % (
            str(metric), str(batch_size), str(top_matches),
            str(max_distance), str(len(snippet))))

    # Given that the snippet can be any length
    if len(snippet) < batch_size:
        if print_output:
            print('functions.luminosity.find_cloudburst_motifs :: skipping snippet: %s, batch_size: %s' % (
                str(len(snippet)), str(batch_size)))
        return matched_motifs, timeseries_matched
    else:
        if print_output:
            print('functions.luminosity.find_cloudburst_motifs :: checking %s, batch_size: %s' % (
                metric, str(batch_size)))

    # Create the subsequence that is being searched for
    n = batch_size
    # snippets = [snippet[i * n:(i + 1) * n] for i in range((len(snippet) + n - 1) // n)]
#    snippets = [snippet]

#        batch_size_anomalous_timeseries_subsequence = timeseries[-batch_size:]
#        batch_size_dataset = [float(item[1]) for item in batch_size_anomalous_timeseries_subsequence]
#    for i_snippet in snippets:

    # batch_size_timeseries_subsequence = i_snippet[-batch_size:]
    batch_size_timeseries_subsequence = snippet
    batch_size_dataset = [float(item[1]) for item in batch_size_timeseries_subsequence]
    motif_timestamp = int(batch_size_timeseries_subsequence[-1][0])

    # Set defaults
    current_best_indices = []
    current_best_dists = []
    best_indices = None
    best_dists = None

    # POC running all through mass3 with maximum pieces (SUPER FAST)
    # and then filtering on max_distance, all_in_range and area
    # percent_different
    # use_mass3 = True
    # use_mass2_batch = False

    # POC running all through mass3 and then filtering FALIED in
    # terms of time taken... due to having to run 22421 motifs
    # through all_in_range and percent_different functions ...
    # just these motifs checked took 62.036366 seconds, the surfacing
    # and transforming of the data AND mass3 to only 2 seconds
    # 2021-04-27 13:45:59 :: 3586421 :: functions.luminosity.find_cloudburst_motifs :: analysed 2 fps of full_duration 86400 in 0.330732 seconds
    # 2021-04-27 13:45:59 :: 3586421 :: functions.luminosity.find_cloudburst_motifs :: 22421 distance_valid_motifs determined in 0.346807 seconds from 81432 motifs_found
    # 2021-04-27 13:45:59 :: 3586421 :: functions.luminosity.find_cloudburst_motifs :: sorted_motifs from distance_valid_motifs in 0.048316 seconds
    # 2021-04-27 13:46:01 :: 3586421 :: functions.luminosity.find_cloudburst_motifs :: percent_different in 0.000590 seconds
    # 2021-04-27 13:46:01 :: 3586421 :: functions.luminosity.find_cloudburst_motifs :: percent_different in 0.000271 seconds
    # ...
    # ...
    # 2021-04-27 13:46:57 :: 3586421 :: functions.luminosity.find_cloudburst_motifs :: percent_different in 0.000373 seconds
    # 2021-04-27 13:46:57 :: 3586421 :: functions.luminosity.find_cloudburst_motifs :: percent_different in 0.000381 seconds
    # 2021-04-27 13:46:58 :: 3586421 :: functions.luminosity.find_cloudburst_motifs :: percent_different in 0.000363 seconds
    # 2021-04-27 13:46:58 :: 3586421 :: functions.luminosity.find_cloudburst_motifs :: percent_different in 0.000348 seconds
    # 2021-04-27 13:47:01 :: 3586421 :: functions.luminosity.find_cloudburst_motifs :: motifs checked in 62.036366 seconds
    # 2021-04-27 13:47:01 :: 3586421 :: functions.luminosity.find_cloudburst_motifs :: 0 motif best match found from 81432 motifs_found, 4 fps where checked {604800: {'fp_count': 2}, 86400: {'fp_count': 2}} (motifs remove due to not in range 22325, percent_different 96) and it took a total of 64.761969 seconds (only mass3) to process telegraf.ssdnodes-26840.mariadb.localhost:3306.mysql.bytes_sent
    # 2021-04-27 13:47:01 :: 3586421 :: inference found 0 matching similar motifs, checked 0 fps in 64.790198 seconds

    if use_mass2_batch:
        try:
            # @added 20210419 - Feature #4014: Ionosphere - inference
            # Handle top_matches being greater than possible kth that can be found
            # mts.mass2_batch error: kth(=50) out of bounds (16)
            use_top_matches = int(top_matches)
            if (len(snippet) / int(batch_size)) <= int(top_matches):
                use_top_matches = round(len(snippet) / int(batch_size)) - 2
                if use_top_matches == 2:
                    use_top_matches = 1
                if use_top_matches < 1:
                    use_top_matches = 1
                logger.info('functions.luminosity.find_cloudburst_motifs :: adjusting top_matches for mass2_batch to %s (the maximum possible top - 1) as top_matches=%s will be out of bounds mts.mass2_batch' % (
                    str(use_top_matches), str(top_matches)))

            start_mass2_batch = timer()
            best_indices, best_dists = mts.mass2_batch(relate_dataset, batch_size_dataset, batch_size=batch_size, top_matches=use_top_matches)
            end_mass2_batch = timer()
            mass2_batch_times.append((end_mass2_batch - start_mass2_batch))
            current_best_indices = best_indices.tolist()
            current_best_dists = best_dists.tolist()

            logger.info('functions.luminosity.find_cloudburst_motifs :: mass2_batch run on batch_size: %s, top_matches: %s, in %6f seconds' % (
                str(batch_size), str(use_top_matches),
                (end_mass2_batch - start_mass2_batch)))
            if print_output:
                print('functions.luminosity.find_cloudburst_motifs :: mass2_batch run on batch_size: %s, top_matches: %s, in %6f seconds' % (
                    str(batch_size), str(use_top_matches),
                    (end_mass2_batch - start_mass2_batch)))

            if debug_logging:
                logger.debug('debug :: functions.luminosity.find_cloudburst_motifs :: best_indices: %s, best_dists: %s' % (
                    str(current_best_indices), str(current_best_dists)))
        except ValueError as e:
            # If mass2_batch reports out of bounds, use mass3
            if 'out of bounds' in str(e):
                use_mass3 = True
                best_dists = ['use_mass3']
                logger.info('functions.luminosity.find_cloudburst_motifs :: mts.mass2_batch will be out of bounds running mass3')
        except Exception as e:
            logger.error('error :: functions.luminosity.find_cloudburst_motifs :: %s mts.mass2_batch error: %s' % (
                str(metric), str(e)))
            if print_output:
                print('error :: functions.luminosity.find_cloudburst_motifs :: %s mts.mass2_batch error: %s' % (
                    str(metric), str(e)))
            return matched_motifs, timeseries_matched
        if not use_mass3:
            try:
                if str(list(best_dists)) == str(list(empty_dists)):
                    logger.info('functions.luminosity.find_cloudburst_motifs :: mts.mass2_batch no similar motif from %s - best_dists: %s' % (
                        str(metric), str(list(best_dists))))
                    if print_output:
                        print('functions.luminosity.find_cloudburst_motifs :: mts.mass2_batch no similar motif from %s - best_dists: %s' % (
                            str(metric), str(list(best_dists))))
                    return matched_motifs, timeseries_matched
            except Exception as e:
                dev_null = e

    # @added 20210423 -
    if use_mass3:
        # pieces should be larger than the query length and as many
        # as possible, a power of two would be best, but as many
        # pieces as possible is the best we can achieve above 265
        query_length = len(batch_size_dataset)
        # if query_length < 256:
        #     pieces = 256
        # else:
        #     pieces = query_length + 2
        pieces = len(snippet) - query_length
        if pieces < query_length:
            pieces = query_length + 2

        check_pieces_length = False
        if check_pieces_length:
            # @modified 20210504 - Feature #4014: Ionosphere - inference
            # Handle the fp_timeseries being the same length (meaning
            # too short) as the query length
            if len(snippet) <= pieces:
                logger.info('functions.luminosity.find_cloudburst_motifs :: skipping running mass3 with %s pieces on metric: %s, batch_size: %s because snippet length is not long enough for the query size' % (
                    str(pieces), str(metric), str(batch_size)))
                if print_output:
                    print('functions.luminosity.find_cloudburst_motifs :: skipping running mass3 with %s pieces on metric: %s, batch_size: %s because snippet length is not long enough for the query size' % (
                        str(pieces), str(metric), str(batch_size)))
                return matched_motifs, timeseries_matched

            # @modified 20210505 - Feature #4014: Ionosphere - inference
            # Skip the batch size if the fp_timeseries is a similar
            # length as the batch_size.  This was specifically added to
            # reduce errors were there may be missing data points in a
            # timeseries and the lengths are not the same.  This was
            # encountered on a batch_size of 1440 with FULL_DURATION
            # 86400 60 second data.   A match was never found at a
            # batch_size > 720 on that data, but errors were occassionally
            # encountered.
            ten_percent_of_batch_size = int(batch_size / 10)
            if (len(snippet) - ten_percent_of_batch_size) < batch_size:
                logger.info('functions.luminosity.find_cloudburst_motifs :: skipping running mass3 on metric: %s, batch_size: %s because the batch_size is too close to length' % (
                    str(metric), str(batch_size)))
                if print_output:
                    print('functions.luminosity.find_cloudburst_motifs :: skipping running mass3 on metric: %s, batch_size: %s because the batch_size is too close to length' % (
                        str(metric), str(batch_size)))
                return matched_motifs, timeseries_matched

        logger.info('functions.luminosity.find_cloudburst_motifs :: running mass3 with %s pieces on on metric: %s, batch_size: %s' % (
            str(pieces), str(metric), str(batch_size)))

        if print_output:
            print('functions.luminosity.find_cloudburst_motifs :: running mass3 with %s pieces on on metric: %s, batch_size: %s' % (
                str(pieces), str(metric), str(batch_size)))

        start_mass3 = timer()
        try:
            best_dists = mts.mass3(relate_dataset, batch_size_dataset, pieces)
            end_mass3 = timer()
        except Exception as e:
            logger.error('error :: functions.luminosity.find_cloudburst_motifs :: metric %s mts.mass3 error: %s' % (
                str(metric), str(e)))
            if print_output:
                print('error :: functions.luminosity.find_cloudburst_motifs :: metric %s mts.mass3 error: %s' % (
                    str(metric), str(e)))
            return matched_motifs, timeseries_matched
        mass3_times.append((end_mass3 - start_mass3))

        current_best_dists = best_dists.tolist()

        # Create current_best_indices as mass2_batch returns
        current_best_indices = []
        if len(relate_dataset) > batch_size:
            for index in enumerate(relate_dataset):
                # if index[0] >= (batch_size - 1):
                # The array starts at batch_size + 1
                # if index[0] >= (batch_size + 1):
                # but that fails on the add_motifs comprehension
                # add_motifs = [[fp_id, current_best_indices[index], best_dist.real, batch_size_timeseries_subsequence, batch_size, max_distance, max_area_percent_diff, max_y, min_y, range_padding, min_y_padded, max_y_padded] for index, best_dist in enumerate(current_best_dists)]
                # IndexError: list index out of range
                if index[0] >= (batch_size - 1):
                    current_best_indices.append(index[0])

            # @modified 20210505 - Feature #4014: Ionosphere - inference
            # Handle the query_length being shorter than the batch_size
            if len(current_best_indices) != len(current_best_dists):
                current_best_indices = []
                if index[0] >= (query_length - 1):
                    current_best_indices.append(index[0])
            if len(current_best_indices) != len(current_best_dists):
                logger.info('functions.luminosity.find_cloudburst_motifs :: discarding mass3 results as current_best_dists length: %s, current_best_indices length: %s do not match, took %6f seconds' % (
                    str(len(current_best_dists)), str(len(current_best_indices)),
                    (end_mass3 - start_mass3)))
                if print_output:
                    print('functions.luminosity.find_cloudburst_motifs :: discarding mass3 results as current_best_dists length: %s, current_best_indices length: %s do not match, took %6f seconds' % (
                        str(len(current_best_dists)), str(len(current_best_indices)),
                        (end_mass3 - start_mass3)))
                return matched_motifs, timeseries_matched
        logger.info('functions.luminosity.find_cloudburst_motifs :: mass3 run, current_best_dists length: %s, current_best_indices length: %s, took %6f seconds' % (
            str(len(current_best_dists)), str(len(current_best_indices)),
            (end_mass3 - start_mass3)))
        if print_output:
            print('functions.luminosity.find_cloudburst_motifs :: mass3 run, current_best_dists length: %s, current_best_indices length: %s, took %6f seconds' % (
                str(len(current_best_dists)), str(len(current_best_indices)),
                (end_mass3 - start_mass3)))

    if not use_mass3:
        if not current_best_indices[0]:
            return matched_motifs, timeseries_matched
    if use_mass3 and not current_best_indices:
        return matched_motifs, timeseries_matched

    # All in one quicker?  Yes
    start_add_motifs = timer()
    add_motifs = []
    try:
        add_motifs = [[metric, current_best_indices[index], best_dist.real, batch_size_timeseries_subsequence, batch_size, max_distance, motif_timestamp] for index, best_dist in enumerate(current_best_dists)]
        if add_motifs:
            motifs_found = motifs_found + add_motifs
    except Exception as e:
        logger.error(traceback.format_exc())
        logger.error('error :: functions.luminosity.find_cloudburst_motifs :: could not add_motifs to motifs_found - %s' % (
            e))
    end_add_motifs = timer()
    logger.info('functions.luminosity.find_cloudburst_motifs :: added %s motifs to motifs_found in %.6f seconds' % (
        str(len(add_motifs)),
        (end_add_motifs - start_add_motifs)))
    # Break if an exact match is found
    # @modified 20210430 - Bug #4044: inference - motif distance override - exact match
    # @modified 20210504 - Bug #4044: inference - motif distance override - exact match
    # if len([item for item in add_motifs if item[2] == 0]) > 0:
    #     exact_matches_found = exact_matches_found + [item for item in add_motifs if item[2] == 0]
    #     break

    # @modified 20210427 - Feature #4014: Ionosphere - inference
    # Finding exact matches can result is more than doubling the
    # runtime when used after mass2_batch runs (which do not find)
    # exact matches, mass3 does.  However the amount of time an
    # exact match is found, is very rare
    # if not use_mass3:
    if not use_mass3 and find_exact_matches:
        # mass3 finds exact matches, mass2_batch does not, so
        # there is no need to find exacts matchs if mass3 was
        # run.
        # FIND EXACT MATCHES
        # Seeing as I cannot reproduce finding nan+nanj which represents an
        # exact match with mts.mass2_batch, do it DIY style - iterate the
        # timeseries and create a batch_size subsequence for every index and
        # compare the values to the anomalous_ts for an exact match.
        # This takes ~0.024850 seconds on a timeseries with 10079 datapoints
        try:
            start_exact_match = timer()
            indexed_relate_dataset = []
            for index, item in enumerate(relate_dataset):
                indexed_relate_dataset.append([index, item])
            last_index = indexed_relate_dataset[-1][0]
            current_index = 0
            while current_index < last_index:
                subsequence = [value for index, value in indexed_relate_dataset[current_index:(current_index + batch_size)]]
                if subsequence == batch_size_dataset:
                    # @modified 20210419 - Feature #4014: Ionosphere - inference
                    # Added batch_size
                    exact_matches_found.append([metric, current_index, 0.0, batch_size_timeseries_subsequence, batch_size, max_distance, motif_timestamp])
                    motifs_found.append([metric, current_index, 0.0, batch_size_timeseries_subsequence, batch_size, max_distance, motif_timestamp])
                current_index += 1
            end_exact_match = timer()
            exact_match_times.append((end_exact_match - start_exact_match))
        except Exception as e:
            logger.error(traceback.format_exc())
            logger.error('error :: functions.luminosity.find_cloudburst_motifs :: could not determine it any exact matches could be found in %s timeseries - %s' % (
                str(metric), e))
        logger.info('functions.luminosity.find_cloudburst_motifs :: exact matches checked in %.6f seconds' % (
            (end_exact_match - start_exact_match)))

        # TODO
        # mass3 ALL, then evaluate, would it be quicker?  No see POC
            # above
    logger.info('functions.luminosity.find_cloudburst_motifs :: mts.mass2_batch runs on %s in %.6f seconds' % (
        str(metric), sum(mass2_batch_times)))
    logger.info('functions.luminosity.find_cloudburst_motifs :: exact_match runs on %s in %.6f seconds' % (
        str(metric), sum(exact_match_times)))
    end_full_duration = timer()
    logger.info('functions.luminosity.find_cloudburst_motifs :: analysed %s in %.6f seconds' % (
        str(metric), (end_full_duration - start_full_duration)))

    # Patterns are sorted by distance
    # The list produced with the mass3 method will include
    # nans
    start_distance_valid_motifs = timer()
    distance_valid_motifs = [item for item in motifs_found if not np.isnan(item[2]) and item[2] <= item[5]]
    end_distance_valid_motifs = timer()
    logger.info('functions.luminosity.find_cloudburst_motifs :: %s distance_valid_motifs determined in %.6f seconds from %s motifs_found' % (
        str(len(distance_valid_motifs)),
        (end_distance_valid_motifs - start_distance_valid_motifs),
        str(len(motifs_found))))
    if print_output:
        print('functions.luminosity.find_cloudburst_motifs :: %s distance_valid_motifs determined in %.6f seconds from %s motifs_found' % (
            str(len(distance_valid_motifs)),
            (end_distance_valid_motifs - start_distance_valid_motifs),
            str(len(motifs_found))))

    start_sorted_motifs = timer()
    sorted_motifs = []
    if motifs_found:
        sorted_motifs = sorted(distance_valid_motifs, key=lambda x: x[2])
        # If the areas under the curve were calculated, the
        # list could be sorted by area_percent_diff then by
        # distance.
        # import operator
        # sorted_motifs = sorted(motifs_found_in_fp, key=operator.itemgetter(2, 2))
    end_sorted_motifs = timer()
    logger.info('functions.luminosity.find_cloudburst_motifs :: sorted_motifs from distance_valid_motifs in %.6f seconds' % (
        (end_sorted_motifs - start_sorted_motifs)))

    start_motifs_check = timer()

    snippet_timestamps = [int(item[0]) for item in snippet]
    for motif in sorted_motifs:
        try:

            add_match = False

            metric = motif[0]
            best_index = motif[1]
            best_dist = motif[2]

            # motif_sequence = motif[3]

            # @modified 20210419 - Feature #4014: Ionosphere - inference
            # Added batch_size
            motif_size = motif[4]

            motif_timestamp = motif[6]

            add_match = True
            match_type = 'distance'

            if motif in exact_matches_found:
                match_type = 'exact'
                if debug_logging:
                    logger.debug('debug :: functions.luminosity.find_cloudburst_motifs :: exact match: %s' % (str(motif)))

            full_relate_timeseries = timeseries
            relate_timeseries = [item for index, item in enumerate(full_relate_timeseries) if index >= best_index and index < (best_index + motif_size)]
            relate_dataset = [item[1] for item in relate_timeseries]
            # relate_dataset_timestamps = [int(item[0]) for item in relate_timeseries]

            matched_period_timestamps = [int(item[0]) for item in relate_timeseries]
            in_period = False
            for matched_period_timestamp in matched_period_timestamps:
                if matched_period_timestamp in snippet_timestamps:
                    in_period = True
            if not in_period:
                add_match = False

            if add_match:
                timestamp = int(relate_timeseries[-1][0])
                timeseries_matched[metric][timestamp]['motif_matches'][motif_timestamp] = motif_size

                motif_id = '%s-%s-%s' % (str(metric), str(int(snippet[-1][0])), str(best_index))
                matched_motifs[motif_id] = {}
                matched_motifs[motif_id]['index'] = best_index
                matched_motifs[motif_id]['distance'] = best_dist
                matched_motifs[motif_id]['size'] = motif_size
                matched_motifs[motif_id]['timestamp'] = timestamp
                matched_motifs[motif_id]['matched_period_timestamps'] = matched_period_timestamps
                matched_motifs[motif_id]['motif_timestamp'] = motif_timestamp
                matched_motifs[motif_id]['type'] = match_type
                matched_motifs[motif_id]['type_id'] = motif_match_types[match_type]
                runtime_end = timer()
                matched_motifs[motif_id]['runtime'] = (runtime_end - start)

                # if SINGLE_MATCH:
                #     break
        except Exception as e:
            logger.error(traceback.format_exc())
            logger.error('error :: functions.luminosity.find_cloudburst_motifs :: metric %s and motif: %s - %s' % (
                str(metric), str(motif), str(e)))
            continue
    end_motifs_check = timer()
    logger.info('functions.luminosity.find_cloudburst_motifs :: motifs checked in %.6f seconds' % (
        (end_motifs_check - start_motifs_check)))
    if print_output:
        print('functions.luminosity.find_cloudburst_motifs :: motifs checked in %.6f seconds' % (
            (end_motifs_check - start_motifs_check)))

    # Sort by distance AND area_percent_diff
    sorted_ordered_matched_motifs_list = []
    if matched_motifs and len(matched_motifs) > 1:
        ordered_matched_motifs_list = []
        for motif_id in list(matched_motifs.keys()):
            distance = matched_motifs[motif_id]['distance']
            ordered_matched_motifs_list.append([motif_id, distance])
        # If the areas under the curve were calculated, the
        # list could be sorted by area_percent_diff then by
        # distance.
        sorted_matched_motifs = {}
        sorted_ordered_matched_motifs_list = sorted(ordered_matched_motifs_list, key=operator.itemgetter(1))
        logger.info('functions.luminosity.find_cloudburst_motifs :: sorting %s matched_motifs by distance' % (
            str(len(sorted_ordered_matched_motifs_list))))

        for motif_id, distance, in sorted_ordered_matched_motifs_list:
            sorted_matched_motifs[motif_id] = matched_motifs[motif_id]
            # if SINGLE_MATCH:
            #     break
        matched_motifs = sorted_matched_motifs.copy()

    end = timer()
    if dev_null:
        del dev_null
    logger.info('functions.luminosity.find_cloudburst_motifs :: %s motif best match found from %s motifs_found and it took a total of %.6f seconds (all mass2/mass3) to process %s' % (
        # str(len(matched_motifs)), str(len(motifs_found)), str(len(fps_checked_for_motifs)),
        str(len(matched_motifs)), str(len(motifs_found)), (end - start), metric))
    if len(matched_motifs) > 0:
        if print_output:
            print('functions.luminosity.find_cloudburst_motifs :: %s motif best match found from %s distance valid motifs of %s motifs_found and it took a total of %.6f seconds (all mass2/mass3) to process %s' % (
                # str(len(matched_motifs)), str(len(motifs_found)), str(len(fps_checked_for_motifs)),
                str(len(matched_motifs)), str(len(distance_valid_motifs)), str(len(motifs_found)),
                (end - start), metric))
            distances = []
            for match in list(matched_motifs.keys()):
                distances.append(matched_motifs[match]['distance'])
            distances_dict = {}
            distances_dict['avg_distance'] = sum(distances) / len(distances)
            distances_dict['distances'] = distances
            print('%s' % str(distances_dict))

    # return matched_motifs, fps_checked_for_motifs
    return matched_motifs, timeseries_matched
