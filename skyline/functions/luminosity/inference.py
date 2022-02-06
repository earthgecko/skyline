from __future__ import division
import logging
import os
import sys
from timeit import default_timer as timer
import traceback
import operator

import numpy as np
import mass_ts as mts

# import matplotlib.image as mpimg
# %matplotlib inline
if True:
    sys.path.insert(0, '/opt/skyline/github/skyline/skyline')
    import settings
    from matched_or_regexed_in_list import matched_or_regexed_in_list
    from functions.timeseries.determine_data_frequency import determine_data_frequency
    from motif_match_types import motif_match_types_dict
    from functions.numpy.percent_different import get_percent_different

    import warnings
    warnings.filterwarnings('ignore')

skyline_app = 'luminosity'
skyline_app_logger = '%sLog' % skyline_app
logger = logging.getLogger(skyline_app_logger)
skyline_app_logfile = '%s/%s.log' % (settings.LOG_PATH, skyline_app)

python_version = int(sys.version_info[0])

this_host = str(os.uname()[1])

# Converting one settings variable into a local variable, just because it is a
# long string otherwise.
try:
    ENABLE_IONOSPHERE_DEBUG = settings.ENABLE_IONOSPHERE_DEBUG
except Exception as e:
    logger.error('error :: functions.luminosity.inference :: cannot determine ENABLE_IONOSPHERE_DEBUG from settings - %s' % e)
    ENABLE_IONOSPHERE_DEBUG = False

try:
    SERVER_METRIC_PATH = '.%s' % settings.SERVER_METRICS_NAME
    if SERVER_METRIC_PATH == '.':
        SERVER_METRIC_PATH = ''
except Exception as e:
    logger.warning('warn :: functions.luminosity.inference :: cannot determine SERVER_METRIC_PATH from settings - %s' % e)
    SERVER_METRIC_PATH = ''
try:
    SINGLE_MATCH = settings.IONOSPHERE_INFERENCE_MOTIFS_SINGLE_MATCH
except Exception as e:
    logger.warning('warn :: functions.luminosity.inference :: cannot determine IONOSPHERE_INFERENCE_MOTIFS_SINGLE_MATCH from settings - %s' % e)
    SINGLE_MATCH = True
try:
    IONOSPHERE_INFERENCE_MOTIFS_TEST_ONLY = settings.IONOSPHERE_INFERENCE_MOTIFS_TEST_ONLY
except Exception as e:
    logger.warning('warn :: functions.luminosity.inference :: cannot determine IONOSPHERE_INFERENCE_MOTIFS_TEST_ONLY from settings - %s' % e)
    IONOSPHERE_INFERENCE_MOTIFS_TEST_ONLY = False
try:
    IONOSPHERE_INFERENCE_MOTIFS_SETTINGS = settings.IONOSPHERE_INFERENCE_MOTIFS_SETTINGS.copy()
except Exception as e:
    logger.warning('warn :: functions.luminosity.inference :: cannot determine IONOSPHERE_INFERENCE_MOTIFS_SETTINGS from settings - %s' % e)
    IONOSPHERE_INFERENCE_MOTIFS_SETTINGS = {}

try:
    IONOSPHERE_INFERENCE_MOTIFS_TOP_MATCHES = settings.IONOSPHERE_INFERENCE_MOTIFS_TOP_MATCHES
except Exception as e:
    logger.warning('warn :: functions.luminosity.inference :: cannot determine IONOSPHERE_INFERENCE_MOTIFS_TOP_MATCHES from settings - %s' % e)
    IONOSPHERE_INFERENCE_MOTIFS_TOP_MATCHES = 20.0

try:
    IONOSPHERE_INFERENCE_MASS_TS_MAX_DISTANCE = settings.IONOSPHERE_INFERENCE_MASS_TS_MAX_DISTANCE
except Exception as e:
    logger.warning('warn :: functions.luminosity.inference :: cannot determine IONOSPHERE_INFERENCE_MASS_TS_MAX_DISTANCE from settings - %s' % e)
    IONOSPHERE_INFERENCE_MASS_TS_MAX_DISTANCE = 20.0

try:
    IONOSPHERE_INFERENCE_MOTIFS_RANGE_PADDING = settings.IONOSPHERE_INFERENCE_MOTIFS_RANGE_PADDING
except Exception as e:
    logger.warning('warn :: functions.luminosity.inference :: cannot determine IONOSPHERE_INFERENCE_MOTIFS_RANGE_PADDING from settings - %s' % e)
    IONOSPHERE_INFERENCE_MOTIFS_RANGE_PADDING = 10

context = 'inference_inference'


def inference(metric, snippet, timeseries, print_output=False):
    """
    Takes a snippet of timeseries, creates motifs at batch_sizes from the
    snippet and searches for those motifs in the given timeseries, returns a
    matched_motifs dict and a timeseries_matched dict

    """
    logger = logging.getLogger(skyline_app_logger)
    child_process_pid = os.getpid()
    logger.info('functions.luminosity.inference :: running for process_pid - %s for %s' % (
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

    metric_resolution = determine_data_frequency(skyline_app, timeseries, False)

    datapoints_per_hour = 3600 / metric_resolution
    datapoints_per_day = 86400 / metric_resolution

    logger.info('functions.luminosity.inference :: looking for similar motifs in timeseries of length: %s' % str(len(timeseries)))

    exact_match_times = []

    relate_dataset = [float(item[1]) for item in timeseries]
    # Min-Max scaling
    minmax_anom_ts = []
    try:
        minmax_anom_ts_values = [x[1] for x in fp_id_metric_ts]
        minmax_anom_ts_values = [float(item[1]) for item in timeseries]
        x_np = np.asarray(minmax_anom_ts_values)
        # Min-Max scaling
        np_minmax = (x_np - x_np.min()) / (x_np.max() - x_np.min())
        for (ts, v) in zip(fp_id_metric_ts, np_minmax):
            minmax_anom_ts.append([ts[0], v])
        logger.info('functions.luminosity.inference :: minmax_anom_ts populated with %s data points for %s' % (
            str(len(timeseries)), metric)
    except:
        logger.error(traceback.format_exc())
        logger.error('error :: functions.luminosity.inference :: could not minmax scale time series for %s' % metric)
    if not minmax_anom_ts:
        logger.error('error :: functions.luminosity.inference :: minmax_anom_ts list not populated')
        return matched_motifs, timeseries_matched

    # Downsample AFTER pre-processing
    relate_dataset = [float(item[1]) for item in minmax_anom_ts]

# HERE
    for namespace_key in list(IONOSPHERE_INFERENCE_MOTIFS_SETTINGS.keys()):
        pattern_found, matched_by_result = matched_or_regexed_in_list(skyline_app, metric, [namespace_key], False)
        if pattern_found:
            namespace_key = namespace_key
            break
    if not pattern_found:
        namespace_key = 'default_inference_batch_sizes'
    for batch_size in list(IONOSPHERE_INFERENCE_MOTIFS_SETTINGS[namespace_key].keys()):
        if not isinstance(batch_size, int):
            logger.error('functions.luminosity.inference :: invalid batch_size IONOSPHERE_INFERENCE_MOTIFS_SETTINGS[namespace_key] - %s' % (
                str(IONOSPHERE_INFERENCE_MOTIFS_SETTINGS[namespace_key])))
            continue

        if len(timeseries) <= batch_size:
            continue

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
            logger.info('functions.luminosity.inference :: batch_size: %s, snippet length: %s, len(indices) < 3, using mass3' % (
                str(batch_size), str(n)))

        try:
            top_matches = IONOSPHERE_INFERENCE_MOTIFS_SETTINGS[namespace_key][batch_size]['top_matches']
        except KeyError:
            top_matches = IONOSPHERE_INFERENCE_MOTIFS_TOP_MATCHES
        except Exception as e:
            logger.error('functions.luminosity.inference :: failed to determine a value from IONOSPHERE_INFERENCE_MOTIFS_SETTINGS top_matches - %s' % (
                e))
            top_matches = IONOSPHERE_INFERENCE_MOTIFS_TOP_MATCHES

        try:
            max_distance = IONOSPHERE_INFERENCE_MOTIFS_SETTINGS[namespace_key][batch_size]['max_distance']
        except KeyError:
            max_distance = IONOSPHERE_INFERENCE_MASS_TS_MAX_DISTANCE
        except Exception as e:
            logger.error('functions.luminosity.inference :: failed to determine a value from IONOSPHERE_INFERENCE_MASS_TS_MAX_DISTANCE max_distance - %s' % (
                e))
            max_distance = IONOSPHERE_INFERENCE_MASS_TS_MAX_DISTANCE

        try:
            range_padding_percent = IONOSPHERE_INFERENCE_MOTIFS_SETTINGS[namespace_key][batch_size]['range_padding_percent']
        except KeyError:
            range_padding_percent = IONOSPHERE_INFERENCE_MOTIFS_RANGE_PADDING
        except Exception as e:
            logger.error('functions.luminosity.inference :: failed to determine a value from IONOSPHERE_INFERENCE_MOTIFS_SETTINGS range_padding_percent - %s' % (
                e))
            range_padding_percent = IONOSPHERE_INFERENCE_MOTIFS_RANGE_PADDING

        # @added 20210425 - Feature #4014: Ionosphere - inference
        max_area_percent_diff = IONOSPHERE_INFERENCE_MOTIFS_SETTINGS[namespace_key][batch_size]['max_area_percent_diff']

        # @added 20210427 - Feature #4014: Ionosphere - inference
        # Finding exact matches can result is more than doubling the
        # runtime when used after mass2_batch runs (which do not find)
        # exact matches, mass3 does.  However the amount of time an
        # exact match is found, is very rare
        try:
            find_exact_matches = IONOSPHERE_INFERENCE_MOTIFS_SETTINGS[namespace_key][batch_size]['find_exact_matches']
        except KeyError:
            find_exact_matches = False
        except Exception as e:
            logger.error('functions.luminosity.inference :: failed to determine a value from IONOSPHERE_INFERENCE_MOTIFS_SETTINGS find_exact_matches - %s' % (
                e))
            find_exact_matches = False

        # if use_mass2_batch:
        adjusted_batch_size = False
        if metric_resolution > 60:
            adjusted_batch_size = int(batch_size)
            batch_size_seconds = batch_size * 60
            batch_size = round(batch_size_seconds / metric_resolution)
            adjusted_max_distance = float(max_distance)
            max_distance_factor = int(round(adjusted_batch_size / float(max_distance)))
            # max_distance = int(round(adjusted_batch_size / max_distance_factor))
            max_distance = int(round(batch_size / max_distance_factor))
        if adjusted_batch_size:
            logger.info('functions.luminosity.inference :: analysis run - batch_size: %s (adjusted from %s), top_matches: %s, max_distance: %s (adjusted from %s), snippet_length: %s' % (
                str(batch_size), str(adjusted_batch_size),
                str(top_matches), str(max_distance),
                str(adjusted_max_distance), str(len(snippet))))
            if print_output:
                print('functions.luminosity.inference :: analysis run - batch_size: %s (adjusted from %s), top_matches: %s, max_distance: %s (adjusted from %s), snippet_length: %s' % (
                    str(batch_size), str(adjusted_batch_size),
                    str(top_matches), str(max_distance),
                    str(adjusted_max_distance), str(len(snippet))))
        else:
            logger.info('functions.luminosity.inference :: analysis run - metric: %s, batch_size: %s, top_matches: %s, max_distance: %s, snippet_length: %s' % (
                str(metric), str(batch_size), str(top_matches),
                str(max_distance), str(len(snippet))))
            if print_output:
                print('functions.luminosity.inference :: analysis run - metric: %s, batch_size: %s, top_matches: %s, max_distance: %s, snippet_length: %s' % (
                    str(metric), str(batch_size), str(top_matches),
                    str(max_distance), str(len(snippet))))

        # Given that the snippet can be any length
        if len(snippet) <= batch_size:
            if print_output:
                print('functions.luminosity.inference :: skipping - batch_size: %s' % str(batch_size))
            continue

        # Create the subsequence that is being searched for
        n = batch_size
        snippets = [snippet[i * n:(i + 1) * n] for i in range((len(snippet) + n - 1) // n)]
        print('functions.luminosity.inference :: checking %s snippets of batch_size: %s' % (
            str(len(snippets)), str(batch_size)))

#        batch_size_anomalous_timeseries_subsequence = timeseries[-batch_size:]
#        batch_size_dataset = [float(item[1]) for item in batch_size_anomalous_timeseries_subsequence]
        for i_snippet in snippets:
            if len(i_snippet) < batch_size:
                if i_snippet == snippets[-1]:
                    datapoints_needed = batch_size - len(i_snippet)
                    new_snippet = snippets[-2][-datapoints_needed:]
                    i_snippet = new_snippet + i_snippet

            batch_size_timeseries_subsequence = i_snippet[-batch_size:]
            batch_size_dataset = [float(item[1]) for item in batch_size_timeseries_subsequence]
            motif_timestamp = int(batch_size_timeseries_subsequence[-1][0])

            # Determine the range_padding and range max and min of the
            # subsequence
            max_y = max(batch_size_dataset)
            min_y = min(batch_size_dataset)
            range_padding = ((max_y - min_y) / 100) * range_padding_percent
            if min_y > 0 and (min_y - range_padding) > 0:
                min_y_padded = min_y - range_padding
            else:
                min_y_padded = min_y
            max_y_padded = max_y + range_padding
            if min_y_padded == max_y_padded:
                min_y_padded = min_y_padded - ((min_y_padded / 100) * range_padding_percent)
                max_y_padded = max_y_padded + ((max_y_padded / 100) * range_padding_percent)

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
            # 2021-04-27 13:45:59 :: 3586421 :: functions.luminosity.inference :: analysed 2 fps of full_duration 86400 in 0.330732 seconds
            # 2021-04-27 13:45:59 :: 3586421 :: functions.luminosity.inference :: 22421 distance_valid_motifs determined in 0.346807 seconds from 81432 motifs_found
            # 2021-04-27 13:45:59 :: 3586421 :: functions.luminosity.inference :: sorted_motifs from distance_valid_motifs in 0.048316 seconds
            # 2021-04-27 13:46:01 :: 3586421 :: functions.luminosity.inference :: percent_different in 0.000590 seconds
            # 2021-04-27 13:46:01 :: 3586421 :: functions.luminosity.inference :: percent_different in 0.000271 seconds
            # ...
            # ...
            # 2021-04-27 13:46:57 :: 3586421 :: functions.luminosity.inference :: percent_different in 0.000373 seconds
            # 2021-04-27 13:46:57 :: 3586421 :: functions.luminosity.inference :: percent_different in 0.000381 seconds
            # 2021-04-27 13:46:58 :: 3586421 :: functions.luminosity.inference :: percent_different in 0.000363 seconds
            # 2021-04-27 13:46:58 :: 3586421 :: functions.luminosity.inference :: percent_different in 0.000348 seconds
            # 2021-04-27 13:47:01 :: 3586421 :: functions.luminosity.inference :: motifs checked in 62.036366 seconds
            # 2021-04-27 13:47:01 :: 3586421 :: functions.luminosity.inference :: 0 motif best match found from 81432 motifs_found, 4 fps where checked {604800: {'fp_count': 2}, 86400: {'fp_count': 2}} (motifs remove due to not in range 22325, percent_different 96) and it took a total of 64.761969 seconds (only mass3) to process telegraf.ssdnodes-26840.mariadb.localhost:3306.mysql.bytes_sent
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
                        logger.info('functions.luminosity.inference :: adjusting top_matches for mass2_batch to %s (the maximum possible top - 1) as top_matches=%s will be out of bounds mts.mass2_batch' % (
                            str(use_top_matches), str(top_matches)))

                    start_mass2_batch = timer()
                    best_indices, best_dists = mts.mass2_batch(relate_dataset, batch_size_dataset, batch_size=batch_size, top_matches=use_top_matches)
                    end_mass2_batch = timer()
                    mass2_batch_times.append((end_mass2_batch - start_mass2_batch))
                    current_best_indices = best_indices.tolist()
                    current_best_dists = best_dists.tolist()

                    logger.info('functions.luminosity.inference :: mass2_batch run on batch_size: %s, top_matches: %s, in %6f seconds' % (
                        str(batch_size), str(use_top_matches),
                        (end_mass2_batch - start_mass2_batch)))
                    if print_output:
                        print('functions.luminosity.inference :: mass2_batch run on batch_size: %s, top_matches: %s, in %6f seconds' % (
                            str(batch_size), str(use_top_matches),
                            (end_mass2_batch - start_mass2_batch)))

                    if debug_logging:
                        logger.debug('debug :: functions.luminosity.inference :: best_indices: %s, best_dists: %s' % (
                            str(current_best_indices), str(current_best_dists)))
                except ValueError as e:
                    # If mass2_batch reports out of bounds, use mass3
                    if 'out of bounds' in str(e):
                        use_mass3 = True
                        best_dists = ['use_mass3']
                        logger.info('functions.luminosity.inference :: mts.mass2_batch will be out of bounds running mass3')
                except Exception as e:
                    logger.error('error :: functions.luminosity.inference :: %s mts.mass2_batch error: %s' % (
                        str(metric), str(e)))
                    continue
                if not use_mass3:
                    try:
                        if str(list(best_dists)) == str(list(empty_dists)):
                            logger.info('functions.luminosity.inference :: mts.mass2_batch no similar motif from id %s - best_dists: %s' % (
                                str(metric), str(list(best_dists))))
                            continue
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

                # @modified 20210504 - Feature #4014: Ionosphere - inference
                # Handle the fp_timeseries being the same length (meaning
                # too short) as the query length
                if len(snippet) <= pieces:
                    logger.info('functions.luminosity.inference :: skipping running mass3 with %s pieces on on metric: %s, batch_size: %s because snippet length is not long enough for the query size' % (
                        str(pieces), str(metric), str(batch_size)))
                    continue

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
                    logger.info('functions.luminosity.inference :: skipping running mass3 on metric: %s, batch_size: %s because the batch_size is too close to length' % (
                        str(metric), str(batch_size)))
                    if print_output:
                        print('functions.luminosity.inference :: skipping running mass3 on metric: %s, batch_size: %s because the batch_size is too close to length' % (
                            str(metric), str(batch_size)))
                    continue

                logger.info('functions.luminosity.inference :: running mass3 with %s pieces on on metric: %s, batch_size: %s' % (
                    str(pieces), str(metric), str(batch_size)))

                print('running mass3 with %s pieces on batch_size: %s for %s with batch_size_dataset: %s' % (
                    str(pieces), str(batch_size), str(motif_timestamp),
                    str(len(batch_size_dataset))))

                if print_output:
                    print('functions.luminosity.inference :: running mass3 with %s pieces on on metric: %s, batch_size: %s' % (
                        str(pieces), str(metric), str(batch_size)))

                start_mass3 = timer()
                try:
                    best_dists = mts.mass3(relate_dataset, batch_size_dataset, pieces)
                    end_mass3 = timer()
                except Exception as e:
                    logger.error('error :: functions.luminosity.inference :: metric %s mts.mass3 error: %s' % (
                        str(metric), str(e)))
                    continue
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
                        logger.info('functions.luminosity.inference :: discarding mass3 results as current_best_dists length: %s, current_best_indices length: %s do not match, took %6f seconds' % (
                            str(len(current_best_dists)), str(len(current_best_indices)),
                            (end_mass3 - start_mass3)))
                        continue

                logger.info('functions.luminosity.inference :: mass3 run, current_best_dists length: %s, current_best_indices length: %s, took %6f seconds' % (
                    str(len(current_best_dists)), str(len(current_best_indices)),
                    (end_mass3 - start_mass3)))
                if print_output:
                    print('functions.luminosity.inference :: mass3 run, current_best_dists length: %s, current_best_indices length: %s, took %6f seconds' % (
                        str(len(current_best_dists)), str(len(current_best_indices)),
                        (end_mass3 - start_mass3)))

            if not use_mass3:
                if not current_best_indices[0]:
                    continue
            if use_mass3 and not current_best_indices:
                continue

            # All in one quicker?  Yes
            start_add_motifs = timer()
            add_motifs = []
            try:
                add_motifs = [[metric, current_best_indices[index], best_dist.real, batch_size_timeseries_subsequence, batch_size, max_distance, max_area_percent_diff, max_y, min_y, range_padding, min_y_padded, max_y_padded, motif_timestamp] for index, best_dist in enumerate(current_best_dists)]
                if add_motifs:
                    motifs_found = motifs_found + add_motifs
            except Exception as e:
                logger.error(traceback.format_exc())
                logger.error('error :: functions.luminosity.inference :: could not add_motifs to motifs_found - %s' % (
                    e))
            end_add_motifs = timer()
            logger.info('functions.luminosity.inference :: added %s motifs to motifs_found in %.6f seconds' % (
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
                            exact_matches_found.append([metric, current_index, 0.0, batch_size_timeseries_subsequence, batch_size, max_distance, max_area_percent_diff, max_y, min_y, range_padding, min_y_padded, max_y_padded, motif_timestamp])
                            motifs_found.append([metric, current_index, 0.0, batch_size_timeseries_subsequence, batch_size, max_distance, max_area_percent_diff, max_y, min_y, range_padding, min_y_padded, max_y_padded, motif_timestamp])
                        current_index += 1
                    end_exact_match = timer()
                    exact_match_times.append((end_exact_match - start_exact_match))
                except Exception as e:
                    logger.error(traceback.format_exc())
                    logger.error('error :: functions.luminosity.inference :: could not determine it any exact matches could be found in %s timeseries - %s' % (
                        str(metric), e))
                logger.info('functions.luminosity.inference :: exact matches checked in %.6f seconds' % (
                    (end_exact_match - start_exact_match)))

        # TODO
        # mass3 ALL, then evaluate, would it be quicker?  No see POC
            # above
    logger.info('functions.luminosity.inference :: mts.mass2_batch runs on %s in %.6f seconds' % (
        str(metric), sum(mass2_batch_times)))
    logger.info('functions.luminosity.inference :: exact_match runs on %s in %.6f seconds' % (
        str(metric), sum(exact_match_times)))
    end_full_duration = timer()
    logger.info('functions.luminosity.inference :: analysed %s in %.6f seconds' % (
        str(metric), (end_full_duration - start_full_duration)))

    # Patterns are sorted by distance
    # The list produced with the mass3 method will include
    # nans
    start_distance_valid_motifs = timer()
    distance_valid_motifs = [item for item in motifs_found if not np.isnan(item[2]) and item[2] <= item[5]]
    end_distance_valid_motifs = timer()
    logger.info('functions.luminosity.inference :: %s distance_valid_motifs determined in %.6f seconds from %s motifs_found' % (
        str(len(distance_valid_motifs)),
        (end_distance_valid_motifs - start_distance_valid_motifs),
        str(len(motifs_found))))
    if print_output:
        print('functions.luminosity.inference :: %s distance_valid_motifs determined in %.6f seconds from %s motifs_found' % (
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
    logger.info('functions.luminosity.inference :: sorted_motifs from distance_valid_motifs in %.6f seconds' % (
        (end_sorted_motifs - start_sorted_motifs)))

    percent_different_removed = 0
    not_in_range_removed = 0
    start_motifs_check = timer()
    for motif in sorted_motifs:
        try:

            add_match = False
            all_in_range = False

            metric = motif[0]
            best_index = motif[1]
            best_dist = motif[2]

            # @added 20210414 - Feature #4014: Ionosphere - inference
            #                   Branch #3590: inference
            # Store the not anomalous motifs
            motif_sequence = motif[3]

            # @modified 20210419 - Feature #4014: Ionosphere - inference
            # Added batch_size
            motif_size = motif[4]

            motif_max_distance = motif[5]
            motif_max_area_percent_diff = motif[6]
            max_y = motif[7]
            min_y = motif[8]
            range_padding = motif[9]
            min_y_padded = motif[10]
            max_y_padded = motif[11]
            motif_timestamp = motif[12]

            if motif in exact_matches_found:
                add_match = True
                match_type = 'exact'
                all_in_range = True
                if debug_logging:
                    logger.debug('debug :: functions.luminosity.inference :: exact match: %s' % (str(motif)))

            full_relate_timeseries = timeseries
            relate_timeseries = [item for index, item in enumerate(full_relate_timeseries) if index >= best_index and index < (best_index + motif_size)]
            relate_dataset = [item[1] for item in relate_timeseries]
            # relate_dataset_timestamps = [int(item[0]) for item in relate_timeseries]

            if not add_match:
                all_in_range = True
                # Just check min and max, faster than loop iteration
                # for value in relate_dataset:
                #     if value < min_y_padded:
                #         all_in_range = False
                #         break
                #     if value > max_y_padded:
                #         all_in_range = False
                #         break
                min_relate_dataset = min(relate_dataset)
                if min_relate_dataset < min_y_padded:
                    all_in_range = False
                max_relate_dataset = max(relate_dataset)
                if max_relate_dataset > max_y_padded:
                    all_in_range = False

                if all_in_range:
                    if max_relate_dataset < (max_y - range_padding):
                        all_in_range = False
                        if debug_logging:
                            logger.debug('debug :: functions.luminosity.inference :: all_in_range: related_max_y: %s less than (max_y - range_padding): (%s - %s) = %s' % (
                                str(max_relate_dataset), str(max_y), str(range_padding),
                                str((max_y - range_padding))))
                    if min_relate_dataset > (min_y + range_padding):
                        all_in_range = False
                        if debug_logging:
                            logger.debug('debug :: functions.luminosity.inference :: all_in_range: related_min_y: %s greater than (min_y + range_padding): (%s + %s) = %s' % (
                                str(min_relate_dataset), str(min_y), str(range_padding),
                                str((min_y + range_padding))))
                if all_in_range:
                    # logger.info('functions.luminosity.inference :: ALL IN RANGE - all_in_range: %s, distance: %s' % (str(all_in_range), str(best_dist)))
                    add_match = True
                    match_type = 'all_in_range'
                else:
                    not_in_range_removed += 1

            compare_percent_different = 100

            # @added 20210423 - Feature #4014: Ionosphere - inference
            # Compute the area using the composite trapezoidal rule to determine
            # if the similarity is similar enough.
            if add_match:
                calculate_areas_under_curves = True
                motif_area = None
                fp_motif_area = None
                percent_different = None
                if calculate_areas_under_curves and add_match:
                    start_percent_different = timer()
                    # dx = int(metric_resolution / 60)
                    try:
                        batch_size_dataset = [float(item[1]) for item in motif_sequence]
                        y_motif = np.array(batch_size_dataset)
                        # motif_area = np.trapz(y_motif, dx=dx)
                        motif_area = np.trapz(y_motif, dx=1)
                    except Exception as e:
                        logger.error('error :: functions.luminosity.inference :: failed to get motif_area with np.trapz - %s' % (
                            e))
                    try:
                        y_fp_motif = np.array(relate_dataset)
                        # fp_motif_area = np.trapz(y_fp_motif, dx=dx)
                        fp_motif_area = np.trapz(y_fp_motif, dx=1)
                    except Exception as e:
                        logger.error('error :: functions.luminosity.inference :: failed to get fp_motif_area with np.trapz - %s' % (
                            e))
                    # @added 20210424 - Feature #4014: Ionosphere - inference
                    # Determine the percentage difference (as a
                    # positive value) of the areas under the
                    # curves.
                    # percent_different = get_percent_different(fp_motif_area, motif_area, True)
                    percent_different = get_percent_different(fp_motif_area, motif_area, False)

                    # @added 20210424 - Feature #4014: Ionosphere - inference
                    # For the purpose of the comparison, if the get_percent_different
                    # returns None set percent_different to 100 as 100 will
                    # always be greater than the motif_max_area_percent_diff
                    if percent_different is None:
                        percent_different = 100

                    if percent_different < 0:
                        new_pdiff = percent_different * -1
                        compare_percent_different = new_pdiff
                    else:
                        compare_percent_different = float(percent_different)

                    if compare_percent_different > motif_max_area_percent_diff:
                        add_match = False
                        percent_different_removed += 1
                        # logger.info('functions.luminosity.inference :: all_in_range match removed area_percent_diff: %s' % (str(percent_different)))
                        # BUT ...
                        # @modified 20210504 - Bug #4044: inference - motif distance override - exact match
                        # Do not add as similar just based on distance
                        # if motif_max_distance > 10:
                        #     if best_dist < 3 and not add_match:
                        #         logger.info('functions.luminosity.inference :: DISTANCE VERY SIMILAR - adding match even though area_percent_diff is greater than max_area_percent_diff because best_dist: %s' % (
                        #             str(best_dist)))
                        #         add_match = True
                        #         percent_different_removed -= 1
                        #         # match_type = 'distance'
                        # if best_dist < 1 and not add_match:
                        #     logger.info('functions.luminosity.inference :: DISTANCE VERY SIMILAR - adding match even though area_percent_diff is greater than max_area_percent_diff because best_dist: %s' % (
                        #         str(best_dist)))
                        #     add_match = True
                        #     percent_different_removed -= 1
                        #     # match_type = 'distance'

                    end_percent_different = timer()
                    logger.info('functions.luminosity.inference :: percent_different in %.6f seconds' % (
                        (end_percent_different - start_percent_different)))

            # @added 20210430 - Bug #4044: inference - motif distance override - exact match
            if compare_percent_different == 0 and best_dist == 0:
                add_match = True
                match_type = 'exact'
                # @added 20210504 - Bug #4044: inference - motif distance override - exact match
                # Handle exact matches here
                exact_matches_found.append(motif)

            if add_match:
                timestamp = int(relate_timeseries[-1][0])
                timeseries_matched[metric][timestamp]['motif_matches'][motif_timestamp] = motif_size

                motif_id = '%s-%s-%s' % (str(metric), str(int(snippet[-1][0])), str(best_index))
                matched_motifs[motif_id] = {}
                matched_motifs[motif_id]['index'] = best_index
                matched_motifs[motif_id]['distance'] = best_dist
                matched_motifs[motif_id]['max_distance'] = motif_max_distance
                matched_motifs[motif_id]['size'] = motif_size
                matched_motifs[motif_id]['timestamp'] = timestamp
                matched_motifs[motif_id]['matched_period_timestamps'] = [int(item[0]) for item in relate_timeseries]
                matched_motifs[motif_id]['motif_timestamp'] = motif_timestamp
                matched_motifs[motif_id]['type'] = match_type
                matched_motifs[motif_id]['type_id'] = motif_match_types[match_type]
                # @added 20210423 - Feature #4014: Ionosphere - inference
                # Compute the area using the composite trapezoidal rule.
                matched_motifs[motif_id]['motif_area'] = motif_area
                matched_motifs[motif_id]['fp_motif_area'] = fp_motif_area
                # @added 20210424 - Feature #4014: Ionosphere - inference
                matched_motifs[motif_id]['area_percent_diff'] = percent_different
                matched_motifs[motif_id]['max_area_percent_diff'] = motif_max_area_percent_diff
                runtime_end = timer()
                matched_motifs[motif_id]['runtime'] = (runtime_end - start)

                # if SINGLE_MATCH:
                #     break
        except Exception as e:
            logger.error(traceback.format_exc())
            logger.error('error :: functions.luminosity.inference :: metric %s and motif: %s - %s' % (
                str(metric), str(motif), str(e)))
            continue
    end_motifs_check = timer()
    logger.info('functions.luminosity.inference :: motifs checked in %.6f seconds' % (
        (end_motifs_check - start_motifs_check)))
    if print_output:
        print('functions.luminosity.inference :: motifs checked in %.6f seconds' % (
            (end_motifs_check - start_motifs_check)))

    # Sort by distance AND area_percent_diff
    sorted_ordered_matched_motifs_list = []
    if matched_motifs and len(matched_motifs) > 1:
        ordered_matched_motifs_list = []
        for motif_id in list(matched_motifs.keys()):
            distance = matched_motifs[motif_id]['distance']
            area_percent_diff = matched_motifs[motif_id]['area_percent_diff']
            ordered_matched_motifs_list.append([motif_id, distance, area_percent_diff])
        # If the areas under the curve were calculated, the
        # list could be sorted by area_percent_diff then by
        # distance.
        sorted_matched_motifs = {}
        sorted_ordered_matched_motifs_list = sorted(ordered_matched_motifs_list, key=operator.itemgetter(1, 2))
        logger.info('functions.luminosity.inference :: sorting %s matched_motifs by distance and area_percent_diff' % (
            str(len(sorted_ordered_matched_motifs_list))))

        for motif_id, distance, area_percent_diff in sorted_ordered_matched_motifs_list:
            sorted_matched_motifs[motif_id] = matched_motifs[motif_id]
            # if SINGLE_MATCH:
            #     break
        matched_motifs = sorted_matched_motifs.copy()

    end = timer()
    if dev_null:
        del dev_null
    logger.info('functions.luminosity.inference :: %s motif best match found from %s motifs_found (motifs removed due to not_in_range %s, percent_different %s) and it took a total of %.6f seconds (all mass2/mass3) to process %s' % (
        # str(len(matched_motifs)), str(len(motifs_found)), str(len(fps_checked_for_motifs)),
        str(len(matched_motifs)), str(len(motifs_found)), str(not_in_range_removed),
        str(percent_different_removed), (end - start), metric))
    print('functions.luminosity.inference :: %s motif best match found from %s distance valid motifs of %s motifs_found (motifs removed due to not_in_range %s, percent_different %s) and it took a total of %.6f seconds (all mass2/mass3) to process %s' % (
        # str(len(matched_motifs)), str(len(motifs_found)), str(len(fps_checked_for_motifs)),
        str(len(matched_motifs)), str(len(distance_valid_motifs)), str(len(motifs_found)), str(not_in_range_removed),
        str(percent_different_removed), (end - start), metric))
    # return matched_motifs, fps_checked_for_motifs
    return matched_motifs, timeseries_matched
