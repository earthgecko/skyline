from __future__ import division
import logging
import os
import sys
from ast import literal_eval
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
    from skyline_functions import (
        mirage_load_metric_vars, write_data_to_file,
        # mysql_select,
        # get_redis_conn, get_redis_conn_decoded, mkdir_p,
        # is_derivative_metric, get_graphite_graph_image, nonNegativeDerivative,
    )
    from common_functions import get_metrics_db_object
    from matched_or_regexed_in_list import matched_or_regexed_in_list
    # from determine_data_frequency import determine_data_frequency
    from functions.timeseries.determine_data_frequency import determine_data_frequency
    from motif_match_types import motif_match_types_dict
    from functions.memcache.get_fp_timeseries import get_fp_timeseries
    from functions.database.queries.fp_timeseries import get_db_fp_timeseries
    from functions.numpy.percent_different import get_percent_different
    from functions.database.queries.get_ionosphere_fp_ids_for_full_duration import get_ionosphere_fp_ids_for_full_duration

    import warnings
    warnings.filterwarnings('ignore')

skyline_app = 'ionosphere'
skyline_app_logger = '%sLog' % skyline_app
logger = logging.getLogger(skyline_app_logger)
skyline_app_logfile = '%s/%s.log' % (settings.LOG_PATH, skyline_app)
skyline_app_loglock = '%s.lock' % skyline_app_logfile
skyline_app_logwait = '%s.wait' % skyline_app_logfile

python_version = int(sys.version_info[0])

this_host = str(os.uname()[1])

# Converting one settings variable into a local variable, just because it is a
# long string otherwise.
try:
    ENABLE_IONOSPHERE_DEBUG = settings.ENABLE_IONOSPHERE_DEBUG
except Exception as e:
    logger.error('error :: inference :: cannot determine ENABLE_IONOSPHERE_DEBUG from settings - %s' % e)
    ENABLE_IONOSPHERE_DEBUG = False

try:
    SERVER_METRIC_PATH = '.%s' % settings.SERVER_METRICS_NAME
    if SERVER_METRIC_PATH == '.':
        SERVER_METRIC_PATH = ''
except Exception as e:
    logger.warning('warn :: inference :: cannot determine SERVER_METRIC_PATH from settings - %s' % e)
    SERVER_METRIC_PATH = ''
try:
    SINGLE_MATCH = settings.IONOSPHERE_INFERENCE_MOTIFS_SINGLE_MATCH
except Exception as e:
    logger.warning('warn :: inference :: cannot determine IONOSPHERE_INFERENCE_MOTIFS_SINGLE_MATCH from settings - %s' % e)
    SINGLE_MATCH = True
try:
    IONOSPHERE_INFERENCE_MOTIFS_TEST_ONLY = settings.IONOSPHERE_INFERENCE_MOTIFS_TEST_ONLY
except Exception as e:
    logger.warning('warn :: inference :: cannot determine IONOSPHERE_INFERENCE_MOTIFS_TEST_ONLY from settings - %s' % e)
    IONOSPHERE_INFERENCE_MOTIFS_TEST_ONLY = False
try:
    IONOSPHERE_INFERENCE_MOTIFS_SETTINGS = settings.IONOSPHERE_INFERENCE_MOTIFS_SETTINGS.copy()
except Exception as e:
    logger.warning('warn :: inference :: cannot determine IONOSPHERE_INFERENCE_MOTIFS_SETTINGS from settings - %s' % e)
    IONOSPHERE_INFERENCE_MOTIFS_SETTINGS = {}

try:
    IONOSPHERE_INFERENCE_MOTIFS_TOP_MATCHES = settings.IONOSPHERE_INFERENCE_MOTIFS_TOP_MATCHES
except Exception as e:
    logger.warning('warn :: inference :: cannot determine IONOSPHERE_INFERENCE_MOTIFS_TOP_MATCHES from settings - %s' % e)
    IONOSPHERE_INFERENCE_MOTIFS_TOP_MATCHES = 20.0

try:
    IONOSPHERE_INFERENCE_MASS_TS_MAX_DISTANCE = settings.IONOSPHERE_INFERENCE_MASS_TS_MAX_DISTANCE
except Exception as e:
    logger.warning('warn :: inference :: cannot determine IONOSPHERE_INFERENCE_MASS_TS_MAX_DISTANCE from settings - %s' % e)
    IONOSPHERE_INFERENCE_MASS_TS_MAX_DISTANCE = 20.0

try:
    IONOSPHERE_INFERENCE_MOTIFS_RANGE_PADDING = settings.IONOSPHERE_INFERENCE_MOTIFS_RANGE_PADDING
except Exception as e:
    logger.warning('warn :: inference :: cannot determine IONOSPHERE_INFERENCE_MOTIFS_RANGE_PADDING from settings - %s' % e)
    IONOSPHERE_INFERENCE_MOTIFS_RANGE_PADDING = 10

context = 'ionosphere_inference'


def ionosphere_motif_inference(metric, timestamp):

    logger = logging.getLogger(skyline_app_logger)
    child_process_pid = os.getpid()
    logger.info('inference :: running for process_pid - %s for %s' % (
        str(child_process_pid), metric))
    full_duration_in_hours = int(settings.FULL_DURATION / 60 / 60)

    start = timer()
    debug_logging = False
    metric_vars_dict = {}
    metric_id = 0
    fp_ids = []
    timeseries = []
    matched_motifs = {}
#    motifs_found_in_fps = []
    motifs_found = []
    dev_null = None

    mass2_batch_times = []
    mass3_times = []
    exact_match_times = []

    nan = np.array([np.nan])
    nanj = complex(0.0, float('nan'))
    empty_dists = np.array(nan + nanj)
    count = 0

    motifs_found = []
    exact_matches_found = []

    fps_timeseries = {}

    motif_match_types = motif_match_types_dict()

    # @added 20210412 - Feature #4014: Ionosphere - inference
    #                   Branch #3590: inference
    # Added fps_checked_for_motifs to enable ionosphere to up the database ionosphere
    # motif related columns
    fps_checked_for_motifs = []

    # @added 20210426 - Feature #4014: Ionosphere - inference
    # Optimise the database select time by getting each ionosphere table fp id
    # row for each fp, so that the single resulting object can be referred to
    # in later in the evaluation stage, rather than making another database
    # select to get the fp generation
    fp_id_rows = {}

    if not IONOSPHERE_INFERENCE_MOTIFS_SETTINGS:
        return matched_motifs, fps_checked_for_motifs

    metric_dir = metric.replace('.', '/')
    metric_timeseries_dir = '%s/%s/%s' % (
        settings.IONOSPHERE_DATA_FOLDER, str(timestamp), metric_dir)
    metric_vars_file = '%s/%s.txt' % (metric_timeseries_dir, metric)
    timeseries_json = '%s/%s.json' % (metric_timeseries_dir, metric)
    full_duration_timeseries_json = '%s/%s.mirage.redis.%sh.json' % (
        metric_timeseries_dir, metric, str(full_duration_in_hours))

    try:
        metric_vars_dict = mirage_load_metric_vars(skyline_app, metric_vars_file, True)
    except Exception as e:
        logger.error('error :: inference :: failed to load metric variables from check file - %s - %s' % (
            metric_vars_file, e))
    if not metric_vars_dict:
        return matched_motifs, fps_checked_for_motifs

    full_duration = metric_vars_dict['metric_vars']['full_duration']

    # TODO
    # Optimize determine metric id from Redis

    # Determine the metric details from the database
    metric_id = 0
    metric_db_object = {}
    start_get_metrics_db_object = timer()
    try:
        metric_db_object = get_metrics_db_object(metric)
    except Exception as e:
        logger.error('error :: inference :: failed to get_metrics_db_object - %s' % (e))
    end_get_metrics_db_object = timer()
    logger.info('inference :: get_metrics_db_object in %.6f seconds' % (
        (end_get_metrics_db_object - start_get_metrics_db_object)))

    try:
        metric_id = int(metric_db_object['id'])
    except Exception as e:
        logger.error('error :: inference :: failed to determine metric_id from metric_db_object %s - %s' % (str(metric_db_object), e))
        metric_id = 0
    if not metric_id:
        return matched_motifs, fps_checked_for_motifs

    full_duration_fp_count = {}

    full_durations = [full_duration]

    full_duration_fp_count[full_duration] = {}
    full_duration_fp_count[full_duration]['fp_count'] = 0

    if os.path.isfile(full_duration_timeseries_json):
        full_durations = [full_duration, settings.FULL_DURATION]
        full_duration_fp_count[settings.FULL_DURATION] = {}
        full_duration_fp_count[settings.FULL_DURATION]['fp_count'] = 0

    fp_ids = []
    for full_duration in full_durations:

        # if SINGLE_MATCH and matched_motifs:
        #     break
        if SINGLE_MATCH and exact_matches_found:
            break

        start_full_duration = timer()
        full_duration_fp_ids = []

        fps_full_duration = {}
        try:

            # @modified 20210426 - Feature #4014: Ionosphere - inference
            # Optimize determine FULL row from DB in one request for all fps
            # query = 'SELECT id from ionosphere WHERE metric_id=%s AND full_duration=%s AND enabled=1' % (
            #     str(metric_id), str(full_duration))
            # results = mysql_select(skyline_app, query)
            fps_full_duration = get_ionosphere_fp_ids_for_full_duration(
                skyline_app, metric_id, full_duration, True)
            if fps_full_duration:
                for current_fp_id in list(fps_full_duration.keys()):
                    try:
                        fp_ids.append(current_fp_id)
                        full_duration_fp_ids.append(current_fp_id)
                        fp_id_rows[current_fp_id] = fps_full_duration[current_fp_id]
                    except Exception as e:
                        logger.error(traceback.format_exc())
                        logger.error('error :: inference :: failed to iterate results from get_ionosphere_fp_ids_for_full_duration for %s - %s' % (metric, e))
        except Exception as e:
            logger.error(traceback.format_exc())
            logger.error('error :: inference :: failed to get fp ids via mysql_select from %s - %s' % (metric, e))

        logger.info('inference :: metric_id: %s, full_duration: %s, full_duration_fp_ids: %s' % (
            str(metric_id), str(full_duration), str(full_duration_fp_ids)))

        if not full_duration_fp_ids:
            logger.info('inference :: metric_id: %s, full_duration: %s, full_duration_fp_ids: %s, continuing no fps' % (
                str(metric_id), str(full_duration), str(full_duration_fp_ids)))
            continue

        full_duration_fp_count[full_duration]['fp_count'] = len(full_duration_fp_ids)

        # Now there are known fps, load the timeseries
        if full_duration == settings.FULL_DURATION:
            timeseries_json_file = full_duration_timeseries_json
        else:
            timeseries_json_file = timeseries_json
        # TODO
        # Optimize?  Takes just less than a second to load each file, get data
        # from Redis?  But literal_eval of the data from Redis will probably
        # have similar overhead, given the overhead literal_eval has on the
        # memcache get_fp_timeseries displayed below.
        start_load_timeseries_json = timer()
        try:
            with open((timeseries_json_file), 'r') as f:
                raw_timeseries = f.read()
            timeseries_array_str = str(raw_timeseries).replace('(', '[').replace(')', ']')
            del raw_timeseries
            timeseries = literal_eval(timeseries_array_str)
            del timeseries_array_str
        except Exception as e:
            logger.error('error :: inference :: failed to load timeseries for %s from %s - %s' % (
                metric, timeseries_json_file, e))
            continue
        end_load_timeseries_json = timer()
        logger.info('inference :: load_timeseries_json in %.6f seconds' % (
            (end_load_timeseries_json - start_load_timeseries_json)))

        metric_resolution = determine_data_frequency(skyline_app, timeseries, False)
        logger.info('inference :: looking for similar motifs in trained fps of full_duration: %s' % str(full_duration))

        for fp_id in full_duration_fp_ids:

            # if SINGLE_MATCH and matched_motifs:
            #    break
            if SINGLE_MATCH and exact_matches_found:
                break

            motifs_found_in_fp = []
            exact_match_times = []

            # Surprisingly this can take up to a second or more to get data from
            # memcache and transform it with literal_eval with long timeseries
            # even when the data is in memcache, no with no database query
            fp_timeseries = None

            # But DO query memcache first if the timeseries is less than 2000
            # because shorter timeseries are just as fast with memcache and
            # results in less queries to the database
            if len(timeseries) < 2000:
                start_get_fp_timeseries = timer()
                try:
                    fp_timeseries = get_fp_timeseries(skyline_app, metric_id, fp_id)
                except Exception as e:
                    logger.error('inference :: did not get fp timeseries with get_fp_timeseries(%s, %s, %s) - %s' % (
                        skyline_app, str(metric_id), str(fp_id), e))
                end_get_fp_timeseries = timer()
                logger.info('inference :: get_fp_timeseries in %.6f seconds' % (
                    (end_get_fp_timeseries - start_get_fp_timeseries)))

            if not fp_timeseries:
                start_get_fp_timeseries = timer()
                try:
                    # Generally quicker to use DB than to literal_eval the memcache
                    # data
                    # fp_timeseries = get_fp_timeseries(skyline_app, metric_id, fp_id)
                    fp_timeseries = get_db_fp_timeseries(skyline_app, metric_id, fp_id)
                except Exception as e:
                    logger.error('inference :: did not get fp timeseries with get_db_fp_timeseries(%s, %s, %s) - %s' % (
                        skyline_app, str(metric_id), str(fp_id), e))
                end_get_fp_timeseries = timer()
                logger.info('inference :: get_db_fp_timeseries in %.6f seconds' % (
                    (end_get_fp_timeseries - start_get_fp_timeseries)))

            # If there is a problem with the database, try memcache
            if not fp_timeseries:
                start_get_fp_timeseries = timer()
                try:
                    fp_timeseries = get_fp_timeseries(skyline_app, metric_id, fp_id)
                except Exception as e:
                    logger.error('inference :: did not get fp timeseries with get_fp_timeseries(%s, %s, %s) - %s' % (
                        skyline_app, str(metric_id), str(fp_id), e))
                end_get_fp_timeseries = timer()
                logger.info('inference :: get_fp_timeseries in %.6f seconds' % (
                    (end_get_fp_timeseries - start_get_fp_timeseries)))

            if not fp_timeseries:
                continue

            start_determine_data_frequency = timer()
            fp_timeseries_resolution = determine_data_frequency(skyline_app, fp_timeseries)
            if metric_resolution != fp_timeseries_resolution:
                logger.info('inference :: potentially anomalous timeseries snippet data frequency does not match fp data frequency')
                continue
            end_determine_data_frequency = timer()
            logger.info('inference :: determine_data_frequency in %.6f seconds' % (
                (end_determine_data_frequency - start_determine_data_frequency)))

            # Add the timeseries to the fps_timeseries dict for later use in
            # the all_in_range and areas under a cirve evaluation
            fps_timeseries[fp_id] = fp_timeseries

            relate_dataset = [float(item[1]) for item in fp_timeseries]

            for namespace_key in list(IONOSPHERE_INFERENCE_MOTIFS_SETTINGS.keys()):
                pattern_found, matched_by_result = matched_or_regexed_in_list(skyline_app, metric, [namespace_key], False)
                if pattern_found:
                    namespace_key = namespace_key
                    break
            if not pattern_found:
                namespace_key = 'default_inference_batch_sizes'
            for batch_size in list(IONOSPHERE_INFERENCE_MOTIFS_SETTINGS[namespace_key].keys()):
                if not isinstance(batch_size, int):
                    logger.error('inference :: invalid batch_size IONOSPHERE_INFERENCE_MOTIFS_SETTINGS[namespace_key] - %s' % (
                        str(IONOSPHERE_INFERENCE_MOTIFS_SETTINGS[namespace_key])))
                    continue

                # if SINGLE_MATCH and matched_motifs:
                #     break

                # @added 20210423 - Feature #4014: Ionosphere - inference
                # The convenience mass2_batch method will not work to find
                # top matches if the number of top_matches to be found are
                # greater than the number of indices in which a match can be
                # found.  In these cases such as trying to find the:
                # batch_size: 1440, top_matches: 50, max_distance: 30, fp_timeseries_length: 1451
                # even setting the top_matches to 1 will result in
                # mass2_batch throwing the error:
                # mts.mass2_batch error: kth(=1) out of bounds (1)
                # So use mass3 as appropriate.
                use_mass3 = False
                use_mass2_batch = True
                n = len(fp_timeseries)
                indices = list(range(0, n - batch_size + 1, batch_size))
                # mass2_batch default is 3 so if there are less than 3
                # indices in which the best macthes can be found, use mass3
                if len(indices) < 3:
                    use_mass3 = True
                    use_mass2_batch = False
                    logger.info('inference :: fp_id: %s, batch_size: %s, fp_timeseries length: %s, len(indices) < 3, using mass3' % (
                        str(fp_id), str(batch_size), str(n)))

                try:
                    top_matches = IONOSPHERE_INFERENCE_MOTIFS_SETTINGS[namespace_key][batch_size]['top_matches']
                except KeyError:
                    top_matches = IONOSPHERE_INFERENCE_MOTIFS_TOP_MATCHES
                except Exception as e:
                    logger.error('inference :: failed to determine a value from IONOSPHERE_INFERENCE_MOTIFS_SETTINGS top_matches - %s' % (
                        e))
                    top_matches = IONOSPHERE_INFERENCE_MOTIFS_TOP_MATCHES

                try:
                    max_distance = IONOSPHERE_INFERENCE_MOTIFS_SETTINGS[namespace_key][batch_size]['max_distance']
                except KeyError:
                    max_distance = IONOSPHERE_INFERENCE_MASS_TS_MAX_DISTANCE
                except Exception as e:
                    logger.error('inference :: failed to determine a value from IONOSPHERE_INFERENCE_MASS_TS_MAX_DISTANCE max_distance - %s' % (
                        e))
                    max_distance = IONOSPHERE_INFERENCE_MASS_TS_MAX_DISTANCE

                try:
                    range_padding_percent = IONOSPHERE_INFERENCE_MOTIFS_SETTINGS[namespace_key][batch_size]['range_padding_percent']
                except KeyError:
                    range_padding_percent = IONOSPHERE_INFERENCE_MOTIFS_RANGE_PADDING
                except Exception as e:
                    logger.error('inference :: failed to determine a value from IONOSPHERE_INFERENCE_MOTIFS_SETTINGS range_padding_percent - %s' % (
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
                    logger.error('inference :: failed to determine a value from IONOSPHERE_INFERENCE_MOTIFS_SETTINGS find_exact_matches - %s' % (
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
                    logger.info('inference :: analysis run - fp_id: %s, batch_size: %s (adjusted from %s), top_matches: %s, max_distance: %s (adjusted from %s), fp_timeseries_length: %s' % (
                        str(fp_id), str(batch_size), str(adjusted_batch_size),
                        str(top_matches), str(max_distance),
                        str(adjusted_max_distance), str(len(fp_timeseries))))
                else:
                    logger.info('inference :: analysis run - fp_id: %s, batch_size: %s, top_matches: %s, max_distance: %s, fp_timeseries_length: %s' % (
                        str(fp_id), str(batch_size), str(top_matches),
                        str(max_distance), str(len(fp_timeseries))))

                # Create the subsequence that is being searched for
                batch_size_anomalous_timeseries_subsequence = timeseries[-batch_size:]
                batch_size_dataset = [float(item[1]) for item in batch_size_anomalous_timeseries_subsequence]

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
                # 2021-04-27 13:45:59 :: 3586421 :: inference :: analysed 2 fps of full_duration 86400 in 0.330732 seconds
                # 2021-04-27 13:45:59 :: 3586421 :: inference :: 22421 distance_valid_motifs determined in 0.346807 seconds from 81432 motifs_found
                # 2021-04-27 13:45:59 :: 3586421 :: inference :: sorted_motifs from distance_valid_motifs in 0.048316 seconds
                # 2021-04-27 13:46:01 :: 3586421 :: inference :: percent_different in 0.000590 seconds
                # 2021-04-27 13:46:01 :: 3586421 :: inference :: percent_different in 0.000271 seconds
                # ...
                # ...
                # 2021-04-27 13:46:57 :: 3586421 :: inference :: percent_different in 0.000373 seconds
                # 2021-04-27 13:46:57 :: 3586421 :: inference :: percent_different in 0.000381 seconds
                # 2021-04-27 13:46:58 :: 3586421 :: inference :: percent_different in 0.000363 seconds
                # 2021-04-27 13:46:58 :: 3586421 :: inference :: percent_different in 0.000348 seconds
                # 2021-04-27 13:47:01 :: 3586421 :: inference :: motifs checked in 62.036366 seconds
                # 2021-04-27 13:47:01 :: 3586421 :: inference :: 0 motif best match found from 81432 motifs_found, 4 fps where checked {604800: {'fp_count': 2}, 86400: {'fp_count': 2}} (motifs remove due to not in range 22325, percent_different 96) and it took a total of 64.761969 seconds (only mass3) to process telegraf.ssdnodes-26840.mariadb.localhost:3306.mysql.bytes_sent
                # 2021-04-27 13:47:01 :: 3586421 :: inference found 0 matching similar motifs, checked 0 fps in 64.790198 seconds

                if use_mass2_batch:
                    try:
                        # @added 20210419 - Feature #4014: Ionosphere - inference
                        # Handle top_matches being greater than possible kth that can be found
                        # mts.mass2_batch error: kth(=50) out of bounds (16)
                        use_top_matches = int(top_matches)
                        if (len(fp_timeseries) / int(batch_size)) <= int(top_matches):
                            use_top_matches = round(len(fp_timeseries) / int(batch_size)) - 2
                            if use_top_matches == 2:
                                use_top_matches = 1
                            if use_top_matches < 1:
                                use_top_matches = 1
                            logger.info('inference :: adjusting top_matches for mass2_batch to %s (the maximum possible top - 1) as top_matches=%s will be out of bounds mts.mass2_batch' % (
                                str(use_top_matches), str(top_matches)))

                        start_mass2_batch = timer()
                        best_indices, best_dists = mts.mass2_batch(relate_dataset, batch_size_dataset, batch_size=batch_size, top_matches=use_top_matches)
                        end_mass2_batch = timer()
                        mass2_batch_times.append((end_mass2_batch - start_mass2_batch))
                        current_best_indices = best_indices.tolist()
                        current_best_dists = best_dists.tolist()
                        logger.info('inference :: mass2_batch run on fp_id: %s, batch_size: %s, top_matches: %s, in %6f seconds' % (
                            str(fp_id),
                            str(batch_size), str(use_top_matches),
                            (end_mass2_batch - start_mass2_batch)))
                        # @added 20210412 - Feature #4014: Ionosphere - inference
                        #                   Branch #3590: inference
                        # Add fp_id to fps_checked_for_motifs to enable ionosphere to update the
                        # motif related columns in the ionosphere database table
                        fps_checked_for_motifs.append(fp_id)

                        if debug_logging:
                            logger.debug('debug :: inference :: fp_id: %s, full_duration: %s, best_indices: %s, best_dists: %s' % (
                                str(fp_id), str(full_duration), str(current_best_indices), str(current_best_dists)))
                    except ValueError as e:
                        # If mass2_batch reports out of bounds, use mass3
                        if 'out of bounds' in str(e):
                            use_mass3 = True
                            best_dists = ['use_mass3']
                            logger.info('inference :: mts.mass2_batch will be out of bounds running mass3')
                    except Exception as e:
                        logger.error('error :: inference :: %s mts.mass2_batch error: %s' % (
                            str(fp_id), str(e)))
                        continue
                    if not use_mass3:
                        try:
                            if str(list(best_dists)) == str(list(empty_dists)):
                                logger.info('inference :: mts.mass2_batch no similar motif from fp id %s - best_dists: %s' % (
                                    str(fp_id), str(list(best_dists))))
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
                    pieces = len(fp_timeseries) - query_length
                    if pieces < query_length:
                        pieces = query_length + 2

                    # @modified 20210504 - Feature #4014: Ionosphere - inference
                    # Handle the fp_timeseries being the same length (meaning
                    # too short) as the query length
                    if len(fp_timeseries) <= pieces:
                        logger.info('inference :: skipping running mass3 with %s pieces on on fp_id: %s, batch_size: %s because fp_timeseries length is not long enough for the query size' % (
                            str(pieces), str(fp_id), str(batch_size)))
                        continue

                    # @modified 20210505 - Feature #4014: Ionosphere - inference
                    # Skip he batch size if the fp_timeseries is a similar
                    # length as the batch_size.  This was specifically added to
                    # reduce errors were there may be missing data points in a
                    # timeseries and the lengths are not the same.  This was
                    # encountered on a batch_size of 1440 with FULL_DURATION
                    # 86400 60 second data.   A match was never found at a
                    # batch_size > 720 on that data, but errors were occassionally
                    # encountered.
                    ten_percent_of_batch_size = int(batch_size / 10)
                    if (len(fp_timeseries) - ten_percent_of_batch_size) < batch_size:
                        logger.info('inference :: skipping running mass3 on fp_id: %s, batch_size: %s because the batch_size is too close to length' % (
                            str(fp_id), str(batch_size)))
                        continue

                    logger.info('inference :: running mass3 with %s pieces on on fp_id: %s, batch_size: %s' % (
                        str(pieces), str(fp_id), str(batch_size)))
                    start_mass3 = timer()
                    try:
                        best_dists = mts.mass3(relate_dataset, batch_size_dataset, pieces)
                        end_mass3 = timer()
                    except Exception as e:
                        logger.error('error :: inference :: fp id %s mts.mass3 error: %s' % (
                            str(fp_id), str(e)))
                        continue
                    mass3_times.append((end_mass3 - start_mass3))
                    # Add fp_id to fps_checked_for_motifs to enable ionosphere to update the
                    # motif related columns in the ionosphere database table
                    fps_checked_for_motifs.append(fp_id)

                    current_best_dists = best_dists.tolist()

                    # Create current_best_indices as mass2_batch returns
                    current_best_indices = []
                    if len(relate_dataset) > batch_size:
                        for index in enumerate(relate_dataset):
                            # if index[0] >= (batch_size - 1):
                            # The array starts at batch_size + 1
                            # if index[0] >= (batch_size + 1):
                            # but that fails on the add_motifs comprehension
                            # add_motifs = [[fp_id, current_best_indices[index], best_dist.real, batch_size_anomalous_timeseries_subsequence, batch_size, max_distance, max_area_percent_diff, max_y, min_y, range_padding, min_y_padded, max_y_padded] for index, best_dist in enumerate(current_best_dists)]
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
                            logger.info('inference :: discarding mass3 results as current_best_dists length: %s, current_best_indices length: %s do not match, took %6f seconds' % (
                                str(len(current_best_dists)), str(len(current_best_indices)),
                                (end_mass3 - start_mass3)))
                            continue

                    logger.info('inference :: mass3 run, current_best_dists length: %s, current_best_indices length: %s, took %6f seconds' % (
                        str(len(current_best_dists)), str(len(current_best_indices)),
                        (end_mass3 - start_mass3)))

                if not use_mass3:
                    if not current_best_indices[0]:
                        continue
                if use_mass3 and not current_best_indices:
                    continue

                iterate_add_motifs = False
                if iterate_add_motifs:
                    start_add_motifs_found = timer()
                    add_motifs_count = 0
                    for index, best_dist in enumerate(current_best_dists):
                        try:
                            """
                            Note: mass2_batch finds similar motifs NOT the same
                            motif, the same motif will result in the best_dists
                            being a 0j with mass3.
                            So it is DIYed with FIND EXACT MATCHES
                            """

                            # Do all in one in the distance_valid_motifs
                            # comprehension after the loop
                            # if best_dist.real > max_distance:
                            #     continue
                            # The list produced with the mass3 method will include
                            # nans
                            # if np.isnan(best_dist.real):
                            #     continue

                            try:
                                # @modified 20210414 - Feature #4014: Ionosphere - inference
                                #                      Branch #3590: inference
                                # Store the not anomalous motifs
                                # motif = [fp_id, current_best_indices[index], best_dist.real]
                                # @modified 20210419 - Feature #4014: Ionosphere - inference
                                # Added batch_size and more
                                motif = [fp_id, current_best_indices[index], best_dist.real, batch_size_anomalous_timeseries_subsequence, batch_size, max_distance, max_area_percent_diff, max_y, min_y, range_padding, min_y_padded, max_y_padded]
                            except Exception as e:
                                dev_null = e
                                motif = []

                            if motif:
                                count += 1
                                motifs_found.append(motif)
                                add_motifs_count += 1
                        except Exception as e:
                            logger.error(traceback.format_exc())
                            logger.error('error :: inference :: could not determine is if fp id %s timeseries at index %s was a match - %s' % (
                                str(fp_id), str(current_best_indices[index]), e))
                            continue
                    end_add_motifs_found = timer()
                    logger.info('inference :: added %s motifs to motifs_found in %.6f seconds' % (
                        str(add_motifs_count),
                        (end_add_motifs_found - start_add_motifs_found)))

                # All in one quicker?  Yes
                start_add_motifs = timer()
                add_motifs = []
                try:
                    add_motifs = [[fp_id, current_best_indices[index], best_dist.real, batch_size_anomalous_timeseries_subsequence, batch_size, max_distance, max_area_percent_diff, max_y, min_y, range_padding, min_y_padded, max_y_padded] for index, best_dist in enumerate(current_best_dists)]
                    if add_motifs:
                        motifs_found = motifs_found + add_motifs
                except Exception as e:
                    logger.error(traceback.format_exc())
                    logger.error('error :: inference :: could not add_motifs to motifs_found - %s' % (
                        e))
                end_add_motifs = timer()
                logger.info('inference :: added %s motifs to motifs_found in %.6f seconds' % (
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
                                exact_matches_found.append([fp_id, current_index, 0.0, batch_size_anomalous_timeseries_subsequence, batch_size, max_distance, max_area_percent_diff, max_y, min_y, range_padding, min_y_padded, max_y_padded])
                                motifs_found.append([fp_id, current_index, 0.0, batch_size_anomalous_timeseries_subsequence, batch_size, max_distance, max_area_percent_diff, max_y, min_y, range_padding, min_y_padded, max_y_padded])
                                motifs_found_in_fp.append([fp_id, current_index, 0.0, batch_size_anomalous_timeseries_subsequence, batch_size, max_distance, max_area_percent_diff, max_y, min_y, range_padding, min_y_padded, max_y_padded])
                            current_index += 1
                        end_exact_match = timer()
                        exact_match_times.append((end_exact_match - start_exact_match))
                    except Exception as e:
                        logger.error(traceback.format_exc())
                        logger.error('error :: inference :: could not determine it any exact matches could be found in fp id %s timeseries - %s' % (
                            str(fp_id), e))
                    logger.info('inference :: exact matches checked in %.6f seconds' % (
                        (end_exact_match - start_exact_match)))

                # TODO
                # mass3 ALL, then evaluate, would it be quicker?  No see POC
                # above
        logger.info('inference :: mts.mass2_batch runs on %s fps of full_duration %s in %.6f seconds' % (
            str(len(mass2_batch_times)), str(full_duration), sum(mass2_batch_times)))
        logger.info('inference :: exact_match runs on %s fps of full_duration %s in %.6f seconds' % (
            str(len(exact_match_times)), str(full_duration), sum(exact_match_times)))
        end_full_duration = timer()
        logger.info('inference :: analysed %s fps of full_duration %s in %.6f seconds' % (
            str(len(set(full_duration_fp_ids))), str(full_duration),
            (end_full_duration - start_full_duration)))

    # Patterns are sorted by distance
    # The list produced with the mass3 method will include
    # nans
    start_distance_valid_motifs = timer()
    distance_valid_motifs = [item for item in motifs_found if not np.isnan(item[2]) and item[2] <= item[5]]
    end_distance_valid_motifs = timer()
    logger.info('inference :: %s distance_valid_motifs determined in %.6f seconds from %s motifs_found' % (
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
    logger.info('inference :: sorted_motifs from distance_valid_motifs in %.6f seconds' % (
        (end_sorted_motifs - start_sorted_motifs)))

    percent_different_removed = 0
    not_in_range_removed = 0
    start_motifs_check = timer()
    for motif in sorted_motifs:
        try:

            add_match = False
            all_in_range = False

            current_fp_id = motif[0]
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

            if motif in exact_matches_found:
                add_match = True
                match_type = 'exact'
                all_in_range = True
                if debug_logging:
                    logger.debug('debug :: inference :: exact match: %s' % (str(motif)))

            full_relate_timeseries = fps_timeseries[current_fp_id]
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
                            logger.debug('debug :: inference :: all_in_range: related_max_y: %s less than (max_y - range_padding): (%s - %s) = %s' % (
                                str(max_relate_dataset), str(max_y), str(range_padding),
                                str((max_y - range_padding))))
                    if min_relate_dataset > (min_y + range_padding):
                        all_in_range = False
                        if debug_logging:
                            logger.debug('debug :: inference :: all_in_range: related_min_y: %s greater than (min_y + range_padding): (%s + %s) = %s' % (
                                str(min_relate_dataset), str(min_y), str(range_padding),
                                str((min_y + range_padding))))
                if all_in_range:
                    # logger.info('inference :: ALL IN RANGE - all_in_range: %s, distance: %s' % (str(all_in_range), str(best_dist)))
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
                        logger.error('error :: inference :: failed to get motif_area with np.trapz - %s' % (
                            e))
                    try:
                        y_fp_motif = np.array(relate_dataset)
                        # fp_motif_area = np.trapz(y_fp_motif, dx=dx)
                        fp_motif_area = np.trapz(y_fp_motif, dx=1)
                    except Exception as e:
                        logger.error('error :: inference :: failed to get fp_motif_area with np.trapz - %s' % (
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
                        # logger.info('inference :: all_in_range match removed area_percent_diff: %s' % (str(percent_different)))
                        # BUT ...
                        # @modified 20210504 - Bug #4044: inference - motif distance override - exact match
                        # Do not add as similar just based on distance
                        # if motif_max_distance > 10:
                        #     if best_dist < 3 and not add_match:
                        #         logger.info('inference :: DISTANCE VERY SIMILAR - adding match even though area_percent_diff is greater than max_area_percent_diff because best_dist: %s' % (
                        #             str(best_dist)))
                        #         add_match = True
                        #         percent_different_removed -= 1
                        #         # match_type = 'distance'
                        # if best_dist < 1 and not add_match:
                        #     logger.info('inference :: DISTANCE VERY SIMILAR - adding match even though area_percent_diff is greater than max_area_percent_diff because best_dist: %s' % (
                        #         str(best_dist)))
                        #     add_match = True
                        #     percent_different_removed -= 1
                        #     # match_type = 'distance'

                    end_percent_different = timer()
                    logger.info('inference :: percent_different in %.6f seconds' % (
                        (end_percent_different - start_percent_different)))

            # @added 20210430 - Bug #4044: inference - motif distance override - exact match
            if compare_percent_different == 0 and best_dist == 0:
                add_match = True
                match_type = 'exact'
                # @added 20210504 - Bug #4044: inference - motif distance override - exact match
                # Handle exact matches here
                exact_matches_found.append(motif)

            generation = 0
            if add_match:
                # @modified 20210426 - Feature #4014: Ionosphere - inference
                # Optimize - remove SELECT generation mysql_select
                # method and use fp_id_rows dict
                # start_select_generation = timer()
                # try:
                #     query = 'SELECT generation FROM ionosphere WHERE id=%s' % (str(fp_id))
                #     results = mysql_select(skyline_app, query)
                #     for result in results:
                #         generation = int(result[0])
                # except Exception as e:
                #     logger.error('error :: inference :: failed to get generation from the database for fp_id %s from ionoshere table - %s' % (
                #         str(fp_id), e))
                # end_select_generation = timer()
                # logger.info('inference :: select_generation in %.6f seconds' % (
                #     (end_select_generation - start_select_generation)))
                try:
                    generation = fp_id_rows[current_fp_id]['generation']
                except Exception as e:
                    logger.error('error :: inference :: failed to get generation from fp_id_rows dict for %s - %s' % (
                        str(current_fp_id), e))

                motif_id = '%s-%s' % (str(current_fp_id), str(best_index))
                matched_motifs[motif_id] = {}
                matched_motifs[motif_id]['metric_id'] = metric_id
                matched_motifs[motif_id]['fp_id'] = current_fp_id
                matched_motifs[motif_id]['index'] = best_index
                matched_motifs[motif_id]['distance'] = best_dist
                matched_motifs[motif_id]['max_distance'] = motif_max_distance
                matched_motifs[motif_id]['size'] = motif_size
                matched_motifs[motif_id]['timestamp'] = timestamp
                matched_motifs[motif_id]['type'] = match_type
                matched_motifs[motif_id]['type_id'] = motif_match_types[match_type]
                # @added 20210414 - Feature #4014: Ionosphere - inference
                #                   Branch #3590: inference
                # Store the not anomalous motifs
                matched_motifs[motif_id]['motif_sequence'] = motif_sequence
                matched_motifs[motif_id]['full_duration'] = full_duration
                matched_motifs[motif_id]['generation'] = generation
                matched_motifs[motif_id]['fp_motif_sequence'] = relate_timeseries
                # @added 20210423 - Feature #4014: Ionosphere - inference
                # Compute the area using the composite trapezoidal rule.
                matched_motifs[motif_id]['motif_area'] = motif_area
                matched_motifs[motif_id]['fp_motif_area'] = fp_motif_area
                # @added 20210424 - Feature #4014: Ionosphere - inference
                matched_motifs[motif_id]['area_percent_diff'] = percent_different
                matched_motifs[motif_id]['max_area_percent_diff'] = motif_max_area_percent_diff
                # @added 20210428 - Feature #4014: Ionosphere - inference
                # Add time taken and fps checked
                matched_motifs[motif_id]['fps_checked'] = len(list(set(fps_checked_for_motifs)))
                runtime_end = timer()
                matched_motifs[motif_id]['runtime'] = (runtime_end - start)

                if SINGLE_MATCH:
                    break
        except Exception as e:
            logger.error(traceback.format_exc())
            logger.error('error :: inference :: fp id %s and motif: %s - %s' % (
                str(fp_id), str(motif), str(e)))
            continue
    end_motifs_check = timer()
    logger.info('inference :: motifs checked in %.6f seconds' % (
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
        logger.info('inference :: sorting %s matched_motifs by distance and area_percent_diff' % (
            str(len(sorted_ordered_matched_motifs_list))))
        if sorted_ordered_matched_motifs_list:
            inference_debug_file = '%s/%s.%s.fp_id.%s.inference.sorted_ordered_matched_motifs.list' % (
                metric_timeseries_dir, str(timestamp), metric, str(fp_id))
            if not os.path.isfile(inference_debug_file):
                try:
                    write_data_to_file(skyline_app, inference_debug_file, 'w', str(sorted_ordered_matched_motifs_list))
                    logger.info('inference :: added inference.sorted_ordered_matched_motifs list file - %s' % (
                        inference_debug_file))
                except Exception as e:
                    logger.info(traceback.format_exc())
                    logger.error('error :: file to create inference_debug_file - %s - %s' % (
                        inference_debug_file, e))

        for motif_id, distance, area_percent_diff in sorted_ordered_matched_motifs_list:
            sorted_matched_motifs[motif_id] = matched_motifs[motif_id]
            if SINGLE_MATCH:
                break
        matched_motifs = sorted_matched_motifs.copy()

    if matched_motifs:
        inference_file = '%s/%s.%s.inference.matched_motifs.dict' % (
            metric_timeseries_dir, str(timestamp), metric)
        if not os.path.isfile(inference_file):
            try:
                write_data_to_file(skyline_app, inference_file, 'w', str(matched_motifs))
                logger.info('inference :: added inference.matched_motifs dict file - %s' % (
                    inference_file))
            except Exception as e:
                logger.info(traceback.format_exc())
                logger.error('error :: file to create inference_file - %s - %s' % (
                    inference_file, e))

    # @added 20210423 - Feature #4014: Ionosphere - inference
    # Since implementing the analyse in loop method of every batch size per fp
    # the motifs_checked_count double increased as the fp_id is added each
    # batch_size, only return uniques to ionosphere
    unique_fps_checked_for_motifs = list(set(fps_checked_for_motifs))

    end = timer()
    if dev_null:
        del dev_null
    logger.info('inference :: %s motif best match found from %s motifs_found, %s fps where checked from %s (motifs removed due to not_in_range %s, percent_different %s) and it took a total of %.6f seconds (all mass2/mass3) to process %s' % (
        # str(len(matched_motifs)), str(len(motifs_found)), str(len(fps_checked_for_motifs)),
        str(len(matched_motifs)), str(len(motifs_found)), str(len(unique_fps_checked_for_motifs)),
        str(full_duration_fp_count), str(not_in_range_removed),
        str(percent_different_removed), (end - start), metric))
    # return matched_motifs, fps_checked_for_motifs
    return matched_motifs, unique_fps_checked_for_motifs
