"""
get_fp_motif.py
"""
import logging
import traceback

from functions.database.queries.get_motifs_matched_row import get_motifs_matched_row
from functions.database.queries.fp_timeseries import get_db_fp_timeseries


# @added 20220317 -
def get_fp_motif(
        current_skyline_app, motifs_matched_id, motifs_matched_row,
        fp_timeseries):
    """
    Return the motif of fp_timeseries for the motif_match_id, e.g.

    fp_motif = {
        'fp_id': fp_id,
        'metric_id': metric_id,
        'motifs_matched_id': motifs_matched_id,
        'fp_motif': [[ts, value], [ts, value], ..., [ts, value]],
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

    function_str = 'functions.ionosphere.get_fp_motif'
    current_skyline_app_logger = current_skyline_app + 'Log'
    current_logger = logging.getLogger(current_skyline_app_logger)

    current_logger.info('%s :: getting fp_motif for motifs_match_id: %s' % (
        function_str, str(motifs_matched_id)))

    fp_motif = {}
    if not motifs_matched_row:
        try:
            motifs_matched_row = get_motifs_matched_row(current_skyline_app, motifs_matched_id)
        except Exception as err:
            current_logger.error(traceback.format_exc())
            current_logger.error('error :: %s :: get_motifs_matched_row failed for motifs_matched id %s - %s' % (
                function_str, str(motifs_matched_id), err))
            return fp_motif

    try:
        fp_id = motifs_matched_row['fp_id']
        metric_id = motifs_matched_row['metric_id']
        fp_motif['fp_id'] = fp_id
        fp_motif['metric_id'] = metric_id
        fp_motif['motifs_matched_id'] = motifs_matched_id
    except Exception as err:
        current_logger.error(traceback.format_exc())
        current_logger.error('error :: %s :: failed build fp_motif dict for motif_matched id %s - %s' % (
            function_str, str(motifs_matched_id), err))
        return fp_motif

    if not fp_timeseries:
        fp_timeseries = []
        try:
            fp_timeseries = get_db_fp_timeseries(current_skyline_app, metric_id, fp_id)
        except Exception as err:
            current_logger.error(traceback.format_exc())
            current_logger.error('error :: %s :: get_db_fp_timeseries failed for fp id %s - %s' % (
                function_str, str(fp_id), err))
    if not fp_timeseries:
        current_logger.error('error :: %s :: no timeseries for fp id %s - %s' % (
            function_str, str(fp_id), err))
        return fp_motif

    try:
        best_index = motifs_matched_row['index']
        size = motifs_matched_row['size']
        # @modified 20220408 - Feature #4014: Ionosphere - inference
        # Use same method as inference.py
        # This is due to the fact that index:(index + size) does not always
        # capture the entire motif due to there perhaps being more a then one
        # data point in a period in relation to echo/full_duration data, the
        # method used inference.py does.
        # fp_motif_ts = fp_timeseries[index:(index + size)]
        # @modified 20220517 - Feature #4014: Ionosphere - inference
        # However in inference.py the Create current_best_indices as mass2_batch returns
        # shifts the index to begin at the best_index for mass3...
        if size < 90:
            # @modified 20220526 - Bug #4588: Ionosphere - inference - further validate all_in_range
            # The change to a simplified chained comparison between the operands
            # did not have the desired result.  The index to end of timeseries
            # was being selected rather than index to index+size.  This resulted
            # in the fp_motifs being incorrect and having incorrect areas, etc.
            # Reverted back to the original method, occassionally pylint is not
            # useful.
            # fp_motif_ts = [item for index, item in enumerate(fp_timeseries) if index >= best_index and index < (best_index + size)]
            # fp_motif_ts = [item for index, item in enumerate(fp_timeseries) if index >= best_index < (best_index + size)]
            fp_motif_ts = [item for index, item in enumerate(fp_timeseries) if index >= best_index and index < (best_index + size)]
        else:
            # @modified 20220526 - Bug #4588: Ionosphere - inference - further validate all_in_range
            # The change to a simplified chained comparison between the operands
            # did not have the desired result.  The index to end of timeseries
            # was being selected rather than index to index+size.  This resulted
            # in the fp_motifs being incorrect and having incorrect areas, etc.
            # Reverted back to the original method, occassionally pylint is not
            # useful.
            # fp_motif_ts = [item for index, item in enumerate(fp_timeseries) if index >= (best_index - size) and index <= best_index]
            # fp_motif_ts = [item for index, item in enumerate(fp_timeseries) if index >= (best_index - size) <= best_index]
            fp_motif_ts = [item for index, item in enumerate(fp_timeseries) if index >= (best_index - size) and index <= best_index]

        # @added 20220718 - Feature #4014: Ionosphere - inference
        # Handle the index being 0 and the size greater that 90
        if best_index == 0:
            fp_motif_ts = [item for index, item in enumerate(fp_timeseries) if index >= best_index and index < (best_index + size)]

        if len(fp_motif_ts) > size:
            fp_motif_ts = fp_motif_ts[-size:]

        # @modified 20220408 - Feature #4014: Ionosphere - inference
        # last_fp_timeseries_index = len(fp_timeseries)
        # if last_fp_timeseries_index < (index + size):
        #     current_logger.info('%s :: adjusting index for fp_motif sequence because (index (%s) + size (%s)) (%s) > last_fp_timeseries_index (%s)' % (
        #         function_str, str(index), str(size), str(index + size),
        #         str(last_fp_timeseries_index)))
        #     index_diff = (index + size) - last_fp_timeseries_index
        #     use_index = index - index_diff
        #     fp_motif_ts = fp_timeseries[use_index:last_fp_timeseries_index]
    except Exception as err:
        current_logger.error(traceback.format_exc())
        current_logger.error('error :: %s :: failed build fp_motif for motifs_matched id %s from fp_timeseries - %s' % (
            function_str, str(motifs_matched_id), err))
    fp_motif['fp_motif_timeseries'] = fp_motif_ts

    return fp_motif
