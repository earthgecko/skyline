"""
downsample_features_profile.py
"""
import logging
import traceback

from functions.database.queries.fp_timeseries import get_db_fp_timeseries


# @added 20220915 -
def downsample_features_profile(current_skyline_app, fp_id):
    """
    Return the motif of fp_timeseries for the motif_match_id, e.g.

    :param current_skyline_app: the app calling the function
    :param match_id: the Ionosphere match id
    :param layers_match_id: the Ionosphere layers match id
    :type current_skyline_app: str
    :type match_id: int
    :type layer_match_id: int
    :return: dictionary of timeseries
    :rtype: dict

    """

    function_str = 'functions.ionosphere.downsample_features_profile'
    current_skyline_app_logger = current_skyline_app + 'Log'
    current_logger = logging.getLogger(current_skyline_app_logger)

    current_logger.info('%s :: getting timeseries for fp: %s' % (
        function_str, str(fp_id)))

    return
