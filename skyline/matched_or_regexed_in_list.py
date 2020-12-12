"""
matched_or_regexed_in_list
"""
import logging
import traceback
import re


# @added 20200423 - Feature #3512: matched_or_regexed_in_list function
#                   Feature #3508: ionosphere_untrainable_metrics
#                   Feature #3486: analyzer_batch
# @added 20201212 - Feature #3880: webapp - utilities - match_metric
# Added debug_log parameter for allowing for debug logging to be turned on in
# webapp utilities requests
def matched_or_regexed_in_list(current_skyline_app, base_name, match_list, debug_log=False):
    """
    Determine if a pattern is in a list as a:
    1) absolute match
    2) match been dotted elements
    3) matched by a regex

    :param current_skyline_app: the app calling the function so the function
        knows which log to write too.
    :param metric_name: the metric name
    :param match_list: the list of items to try and match the metric name in
    :type current_skyline_app: str
    :type metric_name: str
    :type match_list: list
    :return: False, {}
    :rtype:  (boolean, dict)

    Returns (matched, matched_by)

    """
    debug_matched_or_regexed_in_list = None
    if debug_log:
        debug_matched_or_regexed_in_list = True

    if debug_matched_or_regexed_in_list:
        current_skyline_app_logger = str(current_skyline_app) + 'Log'
        current_logger = logging.getLogger(current_skyline_app_logger)
    else:
        current_logger = None

    matched = False

    # @added 20200425 - Feature #3512: matched_or_regexed_in_list function
    # Return a matched by dict
    absolute_match = False
    matched_in_namespace = False
    matched_namespace = None
    matched_in_elements = False
    matched_in_namespace_elements = None
    matched_by_regex = False
    matched_regex = None

    # @added 20200506 - Feature #3512: matched_or_regexed_in_list function
    #                   Feature #3510: Enable Luminosity to handle correlating namespaces only
    # Add what namespace pattern was matched
    matched_namespace = None

    matched_by = {}

    try:
        base_name_namespace_elements = base_name.split('.')
        for match_namespace in match_list:

            # @added 20200425 - Feature #3512: matched_or_regexed_in_list function
            # Added absolute_match
            if base_name == match_namespace:
                matched = True
                absolute_match = True
                if debug_matched_or_regexed_in_list:
                    current_logger.info('%s - matched_or_regexed_in_list - absolute_match - %s matched by being in %s' % (
                        current_skyline_app, base_name, match_namespace))
                break

            if match_namespace in base_name:
                matched = True
                # @added 20200425 - Feature #3512: matched_or_regexed_in_list function
                matched_in_namespace = True
                matched_namespace = match_namespace

                if debug_matched_or_regexed_in_list:
                    current_logger.info('%s - matched_or_regexed_in_list - namespace - %s matched by being in %s' % (
                        current_skyline_app, base_name, match_namespace))
                break

            match_namespace_namespace_elements = match_namespace.split('.')
            elements_matched = set(base_name_namespace_elements) & set(match_namespace_namespace_elements)
            if len(elements_matched) == len(match_namespace_namespace_elements):
                matched = True
                # @added 20200425 - Feature #3512: matched_or_regexed_in_list function
                matched_in_elements = True
                matched_in_namespace_elements = set(match_namespace_namespace_elements)

                if debug_matched_or_regexed_in_list:
                    current_logger.info('%s - matched_or_regexed_in_list - namespace - %s matched by elements of %s' % (
                        current_skyline_app, base_name, match_namespace))
                break
            if not matched:
                try:
                    namespace_match_pattern = re.compile(match_namespace)
                    pattern_match = namespace_match_pattern.match(base_name)
                    if pattern_match:
                        matched = True
                        # @added 20200425 - Feature #3512: matched_or_regexed_in_list function
                        matched_by_regex = True
                        matched_regex = match_namespace
                        if debug_matched_or_regexed_in_list:
                            current_logger.info('%s - matched_or_regexed_in_list - namespace - %s matched by regex of %s' % (
                                current_skyline_app, base_name, match_namespace))
                except:
                    matched = False

        try:
            del base_name_namespace_elements
        except:
            pass
        try:
            del match_namespace_namespace_elements
        except:
            pass
        try:
            del elements_matched
        except:
            pass

        # @added 20200425 - Feature #3512: matched_or_regexed_in_list function
        # Return the matched_by dict
        matched_by['absolute_match'] = absolute_match
        matched_by['matched_in_namespace'] = matched_in_namespace
        matched_by['matched_namespace'] = matched_namespace
        matched_by['matched_in_elements'] = matched_in_elements
        matched_by['matched_in_namespace_elements'] = matched_in_namespace_elements
        matched_by['matched_by_regex'] = matched_by_regex
        matched_by['matched_regex'] = matched_regex

        # @added 20200506 - Feature #3512: matched_or_regexed_in_list function
        #                   Feature #3510: Enable Luminosity to handle correlating namespaces only
        # Add what namespace pattern was matched
        if matched:
            matched_namespace = match_namespace
        matched_by['matched_namespace'] = matched_namespace

    except:
        if not current_logger:
            current_skyline_app_logger = str(current_skyline_app) + 'Log'
            current_logger = logging.getLogger(current_skyline_app_logger)
        current_logger.error(traceback.format_exc())
        current_logger.error('error :: %s :: matched_or_regexed_in_list errored' % current_skyline_app)

    return (matched, matched_by)
