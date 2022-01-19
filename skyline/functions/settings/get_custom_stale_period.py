import logging
import traceback

import settings
from matched_or_regexed_in_list import matched_or_regexed_in_list

LOCAL_DEBUG = False


# @added 20210518 - Feature #4076: CUSTOM_STALE_PERIOD
#                   Branch #1444: thunder
def custom_stale_period(
        current_skyline_app, base_name, external_settings={}, log=False):
    """
    Determine if a metric has a custom stale period defined in
    :mod:`settings.CUSTOM_STALE_PERIOD`.

    :param current_skyline_app: the app calling the function
    :param base_name: the metric base_name
    :param external_settings: the external_settings dict
    :param log: whether to log or not, optional, defaults to False
    :type current_skyline_app: str
    :type base_name: str
    :type external_settings: dict
    :type log: boolean
    :return: stale_period
    :rtype: int

    """

    function_str = 'functions.settings.custom_stale_period'
    if log:
        current_skyline_app_logger = current_skyline_app + 'Log'
        current_logger = logging.getLogger(current_skyline_app_logger)
    else:
        current_logger = None

    try:
        stale_period = int(settings.STALE_PERIOD)
    except AttributeError:
        stale_period = 500
    except TypeError:
        stale_period = 500
    except Exception as e:
        if not log:
            current_skyline_app_logger = current_skyline_app + 'Log'
            current_logger = logging.getLogger(current_skyline_app_logger)
        current_logger.error('error :: %s :: failed to parse STALE_PERIOD from settings - %s' % (
            function_str, e))
        stale_period = 500
    custom_stale_period_int = int(stale_period)

    custom_stale_period_dict = {}
    try:
        custom_stale_period_dict = settings.CUSTOM_STALE_PERIOD.copy()
    except AttributeError:
        stale_period = 500
    except Exception as e:
        if not log:
            current_skyline_app_logger = current_skyline_app + 'Log'
            current_logger = logging.getLogger(current_skyline_app_logger)
        current_logger.error(traceback.format_exc())
        current_logger.error('error :: %s :: failed to parse CUSTOM_STALE_PERIOD from settings - %s' % (
            function_str, e))
        stale_period = 500

    # @added 20210602 - Feature #4000: EXTERNAL_SETTINGS
    # Add any custom stale periods for any namespaces declared in external
    # settings
    if external_settings:
        for config_id in list(external_settings.keys()):
            try:
                alert_on_stale_metrics = False
                try:
                    namespace = external_settings[config_id]['namespace']
                    alert_on_stale_metrics = external_settings[config_id]['alert_on_stale_metrics']['enabled']
                except KeyError:
                    alert_on_stale_metrics = False
                if alert_on_stale_metrics:
                    use_custom_stale_period = None
                    try:
                        use_custom_stale_period = int(float(external_settings[config_id]['alert_on_stale_metrics']['stale_period']))
                    except KeyError:
                        use_custom_stale_period = None
                    if use_custom_stale_period:
                        custom_stale_period_dict[namespace] = use_custom_stale_period
            except Exception as e:
                if not log:
                    current_skyline_app_logger = current_skyline_app + 'Log'
                    current_logger = logging.getLogger(current_skyline_app_logger)
                current_logger.error(traceback.format_exc())
                current_logger.error('error :: %s :: failed to parse external_settings[\'%s\'] - %s' % (
                    function_str, config_id, e))

    if not custom_stale_period_dict:
        return stale_period

    custom_stale_period_namespaces = []
    try:
        custom_stale_period_namespaces = list(custom_stale_period_dict.keys())
    except Exception as e:
        if not log:
            current_skyline_app_logger = current_skyline_app + 'Log'
            current_logger = logging.getLogger(current_skyline_app_logger)
        current_logger.error(traceback.format_exc())
        current_logger.error('error :: %s :: failed to parse CUSTOM_STALE_PERIOD from settings - %s' % (
            function_str, e))
        return stale_period

    if not custom_stale_period_namespaces:
        return stale_period

    # @added 20210620 - Branch #1444: thunder
    #                   Feature #4076: CUSTOM_STALE_PERIOD
    # Handle multi level namespaces
    # Sort the list by the namespaces with the most elements to the least as
    # first match wins
    custom_stale_period_namespaces_elements_list = []
    for custom_stale_period_namespace in custom_stale_period_namespaces:
        namespace_elements = len(custom_stale_period_namespace.split('.'))
        custom_stale_period_namespaces_elements_list.append([custom_stale_period_namespace, namespace_elements])
    sorted_custom_stale_period_namespaces = sorted(custom_stale_period_namespaces_elements_list, key=lambda x: (x[1]), reverse=True)
    if sorted_custom_stale_period_namespaces:
        custom_stale_period_namespaces = [x[0] for x in sorted_custom_stale_period_namespaces]

    pattern_match, matched_by = matched_or_regexed_in_list(current_skyline_app, base_name, custom_stale_period_namespaces, False)
    if pattern_match:
        try:
            matched_namespace = matched_by['matched_namespace']
            custom_stale_period_int = int(float(custom_stale_period_dict[matched_namespace]))
            if log:
                if current_skyline_app == 'analyzer':
                    current_logger.info('metrics_manager :: %s :: custom_stale_period found for %s: %s' % (
                        function_str, base_name, str(custom_stale_period)))
                else:
                    current_logger.info('%s :: custom_stale_period found for %s: %s' % (
                        function_str, base_name, str(custom_stale_period)))
        except ValueError:
            custom_stale_period_int = stale_period
        except Exception as e:
            if not log:
                current_skyline_app_logger = current_skyline_app + 'Log'
                current_logger = logging.getLogger(current_skyline_app_logger)
            current_logger.error(traceback.format_exc())
            current_logger.error('error :: %s :: failed to parse CUSTOM_STALE_PERIOD from settings - %s' % (
                function_str, e))
            custom_stale_period_int = stale_period

    return custom_stale_period_int
