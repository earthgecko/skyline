import logging
import traceback


# @added 20220128 - Feature #4404: flux - external_settings - aggregation
def external_settings_aggregation(
        current_skyline_app, external_settings, log=False):
    """
    Determine the namespace aggregations defined in external_settings

    :param current_skyline_app: the app calling the function
    :param external_settings: the external_settings dict
    :param log: whether to log or not, optional, defaults to False
    :type current_skyline_app: str
    :type external_settings: dict
    :type log: boolean
    :return: external_settings_aggregations
    :rtype: dict

    """

    function_str = 'functions.settings.external_settings_aggregation'
    if log:
        current_skyline_app_logger = current_skyline_app + 'Log'
        current_logger = logging.getLogger(current_skyline_app_logger)
    else:
        current_logger = None

    external_settings_aggregations = {}

    for config_id in list(external_settings.keys()):
        try:
            aggregate = None
            try:
                aggregate = external_settings[config_id]['aggregate']
            except KeyError:
                aggregate = None
            if not aggregate:
                continue
            for namespace_key in list(external_settings[config_id]['aggregate'].keys()):
                external_settings_aggregations[namespace_key] = external_settings[config_id]['aggregate'][namespace_key]
        except Exception as e:
            if not log:
                current_skyline_app_logger = current_skyline_app + 'Log'
                current_logger = logging.getLogger(current_skyline_app_logger)
            current_logger.error(traceback.format_exc())
            current_logger.error('error :: %s :: failed to parse external_settings[\'%s\'] for aggregate - %s' % (
                function_str, config_id, e))
    if log:
        current_logger.info('%s :: %s namespaces found to aggregate in external_settings' % (
            function_str, str(len(external_settings_aggregations))))
    return external_settings_aggregations
