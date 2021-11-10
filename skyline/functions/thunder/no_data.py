import logging
import traceback
from time import time
from ast import literal_eval

import settings
from skyline_functions import get_redis_conn_decoded
from functions.settings.get_custom_stale_period import custom_stale_period
from functions.settings.get_external_settings import get_external_settings
from functions.thunder.send_event import thunder_send_event


# @added 20210603 - Branch #1444: thunder
def thunder_no_data(current_skyline_app, log=True):
    """
    Determine when a top level namespace stops receiving data.

    :param current_skyline_app: the app calling the function
    :param log: whether to log or not, optional, defaults to True
    :type current_skyline_app: str
    :type log: boolean
    :return: namespaces_no_data_dict
    :rtype: dict

    """
    namespaces_no_data_dict = {}
    function_str = 'metrics_manager :: functions.thunder.no_data.thunder_no_data'
    if log:
        current_skyline_app_logger = current_skyline_app + 'Log'
        current_logger = logging.getLogger(current_skyline_app_logger)
    else:
        current_logger = None

    now = int(time())

    try:
        redis_conn_decoded = get_redis_conn_decoded(current_skyline_app)
    except Exception as e:
        if not log:
            current_skyline_app_logger = current_skyline_app + 'Log'
            current_logger = logging.getLogger(current_skyline_app_logger)
        current_logger.error(traceback.format_exc())
        current_logger.error('error :: %s :: failed to get Redis connection - %s' % (
            function_str, e))
        return namespaces_no_data_dict

    metrics_last_timestamp_dict = {}
    hash_key = 'analyzer.metrics.last_timeseries_timestamp'
    try:
        metrics_last_timestamp_dict = redis_conn_decoded.hgetall(hash_key)
    except Exception as e:
        if not log:
            current_skyline_app_logger = current_skyline_app + 'Log'
            current_logger = logging.getLogger(current_skyline_app_logger)
        current_logger.error(traceback.format_exc())
        current_logger.error('error :: %s :: failed to get Redis hash key %s - %s' % (
            function_str, hash_key, e))

    if not metrics_last_timestamp_dict:
        return namespaces_no_data_dict

    # @added 20210720 - Branch #1444: thunder
    # Remove any entries that have been specified
    remove_namespace = None
    remove_namespace_key = 'webapp.thunder.remove.namespace.metrics.last_timeseries_timestamp'
    try:
        remove_namespace = redis_conn_decoded.get(remove_namespace_key)
    except Exception as e:
        if not log:
            current_skyline_app_logger = current_skyline_app + 'Log'
            current_logger = logging.getLogger(current_skyline_app_logger)
        current_logger.error(traceback.format_exc())
        current_logger.error('error :: %s :: failed to get Redis key %s - %s' % (
            function_str, remove_namespace_key, e))
    if remove_namespace:
        if log:
            current_logger.info('%s :: for removal of no_data on namespace %s requested' % (
                function_str, remove_namespace))
        try:
            redis_conn_decoded.delete(remove_namespace_key)
        except Exception as e:
            if not log:
                current_skyline_app_logger = current_skyline_app + 'Log'
                current_logger = logging.getLogger(current_skyline_app_logger)
            current_logger.error(traceback.format_exc())
            current_logger.error('error :: %s :: failed to delete Redis key %s - %s' % (
                function_str, remove_namespace_key, e))
        metrics_removed_from_key = []
        for base_name in list(metrics_last_timestamp_dict.keys()):
            if base_name.startswith(remove_namespace):
                try:
                    redis_conn_decoded.hdel(hash_key, base_name)
                    metrics_removed_from_key.append(base_name)
                except Exception as e:
                    if not log:
                        current_skyline_app_logger = current_skyline_app + 'Log'
                        current_logger = logging.getLogger(current_skyline_app_logger)
                    current_logger.error(traceback.format_exc())
                    current_logger.error('error :: %s :: failed to delete %s from Redis hash key %s - %s' % (
                        function_str, base_name, hash_key, e))
        if log:
            current_logger.info('%s :: %s metrics for namesapce %s removed from Redis hash key %s' % (
                function_str, len(metrics_removed_from_key), remove_namespace,
                hash_key))
        if len(metrics_removed_from_key) > 0:
            level = 'notice'
            event_type = 'no_data'
            message = '%s - %s - was removed from the no_data check' % (
                level, remove_namespace)
            status = 'removed'
            thunder_event = {
                'level': level,
                'event_type': event_type,
                'message': message,
                'app': current_skyline_app,
                'metric': None,
                'source': current_skyline_app,
                'timestamp': time(),
                'expiry': 59,
                'data': {
                    'namespace': remove_namespace,
                    'last_timestamp': 0,
                    'recovered_after_seconds': 0,
                    'total_recent_metrics': 0,
                    'total_stale_metrics': 0,
                    'total_metrics': len(metrics_removed_from_key),
                    'status': status,
                },
            }
            submitted = False
            try:
                submitted = thunder_send_event(current_skyline_app, thunder_event, log=True)
            except Exception as e:
                if not log:
                    current_skyline_app_logger = current_skyline_app + 'Log'
                    current_logger = logging.getLogger(current_skyline_app_logger)
                current_logger.error('error :: %s :: error encountered with thunder_send_event - %s' % (
                    function_str, e))
            if submitted:
                if log:
                    current_logger.info('%s :: send thunder event for removal of no_data on namespace %s' % (
                        function_str, remove_namespace))
            # Update with the new data
            metrics_last_timestamp_dict = {}
            try:
                metrics_last_timestamp_dict = redis_conn_decoded.hgetall(hash_key)
            except Exception as e:
                if not log:
                    current_skyline_app_logger = current_skyline_app + 'Log'
                    current_logger = logging.getLogger(current_skyline_app_logger)
                current_logger.error(traceback.format_exc())
                current_logger.error('error :: %s :: failed to get Redis hash key %s - %s' % (
                    function_str, hash_key, e))

    metrics_last_timestamps = []
    parent_namespaces = []
    unique_base_names = list(metrics_last_timestamp_dict.keys())
    last_traceback = None
    last_error = None
    error_count = 0
    for base_name in unique_base_names:
        try:
            parent_namespace = base_name.split('.')[0]
            metrics_last_timestamps.append([base_name, int(metrics_last_timestamp_dict[base_name])])
            if len(parent_namespace) > 0:
                parent_namespaces.append(parent_namespace)
        except Exception as e:
            last_traceback = traceback.format_exc()
            last_error = e
            error_count += 1
    if last_error:
        if not log:
            current_skyline_app_logger = current_skyline_app + 'Log'
            current_logger = logging.getLogger(current_skyline_app_logger)
        current_logger.error('error :: %s :: errors %s encounterd while creating metrics_last_timestamps, last reported error - %s' % (
            function_str, str(error_count), last_error))
        current_logger.error('error :: %s :: last reported Traceback' % (
            function_str))
        current_logger.error('%s' % (str(last_traceback)))

    external_settings = {}
    try:
        external_settings = get_external_settings(current_skyline_app)
    except Exception as e:
        if not log:
            current_skyline_app_logger = current_skyline_app + 'Log'
            current_logger = logging.getLogger(current_skyline_app_logger)
        current_logger.error(traceback.format_exc())
        current_logger.error('error :: %s :: get_external_settings failed - %s' % (
            function_str, e))
    external_parent_namespaces_stale_periods = {}
    if external_settings:
        for config_id in list(external_settings.keys()):
            no_data_stale_period = None
            try:
                alert_on_no_data = external_settings[config_id]['alert_on_no_data']['enabled']
            except KeyError:
                alert_on_no_data = False
            if alert_on_no_data:
                try:
                    no_data_stale_period = external_settings[config_id]['alert_on_no_data']['stale_period']
                except KeyError:
                    no_data_stale_period = False
            namespace = None
            if no_data_stale_period:
                try:
                    namespace = external_settings[config_id]['namespace']
                except KeyError:
                    namespace = False
            try:
                expiry = external_settings[config_id]['alert_on_no_data']['expiry']
            except KeyError:
                expiry = 1800
            if namespace and no_data_stale_period and expiry:
                external_parent_namespaces_stale_periods[parent_namespace] = {}
                external_parent_namespaces_stale_periods[parent_namespace]['stale_period'] = int(no_data_stale_period)
                external_parent_namespaces_stale_periods[parent_namespace]['expiry'] = int(expiry)

    external_parent_namespaces = []
    if external_parent_namespaces:
        external_parent_namespaces = list(external_parent_namespaces.keys())

    parent_namespaces = list(set(parent_namespaces))

    # @added 20210620 - Branch #1444: thunder
    #                   Feature #4076: CUSTOM_STALE_PERIOD
    # Handle multi level namespaces
    # Sort the list by the namespaces with the most elements to the least as
    # first match wins
    parent_namespace_metrics_processed = []
    custom_stale_period_namespaces = []
    if settings.CUSTOM_STALE_PERIOD:
        custom_stale_period_namespaces = list(settings.CUSTOM_STALE_PERIOD.keys())
        custom_stale_period_namespaces_elements_list = []
        for custom_stale_period_namespace in custom_stale_period_namespaces:
            namespace_elements = len(custom_stale_period_namespace.split('.'))
            custom_stale_period_namespaces_elements_list.append([custom_stale_period_namespace, namespace_elements])
        sorted_custom_stale_period_namespaces = sorted(custom_stale_period_namespaces_elements_list, key=lambda x: (x[1]), reverse=True)
        if sorted_custom_stale_period_namespaces:
            custom_stale_period_namespaces = [x[0] for x in sorted_custom_stale_period_namespaces]
    # Order by setting priority
    parent_namespaces = external_parent_namespaces + custom_stale_period_namespaces + parent_namespaces

    for parent_namespace in parent_namespaces:
        stale_period = int(settings.STALE_PERIOD)

        # Determine if there is a CUSTOM_STALE_PERIOD for the namespace
        namespace_stale_period = 0
        try:
            # DEBUG
            # namespace_stale_period = custom_stale_period(current_skyline_app, parent_namespace, external_settings, log=False)
            namespace_stale_period = custom_stale_period(current_skyline_app, parent_namespace, external_settings, log=True)
            if namespace_stale_period:
                if namespace_stale_period != stale_period:
                    if log:
                        current_logger.info('%s :: \'%s.\' namespace has custom stale period of %s' % (
                            function_str, parent_namespace,
                            str(namespace_stale_period)))
                    stale_period = int(namespace_stale_period)
        except Exception as e:
            if not log:
                current_skyline_app_logger = current_skyline_app + 'Log'
                current_logger = logging.getLogger(current_skyline_app_logger)
            current_logger.error(traceback.format_exc())
            current_logger.error('error :: failed running custom_stale_period - %s' % e)

        expiry = 1800
        if log:
            current_logger.info('%s :: checking \'%s.\' namespace metrics are receiving data' % (
                function_str, parent_namespace))

        # If there is an external_settings entry for the namespace determine the
        # stale_period and expiry
        if parent_namespace in external_parent_namespaces:
            try:
                stale_period = external_parent_namespaces[parent_namespace]['stale_period']
            except KeyError:
                stale_period = int(settings.STALE_PERIOD)
                if not log:
                    current_skyline_app_logger = current_skyline_app + 'Log'
                    current_logger = logging.getLogger(current_skyline_app_logger)
                current_logger.warn('warning :: %s :: failed to get the stale_period for %s from external_settings, using default' % (
                    function_str, parent_namespace))
            except Exception as e:
                if not log:
                    current_skyline_app_logger = current_skyline_app + 'Log'
                    current_logger = logging.getLogger(current_skyline_app_logger)
                current_logger.error(traceback.format_exc())
                current_logger.error('error :: %s :: failed to get stale_period for %s from external_settings - %s' % (
                    function_str, parent_namespace, e))
            try:
                expiry = external_parent_namespaces[parent_namespace]['expiry']
            except KeyError:
                expiry = 1800
                if not log:
                    current_skyline_app_logger = current_skyline_app + 'Log'
                    current_logger = logging.getLogger(current_skyline_app_logger)
                current_logger.warn('warning :: %s :: failed to get the expiry for %s from external_settings, using default' % (
                    function_str, str(expiry)))
            except Exception as e:
                if not log:
                    current_skyline_app_logger = current_skyline_app + 'Log'
                    current_logger = logging.getLogger(current_skyline_app_logger)
                current_logger.error(traceback.format_exc())
                current_logger.error('error :: %s :: failed to get expiry for %s from external_settings - %s' % (
                    function_str, parent_namespace, e))
            if log:
                current_logger.info('%s :: \'%s.\' namespace using external_setting stale_period of %s and expiry of %s' % (
                    function_str, parent_namespace, str(stale_period),
                    str(expiry)))
        # Allow to test
        thunder_test_alert_key_data = None
        thunder_test_alert_key = 'thunder.test.alert.no_data.%s' % parent_namespace
        try:
            thunder_test_alert_key_data = redis_conn_decoded.get(thunder_test_alert_key)
        except Exception as e:
            if not log:
                current_skyline_app_logger = current_skyline_app + 'Log'
                current_logger = logging.getLogger(current_skyline_app_logger)
            current_logger.error(traceback.format_exc())
            current_logger.error('error :: %s :: failed to get Redis key %s - %s' % (
                function_str, thunder_test_alert_key, e))
        if thunder_test_alert_key_data:
            try:
                thunder_test_data = literal_eval(thunder_test_alert_key_data)
                stale_period = thunder_test_data['stale_period']
                expiry = thunder_test_data['expiry']
                if log:
                    current_logger.info('%s :: THUNDER NO_DATA TEST REQUESTED FOR - \'%s.\' namespace using TEST stale_period of %s and expiry of %s' % (
                        function_str, parent_namespace, str(stale_period),
                        str(expiry)))
            except Exception as e:
                if not log:
                    current_skyline_app_logger = current_skyline_app + 'Log'
                    current_logger = logging.getLogger(current_skyline_app_logger)
                current_logger.error(traceback.format_exc())
                current_logger.error('error :: %s :: failed to get stale_period and expiry for Redis key %s - %s' % (
                    function_str, thunder_test_alert_key, e))

        # metrics that are in the parent namespace
        parent_namespace_metrics = [item for item in metrics_last_timestamps if str(item[0]).startswith(parent_namespace)]

        # @added 20210620 - Branch #1444: thunder
        #                   Feature #4076: CUSTOM_STALE_PERIOD
        # Handle multi level namespaces by filtering out metrics that have
        # already been processed in a longer parent_namespace
        if log:
            current_logger.info('%s :: parent_namespace_metrics has %s metrics' % (
                function_str, str(len(parent_namespace_metrics))))
        parent_namespace_metrics = [item for item in parent_namespace_metrics if item[0] not in parent_namespace_metrics_processed]
        if parent_namespace_metrics:
            parent_namespace_metric_names = [item[0] for item in parent_namespace_metrics]
            parent_namespace_metrics_processed = parent_namespace_metrics_processed + parent_namespace_metric_names
        if log:
            current_logger.info('%s :: parent_namespace_metrics has %s metrics after filetring out processed metrics' % (
                function_str, str(len(parent_namespace_metrics))))

        total_metrics = len(parent_namespace_metrics)
        if parent_namespace_metrics:
            parent_namespace_metrics_timestamps = [int(item[1]) for item in parent_namespace_metrics]
            if parent_namespace_metrics_timestamps:
                parent_namespace_metrics_timestamps.sort()
                most_recent_timestamp = parent_namespace_metrics_timestamps[-1]
                last_received_seconds_ago = now - most_recent_timestamp
                if most_recent_timestamp < (now - stale_period):
                    namespaces_no_data_dict[parent_namespace] = {}
                    namespaces_no_data_dict[parent_namespace]['last_timestamp'] = most_recent_timestamp
                    namespaces_no_data_dict[parent_namespace]['expiry'] = expiry
                    namespaces_no_data_dict[parent_namespace]['total_recent_metrics'] = 0
                    namespaces_no_data_dict[parent_namespace]['total_stale_metrics'] = total_metrics
                    namespaces_no_data_dict[parent_namespace]['total_metrics'] = total_metrics
                    if thunder_test_alert_key_data:
                        namespaces_no_data_dict[parent_namespace]['test'] = True
                    else:
                        namespaces_no_data_dict[parent_namespace]['test'] = False
                    if log:
                        current_logger.warn('warning :: %s :: %s \'%s.\' namespace metrics not receiving data, last data received %s seconds ago' % (
                            function_str, str(total_metrics),
                            parent_namespace, str(last_received_seconds_ago)))
                else:
                    if log:
                        current_logger.info('%s :: \'%s.\' namespace metrics receiving data, last data received %s seconds ago - OK' % (
                            function_str, parent_namespace, str(last_received_seconds_ago)))
                    # Alert recovered if thunder alert key exists
                    thunder_alert_key_exists = False
                    thunder_alert_key = 'thunder.alert.no_data.%s' % parent_namespace
                    try:
                        thunder_alert_key_exists = redis_conn_decoded.get(thunder_alert_key)
                    except Exception as e:
                        if not log:
                            current_skyline_app_logger = current_skyline_app + 'Log'
                            current_logger = logging.getLogger(current_skyline_app_logger)
                        current_logger.error(traceback.format_exc())
                        current_logger.error('error :: %s :: failed to get Redis key %s - %s' % (
                            function_str, thunder_alert_key, e))
                    if thunder_alert_key_exists:
                        seconds_no_data = int(float(most_recent_timestamp)) - int(float(thunder_alert_key_exists))
                        stale_timestamps = [ts for ts in parent_namespace_metrics_timestamps if int(ts) < (now - stale_period)]
                        total_stale_metrics = len(stale_timestamps)
                        total_recent_metrics = total_metrics - total_stale_metrics
                        parent_namespace_metrics
                        if log:
                            current_logger.info('%s :: recovery of no_data on namespace %s after %s seconds, with total_recent_metrics: %s, total_stale_metrics: %s, total_metrics: %s' % (
                                function_str, parent_namespace,
                                str(seconds_no_data), str(total_recent_metrics),
                                str(total_stale_metrics), str(total_metrics)))
                        level = 'notice'
                        event_type = 'no_data'
                        message = '%s - %s - metric data being received recovered after %s seconds' % (
                            level, parent_namespace, str(seconds_no_data))
                        status = 'recovered'
                        thunder_event = {
                            'level': level,
                            'event_type': event_type,
                            'message': message,
                            'app': current_skyline_app,
                            'metric': None,
                            'source': current_skyline_app,
                            'timestamp': time(),
                            'expiry': 59,
                            'data': {
                                'namespace': parent_namespace,
                                'last_timestamp': most_recent_timestamp,
                                'recovered_after_seconds': seconds_no_data,
                                'total_recent_metrics': total_recent_metrics,
                                'total_stale_metrics': total_stale_metrics,
                                'total_metrics': total_metrics,
                                'status': status,
                            },
                        }
                        submitted = False
                        try:
                            submitted = thunder_send_event(current_skyline_app, thunder_event, log=True)
                        except Exception as e:
                            if not log:
                                current_skyline_app_logger = current_skyline_app + 'Log'
                                current_logger = logging.getLogger(current_skyline_app_logger)
                            current_logger.error('error :: %s :: error encounterd with thunder_send_event - %s' % (
                                function_str, e))
                        if submitted:
                            if log:
                                current_logger.info('%s :: send thunder event for recovery of no_data on namespace %s after %s seconds' % (
                                    function_str, parent_namespace,
                                    str(seconds_no_data)))

    if namespaces_no_data_dict:
        parent_namespaces = list(namespaces_no_data_dict.keys())
        for parent_namespace in parent_namespaces:
            level = 'alert'
            event_type = 'no_data'
            message = '%s - %s - no metric data being received' % (
                level, parent_namespace)
            status = 'All metrics stop receiving data'
            thunder_test_alert_key_data = False
            try:
                thunder_test_alert_key_data = namespaces_no_data_dict[parent_namespace]['test']
            except KeyError:
                thunder_test_alert_key_data = False
            if thunder_test_alert_key_data:
                message = '%s - %s - no metric data being received - TEST' % (
                    level, parent_namespace)
                status = 'All metrics stop receiving data - TEST'
            thunder_event = {
                'level': level,
                'event_type': event_type,
                'message': message,
                'app': current_skyline_app,
                'metric': None,
                'source': current_skyline_app,
                'timestamp': time(),
                'expiry': namespaces_no_data_dict[parent_namespace]['expiry'],
                'data': {
                    'namespace': parent_namespace,
                    'last_timestamp': namespaces_no_data_dict[parent_namespace]['last_timestamp'],
                    'total_recent_metrics': namespaces_no_data_dict[parent_namespace]['total_recent_metrics'],
                    'total_stale_metrics': namespaces_no_data_dict[parent_namespace]['total_stale_metrics'],
                    'total_metrics': namespaces_no_data_dict[parent_namespace]['total_metrics'],
                    'status': status,
                },
            }
            submitted = False
            try:
                submitted = thunder_send_event(current_skyline_app, thunder_event, log=True)
            except Exception as e:
                if not log:
                    current_skyline_app_logger = current_skyline_app + 'Log'
                    current_logger = logging.getLogger(current_skyline_app_logger)
                current_logger.error('error :: %s :: error encounterd with thunder_send_event - %s' % (
                    function_str, e))
            if submitted:
                if log:
                    current_logger.info('%s :: send thunder event for no_data on namespace %s' % (
                        function_str, parent_namespace))

    return namespaces_no_data_dict
