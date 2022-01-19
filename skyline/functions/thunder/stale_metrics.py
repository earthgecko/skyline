import logging
import traceback
from time import time
from ast import literal_eval

import settings
from skyline_functions import get_redis_conn_decoded
from functions.redis.get_metric_timeseries import get_metric_timeseries
from functions.thunder.send_event import thunder_send_event
from functions.timeseries.determine_data_sparsity import determine_data_sparsity
from functions.settings.get_external_settings import get_external_settings


# @added 20210518 - Branch #1444: thunder
def thunder_stale_metrics(current_skyline_app, log=True):
    """
    Determine stale metrics in each top level namespace.

    :param current_skyline_app: the app calling the function
    :param log: whether to log or not, optional, defaults to True
    :type current_skyline_app: str
    :type log: boolean
    :return: (namespace_stale_metrics_dict, namespace_recovered_metrics_dict)
    :rtype: tuple

    """

    if current_skyline_app == 'analyzer':
        function_str = 'metrics_manager :: functions.thunder.thunder_stale_metrics'
    if current_skyline_app == 'webapp':
        function_str = 'functions.thunder.thunder_stale_metrics'

    if log:
        current_skyline_app_logger = current_skyline_app + 'Log'
        current_logger = logging.getLogger(current_skyline_app_logger)
    else:
        current_logger = None

    def get_sparsity(base_name):
        """
        Determine the metric sparsity
        """
        success = True
        sparsity = None
        timeseries = []
        try:
            timeseries = get_metric_timeseries(current_skyline_app, base_name)
        except Exception as e:
            success = e
            sparsity = None
        if timeseries:
            try:
                sparsity = determine_data_sparsity(current_skyline_app, timeseries, None, False)
            except Exception as e:
                success = e
                sparsity = None
        else:
            success = 'no timeseries data'
            sparsity = None

        return success, sparsity

    now = int(time())
    namespace_stale_metrics_dict = {}
    namespace_recovered_metrics_dict = {}
    alerted_on_stale_metrics_dict = {}

    metrics_last_timestamp_dict = {}
    hash_key = 'analyzer.metrics.last_timeseries_timestamp'
    try:
        redis_conn_decoded = get_redis_conn_decoded(current_skyline_app)
    except Exception as e:
        if not log:
            current_skyline_app_logger = current_skyline_app + 'Log'
            current_logger = logging.getLogger(current_skyline_app_logger)
        current_logger.error(traceback.format_exc())
        current_logger.error('error :: %s :: failed to get Redis connection - %s' % (
            function_str, e))
        return namespace_stale_metrics_dict

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
        return namespace_stale_metrics_dict

    # Do not send stale alerts for any identified sparsely populated metrics
    metrics_sparsity_dict = {}
    data_sparsity_hash_key = 'analyzer.metrics_manager.hash_key.metrics_data_sparsity'
    try:
        metrics_sparsity_dict = redis_conn_decoded.hgetall(data_sparsity_hash_key)
    except Exception as e:
        if not log:
            current_skyline_app_logger = current_skyline_app + 'Log'
            current_logger = logging.getLogger(current_skyline_app_logger)
        current_logger.error(traceback.format_exc())
        current_logger.error('error :: %s :: failed to get Redis hash key %s - %s' % (
            function_str, data_sparsity_hash_key, e))
    sparsely_populated_metrics = []
    metrics_of_known_sparsity = []
    base_names_of_known_sparsity = []
    if metrics_sparsity_dict:
        metrics_of_known_sparsity = list(metrics_sparsity_dict.keys())
        for metric_name in metrics_of_known_sparsity:
            metric_name = str(metric_name)
            if metric_name.startswith(settings.FULL_NAMESPACE):
                base_name = metric_name.replace(settings.FULL_NAMESPACE, '', 1)
            else:
                base_name = metric_name
            base_names_of_known_sparsity.append(base_name)
            sparsity = metrics_sparsity_dict[metric_name]
            if float(sparsity) < settings.SPARSELY_POPULATED_PERCENTAGE:
                sparsely_populated_metrics.append(base_name)
        del metrics_sparsity_dict

    # @added 20210617 - Feature #4144: webapp - stale_metrics API endpoint
    # On webapp report on sparsely populated metrics as well
    exclude_sparsely_populated = False
    if current_skyline_app == 'webapp':
        try:
            exclude_sparsely_populated = redis_conn_decoded.get('webapp.stale_metrics.exclude_sparsely_populated')
            if log:
                current_logger.info('%s :: Redis key webapp.stale_metrics.exclude_sparsely_populated - %s' % (
                    function_str, str(exclude_sparsely_populated)))
        except Exception as e:
            if not log:
                current_skyline_app_logger = current_skyline_app + 'Log'
                current_logger = logging.getLogger(current_skyline_app_logger)
            current_logger.error(traceback.format_exc())
            current_logger.error('error :: %s :: failed to get Redis key webapp.stale_metrics.exclude_sparsely_populated - %s' % (
                function_str, e))
        if not exclude_sparsely_populated:
            sparsely_populated_metrics = []

    # Get all alerted on stale metrics
    alerted_on_stale_metrics_hash_key = 'thunder.alerted_on.stale_metrics'
    try:
        alerted_on_stale_metrics_dict = redis_conn_decoded.hgetall(alerted_on_stale_metrics_hash_key)
    except Exception as e:
        if not log:
            current_skyline_app_logger = current_skyline_app + 'Log'
            current_logger = logging.getLogger(current_skyline_app_logger)
        current_logger.error(traceback.format_exc())
        current_logger.error('error :: %s :: failed to get Redis hash key %s - %s' % (
            function_str, alerted_on_stale_metrics_hash_key, e))
    alerted_on_stale_metrics = []
    if alerted_on_stale_metrics_dict:
        alerted_on_stale_metrics = list(alerted_on_stale_metrics_dict.keys())

    # @added 20210617 - Feature #4144: webapp - stale_metrics API endpoint
    # On webapp report on alerted on metrics as well
    if current_skyline_app == 'webapp':
        alerted_on_stale_metrics = []

    # Get all the known custom stale periods
    custom_stale_metrics_dict = {}
    custom_stale_metrics_hash_key = 'analyzer.metrics_manager.custom_stale_periods'
    try:
        custom_stale_metrics_dict = redis_conn_decoded.hgetall(custom_stale_metrics_hash_key)
    except Exception as e:
        if not log:
            current_skyline_app_logger = current_skyline_app + 'Log'
            current_logger = logging.getLogger(current_skyline_app_logger)
        current_logger.error(traceback.format_exc())
        current_logger.error('error :: %s :: failed to create custom_stale_metrics_dict from Redis hash key %s - %s' % (
            function_str, custom_stale_metrics_hash_key, e))
    custom_stale_metrics = []
    if custom_stale_metrics_dict:
        custom_stale_metrics = list(custom_stale_metrics_dict.keys())

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

    total_stale_metrics_count = 0
    total_recovered_metrics_count = 0
    test_stale_metrics_namespaces = []

    parent_namespaces = list(set(parent_namespaces))

    # @added 20210620 - Branch #1444: thunder
    #                   Feature #4076: CUSTOM_STALE_PERIOD
    # Handle multi level namespaces
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
        # external_parent_namespaces = list(external_parent_namespaces.keys())
        external_parent_namespaces = list(external_parent_namespaces_stale_periods.keys())

    parent_namespace_metrics_processed = []
    custom_stale_period_namespaces = []
    # Sort the list by the namespaces with the most elements to the least as
    # first match wins
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
        parent_namespace_stale_metrics_count = 0
        namespace_stale_metrics_dict[parent_namespace] = {}
        namespace_stale_metrics_dict[parent_namespace]['metrics'] = {}

        namespace_recovered_metrics_dict[parent_namespace] = {}
        namespace_recovered_metrics_dict[parent_namespace]['metrics'] = {}

        # metrics that are in the parent namespace
        parent_namespace_metrics = [item for item in metrics_last_timestamps if str(item[0]).startswith(parent_namespace)]
        unfiltered_parent_namespace_metrics_count = len(parent_namespace_metrics)
        # @added 20210620 - Branch #1444: thunder
        #                   Feature #4076: CUSTOM_STALE_PERIOD
        # Handle multi level namespaces by filtering out metrics that have
        # already been processed in a longer parent_namespace
        parent_namespace_metrics = [item for item in parent_namespace_metrics if str(item[0]) not in parent_namespace_metrics_processed]
        if parent_namespace_metrics:
            parent_namespace_metric_names = [item[0] for item in parent_namespace_metrics]
            parent_namespace_metrics_processed = parent_namespace_metrics_processed + parent_namespace_metric_names
        if log:
            current_logger.info('%s :: checking stale metrics in the \'%s.\' namespace on %s metrics (of %s filtered by processed)' % (
                function_str, parent_namespace, str(len(parent_namespace_metrics)),
                str(unfiltered_parent_namespace_metrics_count)))

        # Now check metrics that are default STALE_PERIOD metrics and are not
        # CUSTOM_STALE_PERIOD metrics
        last_error = None
        stale_period_parent_namespace_metrics = [item for item in parent_namespace_metrics if item[0] not in custom_stale_metrics]
        for base_name, timestamp in stale_period_parent_namespace_metrics:
            if base_name in sparsely_populated_metrics:
                continue
            try:
                # Only alert once on stale metrics and identify as recovered
                if base_name in alerted_on_stale_metrics:
                    if int(timestamp) > (now - settings.STALE_PERIOD):
                        namespace_recovered_metrics_dict[parent_namespace]['metrics'][base_name] = int(timestamp)
                        total_recovered_metrics_count += 1
                    else:
                        continue
                if int(timestamp) < (now - settings.STALE_PERIOD):

                    # Determine the metric sparsity if it is not known
                    if base_name not in base_names_of_known_sparsity:
                        success = None
                        sparsity = None
                        try:
                            success, sparsity = get_sparsity(base_name)
                            if sparsity is not None:
                                if float(sparsity) < settings.SPARSELY_POPULATED_PERCENTAGE:
                                    if current_skyline_app == 'analyzer':
                                        sparsely_populated_metrics.append(base_name)
                                        continue
                                    if current_skyline_app == 'webapp' and exclude_sparsely_populated:
                                        sparsely_populated_metrics.append(base_name)
                                        continue
                            else:
                                if success is not True:
                                    if not log:
                                        current_skyline_app_logger = current_skyline_app + 'Log'
                                        current_logger = logging.getLogger(current_skyline_app_logger)
                                    current_logger.error('error :: %s :: get_sparsity failed for %s - %s' % (
                                        function_str, base_name, str(success)))
                        except Exception as e:
                            if not log:
                                current_skyline_app_logger = current_skyline_app + 'Log'
                                current_logger = logging.getLogger(current_skyline_app_logger)
                            current_logger.error('error :: %s :: get_sparsity failed for %s - %s' % (
                                function_str, base_name, e))

                    namespace_stale_metrics_dict[parent_namespace]['metrics'][base_name] = timestamp
                    total_stale_metrics_count += 1
                    parent_namespace_stale_metrics_count += 1
            except Exception as e:
                last_traceback = traceback.format_exc()
                last_error = e
                error_count += 1
        if last_error:
            if not log:
                current_skyline_app_logger = current_skyline_app + 'Log'
                current_logger = logging.getLogger(current_skyline_app_logger)
            current_logger.error('error :: %s :: errors %s encounterd while determining stale_period_parent_namespace_metrics, last reported error - %s' % (
                function_str, str(error_count), last_error))
            current_logger.error('error :: %s :: last reported Traceback' % (
                function_str))
            current_logger.error('%s' % (str(last_traceback)))

        # Now check metrics that are CUSTOM_STALE_PERIOD metrics
        custom_stale_period_parent_namespace_metrics = [item for item in parent_namespace_metrics if item[0] in custom_stale_metrics]
        last_error = None
        for base_name, timestamp in custom_stale_period_parent_namespace_metrics:
            if base_name in sparsely_populated_metrics:
                continue
            try:
                # Only alert once on stale metrics and identify as recovered
                if base_name in alerted_on_stale_metrics:
                    if int(timestamp) > (now - int(custom_stale_metrics_dict[base_name])):
                        namespace_recovered_metrics_dict[parent_namespace]['metrics'][base_name] = int(timestamp)
                        total_recovered_metrics_count += 1
                    else:
                        continue
                if int(timestamp) < (now - int(custom_stale_metrics_dict[base_name])):

                    # Determine the metric sparsity if it is not known
                    if base_name not in base_names_of_known_sparsity:
                        success = None
                        sparsity = None
                        try:
                            success, sparsity = get_sparsity(base_name)
                            if sparsity is not None:
                                if float(sparsity) < settings.SPARSELY_POPULATED_PERCENTAGE:
                                    # @modified 20210617 - Feature #4144: webapp - stale_metrics API endpoint
                                    # On webapp report on sparsely_populated_metrics on metrics as well
                                    if current_skyline_app == 'analyzer':
                                        sparsely_populated_metrics.append(base_name)
                                        continue
                                    if current_skyline_app == 'webapp' and exclude_sparsely_populated:
                                        sparsely_populated_metrics.append(base_name)
                                        continue
                            else:
                                if success is not True:
                                    if not log:
                                        current_skyline_app_logger = current_skyline_app + 'Log'
                                        current_logger = logging.getLogger(current_skyline_app_logger)
                                    current_logger.error('error :: %s :: get_sparsity failed for %s - %s' % (
                                        function_str, base_name, str(success)))
                        except Exception as e:
                            if not log:
                                current_skyline_app_logger = current_skyline_app + 'Log'
                                current_logger = logging.getLogger(current_skyline_app_logger)
                            current_logger.error('error :: %s :: get_sparsity failed for %s - %s' % (
                                function_str, base_name, e))

                    namespace_stale_metrics_dict[parent_namespace]['metrics'][base_name] = timestamp
                    total_stale_metrics_count += 1
                    parent_namespace_stale_metrics_count += 1
            except Exception as e:
                last_traceback = traceback.format_exc()
                last_error = e
                error_count += 1
        if last_error:
            if not log:
                current_skyline_app_logger = current_skyline_app + 'Log'
                current_logger = logging.getLogger(current_skyline_app_logger)
            current_logger.error('error :: %s :: errors %s encounterd while determining custom_stale_period_parent_namespace_metrics, last reported error - %s' % (
                function_str, str(error_count), last_error))
            current_logger.error('error :: %s :: last reported Traceback' % (
                function_str))
            current_logger.error('%s' % (str(last_traceback)))

        if parent_namespace_stale_metrics_count:
            if log:
                current_logger.info('%s :: %s stale metrics found for %s' % (
                    function_str, str(parent_namespace_stale_metrics_count),
                    parent_namespace))

        # Allow to test
        if not parent_namespace_stale_metrics_count:
            # Allow to test
            thunder_test_alert_key_data = None
            thunder_test_alert_key = 'thunder.test.alert.stale_metrics.%s' % parent_namespace
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
                    stale_count = thunder_test_data['stale_count']
                    if log:
                        current_logger.info('%s :: THUNDER STALE_METRICS TEST REQUESTED FOR - \'%s.\' namespace using TEST stale_period of %s and expiry of %s for %s metrics' % (
                            function_str, parent_namespace, str(stale_period),
                            str(expiry), str(stale_count)))
                except Exception as e:
                    if not log:
                        current_skyline_app_logger = current_skyline_app + 'Log'
                        current_logger = logging.getLogger(current_skyline_app_logger)
                    current_logger.error(traceback.format_exc())
                    current_logger.error('error :: %s :: failed to get stale_period, expiry and stale_count for Redis key %s - %s' % (
                        function_str, thunder_test_alert_key, e))
                for base_name, timestamp in parent_namespace_metrics[-stale_count:]:
                    namespace_stale_metrics_dict[parent_namespace]['metrics'][base_name] = timestamp
                    total_stale_metrics_count += 1
                    parent_namespace_stale_metrics_count += 1
                test_stale_metrics_count = len(list(namespace_stale_metrics_dict[parent_namespace]['metrics'].keys()))
                test_stale_metrics_namespaces.append(parent_namespace)
                if log:
                    current_logger.info('%s :: THUNDER STALE_METRICS TEST REQUESTED FOR - \'%s.\' namespace sending %s TEST stale_metrics' % (
                        function_str, parent_namespace,
                        str(test_stale_metrics_count)))

    if log:
        current_logger.info('%s :: total stale metrics found - %s' % (
            function_str, str(total_stale_metrics_count)))
        current_logger.info('%s :: total recovered stale metrics - %s' % (
            function_str, str(total_recovered_metrics_count)))
        current_logger.info('%s :: skipped checking %s sparsely_populated_metrics' % (
            function_str, str(len(sparsely_populated_metrics))))

    # @modified 20210617 - Feature #4144: webapp - stale_metrics API endpoint
    # On webapp request do not send thunder events
    # if namespace_stale_metrics_dict:
    if namespace_stale_metrics_dict and current_skyline_app == 'analyzer':
        parent_namespaces = list(namespace_stale_metrics_dict.keys())
        for parent_namespace in parent_namespaces:
            stale_metrics = list(namespace_stale_metrics_dict[parent_namespace]['metrics'].keys())
            if len(stale_metrics) > 0:

                # Check if there is a thunder.alert.no_data Redis key for the
                # namespace and skip if there is
                thunder_no_data_alert_key_exists = False
                thunder_no_data_alert_key = 'thunder.alert.no_data.%s' % parent_namespace
                try:
                    thunder_no_data_alert_key_exists = redis_conn_decoded.get(thunder_no_data_alert_key)
                except Exception as e:
                    if not log:
                        current_skyline_app_logger = current_skyline_app + 'Log'
                        current_logger = logging.getLogger(current_skyline_app_logger)
                    current_logger.error(traceback.format_exc())
                    current_logger.error('error :: %s :: failed to get Redis key %s - %s' % (
                        function_str, thunder_no_data_alert_key, e))
                if thunder_no_data_alert_key_exists:
                    if log:
                        current_logger.info('%s :: skipping sending thunder event for stale metrics on %s as thunder no_data alert key exists for the namespace' % (
                            function_str, parent_namespace))
                    continue

                # Check if there is a thunder.alert.analyzer.up.alert Redis key for the
                # namespace and skip if there is
                thunder_analyzer_alert_key_exists = False
                thunder_analyzer_alert_key = 'thunder.alert.analyzer.up.alert'
                try:
                    thunder_analyzer_alert_key_exists = redis_conn_decoded.get(thunder_analyzer_alert_key)
                except Exception as e:
                    if not log:
                        current_skyline_app_logger = current_skyline_app + 'Log'
                        current_logger = logging.getLogger(current_skyline_app_logger)
                    current_logger.error(traceback.format_exc())
                    current_logger.error('error :: %s :: failed to get Redis key %s - %s' % (
                        function_str, thunder_analyzer_alert_key, e))
                if thunder_analyzer_alert_key_exists:
                    if log:
                        current_logger.info('%s :: skipping sending thunder event for stale metrics on %s as thunder analyzer alert key exists' % (
                            function_str, parent_namespace))
                    continue

                level = 'alert'
                event_type = 'stale_metrics'
                message = '%s - %s - no new data for %s metrics' % (
                    level, parent_namespace, str(len(stale_metrics)))
                status = 'not recieving data for some metrics'
                if parent_namespace in test_stale_metrics_namespaces:
                    message = '%s - %s - no new data for %s metrics - TEST' % (
                        level, parent_namespace, str(len(stale_metrics)))
                    status = 'not recieving data for some metrics - TEST'
                thunder_event = {
                    'level': level,
                    'event_type': event_type,
                    'message': message,
                    'app': current_skyline_app,
                    'metric': None,
                    'source': current_skyline_app,
                    'timestamp': time(),
                    'expiry': settings.STALE_PERIOD,
                    'data': {
                        'namespace': parent_namespace,
                        'stale_metrics': stale_metrics,
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
                        current_logger.info('%s :: send thunder event for %s stale metrics on namespace %s' % (
                            function_str, str(len(stale_metrics)),
                            parent_namespace))

    # @modified 20210617 - Feature #4144: webapp - stale_metrics API endpoint
    # On webapp request do not send thunder events
    # if namespace_recovered_metrics_dict and total_recovered_metrics_count:
    if namespace_recovered_metrics_dict and total_recovered_metrics_count and current_skyline_app == 'analyzer':
        parent_namespaces = list(namespace_recovered_metrics_dict.keys())
        for parent_namespace in parent_namespaces:
            stale_metrics = list(namespace_recovered_metrics_dict[parent_namespace]['metrics'].keys())
            if len(stale_metrics) > 0:
                level = 'notice'
                event_type = 'stale_metrics'
                message = '%s - %s - new data for %s metrics' % (
                    level, parent_namespace, str(len(stale_metrics)))
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
                        'stale_metrics': stale_metrics,
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
                        current_logger.info('%s :: send thunder event for %s stale metrics on namespace %s' % (
                            function_str, str(len(stale_metrics)),
                            parent_namespace))

    return namespace_stale_metrics_dict, namespace_recovered_metrics_dict
