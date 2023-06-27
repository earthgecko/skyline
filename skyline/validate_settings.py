import sys
import os
# @added 20220722 - Task #4624: Change all dict copy to deepcopy
import copy

import pytz

sys.path.append(os.path.join(os.path.dirname(os.path.realpath(__file__)), os.pardir))
sys.path.insert(0, os.path.dirname(__file__))
if True:
    import settings


def validate_settings_variables(current_skyline_app):
    """
    This function is used by the agent.py to validate the variables in
    settings.py are valid

    :param current_skyline_app: the skyline app using this function
    :return: ``True`` or ``False``
    :rtype: boolean

    """

    invalid_variables = False

    # @added 20220225 - Feature #4442: settings - LOCAL_EXTERNAL_SETTINGS
    # Report all the untested settings
    # Test all the untested settings not declared above and this way is much
    # cleaner.
    # HOWTO get types of not_tested_settings
    # settings_types = {}
    # for setting in not_tested_settings:
    #     setting_value = getattr(settings, setting)
    #     settings_types[setting] = type(setting_value)
    # settings_types
    settings_variables = [item for item in dir(settings) if '__' not in item]
    settings_types = {
        'ALERTERS_SETTINGS': bool,
        'ALERTS': tuple,
        'ALERT_ON_STALE_METRICS': bool,
        'ALERT_ON_STALE_PERIOD': int,
        'ALGORITHMS': list,
        'ALTERNATIVE_SKYLINE_URLS': list,
        'ANALYZER_ANALYZE_LOW_PRIORITY_METRICS': bool,
        'ANALYZER_CHECK_LAST_TIMESTAMP': bool,
        'ANALYZER_CRUCIBLE_ENABLED': bool,
        'ANALYZER_DYNAMICALLY_ANALYZE_LOW_PRIORITY_METRICS': bool,
        'ANALYZER_ENABLED': bool,
        'ANALYZER_MAD_LOW_PRIORITY_METRICS': int,
        'ANALYZER_OPTIMUM_RUN_DURATION': int,
        'ANALYZER_PROCESSES': int,
        'ANALYZER_SKIP': list,
        'ANALYZER_VERBOSE_LOGGING': bool,
        'ANOMALY_DUMP': str,
        'AWS_OPTS': dict,
        'AWS_SNS_SMS_ALERTS_ENABLED': bool,
        'BATCH_METRICS_CUSTOM_FULL_DURATIONS': dict,
        'BATCH_PROCESSING': bool,
        'BATCH_PROCESSING_DEBUG': bool,
        'BATCH_PROCESSING_NAMESPACES': list,
        'BATCH_PROCESSING_STALE_PERIOD': int,
        'BOREDOM_SET_SIZE': int,
        'BOUNDARY_ALERTER_OPTS': dict,
        'BOUNDARY_ALGORITHMS': list,
        'BOUNDARY_AUTOAGGRERATION': bool,
        'BOUNDARY_AUTOAGGRERATION_METRICS': tuple,
        'BOUNDARY_CRUCIBLE_ENABLED': bool,
        'BOUNDARY_ENABLE_ALERTS': bool,
        'BOUNDARY_METRICS': tuple,
        'BOUNDARY_OPTIMUM_RUN_DURATION': int,
        'BOUNDARY_PAGERDUTY_OPTS': dict,
        'BOUNDARY_PROCESSES': int,
        'BOUNDARY_SLACK_OPTS': dict,
        'BOUNDARY_SMTP_OPTS': dict,
        'CANARY_METRIC': str,
        'CARBON_HOST': str,
        'CARBON_PORT': int,
        'CHECK_AIRGAPS': list,
        'CHECK_DATA_SPARSITY': bool,
        'CHUNK_SIZE': int,
        'CONSENSUS': int,
        'CORRELATE_ALERTS_ONLY': bool,
        'CRUCIBLE_CHECK_PATH': str,
        'CRUCIBLE_DATA_FOLDER': str,
        'CRUCIBLE_PROCESSES': int,
        'CRUCIBLE_TESTS_TIMEOUT': int,
        'CUSTOM_ALERT_OPTS': dict,
        'CUSTOM_ALGORITHMS': dict,
        'CUSTOM_STALE_PERIOD': dict,
        'DATA_UPLOADS_PATH': str,
        'DEBUG_CUSTOM_ALGORITHMS': bool,
        'DOCKER': bool,
        'DOCKER_DISPLAY_REDIS_PASSWORD_IN_REBROW': bool,
        'DOCKER_FAKE_EMAIL_ALERTS': bool,
        'DO_NOT_ALERT_ON_STALE_METRICS': list,
        'DO_NOT_SKIP_LIST': list,
        'DO_NOT_SKIP_SKYLINE_FEEDBACK_NAMESPACES': list,
        'ENABLE_ALERTS': bool,
        'ENABLE_ALGORITHM_RUN_METRICS': bool,
        'ENABLE_ALL_ALGORITHMS_RUN_METRICS': bool,
        'ENABLE_BOUNDARY_DEBUG': bool,
        'ENABLE_CRUCIBLE': bool,
        'ENABLE_CRUCIBLE_DEBUG': bool,
        'ENABLE_DEBUG': bool,
        'ENABLE_FULL_DURATION_ALERTS': bool,
        'ENABLE_IONOSPHERE_DEBUG': bool,
        'ENABLE_LUMINOSITY_DEBUG': bool,
        'ENABLE_MIRAGE': bool,
        'ENABLE_PANORAMA_DEBUG': bool,
        'ENABLE_SECOND_ORDER': bool,
        'ENABLE_WEBAPP_DEBUG': bool,
        'EXTERNAL_ALERTS': dict,
        'EXTERNAL_SETTINGS': dict,
        'FLUX_AGGREGATE_NAMESPACES': dict,
        'FLUX_API_KEYS': dict,
        'FLUX_BACKLOG': int,
        'FLUX_CARBON_HOST': str,
        'FLUX_CARBON_PICKLE_PORT': int,
        'FLUX_CARBON_PORT': int,
        'FLUX_CHECK_LAST_TIMESTAMP': bool,
        'FLUX_EXTERNAL_AGGREGATE_NAMESPACES': bool,
        'FLUX_GRAPHITE_WHISPER_PATH': str,
        'FLUX_IP': str,
        'FLUX_LAST_KNOWN_VALUE_NAMESPACES': list,
        'FLUX_MAX_AGE': int,
        'FLUX_NAMESPACE_QUOTAS': dict,
        'FLUX_PERSIST_QUEUE': bool,
        'FLUX_PORT': int,
        'FLUX_PROCESS_UPLOADS': bool,
        'FLUX_SAVE_UPLOADS': bool,
        'FLUX_SAVE_UPLOADS_PATH': str,
        'FLUX_SELF_API_KEY': str,
        'FLUX_SEND_TO_CARBON': bool,
        'FLUX_SEND_TO_STATSD': bool,
        'FLUX_STATSD_HOST': str,
        'FLUX_STATSD_PORT': int,
        'FLUX_UPLOADS_KEYS': dict,
        'FLUX_VERBOSE_LOGGING': bool,
        'FLUX_WORKERS': int,
        'FLUX_ZERO_FILL_NAMESPACES': list,
        'FLUX_DROP_BUCKET_METRICS': bool,
        'FULLY_POPULATED_PERCENTAGE': float,
        'FULL_DURATION': int,
        'FULL_NAMESPACE': str,
        'GRAPHITE_AUTH_HEADER': bool,
        'GRAPHITE_CONNECT_TIMEOUT': int,
        'GRAPHITE_CUSTOM_HEADERS': dict,
        'GRAPHITE_GRAPH_SETTINGS': str,
        'GRAPHITE_HOST': str,
        'GRAPHITE_PORT': str,
        'GRAPHITE_PROTOCOL': str,
        'GRAPHITE_READ_TIMEOUT': int,
        'GRAPHITE_RENDER_URI': str,
        'GRAPHITE_SOURCE': str,
        'GRAPH_URL': str,
        'HIPCHAT_ENABLED': bool,
        'HORIZON_IP': str,
        'HORIZON_SHARDS': dict,
        'HORIZON_SHARD_DEBUG': bool,
        'HORIZON_SHARD_PICKLE_PORT': int,
        'HTTP_ALERTERS_ENABLED': bool,
        'HTTP_ALERTERS_OPTS': dict,
        'IDENTIFY_AIRGAPS': bool,
        'IDENTIFY_UNORDERED_TIMESERIES': bool,
        'IONOSPHERE_AUTOBUILD': bool,
        'IONOSPHERE_CHECK_MAX_AGE': int,
        'IONOSPHERE_CHECK_PATH': str,
        'IONOSPHERE_CUSTOM_KEEP_TRAINING_TIMESERIES_FOR': list,
        'IONOSPHERE_DATA_FOLDER': str,
        'IONOSPHERE_ECHO_ENABLED': bool,
        'IONOSPHERE_ECHO_FEATURES_PERCENT_SIMILAR': float,
        'IONOSPHERE_ECHO_MAX_FP_CREATE_TIME': int,
        'IONOSPHERE_ECHO_MINMAX_SCALING_FEATURES_PERCENT_SIMILAR': float,
        'IONOSPHERE_ENABLED': bool,
        'IONOSPHERE_FEATURES_PERCENT_SIMILAR': float,
        'IONOSPHERE_GRAPHITE_NOW_GRAPHS_OVERRIDE': bool,
        'IONOSPHERE_HISTORICAL_DATA_FOLDER': str,
        'IONOSPHERE_INFERENCE_MASS_TS_MAX_DISTANCE': float,
        'IONOSPHERE_INFERENCE_MOTIFS_ENABLED': bool,
        'IONOSPHERE_INFERENCE_MOTIFS_RANGE_PADDING': float,
        'IONOSPHERE_INFERENCE_MOTIFS_SETTINGS': dict,
        'IONOSPHERE_INFERENCE_MOTIFS_SINGLE_MATCH': bool,
        'IONOSPHERE_INFERENCE_MOTIFS_TEST_ONLY': bool,
        'IONOSPHERE_INFERENCE_MOTIFS_TOP_MATCHES': int,
        'IONOSPHERE_KEEP_TRAINING_TIMESERIES_FOR': int,
        'IONOSPHERE_LAYERS_USE_APPROXIMATELY_CLOSE': bool,
        'IONOSPHERE_LEARN': bool,
        'IONOSPHERE_LEARN_DEFAULT_FULL_DURATION_DAYS': int,
        'IONOSPHERE_LEARN_DEFAULT_MAX_GENERATIONS': int,
        'IONOSPHERE_LEARN_DEFAULT_MAX_PERCENT_DIFF_FROM_ORIGIN': float,
        'IONOSPHERE_LEARN_DEFAULT_VALID_TIMESERIES_OLDER_THAN_SECONDS': int,
        'IONOSPHERE_LEARN_FOLDER': str,
        'IONOSPHERE_LEARN_NAMESPACE_CONFIG': tuple,
        'IONOSPHERE_MANAGE_PURGE': bool,
        'IONOSPHERE_MAX_RUNTIME': int,
        'IONOSPHERE_MINMAX_SCALING_ENABLED': bool,
        'IONOSPHERE_MINMAX_SCALING_RANGE_TOLERANCE': float,
        'IONOSPHERE_PERFORMANCE_DATA_POPULATE_CACHE': bool,
        'IONOSPHERE_PERFORMANCE_DATA_POPULATE_CACHE_DEPTH': int,
        'IONOSPHERE_PROCESSES': int,
        'IONOSPHERE_PROFILES_FOLDER': str,
        'IONOSPHERE_UNTRAINABLES': list,
        'IONOSPHERE_VERBOSE_LOGGING': bool,
        'IONOSPHERE_LEARN_REPETITIVE_PATTERNS': bool,
        'IONOSPHERE_FIND_REPETITIVE_PATTERNS': bool,
        'IONOSPHERE_REPETITIVE_PATTERNS_MINMAX_AVG_VALUE': float,
        'IONOSPHERE_REPETITIVE_PATTERNS_INCLUDE': dict,
        'IONOSPHERE_REPETITIVE_PATTERNS_EXCLUDE': dict,
        'IONOSPHERE_ENFORCE_DOWNSAMPLING': dict,
        'LOCAL_EXTERNAL_SETTINGS': dict,
        'LOG_PATH': str,
        'LUMINOL_CROSS_CORRELATION_THRESHOLD': float,
        'LUMINOSITY_CLASSIFY_ANOMALIES': bool,
        'LUMINOSITY_CLASSIFY_ANOMALIES_SAVE_PLOTS': bool,
        'LUMINOSITY_CLASSIFY_ANOMALY_ALGORITHMS': list,
        'LUMINOSITY_CLASSIFY_METRICS_LEVEL_SHIFT': bool,
        'LUMINOSITY_CLOUDBURST_ENABLED': bool,
        'LUMINOSITY_CLOUDBURST_PROCESSES': int,
        'LUMINOSITY_CLOUDBURST_RUN_EVERY': int,
        'LUMINOSITY_CLOUDBURST_SKIP_METRICS': list,
        'LUMINOSITY_CORRELATE_ALL': bool,
        'LUMINOSITY_CORRELATE_NAMESPACES_ONLY': list,
        'LUMINOSITY_CORRELATION_MAPS': dict,
        'LUMINOSITY_DATA_FOLDER': str,
        'LUMINOSITY_LEVEL_SHIFT_SKIP_NAMESPACES': list,
        'LUMINOSITY_PROCESSES': int,
        'LUMINOSITY_RELATED_METRICS': bool,
        'LUMINOSITY_RELATED_METRICS_MAX_5MIN_LOADAVG': float,
        'LUMINOSITY_RELATED_METRICS_MINIMUM_CORRELATIONS_COUNT': int,
        'LUMINOSITY_RELATED_METRICS_MIN_CORRELATION_COUNT_PERCENTILE': float,
        'LUMINOSITY_RELATED_TIME_PERIOD': int,
        'MAX_AIRGAP_PERIOD': int,
        'MAX_ANALYZER_PROCESS_RUNTIME': int,
        'MAX_QUEUE_SIZE': int,
        'MAX_RESOLUTION': int,
        'MAX_TOLERABLE_BOREDOM': int,
        'MEMCACHED_SERVER_IP': str,
        'MEMCACHED_SERVER_PORT': int,
        'MEMCACHE_ENABLED': bool,
        'METRICS_INACTIVE_AFTER': int,
        'MINI_DURATION': int,
        'MINI_NAMESPACE': str,
        'MIN_TOLERABLE_LENGTH': int,
        'MIRAGE_ALGORITHMS': list,
        'MIRAGE_ALWAYS_METRICS': list,
        'MIRAGE_AUTOFILL_TOOSHORT': bool,
        'MIRAGE_CHECK_PATH': str,
        'MIRAGE_CHECK_REPETITIVE_DAILY_PEAKS': int,
        # 'MIRAGE_SKIP_IRREGULAR_UNSTABLE': list,
        'MIRAGE_CONSENSUS': int,
        'MIRAGE_CRUCIBLE_ENABLED': bool,
        'MIRAGE_DATA_FOLDER': str,
        'MIRAGE_ENABLE_ALERTS': bool,
        'MIRAGE_ENABLE_SECOND_ORDER': bool,
        'MIRAGE_PERIODIC_CHECK': bool,
        'MIRAGE_PERIODIC_CHECK_INTERVAL': int,
        'MIRAGE_PERIODIC_CHECK_NAMESPACES': list,
        'MIRAGE_PROCESSES': int,
        'MIRAGE_STALE_SECONDS': int,
        'NEGATE_ANALYZER_ALERTS': bool,
        'NON_DERIVATIVE_MONOTONIC_METRICS': list,
        'NUMBER_OF_ANOMALIES_TO_STORE_IN_PANORAMA': int,
        'OCULUS_HOST': str,
        'OTHER_SKYLINE_REDIS_INSTANCES': list,
        'PAGERDUTY_ENABLED': bool,
        'PAGERDUTY_OPTS': dict,
        'PANDAS_VERSION': str,
        'PANORAMA_CHECK_INTERVAL': int,
        'PANORAMA_CHECK_MAX_AGE': int,
        'PANORAMA_CHECK_PATH': str,
        'PANORAMA_DATABASE': str,
        'PANORAMA_DBHOST': str,
        'PANORAMA_DBPORT': str,
        'PANORAMA_DBUSER': str,
        'PANORAMA_DBUSERPASS': str,
        'PANORAMA_ENABLED': bool,
        'PANORAMA_EXPIRY_TIME': int,
        'PANORAMA_INSERT_METRICS_IMMEDIATELY': bool,
        'PANORAMA_PROCESSES': int,
        'PICKLE_PORT': int,
        'PID_PATH': str,
        'PLOT_REDIS_DATA': bool,
        'PROMETHEUS_SETTINGS': dict,
        'REDIS_PASSWORD': str,
        'REDIS_SOCKET_PATH': str,
        'REMOTE_SKYLINE_INSTANCES': list,
        'ROOMBA_BATCH_METRICS_CUSTOM_DURATIONS': list,
        'ROOMBA_DO_NOT_PROCESS_BATCH_METRICS': bool,
        'ROOMBA_GRACE_TIME': int,
        'ROOMBA_OPTIMUM_RUN_DURATION': int,
        'ROOMBA_PROCESSES': int,
        'ROOMBA_TIMEOUT': int,
        'RUN_OPTIMIZED_WORKFLOW': bool,
        'SECRET_KEY': str,
        'SERVER_METRICS_NAME': str,
        'SERVER_PYTZ_TIMEZONE': str,
        'SKIP_AIRGAPS': list,
        'SKIP_CHECK_DATA_SPARSITY_NAMESPACES': list,
        'SKIP_LIST': list,
        'SKYLINE_DIR': str,
        'SKYLINE_FEEDBACK_NAMESPACES': list,
        'SKYLINE_METRICS_CARBON_HOST': str,
        'SKYLINE_METRICS_CARBON_PORT': int,
        'SKYLINE_TMP_DIR': str,
        'SKYLINE_URL': str,
        'SLACK_ENABLED': bool,
        'SLACK_OPTS': dict,
        'SMS_ALERT_OPTS': dict,
        'SMTP_OPTS': dict,
        'SNAB_CHECKS': dict,
        'SNAB_DATA_DIR': str,
        'SNAB_ENABLED': bool,
        'SNAB_FLUX_LOAD_TEST_ENABLED': bool,
        'SNAB_FLUX_LOAD_TEST_METRICS': int,
        'SNAB_FLUX_LOAD_TEST_METRICS_PER_POST': int,
        'SNAB_FLUX_LOAD_TEST_NAMESPACE_PREFIX': str,
        'SNAB_LOAD_TEST_ANALYZER': int,
        'SNAB_anomalyScore': dict,
        'SPARSELY_POPULATED_PERCENTAGE': float,
        'STALE_PERIOD': int,
        'START_IF_NO_DB': bool,
        'SYNC_CLUSTER_FILES': bool,
        'SYSLOG_ENABLED': bool,
        'SYSLOG_OPTS': dict,
        'TARGET_HOURS': str,
        'THUNDER_CHECKS': dict,
        'THUNDER_ENABLED': bool,
        'THUNDER_OPTS': dict,
        'UDP_PORT': int,
        'VERIFY_SSL': bool,
        'VISTA_DO_NOT_SUBMIT_CURRENT_MINUTE': bool,
        'VISTA_ENABLED': bool,
        'VISTA_FETCHER_PROCESSES': int,
        'VISTA_FETCHER_PROCESS_MAX_RUNTIME': int,
        'VISTA_FETCH_METRICS': tuple,
        'VISTA_GRAPHITE_BATCH_SIZE': int,
        'VISTA_VERBOSE_LOGGING': bool,
        'VISTA_WORKER_PROCESSES': int,
        'WEBAPP_ACCEPT_DATA_UPLOADS': bool,
        'WEBAPP_ALLOWED_IPS': list,
        'WEBAPP_AUTH_ENABLED': bool,
        'WEBAPP_AUTH_USER': str,
        'WEBAPP_AUTH_USER_PASSWORD': str,
        'WEBAPP_FIXED_TIMEZONE': str,
        'WEBAPP_GUNICORN_BACKLOG': int,
        'WEBAPP_GUNICORN_WORKERS': int,
        'WEBAPP_IP': str,
        'WEBAPP_IP_RESTRICTED': bool,
        'WEBAPP_JAVASCRIPT_DEBUG': bool,
        'WEBAPP_PORT': int,
        'WEBAPP_PREPROCESS_AGGREGATE_BY': str,
        'WEBAPP_PREPROCESS_TIMESERIES': bool,
        'WEBAPP_SERVER': str,
        'WEBAPP_USER_TIMEZONE': bool,
        'WORKER_PROCESSES': int,
        'FLUX_OTEL_ENABLED': bool,
        'OTEL_ENABLED': bool,
        'OTEL_JAEGEREXPORTER_AGENT_HOST_NAME': str,
        'OTEL_JAEGEREXPORTER_AGENT_PORT': int,
        'WEBAPP_SERVE_JAEGER': bool,
        'LAST_KNOWN_VALUE_NAMESPACES': list,
        'MONOTONIC_METRIC_NAMESPACES': list,
        'ZERO_FILL_NAMESPACES': list,
        'PROMETHEUS_INGESTION': bool,
        'VICTORIAMETRICS_ENABLED': bool,
        'VICTORIAMETRICS_OPTS': dict,
        'MEMRAY_ENABLED': bool,
        'VORTEX_ENABLED': bool,
        'VORTEX_TIMESERIES_JSON_TO_DISK': bool,
        'VORTEX_FULL_DURATION_RESOLUTIONS': dict,
        'VORTEX_ALGORITHMS': dict,
    }
    settings_tested = []

    # Validate settings variables
    REDIS_SOCKET_PATH = None
    try:
        if not isinstance(settings.REDIS_SOCKET_PATH, str):
            print('error :: REDIS_SOCKET_PATH in settings.py is not a str')
            invalid_variables = True
        else:
            REDIS_SOCKET_PATH = settings.REDIS_SOCKET_PATH
    except AttributeError:
        print('error :: the REDIS_SOCKET_PATH str is not defined in settings.py')
        invalid_variables = True
    except Exception as e:
        print('error :: the REDIS_SOCKET_PATH str in settings.py - %s' % e)
        invalid_variables = True
    if REDIS_SOCKET_PATH:
        settings_tested.append('REDIS_SOCKET_PATH')
        if not os.path.exists(os.path.dirname(REDIS_SOCKET_PATH)):
            print('error :: the directory specified in REDIS_SOCKET_PATH in settings.py does not exist')
            invalid_variables = True

    # @added 20180519 - Feature #2378: Add redis auth to Skyline and rebrow
    TEST_REDIS_PASSWORD = None
    try:
        TEST_REDIS_PASSWORD = settings.REDIS_PASSWORD
        if TEST_REDIS_PASSWORD is None:
            print('WARNING :: REDIS_PASSWORD is set to None, please considering enabling Redis authentication')
            print('WARNING :: by setting the Redis variable requirepass in your redis.conf and restarting')
            print('WARNING :: Redis, then set the REDIS_PASSWORD in your settings.py and restart your')
            print('WARNING :: Skyline services.')
            print('WARNING :: See https://redis.io/topics/security and http://antirez.com/news/96 for more info')
            print('If you have some valid reason for not wanting to run Redis with no password')
            print('and understand the implications of this, please note that')
            print('Skyline is NOT TESTED without Redis authentication so you may experience issues')
            print('if you do open an issue on github.')
        settings_tested.append('REDIS_PASSWORD')
    except AttributeError:
        print('error :: the REDIS_PASSWORD variable is not defined in settings.py')
        print('If you have some valid reason for not wanting to run Redis with no password')
        print('and understand the implications of this, add the variable to settings.py as:')
        print('REDIS_PASSWORD = None')
        print('Skyline is NOT TESTED without Redis authentication so you may experience issues')
        print('if you do open an issue on github.')
        invalid_variables = True
    except Exception as e:
        print('warning :: REDIS_PASSWORD is not set in settings.py - %s' % e)
        invalid_variables = True

    try:
        if not isinstance(settings.REDIS_PASSWORD, str):
            if isinstance(settings.REDIS_PASSWORD, bool):
                if settings.REDIS_PASSWORD is not None:
                    print('error :: REDIS_PASSWORD in settings.py must be a str or boolean of None')
                    invalid_variables = True
            else:
                print('error :: REDIS_PASSWORD in settings.py is not a str')
                invalid_variables = True
    except AttributeError:
        print('error :: the REDIS_PASSWORD str is not defined in settings.py')
        invalid_variables = True
    except Exception as e:
        print('error :: the REDIS_PASSWORD str in settings.py - %s' % e)
        invalid_variables = True

    # TEST_OTHER_SKYLINE_REDIS_INSTANCES = None
    # try:
    #     TEST_OTHER_SKYLINE_REDIS_INSTANCES = settings.OTHER_SKYLINE_REDIS_INSTANCES
    #     if TEST_OTHER_SKYLINE_REDIS_INSTANCES:
    #         for redis_ip, redis_port, redis_password in settings.OTHER_SKYLINE_REDIS_INSTANCES:
    #             if not redis_password:
    #                 print('WARNING :: the Redis password for %s is to False in the OTHER_SKYLINE_REDIS_INSTANCES') % str(redis_ip)
    #                 print('WARNING :: variable in settings.py, please considering enabling Redis authentication')
    #                 print('WARNING :: on %s by setting the Redis variable requirepass in the redis.conf and restarting') % str(redis_ip)
    #                 print('WARNING :: Redis, then set the Redis password for %s in OTHER_SKYLINE_REDIS_INSTANCES') % str(redis_ip)
    #                 print('WARNING :: in your settings.py and restart your Skyline luminosity service.')
    #                 print('WARNING :: See https://redis.io/topics/security and http://antirez.com/news/96 for more info')
    # except Exception as e:
    #     pass

    try:
        if not isinstance(settings.OTHER_SKYLINE_REDIS_INSTANCES, list):
            settings_tested.append('OTHER_SKYLINE_REDIS_INSTANCES')
            print('error :: OTHER_SKYLINE_REDIS_INSTANCES in settings.py must be an empty list []')
            invalid_variables = True
        else:
            settings_tested.append('OTHER_SKYLINE_REDIS_INSTANCES')
            if len(settings.OTHER_SKYLINE_REDIS_INSTANCES) > 0:
                print('error :: OTHER_SKYLINE_REDIS_INSTANCES is a DEPRECATED and must now be an empty list []')
                print('warning :: this DEPRECATION warning is here to advise you that Skyline no longer')
                print('warning :: requires direct access to your remote Redis instances and now is the webapp API')
                print('warning :: API to retrieve metric Redis time series from remote Skyline cluster instances.')
                print('warning :: Please consider removing anyway firewalls rules that are no longer required and.')
                print('warning :: bind Redis to 127.0.0.1 only.')
                invalid_variables = True
    except AttributeError:
        print('error :: OTHER_SKYLINE_REDIS_INSTANCES is not defined in settings.py and must be an empty list []')
        invalid_variables = True
    except Exception as e:
        print('error :: the OTHER_SKYLINE_REDIS_INSTANCES in settings.py - %s' % e)
        invalid_variables = True

    # @added 20210606 - Task #4120: Add all settings to validate_settings.py tests
    settings_tested.append('SECRET_KEY')
    try:
        if not isinstance(settings.SECRET_KEY, str):
            print('error :: SECRET_KEY in settings.py is not a str')
            invalid_variables = True
    except AttributeError:
        print('error :: the SECRET_KEY str is not defined in settings.py')
        invalid_variables = True
    except Exception as e:
        print('error :: the SECRET_KEY str in settings.py - %s' % e)
        invalid_variables = True
    LOG_PATH = None
    settings_tested.append('LOG_PATH')
    try:

        if not isinstance(settings.LOG_PATH, str):
            print('error :: LOG_PATH in settings.py is not a str')
            invalid_variables = True
        else:
            LOG_PATH = settings.LOG_PATH
    except AttributeError:
        print('error :: the LOG_PATH str is not defined in settings.py')
        invalid_variables = True
    except Exception as e:
        print('error :: the LOG_PATH str in settings.py - %s' % e)
        invalid_variables = True
    if LOG_PATH:
        if not os.path.exists(LOG_PATH):
            print('error :: the LOG_PATH directory specificed in settings.py does not exist')
            invalid_variables = True
    PID_PATH = None
    settings_tested.append('PID_PATH')
    try:
        if not isinstance(settings.PID_PATH, str):
            print('error :: PID_PATH in settings.py is not a str')
            invalid_variables = True
        else:
            PID_PATH = settings.PID_PATH
    except AttributeError:
        print('error :: the PID_PATH str is not defined in settings.py')
        invalid_variables = True
    except Exception as e:
        print('error :: the PID_PATH str in settings.py - %s' % e)
        invalid_variables = True
    if PID_PATH:
        if not os.path.exists(PID_PATH):
            print('error :: the PID_PATH directory specificed in settings.py does not exist')
            invalid_variables = True
    SKYLINE_DIR = None
    settings_tested.append('SKYLINE_DIR')
    try:
        if not isinstance(settings.SKYLINE_DIR, str):
            print('error :: SKYLINE_DIR in settings.py is not a str')
            invalid_variables = True
        else:
            SKYLINE_DIR = settings.SKYLINE_DIR
    except AttributeError:
        print('error :: the SKYLINE_DIR str is not defined in settings.py')
        invalid_variables = True
    except Exception as e:
        print('error :: the SKYLINE_DIR str in settings.py - %s' % e)
        invalid_variables = True
    if SKYLINE_DIR:
        if not os.path.exists(SKYLINE_DIR):
            print('error :: the SKYLINE_DIR directory specificed in settings.py does not exist')
            invalid_variables = True

    SKYLINE_TMP_DIR = None
    settings_tested.append('SKYLINE_TMP_DIR')
    try:
        if not isinstance(settings.SKYLINE_TMP_DIR, str):
            print('error :: SKYLINE_TMP_DIR in settings.py is not a str')
            invalid_variables = True
        else:
            SKYLINE_TMP_DIR = settings.SKYLINE_TMP_DIR
    except AttributeError:
        print('error :: the SKYLINE_TMP_DIR str is not defined in settings.py')
        invalid_variables = True
    except Exception as e:
        print('error :: the SKYLINE_TMP_DIR str in settings.py - %s' % e)
        invalid_variables = True
    if SKYLINE_TMP_DIR:
        if not os.path.exists(SKYLINE_TMP_DIR):
            print('error :: the SKYLINE_TMP_DIR directory specificed in settings.py does not exist')
            invalid_variables = True

    settings_tested.append('FULL_NAMESPACE')
    try:
        if not isinstance(settings.FULL_NAMESPACE, str):
            print('error :: FULL_NAMESPACE in settings.py is not a str')
            invalid_variables = True
        else:
            if len(settings.FULL_NAMESPACE) == 0:
                print('error :: FULL_NAMESPACE in settings.py is an empty str, a FULL_NAMESPACE is required')
                invalid_variables = True
    except AttributeError:
        print('error :: the FULL_NAMESPACE str is not defined in settings.py')
        invalid_variables = True
    except Exception as e:
        print('error :: the FULL_NAMESPACE str in settings.py - %s' % e)
        invalid_variables = True

    settings_tested.append('ENABLE_DEBUG')
    try:
        if not isinstance(settings.ENABLE_DEBUG, bool):
            print('error :: ENABLE_DEBUG in settings.py is not a boolean')
            invalid_variables = True
    except AttributeError:
        print('error :: the ENABLE_DEBUG is not defined in settings.py')
        invalid_variables = True
    except Exception as e:
        print('error :: the ENABLE_DEBUG in settings.py - %s' % e)
        invalid_variables = True

    settings_tested.append('FULL_DURATION')
    try:
        if not isinstance(settings.FULL_DURATION, int):
            print('error :: FULL_DURATION in settings.py is not an int')
            invalid_variables = True
        else:
            if settings.FULL_DURATION == 0:
                print('error :: FULL_DURATION in settings.py is set to 0, this is invalid')
                invalid_variables = True
            if settings.FULL_DURATION > 86400:
                print('WARNING :: FULL_DURATION in settings.py is set to greater than 86400, this has Redis memory and Analyzer run_time implications')
    except AttributeError:
        print('error :: the FULL_DURATION is not defined in settings.py')
        invalid_variables = True
    except Exception as e:
        print('error :: the FULL_DURATION in settings.py - %s' % e)
        invalid_variables = True

    settings_tested.append('VERIFY_SSL')
    try:
        if not isinstance(settings.VERIFY_SSL, bool):
            print('error :: VERIFY_SSL in settings.py is not a boolean')
            invalid_variables = True
    except AttributeError:
        print('error :: the VERIFY_SSL is not defined in settings.py')
        invalid_variables = True
    except Exception as e:
        print('error :: the VERIFY_SSL in settings.py - %s' % e)
        invalid_variables = True

    settings_tested.append('GRAPHITE_HOST')
    try:
        if not isinstance(settings.GRAPHITE_HOST, str):
            print('error :: GRAPHITE_HOST in settings.py is not a str')
            invalid_variables = True
        else:
            if settings.GRAPHITE_HOST == 'YOUR_GRAPHITE_HOST.example.com':
                print('error :: GRAPHITE_HOST in settings.py is not set to a valid GRAPHITE_HOST')
                invalid_variables = True
    except AttributeError:
        print('error :: the GRAPHITE_HOST str is not defined in settings.py')
        invalid_variables = True
    except Exception as e:
        print('error :: the GRAPHITE_HOST str in settings.py - %s' % e)
        invalid_variables = True

    settings_tested.append('GRAPHITE_PROTOCOL')
    try:
        if not isinstance(settings.GRAPHITE_PROTOCOL, str):
            print('error :: GRAPHITE_PROTOCOL in settings.py is not a str')
            invalid_variables = True
        else:
            if settings.GRAPHITE_PROTOCOL not in ['http', 'https']:
                print('error :: GRAPHITE_PROTOCOL in settings.py is not set to a valid GRAPHITE_PROTOCOL')
                invalid_variables = True
    except AttributeError:
        print('error :: the GRAPHITE_PROTOCOL str is not defined in settings.py')
        invalid_variables = True
    except Exception as e:
        print('error :: the GRAPHITE_PROTOCOL str in settings.py - %s' % e)
        invalid_variables = True

    settings_tested.append('GRAPHITE_PORT')
    try:
        if not isinstance(settings.GRAPHITE_PORT, str):
            print('error :: GRAPHITE_PORT in settings.py is not a str')
            invalid_variables = True
        else:
            try:
                if int(settings.GRAPHITE_PORT) not in list(range(1, 65535)):
                    print('error :: GRAPHITE_PORT str in settings.py does not represent a valid port')
                    invalid_variables = True
            except ValueError:
                print('error :: GRAPHITE_PORT str in settings.py does not represent a valid port')
                invalid_variables = True
    except AttributeError:
        print('error :: the GRAPHITE_PORT str is not defined in settings.py')
        invalid_variables = True
    except Exception as e:
        print('error :: the GRAPHITE_PORT str in settings.py - %s' % e)
        invalid_variables = True

    settings_tested.append('GRAPHITE_CONNECT_TIMEOUT')
    try:
        if not isinstance(settings.GRAPHITE_CONNECT_TIMEOUT, int):
            print('error :: GRAPHITE_CONNECT_TIMEOUT in settings.py is not an int')
            invalid_variables = True
    except AttributeError:
        print('error :: the GRAPHITE_CONNECT_TIMEOUT is not defined in settings.py')
        invalid_variables = True
    except Exception as e:
        print('error :: the GRAPHITE_CONNECT_TIMEOUT in settings.py - %s' % e)
        invalid_variables = True

    settings_tested.append('GRAPHITE_READ_TIMEOUT')
    try:
        if not isinstance(settings.GRAPHITE_READ_TIMEOUT, int):
            print('error :: GRAPHITE_READ_TIMEOUT in settings.py is not an int')
            invalid_variables = True
    except AttributeError:
        print('error :: the GRAPHITE_READ_TIMEOUT is not defined in settings.py')
        invalid_variables = True
    except Exception as e:
        print('error :: the GRAPHITE_READ_TIMEOUT in settings.py - %s' % e)
        invalid_variables = True

    settings_tested.append('GRAPHITE_GRAPH_SETTINGS')
    try:
        if not isinstance(settings.GRAPHITE_GRAPH_SETTINGS, str):
            print('error :: GRAPHITE_GRAPH_SETTINGS in settings.py is not a str')
            invalid_variables = True
    except AttributeError:
        print('error :: the GRAPHITE_GRAPH_SETTINGS str is not defined in settings.py')
        invalid_variables = True
    except Exception as e:
        print('error :: the GRAPHITE_GRAPH_SETTINGS str in settings.py - %s' % e)
        invalid_variables = True

    settings_tested.append('TARGET_HOURS')
    try:
        if not isinstance(settings.TARGET_HOURS, str):
            print('error :: TARGET_HOURS in settings.py is not a str')
            invalid_variables = True
        else:
            try:
                int(settings.TARGET_HOURS)
            except ValueError:
                print('error :: TARGET_HOURS str in settings.py does not represent a valid number')
                invalid_variables = True
    except AttributeError:
        print('error :: the TARGET_HOURS str is not defined in settings.py')
        invalid_variables = True
    except Exception as e:
        print('error :: the TARGET_HOURS str in settings.py - %s' % e)
        invalid_variables = True

    settings_tested.append('GRAPHITE_RENDER_URI')
    try:
        if not isinstance(settings.GRAPHITE_RENDER_URI, str):
            print('error :: GRAPHITE_RENDER_URI in settings.py is not a str')
            invalid_variables = True
        else:
            if len(settings.GRAPHITE_RENDER_URI) == 0:
                print('error :: GRAPHITE_RENDER_URI in settings.py is an empty str, a GRAPHITE_RENDER_URI is required')
                invalid_variables = True
    except AttributeError:
        print('error :: the GRAPHITE_RENDER_URI str is not defined in settings.py')
        invalid_variables = True
    except Exception as e:
        print('error :: the GRAPHITE_RENDER_URI str in settings.py - %s' % e)
        invalid_variables = True

    settings_tested.append('GRAPH_URL')
    try:
        if not isinstance(settings.GRAPH_URL, str):
            print('error :: GRAPH_URL in settings.py is not a str')
            invalid_variables = True
        else:
            if len(settings.GRAPH_URL) == 0:
                print('error :: GRAPH_URL in settings.py is an empty str, a GRAPH_URL is required')
                invalid_variables = True
    except AttributeError:
        print('error :: the GRAPH_URL str is not defined in settings.py')
        invalid_variables = True
    except Exception as e:
        print('error :: the GRAPHITE_URL str in settings.py - %s' % e)
        invalid_variables = True

    settings_tested.append('CARBON_HOST')
    try:
        if not isinstance(settings.CARBON_HOST, str):
            print('error :: CARBON_HOST in settings.py is not a str')
            invalid_variables = True
        else:
            if len(settings.CARBON_HOST) == 0:
                print('error :: CARBON_HOST in settings.py is an empty str, a CARBON_HOST is required')
                invalid_variables = True
    except AttributeError:
        print('error :: the CARBON_HOST str is not defined in settings.py')
        invalid_variables = True
    except Exception as e:
        print('error :: the CARBON_HOST str in settings.py - %s' % e)
        invalid_variables = True

    settings_tested.append('CARBON_PORT')
    try:
        if settings.CARBON_PORT not in list(range(1, 65535)):
            print('error :: CARBON_PORT in settings.py does not represent a valid port')
            invalid_variables = True
    except ValueError:
        print('error :: CARBON_PORT in settings.py does not represent a valid port')
        invalid_variables = True
    except AttributeError:
        print('error :: the CARBON_PORT is not defined in settings.py')
        invalid_variables = True
    except Exception as e:
        print('error :: the CARBON_PORT in settings.py - %s' % e)
        invalid_variables = True

    settings_tested.append('SKYLINE_METRICS_CARBON_HOST')
    try:
        if not isinstance(settings.SKYLINE_METRICS_CARBON_HOST, str):
            print('error :: SKYLINE_METRICS_CARBON_HOST in settings.py is not a str')
            invalid_variables = True
        else:
            if len(settings.SKYLINE_METRICS_CARBON_HOST) == 0:
                print('error :: SKYLINE_METRICS_CARBON_HOST in settings.py is an empty str, a SKYLINE_METRICS_CARBON_HOST is required')
                invalid_variables = True
    except AttributeError:
        print('error :: the SKYLINE_METRICS_CARBON_HOST str is not defined in settings.py')
        invalid_variables = True
    except Exception as e:
        print('error :: the SKYLINE_METRICS_CARBON_HOST str in settings.py - %s' % e)
        invalid_variables = True

    settings_tested.append('SKYLINE_METRICS_CARBON_PORT')
    try:
        if settings.SKYLINE_METRICS_CARBON_PORT not in list(range(1, 65535)):
            print('error :: SKYLINE_METRICS_CARBON_PORT in settings.py does not represent a valid port')
            invalid_variables = True
    except ValueError:
        print('error :: SKYLINE_METRICS_CARBON_PORT in settings.py does not represent a valid port')
        invalid_variables = True
    except AttributeError:
        print('error :: the SKYLINE_METRICS_CARBON_PORT is not defined in settings.py')
        invalid_variables = True
    except Exception as e:
        print('error :: the SKYLINE_METRICS_CARBON_PORT in settings.py - %s' % e)
        invalid_variables = True

    settings_tested.append('SERVER_METRICS_NAME')
    try:
        if not isinstance(settings.SERVER_METRICS_NAME, str):
            print('error :: SERVER_METRICS_NAME in settings.py is not a str')
            invalid_variables = True
        else:
            if settings.SERVER_METRICS_NAME == 'YOUR_HOSTNAME':
                print('error :: SERVER_METRICS_NAME in settings.py is not set to a valid SERVER_METRICS_NAME this must be the hostname you want to record metrics as')
                invalid_variables = True
    except AttributeError:
        print('error :: the SERVER_METRICS_NAME str is not defined in settings.py')
        invalid_variables = True
    except Exception as e:
        print('error :: the SERVER_METRICS_NAME str in settings.py - %s' % e)
        invalid_variables = True

    settings_tested.append('SKYLINE_FEEDBACK_NAMESPACES')
    try:
        if not isinstance(settings.SKYLINE_FEEDBACK_NAMESPACES, list):
            print('error :: SKYLINE_FEEDBACK_NAMESPACES in settings.py is not a list')
            invalid_variables = True
    except AttributeError:
        print('error :: SKYLINE_FEEDBACK_NAMESPACES is not defined in settings.py')
        invalid_variables = True
    except Exception as e:
        print('error :: the SKYLINE_FEEDBACK_NAMESPACES list in settings.py - %s' % e)
        invalid_variables = True

    settings_tested.append('DO_NOT_SKIP_SKYLINE_FEEDBACK_NAMESPACES')
    try:
        if not isinstance(settings.DO_NOT_SKIP_SKYLINE_FEEDBACK_NAMESPACES, list):
            print('error :: DO_NOT_SKIP_SKYLINE_FEEDBACK_NAMESPACES in settings.py is not a list')
            invalid_variables = True
    except AttributeError:
        print('error :: DO_NOT_SKIP_SKYLINE_FEEDBACK_NAMESPACES is not defined in settings.py')
        invalid_variables = True
    except Exception as e:
        print('error :: the DO_NOT_SKIP_SKYLINE_FEEDBACK_NAMESPACES list in settings.py - %s' % e)
        invalid_variables = True

    settings_tested.append('CRUCIBLE_CHECK_PATH')
    CRUCIBLE_CHECK_PATH = None
    try:
        if not isinstance(settings.CRUCIBLE_CHECK_PATH, str):
            print('error :: CRUCIBLE_CHECK_PATH in settings.py is not a str')
            invalid_variables = True
        else:
            CRUCIBLE_CHECK_PATH = settings.CRUCIBLE_CHECK_PATH
    except AttributeError:
        print('error :: the CRUCIBLE_CHECK_PATH str is not defined in settings.py')
        invalid_variables = True
    except Exception as e:
        print('error :: the CRUCIBLE_CHECK_PATH str in settings.py - %s' % e)
        invalid_variables = True
    if CRUCIBLE_CHECK_PATH:
        if not os.path.exists(CRUCIBLE_CHECK_PATH):
            print('error :: the CRUCIBLE_CHECK_PATH directory specificed in settings.py does not exist')
            invalid_variables = True

    settings_tested.append('PANORAMA_CHECK_PATH')
    PANORAMA_CHECK_PATH = None
    try:
        if not isinstance(settings.PANORAMA_CHECK_PATH, str):
            print('error :: PANORAMA_CHECK_PATH in settings.py is not a str')
            invalid_variables = True
        else:
            PANORAMA_CHECK_PATH = settings.PANORAMA_CHECK_PATH
    except AttributeError:
        print('error :: the PANORAMA_CHECK_PATH str is not defined in settings.py')
        invalid_variables = True
    except Exception as e:
        print('error :: the PANORAMA_CHECK_PATH str in settings.py - %s' % e)
        invalid_variables = True
    if PANORAMA_CHECK_PATH:
        if not os.path.exists(PANORAMA_CHECK_PATH):
            print('error :: the PANORAMA_CHECK_PATH directory specificed in settings.py does not exist')
            invalid_variables = True

    settings_tested.append('DATA_UPLOADS_PATH')
    DATA_UPLOADS_PATH = None
    try:
        if not isinstance(settings.DATA_UPLOADS_PATH, str):
            print('error :: DATA_UPLOADS_PATH in settings.py is not a str')
            invalid_variables = True
        else:
            DATA_UPLOADS_PATH = settings.DATA_UPLOADS_PATH
    except AttributeError:
        print('error :: the DATA_UPLOADS_PATH str is not defined in settings.py')
        invalid_variables = True
    except Exception as e:
        print('error :: the DATA_UPLOADS_PATH str in settings.py - %s' % e)
        invalid_variables = True
    if DATA_UPLOADS_PATH:
        if not os.path.exists(os.path.dirname(DATA_UPLOADS_PATH)):
            print('error :: the DATA_UPLOADS_PATH parent directory specificed in settings.py does not exist')
            invalid_variables = True

    settings_tested.append('PANDAS_VERSION')
    try:
        if not isinstance(settings.PANDAS_VERSION, str):
            print('error :: PANDAS_VERSION in settings.py is not a str')
            invalid_variables = True
    except AttributeError:
        print('error :: the PANDAS_VERSION str is not defined in settings.py')
        invalid_variables = True
    except Exception as e:
        print('error :: the PANDAS_VERSION str in settings.py - %s' % e)
        invalid_variables = True

    settings_tested.append('ALERTERS_SETTINGS')
    try:
        if not isinstance(settings.ALERTERS_SETTINGS, bool):
            print('error :: ALERTERS_SETTINGS in settings.py is not a boolean')
            invalid_variables = True
    except AttributeError:
        print('error :: the ALERTERS_SETTINGS is not defined in settings.py')
        invalid_variables = True
    except Exception as e:
        print('error :: the ALERTERS_SETTINGS in settings.py - %s' % e)
        invalid_variables = True

    settings_tested.append('SYSLOG_ENABLED')
    try:
        if not isinstance(settings.SYSLOG_ENABLED, bool):
            print('error :: SYSLOG_ENABLED in settings.py is not a boolean')
            invalid_variables = True
    except AttributeError:
        print('error :: the SYSLOG_ENABLED is not defined in settings.py')
        invalid_variables = True
    except Exception as e:
        print('error :: the SYSLOG_ENABLED in settings.py - %s' % e)
        invalid_variables = True

    settings_tested.append('PAGERDUTY_ENABLED')
    try:
        if not isinstance(settings.PAGERDUTY_ENABLED, bool):
            print('error :: PAGERDUTY_ENABLED in settings.py is not a boolean')
            invalid_variables = True
    except AttributeError:
        print('error :: the PAGERDUTY_ENABLED is not defined in settings.py')
        invalid_variables = True
    except Exception as e:
        print('error :: the PAGERDUTY_ENABLED in settings.py - %s' % e)
        invalid_variables = True

    settings_tested.append('HTTP_ALERTERS_ENABLED')
    try:
        if not isinstance(settings.HTTP_ALERTERS_ENABLED, bool):
            print('error :: HTTP_ALERTERS_ENABLED in settings.py is not a boolean')
            invalid_variables = True
    except AttributeError:
        print('error :: the HTTP_ALERTERS_ENABLED is not defined in settings.py')
        invalid_variables = True
    except Exception as e:
        print('error :: the HTTP_ALERTERS_ENABLED in settings.py - %s' % e)
        invalid_variables = True

    settings_tested.append('START_IF_NO_DB')
    try:
        if not isinstance(settings.START_IF_NO_DB, bool):
            print('error :: START_IF_NO_DB in settings.py is not a boolean')
            invalid_variables = True
    except AttributeError:
        print('error :: the START_IF_NO_DB is not defined in settings.py')
        invalid_variables = True
    except Exception as e:
        print('error :: the START_IF_NO_DB in settings.py - %s' % e)
        invalid_variables = True

    settings_tested.append('ANALYZER_ENABLED')
    try:
        if not isinstance(settings.ANALYZER_ENABLED, bool):
            print('error :: ANALYZER_ENABLED in settings.py is not a boolean')
            invalid_variables = True
    except AttributeError:
        print('error :: the ANALYZER_ENABLED is not defined in settings.py')
        invalid_variables = True
    except Exception as e:
        print('error :: the ANALYZER_ENABLED in settings.py - %s' % e)
        invalid_variables = True

    settings_tested.append('ANALYZER_VERBOSE_LOGGING')
    try:
        if not isinstance(settings.ANALYZER_VERBOSE_LOGGING, bool):
            print('error :: ANALYZER_VERBOSE_LOGGING in settings.py is not a boolean')
            invalid_variables = True
    except AttributeError:
        print('error :: the ANALYZER_VERBOSE_LOGGING is not defined in settings.py')
        invalid_variables = True
    except Exception as e:
        print('error :: the ANALYZER_VERBOSE_LOGGING in settings.py - %s' % e)
        invalid_variables = True

    settings_tested.append('ANOMALY_DUMP')
    ANOMALY_DUMP = None
    try:
        if not isinstance(settings.ANOMALY_DUMP, str):
            print('error :: ANOMALY_DUMP in settings.py is not a str')
            invalid_variables = True
        else:
            ANOMALY_DUMP = settings.ANOMALY_DUMP
    except AttributeError:
        print('error :: the ANOMALY_DUMP str is not defined in settings.py')
        invalid_variables = True
    except Exception as e:
        print('error :: the ANOMALY_DUMP str in settings.py - %s' % e)
        invalid_variables = True
    if ANOMALY_DUMP:
        anomaly_dump_path = '%s/%s' % (
            os.path.dirname(__file__), os.path.dirname(ANOMALY_DUMP))
        if not os.path.exists(anomaly_dump_path):
            print('error :: the directory of the file specified in ANOMALY_DUMP in settings.py does not exist')
            invalid_variables = True

    settings_tested.append('STALE_PERIOD')
    try:
        if not isinstance(settings.STALE_PERIOD, int):
            print('error :: STALE_PERIOD in settings.py is not an int')
            invalid_variables = True
    except AttributeError:
        print('error :: the STALE_PERIOD is not defined in settings.py')
        invalid_variables = True
    except Exception as e:
        print('error :: the STALE_PERIOD in settings.py - %s' % e)
        invalid_variables = True

    settings_tested.append('ALERT_ON_STALE_METRICS')
    try:
        if not isinstance(settings.ALERT_ON_STALE_METRICS, bool):
            print('error :: ALERT_ON_STALE_METRICS in settings.py is not a boolean')
            invalid_variables = True
    except AttributeError:
        print('error :: the ALERT_ON_STALE_METRICS is not defined in settings.py')
        invalid_variables = True
    except Exception as e:
        print('error :: the ALERT_ON_STALE_METRICS in settings.py - %s' % e)
        invalid_variables = True

    settings_tested.append('ALERT_ON_STALE_PERIOD')
    try:
        if not isinstance(settings.ALERT_ON_STALE_PERIOD, int):
            print('error :: ALERT_ON_STALE_PERIOD in settings.py is not an int')
            invalid_variables = True
    except AttributeError:
        print('error :: the ALERT_ON_STALE_PERIOD is not defined in settings.py')
        invalid_variables = True
    except Exception as e:
        print('error :: the ALERT_ON_STALE_PERIOD in settings.py - %s' % e)
        invalid_variables = True

    settings_tested.append('MIN_TOLERABLE_LENGTH')
    try:
        if not isinstance(settings.MIN_TOLERABLE_LENGTH, int):
            print('error :: MIN_TOLERABLE_LENGTH in settings.py is not an int')
            invalid_variables = True
    except AttributeError:
        print('error :: the MIN_TOLERABLE_LENGTH is not defined in settings.py')
        invalid_variables = True
    except Exception as e:
        print('error :: the MIN_TOLERABLE_LENGTH in settings.py - %s' % e)
        invalid_variables = True

    settings_tested.append('MAX_TOLERABLE_BOREDOM')
    try:
        if not isinstance(settings.MAX_TOLERABLE_BOREDOM, int):
            print('error :: MAX_TOLERABLE_BOREDOM in settings.py is not an int')
            invalid_variables = True
    except AttributeError:
        print('error :: the MAX_TOLERABLE_BOREDOM is not defined in settings.py')
        invalid_variables = True
    except Exception as e:
        print('error :: the MAX_TOLERABLE_BOREDOM in settings.py - %s' % e)
        invalid_variables = True

    settings_tested.append('IDENTIFY_AIRGAPS')
    try:
        if not isinstance(settings.IDENTIFY_AIRGAPS, bool):
            print('error :: IDENTIFY_AIRGAPS in settings.py is not a boolean')
            invalid_variables = True
    except AttributeError:
        print('error :: the IDENTIFY_AIRGAPS is not defined in settings.py')
        invalid_variables = True
    except Exception as e:
        print('error :: the IDENTIFY_AIRGAPS in settings.py - %s' % e)
        invalid_variables = True

    settings_tested.append('MAX_AIRGAP_PERIOD')
    try:
        if not isinstance(settings.MAX_AIRGAP_PERIOD, int):
            print('error :: MAX_AIRGAP_PERIOD in settings.py is not an int')
            invalid_variables = True
    except AttributeError:
        print('error :: the MAX_AIRGAP_PERIOD is not defined in settings.py')
        invalid_variables = True
    except Exception as e:
        print('error :: the MAX_AIRGAP_PERIOD in settings.py - %s' % e)
        invalid_variables = True

    settings_tested.append('CHECK_AIRGAPS')
    try:
        if not isinstance(settings.CHECK_AIRGAPS, list):
            print('error :: CHECK_AIRGAPS in settings.py is not a list')
            invalid_variables = True
    except AttributeError:
        print('error :: CHECK_AIRGAPS is not defined in settings.py')
        invalid_variables = True
    except Exception as e:
        print('error :: the CHECK_AIRGAPS list in settings.py - %s' % e)
        invalid_variables = True

    settings_tested.append('SKIP_AIRGAPS')
    try:
        if not isinstance(settings.SKIP_AIRGAPS, list):
            print('error :: SKIP_AIRGAPS in settings.py is not a list')
            invalid_variables = True
    except AttributeError:
        print('error :: SKIP_AIRGAPS is not defined in settings.py')
        invalid_variables = True
    except Exception as e:
        print('error :: the SKIP_AIRGAPS list in settings.py - %s' % e)
        invalid_variables = True

    settings_tested.append('IDENTIFY_UNORDERED_TIMESERIES')
    try:
        if not isinstance(settings.IDENTIFY_UNORDERED_TIMESERIES, bool):
            print('error :: IDENTIFY_UNORDERED_TIMESERIES in settings.py is not a boolean')
            invalid_variables = True
    except AttributeError:
        print('error :: the IDENTIFY_UNORDERED_TIMESERIES is not defined in settings.py')
        invalid_variables = True
    except Exception as e:
        print('error :: the IDENTIFY_UNORDERED_TIMESERIES in settings.py - %s' % e)
        invalid_variables = True

    settings_tested.append('CHECK_DATA_SPARSITY')
    try:
        if not isinstance(settings.CHECK_DATA_SPARSITY, bool):
            print('error :: CHECK_DATA_SPARSITY in settings.py is not a boolean')
            invalid_variables = True
    except AttributeError:
        print('error :: the CHECK_DATA_SPARSITY is not defined in settings.py')
        invalid_variables = True
    except Exception as e:
        print('error :: the CHECK_DATA_SPARSITY in settings.py - %s' % e)
        invalid_variables = True

    settings_tested.append('SKIP_CHECK_DATA_SPARSITY_NAMESPACES')
    try:
        if not isinstance(settings.SKIP_CHECK_DATA_SPARSITY_NAMESPACES, list):
            print('error :: SKIP_CHECK_DATA_SPARSITY_NAMESPACES in settings.py is not a list')
            invalid_variables = True
    except AttributeError:
        print('error :: SKIP_CHECK_DATA_SPARSITY_NAMESPACES is not defined in settings.py')
        invalid_variables = True
    except Exception as e:
        print('error :: the SKIP_CHECK_DATA_SPARSITY_NAMESPACES list in settings.py - %s' % e)
        invalid_variables = True

    settings_tested.append('ANALYZER_CHECK_LAST_TIMESTAMP')
    try:
        if not isinstance(settings.ANALYZER_CHECK_LAST_TIMESTAMP, bool):
            print('error :: ANALYZER_CHECK_LAST_TIMESTAMP in settings.py is not a boolean')
            invalid_variables = True
    except AttributeError:
        print('error :: the ANALYZER_CHECK_LAST_TIMESTAMP is not defined in settings.py')
        invalid_variables = True
    except Exception as e:
        print('error :: the ANALYZER_CHECK_LAST_TIMESTAMP in settings.py - %s' % e)
        invalid_variables = True

    settings_tested.append('BATCH_PROCESSING')
    try:
        if not isinstance(settings.BATCH_PROCESSING, bool):
            print('error :: BATCH_PROCESSING in settings.py is not a boolean')
            invalid_variables = True
    except AttributeError:
        print('error :: the BATCH_PROCESSING is not defined in settings.py')
        invalid_variables = True
    except Exception as e:
        print('error :: the BATCH_PROCESSING in settings.py - %s' % e)
        invalid_variables = True

    settings_tested.append('BATCH_PROCESSING_STALE_PERIOD')
    try:
        if not isinstance(settings.BATCH_PROCESSING_STALE_PERIOD, int):
            print('error :: BATCH_PROCESSING_STALE_PERIOD in settings.py is not an int')
            invalid_variables = True
    except AttributeError:
        print('error :: the BATCH_PROCESSING_STALE_PERIOD is not defined in settings.py')
        invalid_variables = True
    except Exception as e:
        print('error :: the BATCH_PROCESSING_STALE_PERIOD in settings.py - %s' % e)
        invalid_variables = True

    settings_tested.append('BATCH_PROCESSING_DEBUG')
    try:
        if not isinstance(settings.BATCH_PROCESSING_DEBUG, bool):
            print('error :: BATCH_PROCESSING_DEBUG in settings.py is not a boolean')
            invalid_variables = True
    except AttributeError:
        print('error :: the BATCH_PROCESSING_DEBUG is not defined in settings.py')
        invalid_variables = True
    except Exception as e:
        print('error :: the BATCH_PROCESSING_DEBUG in settings.py - %s' % e)
        invalid_variables = True

    settings_tested.append('BATCH_PROCESSING_NAMESPACES')
    try:
        if not isinstance(settings.BATCH_PROCESSING_NAMESPACES, list):
            print('error :: BATCH_PROCESSING_NAMESPACES in settings.py is not a list')
            invalid_variables = True
    except AttributeError:
        print('error :: BATCH_PROCESSING_NAMESPACES is not defined in settings.py')
        invalid_variables = True
    except Exception as e:
        print('error :: the BATCH_PROCESSING_NAMESPACES list in settings.py - %s' % e)
        invalid_variables = True

    settings_tested.append('METRICS_INACTIVE_AFTER')
    try:
        if not isinstance(settings.METRICS_INACTIVE_AFTER, int):
            print('error :: METRICS_INACTIVE_AFTER in settings.py is not an int')
            invalid_variables = True
    except AttributeError:
        print('error :: the METRICS_INACTIVE_AFTER is not defined in settings.py')
        invalid_variables = True
    except Exception as e:
        print('error :: the METRICS_INACTIVE_AFTER in settings.py - %s' % e)
        invalid_variables = True

    settings_tested.append('CANARY_METRIC')
    try:
        if not isinstance(settings.CANARY_METRIC, str):
            print('error :: CANARY_METRIC in settings.py is not a str')
            invalid_variables = True
    except AttributeError:
        print('error :: the CANARY_METRIC str is not defined in settings.py')
        invalid_variables = True
    except Exception as e:
        print('error :: the CANARY_METRIC str in settings.py - %s' % e)
        invalid_variables = True

    settings_tested.append('ALGORITHMS')
    try:
        if not isinstance(settings.ALGORITHMS, list):
            print('error :: ALGORITHMS in settings.py is not a list')
            invalid_variables = True
    except AttributeError:
        print('error :: ALGORITHMS is not defined in settings.py')
        invalid_variables = True
    except Exception as e:
        print('error :: the ALGORITHMS list in settings.py - %s' % e)
        invalid_variables = True

    settings_tested.append('ANALYZER_PROCESSES')
    try:
        if not isinstance(settings.ANALYZER_PROCESSES, int):
            print('error :: ANALYZER_PROCESSES in settings.py is not an int')
            invalid_variables = True
    except AttributeError:
        print('error :: the ANALYZER_PROCESSES is not defined in settings.py')
        invalid_variables = True
    except Exception as e:
        print('error :: the ANALYZER_PROCESSES in settings.py - %s' % e)
        invalid_variables = True

    settings_tested.append('CONSENSUS')
    try:
        if not isinstance(settings.CONSENSUS, int):
            print('error :: CONSENSUS in settings.py is not an int')
            invalid_variables = True
        else:
            if settings.CONSENSUS == 0 or settings.CONSENSUS > len(settings.ALGORITHMS):
                print('error :: CONSENSUS in settings.py is not a valid value for CONSENSUS')
                invalid_variables = True
    except AttributeError:
        print('error :: the CONSENSUS is not defined in settings.py')
        invalid_variables = True
    except Exception as e:
        print('error :: the CONSENSUS in settings.py - %s' % e)
        invalid_variables = True

    settings_tested.append('ANALYZER_ANALYZE_LOW_PRIORITY_METRICS')
    try:
        if not isinstance(settings.ANALYZER_ANALYZE_LOW_PRIORITY_METRICS, bool):
            print('error :: ANALYZER_ANALYZE_LOW_PRIORITY_METRICS in settings.py is not a boolean')
            invalid_variables = True
    except AttributeError:
        print('error :: the ANALYZER_ANALYZE_LOW_PRIORITY_METRICS is not defined in settings.py')
        invalid_variables = True
    except Exception as e:
        print('error :: the ANALYZER_ANALYZE_LOW_PRIORITY_METRICS in settings.py - %s' % e)
        invalid_variables = True

    settings_tested.append('ANALYZER_DYNAMICALLY_ANALYZE_LOW_PRIORITY_METRICS')
    try:
        if not isinstance(settings.ANALYZER_DYNAMICALLY_ANALYZE_LOW_PRIORITY_METRICS, bool):
            print('error :: ANALYZER_DYNAMICALLY_ANALYZE_LOW_PRIORITY_METRICS in settings.py is not a boolean')
            invalid_variables = True
    except AttributeError:
        print('error :: the ANALYZER_DYNAMICALLY_ANALYZE_LOW_PRIORITY_METRICS is not defined in settings.py')
        invalid_variables = True
    except Exception as e:
        print('error :: the ANALYZER_DYNAMICALLY_ANALYZE_LOW_PRIORITY_METRICS in settings.py - %s' % e)
        invalid_variables = True

    settings_tested.append('ANALYZER_MAD_LOW_PRIORITY_METRICS')
    try:
        if not isinstance(settings.ANALYZER_MAD_LOW_PRIORITY_METRICS, int):
            print('error :: ANALYZER_MAD_LOW_PRIORITY_METRICS in settings.py is not an int')
            invalid_variables = True
        else:
            if settings.ANALYZER_MAD_LOW_PRIORITY_METRICS > 15:
                print('WARNING :: ANALYZER_MAD_LOW_PRIORITY_METRICS in settings.py is greater than 15, will result in a performance loss')
                invalid_variables = True
    except AttributeError:
        print('error :: the ANALYZER_MAD_LOW_PRIORITY_METRICS is not defined in settings.py')
        invalid_variables = True
    except Exception as e:
        print('error :: the ANALYZER_MAD_LOW_PRIORITY_METRICS in settings.py - %s' % e)
        invalid_variables = True

    settings_tested.append('ANALYZER_SKIP')
    try:
        if not isinstance(settings.ANALYZER_SKIP, list):
            print('error :: ANALYZER_SKIP in settings.py is not a list')
            invalid_variables = True
    except AttributeError:
        print('error :: ANALYZER_SKIP is not defined in settings.py')
        invalid_variables = True
    except Exception as e:
        print('error :: the ANALYZER_SKIP list in settings.py - %s' % e)
        invalid_variables = True

    # @added 20210605 - Feature #4118: crucible - custom_algorithms
    #                   Feature #3566: custom_algorithms
    settings_tested.append('CUSTOM_ALGORITHMS')
    try:
        if not isinstance(settings.CUSTOM_ALGORITHMS, dict):
            print('error :: CUSTOM_ALGORITHMS in settings.py is not a dict')
            invalid_variables = True
    except AttributeError:
        print('error :: the CUSTOM_ALGORITHMS dict is not defined in settings.py')
        invalid_variables = True
    except Exception as e:
        print('error :: the CUSTOM_ALGORITHMS dict is not defined in settings.py - %s' % e)
        invalid_variables = True

    settings_tested.append('DEBUG_CUSTOM_ALGORITHMS')
    try:
        if not isinstance(settings.DEBUG_CUSTOM_ALGORITHMS, bool):
            print('error :: DEBUG_CUSTOM_ALGORITHMS in settings.py is not a boolean')
            invalid_variables = True
    except AttributeError:
        print('error :: the DEBUG_CUSTOM_ALGORITHMS is not defined in settings.py')
        invalid_variables = True
    except Exception as e:
        print('error :: the DEBUG_CUSTOM_ALGORITHMS in settings.py - %s' % e)
        invalid_variables = True

    settings_tested.append('RUN_OPTIMIZED_WORKFLOW')
    try:
        if not isinstance(settings.RUN_OPTIMIZED_WORKFLOW, bool):
            print('error :: RUN_OPTIMIZED_WORKFLOW in settings.py is not a boolean')
            invalid_variables = True
    except AttributeError:
        print('error :: the RUN_OPTIMIZED_WORKFLOW is not defined in settings.py')
        invalid_variables = True
    except Exception as e:
        print('error :: the RUN_OPTIMIZED_WORKFLOW in settings.py - %s' % e)
        invalid_variables = True

    settings_tested.append('ENABLE_ALGORITHM_RUN_METRICS')
    try:
        if not isinstance(settings.ENABLE_ALGORITHM_RUN_METRICS, bool):
            print('error :: ENABLE_ALGORITHM_RUN_METRICS in settings.py is not a boolean')
            invalid_variables = True
    except AttributeError:
        print('error :: the ENABLE_ALGORITHM_RUN_METRICS is not defined in settings.py')
        invalid_variables = True
    except Exception as e:
        print('error :: the ENABLE_ALGORITHM_RUN_METRICS in settings.py - %s' % e)
        invalid_variables = True

    settings_tested.append('ENABLE_ALL_ALGORITHMS_RUN_METRICS')
    try:
        if not isinstance(settings.ENABLE_ALL_ALGORITHMS_RUN_METRICS, bool):
            print('error :: ENABLE_ALL_ALGORITHMS_RUN_METRICS in settings.py is not a boolean')
            invalid_variables = True
    except AttributeError:
        print('error :: the ENABLE_ALL_ALGORITHMS_RUN_METRICS is not defined in settings.py')
        invalid_variables = True
    except Exception as e:
        print('error :: the ENABLE_ALL_ALGORITHMS_RUN_METRICS in settings.py - %s' % e)
        invalid_variables = True

    settings_tested.append('ENABLE_SECOND_ORDER')
    try:
        if not isinstance(settings.ENABLE_SECOND_ORDER, bool):
            print('error :: ENABLE_SECOND_ORDER in settings.py is not a boolean')
            invalid_variables = True
    except AttributeError:
        print('error :: the ENABLE_SECOND_ORDER is not defined in settings.py')
        invalid_variables = True
    except Exception as e:
        print('error :: the ENABLE_SECOND_ORDER in settings.py - %s' % e)
        invalid_variables = True

    settings_tested.append('ENABLE_ALERTS')
    try:
        if not isinstance(settings.ENABLE_ALERTS, bool):
            print('error :: ENABLE_ALERTS in settings.py is not a boolean')
            invalid_variables = True
    except AttributeError:
        print('error :: the ENABLE_ALERTS is not defined in settings.py')
        invalid_variables = True
    except Exception as e:
        print('error :: the ENABLE_ALERTS in settings.py - %s' % e)
        invalid_variables = True

    settings_tested.append('ENABLE_MIRAGE')
    try:
        if not isinstance(settings.ENABLE_MIRAGE, bool):
            print('error :: ENABLE_MIRAGE in settings.py is not a boolean')
            invalid_variables = True
    except AttributeError:
        print('error :: the ENABLE_MIRAGE is not defined in settings.py')
        invalid_variables = True
    except Exception as e:
        print('error :: the ENABLE_MIRAGE in settings.py - %s' % e)
        invalid_variables = True

    settings_tested.append('ENABLE_FULL_DURATION_ALERTS')
    try:
        if not isinstance(settings.ENABLE_FULL_DURATION_ALERTS, bool):
            print('error :: ENABLE_FULL_DURATION_ALERTS in settings.py is not a boolean')
            invalid_variables = True
    except AttributeError:
        print('error :: the ENABLE_FULL_DURATION_ALERTS is not defined in settings.py')
        invalid_variables = True
    except Exception as e:
        print('error :: the ENABLE_FULL_DURATION_ALERTS in settings.py - %s' % e)
        invalid_variables = True

    settings_tested.append('ANALYZER_CRUCIBLE_ENABLED')
    try:
        if not isinstance(settings.ANALYZER_CRUCIBLE_ENABLED, bool):
            print('error :: ANALYZER_CRUCIBLE_ENABLED in settings.py is not a boolean')
            invalid_variables = True
    except AttributeError:
        print('error :: the ANALYZER_CRUCIBLE_ENABLED is not defined in settings.py')
        invalid_variables = True
    except Exception as e:
        print('error :: the ANALYZER_CRUCIBLE_ENABLED in settings.py - %s' % e)
        invalid_variables = True

    # @added 20181009 - Feature #2618: alert_slack
    settings_tested.append('ALERTS')
    TEST_ALERTS = None
    try:
        if not isinstance(settings.ALERTS, tuple):
            print('error :: ALERTS in settings.py is not a tuple')
            invalid_variables = True
        else:
            TEST_ALERTS = settings.ALERTS
    except AttributeError:
        print('error :: the ALERTS is not defined in settings.py')
        invalid_variables = True
    except Exception as e:
        print('error :: the ALERTS in settings.py - %s' % e)
        invalid_variables = True

    settings_tested.append('EXTERNAL_ALERTS')
    try:
        if not isinstance(settings.EXTERNAL_ALERTS, dict):
            print('error :: EXTERNAL_ALERTS in settings.py is not a dict')
            invalid_variables = True
    except AttributeError:
        print('error :: the EXTERNAL_ALERTS dict is not defined in settings.py')
        invalid_variables = True
    except Exception as e:
        print('error :: the EXTERNAL_ALERTS dict is not defined in settings.py - %s' % e)
        invalid_variables = True

    settings_tested.append('DO_NOT_ALERT_ON_STALE_METRICS')
    try:
        if not isinstance(settings.DO_NOT_ALERT_ON_STALE_METRICS, list):
            print('error :: DO_NOT_ALERT_ON_STALE_METRICS in settings.py is not a list')
            invalid_variables = True
    except AttributeError:
        print('error :: DO_NOT_ALERT_ON_STALE_METRICS is not defined in settings.py')
        invalid_variables = True
    except Exception as e:
        print('error :: the DO_NOT_ALERT_ON_STALE_METRICS list in settings.py - %s' % e)
        invalid_variables = True

    settings_tested.append('PLOT_REDIS_DATA')
    try:
        if not isinstance(settings.PLOT_REDIS_DATA, bool):
            print('error :: PLOT_REDIS_DATA in settings.py is not a boolean')
            invalid_variables = True
    except AttributeError:
        print('error :: the PLOT_REDIS_DATA is not defined in settings.py')
        invalid_variables = True
    except Exception as e:
        print('error :: the PLOT_REDIS_DATA in settings.py - %s' % e)
        invalid_variables = True

    settings_tested.append('NON_DERIVATIVE_MONOTONIC_METRICS')
    try:
        if not isinstance(settings.NON_DERIVATIVE_MONOTONIC_METRICS, list):
            print('error :: NON_DERIVATIVE_MONOTONIC_METRICS in settings.py is not a list')
            invalid_variables = True
    except AttributeError:
        print('error :: NON_DERIVATIVE_MONOTONIC_METRICS is not defined in settings.py')
        invalid_variables = True
    except Exception as e:
        print('error :: the NON_DERIVATIVE_MONOTONIC_METRICS list in settings.py - %s' % e)
        invalid_variables = True

    settings_tested.append('SMTP_OPTS')
    try:
        if not isinstance(settings.SMTP_OPTS, dict):
            print('error :: SMTP_OPTS in settings.py is not a dict')
            invalid_variables = True
    except AttributeError:
        print('error :: the SMTP_OPTS dict is not defined in settings.py')
        invalid_variables = True
    except Exception as e:
        print('error :: the SMTP_OPTS dict is not defined in settings.py - %s' % e)
        invalid_variables = True

    settings_tested.append('SLACK_OPTS')
    try:
        if not isinstance(settings.SLACK_OPTS, dict):
            print('error :: SLACK_OPTS in settings.py is not a dict')
            invalid_variables = True
    except AttributeError:
        print('error :: the SLACK_OPTS dict is not defined in settings.py')
        invalid_variables = True
    except Exception as e:
        print('error :: the SLACK_OPTS dict is not defined in settings.py - %s' % e)
        invalid_variables = True

    settings_tested.append('PAGERDUTY_OPTS')
    try:
        if not isinstance(settings.PAGERDUTY_OPTS, dict):
            print('error :: PAGERDUTY_OPTS in settings.py is not a dict')
            invalid_variables = True
    except AttributeError:
        print('error :: the PAGERDUTY_OPTS dict is not defined in settings.py')
        invalid_variables = True
    except Exception as e:
        print('error :: the PAGERDUTY_OPTS dict is not defined in settings.py - %s' % e)
        invalid_variables = True

    settings_tested.append('SYSLOG_OPTS')
    try:
        if not isinstance(settings.SYSLOG_OPTS, dict):
            print('error :: SYSLOG_OPTS in settings.py is not a dict')
            invalid_variables = True
    except AttributeError:
        print('error :: the SYSLOG_OPTS dict is not defined in settings.py')
        invalid_variables = True
    except Exception as e:
        print('error :: the SYSLOG_OPTS dict is not defined in settings.py - %s' % e)
        invalid_variables = True

    settings_tested.append('HTTP_ALERTERS_OPTS')
    try:
        if not isinstance(settings.HTTP_ALERTERS_OPTS, dict):
            print('error :: HTTP_ALERTERS_OPTS in settings.py is not a dict')
            invalid_variables = True
    except AttributeError:
        print('error :: the HTTP_ALERTERS_OPTS dict is not defined in settings.py')
        invalid_variables = True
    except Exception as e:
        print('error :: the HTTP_ALERTERS_OPTS dict is not defined in settings.py - %s' % e)
        invalid_variables = True

    settings_tested.append('CUSTOM_ALERT_OPTS')
    try:
        if not isinstance(settings.CUSTOM_ALERT_OPTS, dict):
            print('error :: CUSTOM_ALERT_OPTS in settings.py is not a dict')
            invalid_variables = True
    except AttributeError:
        print('error :: the CUSTOM_ALERT_OPTS dict is not defined in settings.py')
        invalid_variables = True
    except Exception as e:
        print('error :: the CUSTOM_ALERT_OPTS dict is not defined in settings.py - %s' % e)
        invalid_variables = True

    settings_tested.append('WORKER_PROCESSES')
    try:
        if not isinstance(settings.WORKER_PROCESSES, int):
            print('error :: WORKER_PROCESSES in settings.py is not an int')
            invalid_variables = True
        else:
            if settings.WORKER_PROCESSES == 0:
                print('error :: WORKER_PROCESSES in settings.py is not a valid value for WORKER_PROCESSES')
                invalid_variables = True
    except AttributeError:
        print('error :: the WORKER_PROCESSES is not defined in settings.py')
        invalid_variables = True
    except Exception as e:
        print('error :: the WORKER_PROCESSES in settings.py - %s' % e)
        invalid_variables = True

    settings_tested.append('HORIZON_IP')
    try:
        if not isinstance(settings.HORIZON_IP, str):
            print('error :: HORIZON_IP in settings.py is not a str')
            invalid_variables = True
    except AttributeError:
        print('error :: the HORIZON_IP str is not defined in settings.py')
        invalid_variables = True
    except Exception as e:
        print('error :: the HORIZON_IP str in settings.py - %s' % e)
        invalid_variables = True

    settings_tested.append('PICKLE_PORT')
    try:
        if settings.PICKLE_PORT not in list(range(1, 65535)):
            print('error :: PICKLE_PORT in settings.py does not represent a valid port')
            invalid_variables = True
    except ValueError:
        print('error :: PICKLE_PORT in settings.py does not represent a valid port')
        invalid_variables = True
    except AttributeError:
        print('error :: the PICKLE_PORT is not defined in settings.py')
        invalid_variables = True
    except Exception as e:
        print('error :: the PICKLE_PORT in settings.py - %s' % e)
        invalid_variables = True

    settings_tested.append('UDP_PORT')
    try:
        if settings.UDP_PORT not in list(range(1, 65535)):
            print('error :: UDP_PORT in settings.py does not represent a valid port')
            invalid_variables = True
    except ValueError:
        print('error :: UDP_PORT in settings.py does not represent a valid port')
        invalid_variables = True
    except AttributeError:
        print('error :: the UDP_PORT is not defined in settings.py')
        invalid_variables = True
    except Exception as e:
        print('error :: the UDP_PORT in settings.py - %s' % e)
        invalid_variables = True

    settings_tested.append('CHUNK_SIZE')
    try:
        if not isinstance(settings.CHUNK_SIZE, int):
            print('error :: CHUNK_SIZE in settings.py is not an int')
            invalid_variables = True
    except AttributeError:
        print('error :: the CHUNK_SIZE is not defined in settings.py')
        invalid_variables = True
    except Exception as e:
        print('error :: the CHUNK_SIZE in settings.py - %s' % e)
        invalid_variables = True

    settings_tested.append('MAX_QUEUE_SIZE')
    try:
        if not isinstance(settings.MAX_QUEUE_SIZE, int):
            print('error :: MAX_QUEUE_SIZE in settings.py is not an int')
            invalid_variables = True
    except AttributeError:
        print('error :: the MAX_QUEUE_SIZE is not defined in settings.py')
        invalid_variables = True
    except Exception as e:
        print('error :: the MAX_QUEUE_SIZE in settings.py - %s' % e)
        invalid_variables = True

    settings_tested.append('ROOMBA_PROCESSES')
    try:
        if not isinstance(settings.ROOMBA_PROCESSES, int):
            print('error :: ROOMBA_PROCESSES in settings.py is not an int')
            invalid_variables = True
    except AttributeError:
        print('error :: the ROOMBA_PROCESSES is not defined in settings.py')
        invalid_variables = True
    except Exception as e:
        print('error :: the ROOMBA_PROCESSES in settings.py - %s' % e)
        invalid_variables = True

    settings_tested.append('ROOMBA_GRACE_TIME')
    try:
        if not isinstance(settings.ROOMBA_GRACE_TIME, int):
            print('error :: ROOMBA_GRACE_TIME in settings.py is not an int')
            invalid_variables = True
    except AttributeError:
        print('error :: the ROOMBA_GRACE_TIME is not defined in settings.py')
        invalid_variables = True
    except Exception as e:
        print('error :: the ROOMBA_GRACE_TIME in settings.py - %s' % e)
        invalid_variables = True

    settings_tested.append('ROOMBA_OPTIMUM_RUN_DURATION')
    try:
        if not isinstance(settings.ROOMBA_OPTIMUM_RUN_DURATION, int):
            print('error :: ROOMBA_OPTIMUM_RUN_DURATION in settings.py is not an int')
            invalid_variables = True
    except AttributeError:
        print('error :: the ROOMBA_OPTIMUM_RUN_DURATION is not defined in settings.py')
        invalid_variables = True
    except Exception as e:
        print('error :: the ROOMBA_OPTIMUM_RUN_DURATION in settings.py - %s' % e)
        invalid_variables = True

    settings_tested.append('ROOMBA_TIMEOUT')
    try:
        if not isinstance(settings.ROOMBA_TIMEOUT, int):
            print('error :: ROOMBA_TIMEOUT in settings.py is not an int')
            invalid_variables = True
    except AttributeError:
        print('error :: the ROOMBA_TIMEOUT is not defined in settings.py')
        invalid_variables = True
    except Exception as e:
        print('error :: the ROOMBA_TIMEOUT in settings.py - %s' % e)
        invalid_variables = True

    settings_tested.append('ROOMBA_DO_NOT_PROCESS_BATCH_METRICS')
    try:
        if not isinstance(settings.ROOMBA_DO_NOT_PROCESS_BATCH_METRICS, bool):
            print('error :: ROOMBA_DO_NOT_PROCESS_BATCH_METRICS in settings.py is not a boolean')
            invalid_variables = True
    except AttributeError:
        print('error :: the ROOMBA_DO_NOT_PROCESS_BATCH_METRICS is not defined in settings.py')
        invalid_variables = True
    except Exception as e:
        print('error :: the ROOMBA_DO_NOT_PROCESS_BATCH_METRICS in settings.py - %s' % e)
        invalid_variables = True

    settings_tested.append('ROOMBA_BATCH_METRICS_CUSTOM_DURATIONS')
    try:
        if not isinstance(settings.ROOMBA_BATCH_METRICS_CUSTOM_DURATIONS, list):
            print('error :: ROOMBA_BATCH_METRICS_CUSTOM_DURATIONS in settings.py is not a list')
            invalid_variables = True
    except AttributeError:
        print('error :: ROOMBA_BATCH_METRICS_CUSTOM_DURATIONS is not defined in settings.py')
        invalid_variables = True
    except Exception as e:
        print('error :: the ROOMBA_BATCH_METRICS_CUSTOM_DURATIONS list in settings.py - %s' % e)
        invalid_variables = True

    settings_tested.append('MAX_RESOLUTION')
    try:
        if not isinstance(settings.MAX_RESOLUTION, int):
            print('error :: MAX_RESOLUTION in settings.py is not an int')
            invalid_variables = True
    except AttributeError:
        print('error :: the MAX_RESOLUTION is not defined in settings.py')
        invalid_variables = True
    except Exception as e:
        print('error :: the MAX_RESOLUTION in settings.py - %s' % e)
        invalid_variables = True

    settings_tested.append('HORIZON_SHARDS')
    try:
        if not isinstance(settings.HORIZON_SHARDS, dict):
            print('error :: HORIZON_SHARDS in settings.py is not a dict')
            invalid_variables = True
    except AttributeError:
        print('error :: the HORIZON_SHARDS dict is not defined in settings.py')
        invalid_variables = True
    except Exception as e:
        print('error :: the HORIZON_SHARDS dict is not defined in settings.py - %s' % e)
        invalid_variables = True

    settings_tested.append('HORIZON_SHARD_PICKLE_PORT')
    try:
        if settings.HORIZON_SHARD_PICKLE_PORT not in list(range(1, 65535)):
            print('error :: HORIZON_SHARD_PICKLE_PORT in settings.py does not represent a valid port')
            invalid_variables = True
    except ValueError:
        print('error :: HORIZON_SHARD_PICKLE_PORT in settings.py does not represent a valid port')
        invalid_variables = True
    except AttributeError:
        print('error :: the HORIZON_SHARD_PICKLE_PORT is not defined in settings.py')
        invalid_variables = True
    except Exception as e:
        print('error :: the HORIZON_SHARD_PICKLE_PORT in settings.py - %s' % e)
        invalid_variables = True

    settings_tested.append('HORIZON_SHARD_DEBUG')
    try:
        if not isinstance(settings.HORIZON_SHARD_DEBUG, bool):
            print('error :: HORIZON_SHARD_DEBUG in settings.py is not a boolean')
            invalid_variables = True
    except AttributeError:
        print('error :: the HORIZON_SHARD_DEBUG is not defined in settings.py')
        invalid_variables = True
    except Exception as e:
        print('error :: the HORIZON_SHARD_DEBUG in settings.py - %s' % e)
        invalid_variables = True

    settings_tested.append('SYNC_CLUSTER_FILES')
    try:
        if not isinstance(settings.SYNC_CLUSTER_FILES, bool):
            print('error :: SYNC_CLUSTER_FILES in settings.py is not a boolean')
            invalid_variables = True
    except AttributeError:
        print('error :: the SYNC_CLUSTER_FILES is not defined in settings.py')
        invalid_variables = True
    except Exception as e:
        print('error :: the SYNC_CLUSTER_FILES in settings.py - %s' % e)
        invalid_variables = True

    settings_tested.append('SKIP_LIST')
    try:
        if not isinstance(settings.SKIP_LIST, list):
            print('error :: SKIP_LIST in settings.py is not a list')
            invalid_variables = True
    except AttributeError:
        print('error :: SKIP_LIST is not defined in settings.py')
        invalid_variables = True
    except Exception as e:
        print('error :: the SKIP_LIST list in settings.py - %s' % e)
        invalid_variables = True

    settings_tested.append('DO_NOT_SKIP_LIST')
    try:
        if not isinstance(settings.DO_NOT_SKIP_LIST, list):
            print('error :: DO_NOT_SKIP_LIST in settings.py is not a list')
            invalid_variables = True
    except AttributeError:
        print('error :: DO_NOT_SKIP_LIST is not defined in settings.py')
        invalid_variables = True
    except Exception as e:
        print('error :: the DO_NOT_SKIP_LIST list in settings.py - %s' % e)
        invalid_variables = True

    # @added 20210519 - Branch #1444: thunder
    #                   Feature #4076: CUSTOM_STALE_PERIOD
    settings_tested.append('THUNDER_ENABLED')
    try:
        if not isinstance(settings.THUNDER_ENABLED, bool):
            print('error :: THUNDER_ENABLED in settings.py is not a boolean')
            invalid_variables = True
    except AttributeError:
        print('error :: THUNDER_ENABLED is not defined in settings.py')
        invalid_variables = True
    except Exception as e:
        print('error :: THUNDER_ENABLED is not defined in settings.py - %s' % e)
        invalid_variables = True
    settings_tested.append('THUNDER_CHECKS')
    try:
        if not isinstance(settings.THUNDER_CHECKS, dict):
            print('error :: THUNDER_CHECKS in settings.py is not a dict')
            invalid_variables = True
    except AttributeError:
        print('error :: the THUNDER_CHECKS dict is not defined in settings.py')
        invalid_variables = True
    except Exception as e:
        print('error :: the THUNDER_CHECKS dict is not defined in settings.py - %s' % e)
        invalid_variables = True
    settings_tested.append('THUNDER_OPTS')
    THUNDER_OPTS = None
    try:
        if not isinstance(settings.THUNDER_OPTS, dict):
            print('error :: THUNDER_OPTS in settings.py is not a dict')
            invalid_variables = True
        else:
            # @modified 20220722 - Task #4624: Change all dict copy to deepcopy
            # THUNDER_OPTS = settings.THUNDER_OPTS.copy()
            THUNDER_OPTS = copy.deepcopy(settings.THUNDER_OPTS)
    except AttributeError:
        print('error :: the THUNDER_OPTS dict is not defined in settings.py')
        invalid_variables = True
    except Exception as e:
        print('error :: the THUNDER_OPTS dict is not defined in settings.py - %s' % e)
        invalid_variables = True
    # Validate THUNDER_OPTS have been changed from default
    if THUNDER_OPTS:
        alert_via_smtp = False
        thunder_alert_channel_set = False
        try:
            alert_via_smtp = THUNDER_OPTS['alert_via_smtp']
            if not isinstance(alert_via_smtp, bool):
                print('error :: THUNDER_OPTS[\'alert_via_smtp\'] not a boolean')
                invalid_variables = True
        except AttributeError:
            print('error :: THUNDER_OPTS[\'alert_via_smtp\'] is not defined')
            invalid_variables = True
        except Exception as e:
            print('error :: THUNDER_OPTS[\'alert_via_smtp\'] error - %s' % e)
            invalid_variables = True
        if alert_via_smtp:
            smtp_recipients = []
            try:
                smtp_recipients = THUNDER_OPTS['smtp_recipients']
                if not isinstance(smtp_recipients, list):
                    print('error :: THUNDER_OPTS[\'smtp_recipients\'] not a list')
                    invalid_variables = True
                    smtp_recipients = []
            except AttributeError:
                print('error :: THUNDER_OPTS[\'smtp_recipients\'] is not defined')
                invalid_variables = True
            except Exception as e:
                print('error :: THUNDER_OPTS[\'smtp_recipients\'] error - %s' % e)
                invalid_variables = True
            if smtp_recipients:
                if smtp_recipients == ['you@your_domain.com', 'them@your_domain.com']:
                    print('error :: THUNDER_OPTS[\'alert_via_smtp\'] is True but THUNDER_OPTS[\'smtp_sender\'] has not been set to real emails')
                    invalid_variables = True
                else:
                    thunder_alert_channel_set = True

        alert_via_slack = False
        try:
            alert_via_slack = THUNDER_OPTS['alert_via_slack']
            if not isinstance(alert_via_slack, bool):
                print('error :: THUNDER_OPTS[\'alert_via_slack\'] not a boolean')
                invalid_variables = True
        except AttributeError:
            print('error :: THUNDER_OPTS[\'alert_via_slack\'] is not defined')
            invalid_variables = True
        except Exception as e:
            print('error :: THUNDER_OPTS[\'alert_via_slack\'] error - %s' % e)
            invalid_variables = True
        if alert_via_slack:
            slack_channel = None
            try:
                slack_channel = THUNDER_OPTS['slack_channel']
                if not isinstance(slack_channel, str):
                    print('error :: THUNDER_OPTS[\'slack_channel\'] not a str')
                    invalid_variables = True
                    slack_channel = None
            except AttributeError:
                print('error :: THUNDER_OPTS[\'slack_channel\'] is not defined')
                invalid_variables = True
            except Exception as e:
                print('error :: THUNDER_OPTS[\'slack_channel\'] error - %s' % e)
                invalid_variables = True
            if slack_channel:
                thunder_alert_channel_set = True

        alert_via_pagerduty = False
        try:
            alert_via_pagerduty = THUNDER_OPTS['alert_via_pagerduty']
            if not isinstance(alert_via_pagerduty, bool):
                print('error :: THUNDER_OPTS[\'alert_via_pagerduty\'] not a boolean')
                invalid_variables = True
        except AttributeError:
            print('error :: THUNDER_OPTS[\'alert_via_pagerduty\'] is not defined')
            invalid_variables = True
        except Exception as e:
            print('error :: THUNDER_OPTS[\'alert_via_pagerduty\'] error - %s' % e)
            invalid_variables = True
        if alert_via_pagerduty:
            thunder_alert_channel_set = True
    if not thunder_alert_channel_set:
        print('error :: not alert_via_ set in THUNDER_OPTS by defining at least 1 alert_via_')
        invalid_variables = True

    settings_tested.append('PANORAMA_ENABLED')
    try:
        if not isinstance(settings.PANORAMA_ENABLED, bool):
            print('error :: PANORAMA_ENABLED in settings.py is not a boolean')
            invalid_variables = True
    except AttributeError:
        print('error :: the PANORAMA_ENABLED is not defined in settings.py')
        invalid_variables = True
    except Exception as e:
        print('error :: the PANORAMA_ENABLED in settings.py - %s' % e)
        invalid_variables = True

    settings_tested.append('PANORAMA_PROCESSES')
    try:
        if not isinstance(settings.PANORAMA_PROCESSES, int):
            print('error :: PANORAMA_PROCESSES in settings.py is not an int')
            invalid_variables = True
    except AttributeError:
        print('error :: the PANORAMA_PROCESSES is not defined in settings.py')
        invalid_variables = True
    except Exception as e:
        print('error :: the PANORAMA_PROCESSES in settings.py - %s' % e)
        invalid_variables = True

    settings_tested.append('ENABLE_PANORAMA_DEBUG')
    try:
        if not isinstance(settings.ENABLE_PANORAMA_DEBUG, bool):
            print('error :: ENABLE_PANORAMA_DEBUG in settings.py is not a boolean')
            invalid_variables = True
    except AttributeError:
        print('error :: the ENABLE_PANORAMA_DEBUG is not defined in settings.py')
        invalid_variables = True
    except Exception as e:
        print('error :: the ENABLE_PANORAMA_DEBUG in settings.py - %s' % e)
        invalid_variables = True

    settings_tested.append('PANORAMA_DATABASE')
    try:
        if not isinstance(settings.PANORAMA_DATABASE, str):
            print('error :: PANORAMA_DATABASE in settings.py is not a str')
            invalid_variables = True
    except AttributeError:
        print('error :: the PANORAMA_DATABASE str is not defined in settings.py')
        invalid_variables = True
    except Exception as e:
        print('error :: the PANORAMA_DATABASE str in settings.py - %s' % e)
        invalid_variables = True

    settings_tested.append('PANORAMA_DBHOST')
    try:
        if not isinstance(settings.PANORAMA_DBHOST, str):
            print('error :: PANORAMA_DBHOST in settings.py is not a str')
            invalid_variables = True
    except AttributeError:
        print('error :: the PANORAMA_DBHOST str is not defined in settings.py')
        invalid_variables = True
    except Exception as e:
        print('error :: the PANORAMA_DBHOST str in settings.py - %s' % e)
        invalid_variables = True

    settings_tested.append('PANORAMA_DBPORT')
    try:
        if not isinstance(settings.PANORAMA_DBPORT, str):
            print('error :: PANORAMA_DBPORT in settings.py is not a str')
            invalid_variables = True
    except AttributeError:
        print('error :: the PANORAMA_DBPORT str is not defined in settings.py')
        invalid_variables = True
    except Exception as e:
        print('error :: the PANORAMA_DBPORT str in settings.py - %s' % e)
        invalid_variables = True

    settings_tested.append('PANORAMA_DBUSER')
    try:
        if not isinstance(settings.PANORAMA_DBUSER, str):
            print('error :: PANORAMA_DBUSER in settings.py is not a str')
            invalid_variables = True
    except AttributeError:
        print('error :: the PANORAMA_DBUSER str is not defined in settings.py')
        invalid_variables = True
    except Exception as e:
        print('error :: the PANORAMA_DBUSER str in settings.py - %s' % e)
        invalid_variables = True

    settings_tested.append('PANORAMA_DBUSERPASS')
    try:
        if not isinstance(settings.PANORAMA_DBUSERPASS, str):
            print('error :: PANORAMA_DBUSERPASS in settings.py is not a str')
            invalid_variables = True
    except AttributeError:
        print('error :: the PANORAMA_DBUSERPASS str is not defined in settings.py')
        invalid_variables = True
    except Exception as e:
        print('error :: the PANORAMA_DBUSERPASS str in settings.py - %s' % e)
        invalid_variables = True

    settings_tested.append('NUMBER_OF_ANOMALIES_TO_STORE_IN_PANORAMA')
    try:
        if not isinstance(settings.NUMBER_OF_ANOMALIES_TO_STORE_IN_PANORAMA, int):
            print('error :: NUMBER_OF_ANOMALIES_TO_STORE_IN_PANORAMA in settings.py is not an int')
            invalid_variables = True
    except AttributeError:
        print('error :: the NUMBER_OF_ANOMALIES_TO_STORE_IN_PANORAMA is not defined in settings.py')
        invalid_variables = True
    except Exception as e:
        print('error :: the NUMBER_OF_ANOMALIES_TO_STORE_IN_PANORAMA in settings.py - %s' % e)
        invalid_variables = True

    settings_tested.append('PANORAMA_EXPIRY_TIME')
    try:
        if not isinstance(settings.PANORAMA_EXPIRY_TIME, int):
            print('error :: PANORAMA_EXPIRY_TIME in settings.py is not an int')
            invalid_variables = True
    except AttributeError:
        print('error :: the PANORAMA_EXPIRY_TIME is not defined in settings.py')
        invalid_variables = True
    except Exception as e:
        print('error :: the PANORAMA_EXPIRY_TIME in settings.py - %s' % e)
        invalid_variables = True

    settings_tested.append('PANORAMA_CHECK_MAX_AGE')
    try:
        if not isinstance(settings.PANORAMA_CHECK_MAX_AGE, int):
            print('error :: PANORAMA_CHECK_MAX_AGE in settings.py is not an int')
            invalid_variables = True
    except AttributeError:
        print('error :: the PANORAMA_CHECK_MAX_AGE is not defined in settings.py')
        invalid_variables = True
    except Exception as e:
        print('error :: the PANORAMA_CHECK_MAX_AGE in settings.py - %s' % e)
        invalid_variables = True

    settings_tested.append('PANORAMA_CHECK_INTERVAL')
    try:
        if not isinstance(settings.PANORAMA_CHECK_INTERVAL, int):
            print('error :: PANORAMA_CHECK_INTERVAL in settings.py is not an int')
            invalid_variables = True
    except AttributeError:
        print('error :: the PANORAMA_CHECK_INTERVAL is not defined in settings.py')
        invalid_variables = True
    except Exception as e:
        print('error :: the PANORAMA_CHECK_INTERVAL in settings.py - %s' % e)
        invalid_variables = True

    settings_tested.append('PANORAMA_INSERT_METRICS_IMMEDIATELY')
    try:
        if not isinstance(settings.PANORAMA_INSERT_METRICS_IMMEDIATELY, bool):
            print('error :: PANORAMA_INSERT_METRICS_IMMEDIATELY in settings.py is not a boolean')
            invalid_variables = True
    except AttributeError:
        print('error :: the PANORAMA_INSERT_METRICS_IMMEDIATELY is not defined in settings.py')
        invalid_variables = True
    except Exception as e:
        print('error :: the PANORAMA_INSERT_METRICS_IMMEDIATELY in settings.py - %s' % e)
        invalid_variables = True

    TODO = [
        """
        MIRAGE_PROCESSES NOT TESTED
        MIRAGE_DATA_FOLDER NOT TESTED
        MIRAGE_ALGORITHMS NOT TESTED
        MIRAGE_STALE_SECONDS NOT TESTED
        MIRAGE_CONSENSUS NOT TESTED
        MIRAGE_ENABLE_SECOND_ORDER NOT TESTED
        MIRAGE_ENABLE_ALERTS NOT TESTED
        NEGATE_ANALYZER_ALERTS NOT TESTED
        MIRAGE_CRUCIBLE_ENABLED NOT TESTED
        MIRAGE_PERIODIC_CHECK NOT TESTED
        MIRAGE_PERIODIC_CHECK_INTERVAL NOT TESTED
        MIRAGE_PERIODIC_CHECK_NAMESPACES NOT TESTED
        MIRAGE_ALWAYS_METRICS NOT TESTED
        MIRAGE_AUTOFILL_TOOSHORT NOT TESTED
        BOUNDARY_PROCESSES NOT TESTED
        BOUNDARY_OPTIMUM_RUN_DURATION NOT TESTED
        ENABLE_BOUNDARY_DEBUG NOT TESTED
        BOUNDARY_ALGORITHMS NOT TESTED
        BOUNDARY_ENABLE_ALERTS NOT TESTED
        BOUNDARY_CRUCIBLE_ENABLED NOT TESTED
        BOUNDARY_METRICS NOT TESTED
        BOUNDARY_AUTOAGGRERATION NOT TESTED
        BOUNDARY_AUTOAGGRERATION_METRICS NOT TESTED
        BOUNDARY_ALERTER_OPTS NOT TESTED
        BOUNDARY_SMTP_OPTS NOT TESTED
        BOUNDARY_HIPCHAT_OPTS NOT TESTED
        BOUNDARY_PAGERDUTY_OPTS NOT TESTED
        BOUNDARY_SLACK_OPTS NOT TESTED
        ENABLE_CRUCIBLE tested
        CRUCIBLE_PROCESSES NOT TESTED
        CRUCIBLE_TESTS_TIMEOUT NOT TESTED
        ENABLE_CRUCIBLE_DEBUG NOT TESTED
        CRUCIBLE_DATA_FOLDER NOT TESTED
        WEBAPP_SERVER NOT TESTED
        WEBAPP_GUNICORN_WORKERS NOT TESTED
        WEBAPP_GUNICORN_BACKLOG NOT TESTED
        WEBAPP_IP NOT TESTED
        WEBAPP_PORT NOT TESTED
        WEBAPP_AUTH_ENABLED NOT TESTED
        WEBAPP_AUTH_USER NOT TESTED
        WEBAPP_AUTH_USER_PASSWORD NOT TESTED
        WEBAPP_IP_RESTRICTED NOT TESTED
        WEBAPP_ALLOWED_IPS NOT TESTED
        WEBAPP_USER_TIMEZONE NOT TESTED
        WEBAPP_FIXED_TIMEZONE NOT TESTED
        WEBAPP_ACCEPT_DATA_UPLOADS NOT TESTED
        WEBAPP_JAVASCRIPT_DEBUG NOT TESTED
        ENABLE_WEBAPP_DEBUG tested
        WEBAPP_PREPROCESS_TIMESERIES NOT TESTED
        WEBAPP_PREPROCESS_AGGREGATE_BY NOT TESTED
        IONOSPHERE_CHECK_PATH tested
        IONOSPHERE_ENABLED tested
        IONOSPHERE_VERBOSE_LOGGING NOT TESTED
        IONOSPHERE_PROCESSES tested
        IONOSPHERE_MAX_RUNTIME NOT TESTED
        ENABLE_IONOSPHERE_DEBUG tested
        IONOSPHERE_DATA_FOLDER tested
        IONOSPHERE_HISTORICAL_DATA_FOLDER NOT TESTED
        IONOSPHERE_PROFILES_FOLDER tested
        IONOSPHERE_LEARN_FOLDER tested
        IONOSPHERE_CHECK_MAX_AGE tested
        IONOSPHERE_KEEP_TRAINING_TIMESERIES_FOR tested
        IONOSPHERE_CUSTOM_KEEP_TRAINING_TIMESERIES_FOR NOT TESTED
        IONOSPHERE_MANAGE_PURGE NOT TESTED
        IONOSPHERE_GRAPHITE_NOW_GRAPHS_OVERRIDE NOT TESTED
        SKYLINE_URL NOT TESTED
        SERVER_PYTZ_TIMEZONE tested
        IONOSPHERE_FEATURES_PERCENT_SIMILAR tested
        IONOSPHERE_MINMAX_SCALING_ENABLED NOT TESTED
        IONOSPHERE_MINMAX_SCALING_RANGE_TOLERANCE NOT TESTED
        IONOSPHERE_ECHO_ENABLED NOT TESTED
        IONOSPHERE_ECHO_MAX_FP_CREATE_TIME NOT TESTED
        IONOSPHERE_ECHO_FEATURES_PERCENT_SIMILAR NOT TESTED
        IONOSPHERE_ECHO_MINMAX_SCALING_FEATURES_PERCENT_SIMILAR NOT TESTED
        IONOSPHERE_LAYERS_USE_APPROXIMATELY_CLOSE NOT TESTED
        IONOSPHERE_LEARN tested
        IONOSPHERE_LEARN_DEFAULT_MAX_GENERATIONS NOT TESTED
        IONOSPHERE_LEARN_DEFAULT_MAX_PERCENT_DIFF_FROM_ORIGIN tested
        IONOSPHERE_LEARN_DEFAULT_FULL_DURATION_DAYS tested
        IONOSPHERE_LEARN_DEFAULT_VALID_TIMESERIES_OLDER_THAN_SECONDS tested
        IONOSPHERE_LEARN_NAMESPACE_CONFIG tested
        IONOSPHERE_AUTOBUILD tested
        IONOSPHERE_UNTRAINABLES NOT TESTED
        IONOSPHERE_PERFORMANCE_DATA_POPULATE_CACHE NOT TESTED
        IONOSPHERE_PERFORMANCE_DATA_POPULATE_CACHE_DEPTH NOT TESTED
        IONOSPHERE_INFERENCE_MOTIFS_ENABLED NOT TESTED
        IONOSPHERE_INFERENCE_MOTIFS_SETTINGS NOT TESTED
        IONOSPHERE_INFERENCE_MOTIFS_TOP_MATCHES NOT TESTED
        IONOSPHERE_INFERENCE_MASS_TS_MAX_DISTANCE NOT TESTED
        IONOSPHERE_INFERENCE_MOTIFS_RANGE_PADDING NOT TESTED
        IONOSPHERE_INFERENCE_MOTIFS_SINGLE_MATCH NOT TESTED
        IONOSPHERE_INFERENCE_MOTIFS_TEST_ONLY NOT TESTED
        MEMCACHE_ENABLED tested
        MEMCACHED_SERVER_IP tested
        MEMCACHED_SERVER_PORT tested
        LUMINOSITY_PROCESSES NOT TESTED
        ENABLE_LUMINOSITY_DEBUG NOT TESTED
        LUMINOSITY_DATA_FOLDER NOT TESTED
        OTHER_SKYLINE_REDIS_INSTANCES tested
        OTHER_SKYLINE_REDIS_INSTANCES tested
        ALTERNATIVE_SKYLINE_URLS NOT TESTED
        ALTERNATIVE_SKYLINE_URLS NOT TESTED
        REMOTE_SKYLINE_INSTANCES NOT TESTED
        CORRELATE_ALERTS_ONLY NOT TESTED
        LUMINOL_CROSS_CORRELATION_THRESHOLD tested
        LUMINOSITY_RELATED_TIME_PERIOD NOT TESTED
        LUMINOSITY_CORRELATE_ALL NOT TESTED
        LUMINOSITY_CORRELATE_NAMESPACES_ONLY NOT TESTED
        LUMINOSITY_CORRELATION_MAPS NOT TESTED
        LUMINOSITY_CLASSIFY_METRICS_LEVEL_SHIFT NOT TESTED
        LUMINOSITY_LEVEL_SHIFT_SKIP_NAMESPACES NOT TESTED
        LUMINOSITY_CLASSIFY_ANOMALIES NOT TESTED
        LUMINOSITY_CLASSIFY_ANOMALY_ALGORITHMS NOT TESTED
        LUMINOSITY_CLASSIFY_ANOMALIES_SAVE_PLOTS NOT TESTED
        DOCKER NOT TESTED
        DOCKER_DISPLAY_REDIS_PASSWORD_IN_REBROW NOT TESTED
        DOCKER_FAKE_EMAIL_ALERTS NOT TESTED
        FLUX_IP NOT TESTED
        FLUX_PORT NOT TESTED
        FLUX_WORKERS NOT TESTED
        FLUX_VERBOSE_LOGGING NOT TESTED
        FLUX_SELF_API_KEY NOT TESTED
        FLUX_API_KEYS NOT TESTED
        FLUX_BACKLOG NOT TESTED
        FLUX_MAX_AGE NOT TESTED
        FLUX_PERSIST_QUEUE NOT TESTED
        FLUX_CHECK_LAST_TIMESTAMP NOT TESTED
        FLUX_SEND_TO_CARBON NOT TESTED
        FLUX_CARBON_HOST NOT TESTED
        FLUX_CARBON_PORT NOT TESTED
        FLUX_CARBON_PICKLE_PORT NOT TESTED
        FLUX_GRAPHITE_WHISPER_PATH NOT TESTED
        FLUX_PROCESS_UPLOADS NOT TESTED
        FLUX_SAVE_UPLOADS NOT TESTED
        FLUX_SAVE_UPLOADS_PATH NOT TESTED
        FLUX_UPLOADS_KEYS NOT TESTED
        FLUX_ZERO_FILL_NAMESPACES NOT TESTED
        FLUX_LAST_KNOWN_VALUE_NAMESPACES NOT TESTED
        FLUX_AGGREGATE_NAMESPACES NOT TESTED
        FLUX_EXTERNAL_AGGREGATE_NAMESPACES NOT TESTED
        FLUX_SEND_TO_STATSD NOT TESTED
        FLUX_STATSD_HOST NOT TESTED
        FLUX_STATSD_PORT NOT TESTED
        VISTA_ENABLED NOT TESTED
        VISTA_VERBOSE_LOGGING NOT TESTED
        VISTA_FETCHER_PROCESSES NOT TESTED
        VISTA_FETCHER_PROCESS_MAX_RUNTIME NOT TESTED
        VISTA_WORKER_PROCESSES NOT TESTED
        VISTA_DO_NOT_SUBMIT_CURRENT_MINUTE NOT TESTED
        VISTA_FETCH_METRICS NOT TESTED
        VISTA_GRAPHITE_BATCH_SIZE NOT TESTED
        SNAB_ENABLED NOT TESTED
        SNAB_DATA_DIR NOT TESTED
        SNAB_anomalyScore NOT TESTED
        SNAB_CHECKS NOT TESTED
        SNAB_LOAD_TEST_ANALYZER NOT TESTED
        SNAB_FLUX_LOAD_TEST_ENABLED NOT TESTED
        SNAB_FLUX_LOAD_TEST_METRICS NOT TESTED
        SNAB_FLUX_LOAD_TEST_METRICS_PER_POST NOT TESTED
        SNAB_FLUX_LOAD_TEST_NAMESPACE_PREFIX NOT TESTED
        EXTERNAL_SETTINGS tested
        """
    ]
    # cat /home/gary/sandbox/of/github/earthgecko/skyline/SNAB/skyline/skyline/settings.py | grep "^[A-Z].* = " | cut -d' ' -f1 > /tmp/SETTINGS.txt
    # for i in $(cat /tmp/SETTINGS.txt)
    # do
    #   FOUND=$(cat /home/gary/sandbox/of/github/earthgecko/skyline/SNAB/skyline/skyline/validate_settings.py | grep -c "settings.$i")
    #   if [ $FOUND -eq 0 ]; then
    #     echo "$i NOT TESTED"
    #   else
    #     echo "$i tested"
    #   fi
    # done

    settings_tested.append('MAX_ANALYZER_PROCESS_RUNTIME')
    try:
        TEST_MAX_ANALYZER_PROCESS_RUNTIME = settings.MAX_ANALYZER_PROCESS_RUNTIME + 1
    except Exception as e:
        print('error :: MAX_ANALYZER_PROCESS_RUNTIME is not set in settings.py - %s' % e)
        invalid_variables = True

    settings_tested.append('ANALYZER_OPTIMUM_RUN_DURATION')
    try:
        TEST_ANALYZER_OPTIMUM_RUN_DURATION = settings.ANALYZER_OPTIMUM_RUN_DURATION + 1
    except Exception as e:
        print('error :: ANALYZER_OPTIMUM_RUN_DURATION is not set in settings.py')
        invalid_variables = True

    settings_tested.append('MIRAGE_CHECK_PATH')
    try:
        TEST_MIRAGE_CHECK_PATH = settings.MIRAGE_CHECK_PATH
    except Exception as e:
        print('error :: MIRAGE_CHECK_PATH is not set in settings.py')
        invalid_variables = True

    settings_tested.append('ENABLE_CRUCIBLE')
    try:
        TEST_ENABLE_CRUCIBLE = settings.ENABLE_CRUCIBLE
    except Exception as e:
        print('error :: ENABLE_CRUCIBLE is not set in settings.py')
        invalid_variables = True

    settings_tested.append('IONOSPHERE_ENABLED')
    try:
        TEST_IONOSPHERE_ENABLED = settings.IONOSPHERE_ENABLED
    except Exception as e:
        print('error :: IONOSPHERE_ENABLED is not set in settings.py')
        invalid_variables = True

    settings_tested.append('ENABLE_WEBAPP_DEBUG')
    if current_skyline_app == 'webapp':
        try:
            TEST_ENABLE_WEBAPP_DEBUG = settings.ENABLE_WEBAPP_DEBUG
        except Exception as e:
            print('error :: ENABLE_WEBAPP_DEBUG is not set in settings.py')
            invalid_variables = True

    settings_tested.append('IONOSPHERE_CHECK_PATH')
    try:
        TEST_IONOSPHERE_CHECK_PATH = settings.IONOSPHERE_CHECK_PATH
    except Exception as e:
        print('error :: IONOSPHERE_CHECK_PATH is not set in settings.py')
        invalid_variables = True

    settings_tested.append('IONOSPHERE_DATA_FOLDER')
    try:
        TEST_IONOSPHERE_DATA_FOLDER = settings.IONOSPHERE_DATA_FOLDER
    except Exception as e:
        print('error :: IONOSPHERE_DATA_FOLDER is not set in settings.py')
        invalid_variables = True

    settings_tested.append('IONOSPHERE_PROFILES_FOLDER')
    try:
        TEST_IONOSPHERE_PROFILES_FOLDER = settings.IONOSPHERE_PROFILES_FOLDER
    except Exception as e:
        print('error :: IONOSPHERE_PROFILES_FOLDER is not set in settings.py')
        invalid_variables = True

    settings_tested.append('IONOSPHERE_PROCESSES')
    settings_tested.append('ENABLE_IONOSPHERE_DEBUG')
    settings_tested.append('IONOSPHERE_CHECK_MAX_AGE')
    settings_tested.append('IONOSPHERE_KEEP_TRAINING_TIMESERIES_FOR')
    settings_tested.append('IONOSPHERE_FEATURES_PERCENT_SIMILAR')
    settings_tested.append('SERVER_PYTZ_TIMEZONE')
    settings_tested.append('IONOSPHERE_LEARN')
    settings_tested.append('IONOSPHERE_LEARN_FOLDER')
    settings_tested.append('IONOSPHERE_AUTOBUILD')
    settings_tested.append('IONOSPHERE_LEARN_DEFAULT_FULL_DURATION_DAYS')
    settings_tested.append('IONOSPHERE_LEARN_DEFAULT_VALID_TIMESERIES_OLDER_THAN_SECONDS')
    settings_tested.append('IONOSPHERE_LEARN_DEFAULT_MAX_PERCENT_DIFF_FROM_ORIGIN')
    settings_tested.append('IONOSPHERE_LEARN_NAMESPACE_CONFIG')
    if current_skyline_app == 'ionosphere':
        try:
            TEST_IONOSPHERE_PROCESSES = 1 + settings.IONOSPHERE_PROCESSES
        except Exception as e:
            print('error :: IONOSPHERE_PROCESSES is not set in settings.py')
            invalid_variables = True

        try:
            TEST_ENABLE_IONOSPHERE_DEBUG = settings.ENABLE_IONOSPHERE_DEBUG
        except Exception as e:
            print('error :: ENABLE_IONOSPHERE_DEBUG is not set in settings.py')
            invalid_variables = True

        try:
            TEST_IONOSPHERE_CHECK_MAX_AGE = 1 + settings.IONOSPHERE_CHECK_MAX_AGE
        except Exception as e:
            print('error :: IONOSPHERE_CHECK_MAX_AGE is not set in settings.py')
            invalid_variables = True

        try:
            TEST_IONOSPHERE_KEEP_TRAINING_TIMESERIES_FOR = 1 + settings.IONOSPHERE_KEEP_TRAINING_TIMESERIES_FOR
        except Exception as e:
            print('error :: IONOSPHERE_KEEP_TRAINING_TIMESERIES_FOR is not set in settings.py')
            invalid_variables = True

        try:
            TEST_IONOSPHERE_FEATURES_PERCENT_SIMILAR = 1 + settings.IONOSPHERE_FEATURES_PERCENT_SIMILAR
        except Exception as e:
            print('error :: IONOSPHERE_FEATURES_PERCENT_SIMILAR is not set in settings.py')
            invalid_variables = True

        # @added 20170122 - Feature #1872: Ionosphere - features profile page by id only
        try:
            TEST_SERVER_PYTZ_TIMEZONE = pytz.timezone(settings.SERVER_PYTZ_TIMEZONE)
        except Exception as e:
            print('error :: SERVER_PYTZ_TIMEZONE is not set to a pytz timezone in settings.py')
            invalid_variables = True

        # @added 20170109 - Feature #1854: Ionosphere learn
        # Added the Ionosphere LEARN related variables
        ionosphere_learning_enabled = False
        try:
            ionosphere_learn_enabled = settings.IONOSPHERE_LEARN
        except Exception as e:
            print('error :: IONOSPHERE_LEARN is not set in settings.py')
            invalid_variables = True

        if ionosphere_learning_enabled:
            try:
                TEST_IONOSPHERE_LEARN_FOLDER = settings.IONOSPHERE_LEARN_FOLDER
            except Exception as e:
                print('error :: IONOSPHERE_LEARN_FOLDER is not set in settings.py')
                invalid_variables = True

            # @added 20160113 - Feature #1858: Ionosphere - autobuild features_profiles dir
            try:
                TEST_IONOSPHERE_AUTOBUILD = settings.IONOSPHERE_AUTOBUILD
            except Exception as e:
                print('error :: IONOSPHERE_AUTOBUILD is not set in settings.py')
                invalid_variables = True

        # @modified 20170115 - Feature #1854: Ionosphere learn - generations
        # These are now used in a shared context in terms of being required
        # by Panorama and ionosphere/learn
        try:
            TEST_IONOSPHERE_LEARN_DEFAULT_FULL_DURATION_DAYS = 1 + settings.IONOSPHERE_LEARN_DEFAULT_FULL_DURATION_DAYS
        except Exception as e:
            print('error :: IONOSPHERE_LEARN_DEFAULT_FULL_DURATION_DAYS is not set in settings.py')
            invalid_variables = True

        try:
            TEST_IONOSPHERE_LEARN_DEFAULT_VALID_TIMESERIES_OLDER_THAN_SECONDS = 1 + settings.IONOSPHERE_LEARN_DEFAULT_VALID_TIMESERIES_OLDER_THAN_SECONDS
        except Exception as e:
            print('error :: IONOSPHERE_LEARN_DEFAULT_VALID_TIMESERIES_OLDER_THAN_SECONDS is not set in settings.py')
            invalid_variables = True

        try:
            TEST_IONOSPHERE_LEARN_DEFAULT_MAX_PERCENT_DIFF_FROM_ORIGIN = 1 + settings.IONOSPHERE_LEARN_DEFAULT_MAX_PERCENT_DIFF_FROM_ORIGIN
        except Exception as e:
            print('error :: IONOSPHERE_LEARN_DEFAULT_MAX_PERCENT_DIFF_FROM_ORIGIN is not set in settings.py')
            invalid_variables = True

        try:
            TEST_IONOSPHERE_LEARN_NAMESPACE_CONFIG = settings.IONOSPHERE_LEARN_NAMESPACE_CONFIG
        except Exception as e:
            print('error :: IONOSPHERE_LEARN_NAMESPACE_CONFIG is not set in settings.py')
            invalid_variables = True

    # @added 20170809 - Task #2132: Optimise Ionosphere DB usage
    settings_tested.append('MEMCACHE_ENABLED')
    try:
        TEST_MEMCACHE_ENABLED = settings.MEMCACHE_ENABLED
    except Exception as e:
        print('error :: MEMCACHE_ENABLED is not set in settings.py')
        invalid_variables = True
    settings_tested.append('MEMCACHED_SERVER_IP')
    try:
        TEST_MEMCACHED_SERVER_IP = settings.MEMCACHED_SERVER_IP
    except Exception as e:
        print('error :: MEMCACHED_SERVER_IP is not set in settings.py')
        invalid_variables = True
    settings_tested.append('MEMCACHED_SERVER_PORT')
    try:
        TEST_MEMCACHED_SERVER_PORT = settings.MEMCACHED_SERVER_PORT
    except Exception as e:
        print('error :: MEMCACHED_SERVER_PORT is not set in settings.py')
        invalid_variables = True

    # @added 20180524 - Branch #2270: luminosity
    settings_tested.append('LUMINOL_CROSS_CORRELATION_THRESHOLD')
    try:
        TEST_LUMINOL_CROSS_CORRELATION_THRESHOLD = isinstance(settings.LUMINOL_CROSS_CORRELATION_THRESHOLD, float)
    except Exception as e:
        print('error :: LUMINOL_CROSS_CORRELATION_THRESHOLD is not set as a float in settings.py')
        invalid_variables = True
    if TEST_LUMINOL_CROSS_CORRELATION_THRESHOLD:
        if settings.LUMINOL_CROSS_CORRELATION_THRESHOLD >= 1.0:
            print('error :: LUMINOL_CROSS_CORRELATION_THRESHOLD should be a float between 0.0 and 1.0000')
            invalid_variables = True
        if settings.LUMINOL_CROSS_CORRELATION_THRESHOLD <= 0.0:
            print('error :: LUMINOL_CROSS_CORRELATION_THRESHOLD should be a float between 0.0 and 1.0000')
            invalid_variables = True

    settings_tested.append('SLACK_ENABLED')
    try:
        TEST_SLACK_ENABLED = settings.SLACK_ENABLED
    except Exception as e:
        TEST_SLACK_ENABLED = False
    if TEST_SLACK_ENABLED and TEST_ALERTS:
        # Test that all slack alert tuples are declared AFTER smtp alert tuples
        slack_order_set = False
        smtp_set = False
        for alert in settings.ALERTS:
            if alert[1] == 'smtp':
                smtp_set = True
                if slack_order_set:
                    print('error :: a slack alert tuple set before an smtp alert tuple')
                    invalid_variables = True
                    break
            if alert[1] == 'slack':
                if not smtp_set:
                    print('error :: a slack alert tuple set before an smtp alert tuple')
                    invalid_variables = True
                    break
                slack_order_set = True

    # @added 20210518 - Feature #4076: CUSTOM_STALE_PERIOD
    settings_tested.append('CUSTOM_STALE_PERIOD')
    try:
        if not isinstance(settings.CUSTOM_STALE_PERIOD, dict):
            print('error :: CUSTOM_STALE_PERIOD in settings.py is not a dict')
            invalid_variables = True
    except AttributeError:
        print('error :: the CUSTOM_STALE_PERIOD dict is not defined in settings.py')
        invalid_variables = True
    except Exception as e:
        print('error :: the CUSTOM_STALE_PERIOD dict is not defined in settings.py - %s' % e)
        invalid_variables = True

    # @added 20210603 - Feature #4000: EXTERNAL_SETTINGS
    settings_tested.append('EXTERNAL_SETTINGS')
    try:
        if not isinstance(settings.EXTERNAL_SETTINGS, dict):
            print('error :: EXTERNAL_SETTINGS in settings.py is not a dict')
            invalid_variables = True
    except AttributeError:
        print('error :: the EXTERNAL_SETTINGS dict is not defined in settings.py')
        invalid_variables = True
    except Exception as e:
        print('error :: the EXTERNAL_SETTINGS dict is not defined in settings.py - %s' % e)
        invalid_variables = True

    # @added 20210619 - Feature #4148: analyzer.metrics_manager.resolutions
    #                   Bug #4146: check_data_sparsity - incorrect on low fidelity and inconsistent metrics
    #                   Feature #3870: metrics_manager - check_data_sparsity
    settings_tested.append('FULLY_POPULATED_PERCENTAGE')
    try:
        if not isinstance(settings.FULLY_POPULATED_PERCENTAGE, float):
            print('error :: FULLY_POPULATED_PERCENTAGE in settings.py is not a float')
            invalid_variables = True
    except AttributeError:
        print('error :: FULLY_POPULATED_PERCENTAGE is not defined in settings.py')
        invalid_variables = True
    except Exception as e:
        print('error :: FULLY_POPULATED_PERCENTAGE is not defined in settings.py - %s' % e)
        invalid_variables = True
    settings_tested.append('SPARSELY_POPULATED_PERCENTAGE')
    try:
        if not isinstance(settings.SPARSELY_POPULATED_PERCENTAGE, float):
            print('error :: SPARSELY_POPULATED_PERCENTAGE in settings.py is not a float')
            invalid_variables = True
    except AttributeError:
        print('error :: SPARSELY_POPULATED_PERCENTAGE is not defined in settings.py')
        invalid_variables = True
    except Exception as e:
        print('error :: SPARSELY_POPULATED_PERCENTAGE is not defined in settings.py - %s' % e)
        invalid_variables = True

    # @added 20210724 - Feature #4196: functions.aws.send_sms
    settings_tested.append('AWS_OPTS')
    try:
        if not isinstance(settings.AWS_OPTS, dict):
            print('error :: AWS_OPTS in settings.py is not a dict')
            invalid_variables = True
    except AttributeError:
        print('error :: AWS_OPTS is not defined in settings.py')
        invalid_variables = True
    except Exception as e:
        print('error :: AWS_OPTS is not defined in settings.py - %s' % e)
        invalid_variables = True
    settings_tested.append('AWS_SNS_SMS_ALERTS_ENABLED')
    try:
        if not isinstance(settings.AWS_SNS_SMS_ALERTS_ENABLED, bool):
            print('error :: AWS_SNS_SMS_ALERTS_ENABLED in settings.py is not a bool')
            invalid_variables = True
    except AttributeError:
        print('error :: AWS_SNS_SMS_ALERTS_ENABLED is not defined in settings.py')
        invalid_variables = True
    except Exception as e:
        print('error :: AWS_SNS_SMS_ALERTS_ENABLED is not defined in settings.py - %s' % e)
        invalid_variables = True
    settings_tested.append('SMS_ALERT_OPTS')
    try:
        if not isinstance(settings.SMS_ALERT_OPTS, dict):
            print('error :: SMS_ALERT_OPTS in settings.py is not a dict')
            invalid_variables = True
    except AttributeError:
        print('error :: SMS_ALERT_OPTS is not defined in settings.py')
        invalid_variables = True
    except Exception as e:
        print('error :: SMS_ALERT_OPTS is not defined in settings.py - %s' % e)
        invalid_variables = True

    # @added 20210730 - Feature #4164: luminosity - cloudbursts
    settings_tested.append('LUMINOSITY_CLOUDBURST_ENABLED')
    try:
        if not isinstance(settings.LUMINOSITY_CLOUDBURST_ENABLED, bool):
            print('error :: LUMINOSITY_CLOUDBURST_ENABLED in settings.py is not a boolean')
            invalid_variables = True
    except AttributeError:
        print('error :: LUMINOSITY_CLOUDBURST_ENABLED is not defined in settings.py')
        invalid_variables = True
    except Exception as e:
        print('error :: LUMINOSITY_CLOUDBURST_ENABLED is not defined in settings.py - %s' % e)
        invalid_variables = True

    # @added 20210930 - Feature #4264: luminosity - cross_correlation_relationships
    settings_tested.append('LUMINOSITY_RELATED_METRICS')
    try:
        if not isinstance(settings.LUMINOSITY_RELATED_METRICS, bool):
            print('error :: LUMINOSITY_RELATED_METRICS in settings.py is not a boolean')
            invalid_variables = True
    except AttributeError:
        print('error :: LUMINOSITY_RELATED_METRICS is not defined in settings.py')
        invalid_variables = True
    except Exception as e:
        print('error :: LUMINOSITY_RELATED_METRICS is not defined in settings.py - %s' % e)
        invalid_variables = True
    settings_tested.append('LUMINOSITY_RELATED_METRICS_MAX_5MIN_LOADAVG')
    try:
        if not isinstance(settings.LUMINOSITY_RELATED_METRICS_MAX_5MIN_LOADAVG, float):
            print('error :: LUMINOSITY_RELATED_METRICS_MAX_5MIN_LOADAVG in settings.py is not a float')
            invalid_variables = True
    except AttributeError:
        print('error :: LUMINOSITY_RELATED_METRICS_MAX_5MIN_LOADAVG is not defined in settings.py')
        invalid_variables = True
    except Exception as e:
        print('error :: LUMINOSITY_RELATED_METRICS_MAX_5MIN_LOADAVG is not defined in settings.py - %s' % e)
        invalid_variables = True
    settings_tested.append('LUMINOSITY_RELATED_METRICS_MIN_CORRELATION_COUNT_PERCENTILE')
    try:
        if not isinstance(settings.LUMINOSITY_RELATED_METRICS_MIN_CORRELATION_COUNT_PERCENTILE, float):
            print('error :: LUMINOSITY_RELATED_METRICS_MIN_CORRELATION_COUNT_PERCENTILE in settings.py is not a float')
            invalid_variables = True
    except AttributeError:
        print('error :: LUMINOSITY_RELATED_METRICS_MIN_CORRELATION_COUNT_PERCENTILE is not defined in settings.py')
        invalid_variables = True
    except Exception as e:
        print('error :: LUMINOSITY_RELATED_METRICS_MIN_CORRELATION_COUNT_PERCENTILE is not defined in settings.py - %s' % e)
        invalid_variables = True
    settings_tested.append('LUMINOSITY_RELATED_METRICS_MINIMUM_CORRELATIONS_COUNT')
    try:
        if not isinstance(settings.LUMINOSITY_RELATED_METRICS_MINIMUM_CORRELATIONS_COUNT, int):
            print('error :: LUMINOSITY_RELATED_METRICS_MINIMUM_CORRELATIONS_COUNT in settings.py is not an int')
            invalid_variables = True
    except AttributeError:
        print('error :: LUMINOSITY_RELATED_METRICS_MINIMUM_CORRELATIONS_COUNT is not defined in settings.py')
        invalid_variables = True
    except Exception as e:
        print('error :: LUMINOSITY_RELATED_METRICS_MINIMUM_CORRELATIONS_COUNT is not defined in settings.py - %s' % e)
        invalid_variables = True

    settings_tested.append('BATCH_METRICS_CUSTOM_FULL_DURATIONS')
    try:
        if not isinstance(settings.BATCH_METRICS_CUSTOM_FULL_DURATIONS, dict):
            print('error :: BATCH_METRICS_CUSTOM_FULL_DURATIONS in settings.py is not a dict')
            invalid_variables = True
    except AttributeError:
        print('error :: BATCH_METRICS_CUSTOM_FULL_DURATIONS is not defined in settings.py')
        invalid_variables = True
    except Exception as e:
        print('error :: BATCH_METRICS_CUSTOM_FULL_DURATIONS is not defined in settings.py - %s' % e)
        invalid_variables = True

    settings_tested.append('PROMETHEUS_SETTINGS')
    try:
        if not isinstance(settings.PROMETHEUS_SETTINGS, dict):
            print('error :: PROMETHEUS_SETTINGS in settings.py is not a dict')
            invalid_variables = True
    except AttributeError:
        print('error :: PROMETHEUS_SETTINGS is not defined in settings.py')
        invalid_variables = True
    except Exception as e:
        print('error :: PROMETHEUS_SETTINGS is not defined in settings.py - %s' % e)
        invalid_variables = True

    for setting in list(settings_types.keys()):
        if setting in settings_tested:
            continue
        setting_type = settings_types[setting]
        settings_tested.append(setting)
        try:
            settings_value = getattr(settings, setting)
            if not isinstance(settings_value, setting_type):
                if setting == 'FLUX_STATSD_HOST':
                    continue
                print('error :: %s in settings.py is not a %s' % (setting, str(setting_type)))
                invalid_variables = True
        except AttributeError:
            print('error :: %s is not defined in settings.py' % setting)
            invalid_variables = True
        except Exception as err:
            print('error :: %s is not defined in settings.py - %s' % (setting, err))
            invalid_variables = True

    if current_skyline_app == 'test_settings':

        # @added 20220225 - Feature #4442: settings - LOCAL_EXTERNAL_SETTINGS
        # Report all the untested settings
        not_tested_settings = []
        for setting in settings_variables:
            if setting not in settings_tested:
                if 'HIPCHAT' in str(setting):
                    continue
                not_tested_settings.append(setting)
        if not_tested_settings:
            print('info :: validate_settings.py :: Not tested variables: %s' % str(not_tested_settings))

        if invalid_variables:
            print('error :: validate_settings.py :: tests on settings.py FAILED')
        else:
            print('info :: validate_settings.py :: tests on settings.py PASSED OK')

    if invalid_variables:
        print('error :: invalid or missing variables in settings.py, exiting, please fix settings.py')
        return False

    # print('all tested variables in settings.py passed OK')
    return True
