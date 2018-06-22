import logging
import traceback
import pytz
import settings


def validate_settings_variables(current_skyline_app):
    """
    This function is used by the agent.py to validate the variables in
    settings.py are valid

    :param current_skyline_app: the skyline app using this function
    :return: ``True`` or ``False``
    :rtype: boolean

    """

    current_skyline_app_logger = current_skyline_app + 'Log'
    current_logger = logging.getLogger(current_skyline_app_logger)

    invalid_variables = False

    # Validate settings variables
    try:
        TEST_REDIS_SOCKET_PATH = settings.REDIS_SOCKET_PATH
    except:
        current_logger.error('error :: REDIS_SOCKET_PATH is not set in settings.py')
        current_logger.info(traceback.format_exc())
        current_logger.error('error :: exiting, please fix setting.py')
        print ('error :: REDIS_SOCKET_PATH is not set in settings.py')
        invalid_variables = True

    # @added 20180519 - Feature #2378: Add redis auth to Skyline and rebrow
    try:
        TEST_REDIS_PASSWORD = settings.REDIS_PASSWORD
        if not settings.REDIS_PASSWORD:
            print ('WARNING :: REDIS_PASSWORD is to None, please considering enabling Redis authentication')
            print ('WARNING :: by setting the Redis variable requirepass in your redis.conf and restarting')
            print ('WARNING :: Redis, then set the REDIS_PASSWORD in your settings.py and restart your')
            print ('WARNING :: Skyline services.')
            print ('WARNING :: See https://redis.io/topics/security and http://antirez.com/news/96 for more info')
    except:
        current_logger.error('error :: REDIS_PASSWORD is not set in settings.py')
        current_logger.info(traceback.format_exc())
        current_logger.error('error :: exiting, please fix setting.py')
        print ('error :: REDIS_PASSWORD is not set in settings.py')
        invalid_variables = True

    try:
        TEST_OTHER_SKYLINE_REDIS_INSTANCES = settings.OTHER_SKYLINE_REDIS_INSTANCES
        if settings.OTHER_SKYLINE_REDIS_INSTANCES:
            for redis_ip, redis_port, redis_password in settings.OTHER_SKYLINE_REDIS_INSTANCES:
                if not redis_password:
                    print ('WARNING :: the Redis password for %s is to False in the OTHER_SKYLINE_REDIS_INSTANCES') % str(redis_ip)
                    print ('WARNING :: variable in settings.py, please considering enabling Redis authentication')
                    print ('WARNING :: on %s by setting the Redis variable requirepass in the redis.conf and restarting') % str(redis_ip)
                    print ('WARNING :: Redis, then set the Redis password for %s in OTHER_SKYLINE_REDIS_INSTANCES') % str(redis_ip)
                    print ('WARNING :: in your settings.py and restart your Skyline luminosity service.')
                    print ('WARNING :: See https://redis.io/topics/security and http://antirez.com/news/96 for more info')
    except:
        pass

    try:
        TEST_SKYLINE_TMP_DIR = settings.SKYLINE_TMP_DIR
    except:
        current_logger.error('error :: SKYLINE_TMP_DIR is not set in settings.py')
        current_logger.info(traceback.format_exc())
        current_logger.error('error :: exiting, please fix setting.py')
        print ('error :: SKYLINE_TMP_DIR is not set in settings.py')
        invalid_variables = True

    try:
        TEST_FULL_NAMESPACE_IN_SETTINGS = settings.FULL_NAMESPACE
    except:
        current_logger.error('error :: FULL_NAMESPACE is not set in settings.py')
        current_logger.info(traceback.format_exc())
        current_logger.error('error :: exiting, please fix setting.py')
        print ('error :: FULL_NAMESPACE is not set in settings.py')
        invalid_variables = True

    try:
        TEST_CRUCIBLE_CHECK_PATH = settings.CRUCIBLE_CHECK_PATH
    except:
        current_logger.error('error :: CRUCIBLE_CHECK_PATH is not set in settings.py')
        current_logger.info(traceback.format_exc())
        current_logger.error('error :: exiting, please fix setting.py')
        print ('error :: CRUCIBLE_CHECK_PATH is not set in settings.py')
        valid_variables = True
    try:
        PANORAMA_CHECK_PATH = settings.PANORAMA_CHECK_PATH
    except:
        current_logger.error('error :: PANORAMA_CHECK_PATH is not set in settings.py')
        current_logger.info(traceback.format_exc())
        current_logger.error('error :: exiting, please fix setting.py')
        print ('error :: PANORAMA_CHECK_PATH is not set in settings.py')
        valid_variables = True

    if current_skyline_app in 'analyzer':
        try:
            TEST_ALGORITHMS_IN_SETTINGS = settings.ALGORITHMS
        except:
            current_logger.error('error :: ALGORITHMS is not set in settings.py')
            current_logger.info(traceback.format_exc())
            current_logger.error('error :: exiting, please fix setting.py')
            print ('error :: ALGORITHMS is not set in settings.py')
            invalid_variables = True

        try:
            TEST_ANALYZER_PROCESSES = settings.ANALYZER_PROCESSES + 1
        except:
            current_logger.error('error :: ANALYZER_PROCESSES is not set to an int in settings.py')
            current_logger.info(traceback.format_exc())
            current_logger.error('error :: exiting, please fix setting.py')
            print ('error :: ANALYZER_PROCESSES is not set in settings.py')
            invalid_variables = True

        try:
            TEST_MAX_ANALYZER_PROCESS_RUNTIME = settings.MAX_ANALYZER_PROCESS_RUNTIME + 1
        except:
            current_logger.error('error :: MAX_ANALYZER_PROCESS_RUNTIME is not set to an int in settings.py')
            current_logger.info(traceback.format_exc())
            current_logger.error('error :: exiting, please fix setting.py')
            print ('error :: MAX_ANALYZER_PROCESS_RUNTIME is not set in settings.py')
            invalid_variables = True

        try:
            TEST_ANALYZER_OPTIMUM_RUN_DURATION = settings.ANALYZER_OPTIMUM_RUN_DURATION + 1
        except:
            current_logger.error('error :: ANALYZER_OPTIMUM_RUN_DURATION is not set to an int in settings.py')
            current_logger.info(traceback.format_exc())
            current_logger.error('error :: exiting, please fix setting.py')
            print ('error :: ANALYZER_OPTIMUM_RUN_DURATION is not set in settings.py')
            invalid_variables = True

        try:
            TEST_ANALYZER_CRUCIBLE_ENABLED = settings.ANALYZER_CRUCIBLE_ENABLED
        except:
            current_logger.error('error :: ANALYZER_CRUCIBLE_ENABLED is not set in settings.py')
            current_logger.info(traceback.format_exc())
            print ('error :: ANALYZER_CRUCIBLE_ENABLED is not set in settings.py')
            invalid_variables = True

    try:
        TEST_ENABLE_ALERTS = settings.ENABLE_ALERTS
    except:
        current_logger.error('error :: ENABLE_ALERTS is not set in settings.py')
        current_logger.info(traceback.format_exc())
        current_logger.error('error :: exiting, please fix setting.py')
        print ('error :: ENABLE_ALERTS is not set in settings.py')
        invalid_variables = True

    try:
        TEST_MIRAGE_CHECK_PATH = settings.MIRAGE_CHECK_PATH
    except:
        current_logger.error('error :: MIRAGE_CHECK_PATH is not set in settings.py')
        current_logger.info(traceback.format_exc())
        current_logger.error('error :: exiting, please fix setting.py')
        print ('error :: MIRAGE_CHECK_PATH is not set in settings.py')
        invalid_variables = True

    try:
        TEST_ENABLE_CRUCIBLE = settings.ENABLE_CRUCIBLE
    except:
        current_logger.error('error :: ENABLE_CRUCIBLE is not set in settings.py')
        current_logger.info(traceback.format_exc())
        current_logger.error('error :: exiting, please fix setting.py')
        print ('error :: ENABLE_CRUCIBLE is not set in settings.py')
        invalid_variables = True

    try:
        TEST_PANORAMA_ENABLED = settings.PANORAMA_ENABLED
    except:
        current_logger.error('error :: PANORAMA_ENABLED is not set in settings.py')
        current_logger.info(traceback.format_exc())
        current_logger.error('error :: exiting, please fix setting.py')
        print ('error :: PANORAMA_ENABLED is not set in settings.py')
        invalid_variables = True

    try:
        TEST_IONOSPHERE_ENABLED = settings.IONOSPHERE_ENABLED
    except:
        current_logger.error('error :: IONOSPHERE_ENABLED is not set in settings.py')
        current_logger.info(traceback.format_exc())
        current_logger.error('error :: exiting, please fix setting.py')
        print ('error :: IONOSPHERE_ENABLED is not set in settings.py')
        invalid_variables = True

    if current_skyline_app == 'webapp':
        try:
            TEST_ENABLE_WEBAPP_DEBUG = settings.ENABLE_WEBAPP_DEBUG
        except:
            current_logger.error('error :: ENABLE_WEBAPP_DEBUG is not set in settings.py')
            current_logger.info(traceback.format_exc())
            current_logger.error('error :: exiting, please fix setting.py')
            print ('error :: ENABLE_WEBAPP_DEBUG is not set in settings.py')
            invalid_variables = True

    try:
        TEST_IONOSPHERE_ENABLED = settings.IONOSPHERE_ENABLED
    except:
        current_logger.error('error :: IONOSPHERE_ENABLED is not set in settings.py')
        current_logger.info(traceback.format_exc())
        current_logger.error('error :: exiting, please fix setting.py')
        print ('error :: IONOSPHERE_ENABLED is not set in settings.py')
        invalid_variables = True

    try:
        TEST_IONOSPHERE_CHECK_PATH = settings.IONOSPHERE_CHECK_PATH
    except:
        current_logger.error('error :: IONOSPHERE_CHECK_PATH is not set in settings.py')
        current_logger.info(traceback.format_exc())
        current_logger.error('error :: exiting, please fix setting.py')
        print ('error :: IONOSPHERE_CHECK_PATH is not set in settings.py')
        invalid_variables = True

    try:
        TEST_IONOSPHERE_DATA_FOLDER = settings.IONOSPHERE_DATA_FOLDER
    except:
        current_logger.error('error :: IONOSPHERE_DATA_FOLDER is not set in settings.py')
        current_logger.info(traceback.format_exc())
        current_logger.error('error :: exiting, please fix setting.py')
        print ('error :: IONOSPHERE_DATA_FOLDER is not set in settings.py')
        invalid_variables = True

    try:
        TEST_IONOSPHERE_PROFILES_FOLDER = settings.IONOSPHERE_PROFILES_FOLDER
    except:
        current_logger.error('error :: IONOSPHERE_PROFILES_FOLDER is not set in settings.py')
        current_logger.info(traceback.format_exc())
        current_logger.error('error :: exiting, please fix setting.py')
        print ('error :: IONOSPHERE_PROFILES_FOLDER is not set in settings.py')
        invalid_variables = True

    if current_skyline_app == 'ionosphere':
        try:
            TEST_IONOSPHERE_PROCESSES = 1 + settings.IONOSPHERE_PROCESSES
        except:
            current_logger.error('error :: IONOSPHERE_PROCESSES is not set in settings.py')
            current_logger.info(traceback.format_exc())
            current_logger.error('error :: exiting, please fix setting.py')
            print ('error :: IONOSPHERE_PROCESSES is not set in settings.py')
            invalid_variables = True

        try:
            TEST_ENABLE_IONOSPHERE_DEBUG = settings.ENABLE_IONOSPHERE_DEBUG
        except:
            current_logger.error('error :: ENABLE_IONOSPHERE_DEBUG is not set in settings.py')
            current_logger.info(traceback.format_exc())
            current_logger.error('error :: exiting, please fix setting.py')
            print ('error :: ENABLE_IONOSPHERE_DEBUG is not set in settings.py')
            invalid_variables = True

        try:
            TEST_IONOSPHERE_CHECK_MAX_AGE = 1 + settings.IONOSPHERE_CHECK_MAX_AGE
        except:
            current_logger.error('error :: IONOSPHERE_CHECK_MAX_AGE is not set in settings.py')
            current_logger.info(traceback.format_exc())
            current_logger.error('error :: exiting, please fix setting.py')
            print ('error :: IONOSPHERE_CHECK_MAX_AGE is not set in settings.py')
            invalid_variables = True

        try:
            TEST_IONOSPHERE_KEEP_TRAINING_TIMESERIES_FOR = 1 + settings.IONOSPHERE_KEEP_TRAINING_TIMESERIES_FOR
        except:
            current_logger.error('error :: IONOSPHERE_KEEP_TRAINING_TIMESERIES_FOR is not set in settings.py')
            current_logger.info(traceback.format_exc())
            current_logger.error('error :: exiting, please fix setting.py')
            print ('error :: IONOSPHERE_KEEP_TRAINING_TIMESERIES_FOR is not set in settings.py')
            invalid_variables = True

        try:
            TEST_IONOSPHERE_FEATURES_PERCENT_SIMILAR = 1 + settings.IONOSPHERE_FEATURES_PERCENT_SIMILAR
        except:
            current_logger.error('error :: IONOSPHERE_FEATURES_PERCENT_SIMILAR is not set as a float in settings.py')
            current_logger.info(traceback.format_exc())
            current_logger.error('error :: exiting, please fix setting.py')
            print ('error :: IONOSPHERE_FEATURES_PERCENT_SIMILAR is not set in settings.py')
            invalid_variables = True

        # @added 20170122 - Feature #1872: Ionosphere - features profile page by id only
        try:
            TEST_SERVER_PYTZ_TIMEZONE = pytz.timezone(settings.SERVER_PYTZ_TIMEZONE)
        except:
            current_logger.error('error :: SERVER_PYTZ_TIMEZONE is not set to a pytz timezone in settings.py')
            current_logger.info(traceback.format_exc())
            current_logger.error('error :: exiting, please fix setting.py')
            print ('error :: SERVER_PYTZ_TIMEZONE is not set to a pytz timezone in settings.py')
            invalid_variables = True

        # @added 20170109 - Feature #1854: Ionosphere learn
        # Added the Ionosphere LEARN related variables
        ionosphere_learning_enabled = False
        try:
            ionosphere_learn_enabled = settings.IONOSPHERE_LEARN
        except:
            current_logger.error('error :: IONOSPHERE_LEARN is not set as a boolean in settings.py')
            current_logger.info(traceback.format_exc())
            current_logger.error('error :: exiting, please fix setting.py')
            print ('error :: IONOSPHERE_LEARN is not set in settings.py')
            invalid_variables = True

        if ionosphere_learning_enabled:
            try:
                TEST_IONOSPHERE_LEARN_FOLDER = settings.IONOSPHERE_LEARN_FOLDER
            except:
                current_logger.error('error :: IONOSPHERE_LEARN_FOLDER is not set in settings.py')
                current_logger.info(traceback.format_exc())
                current_logger.error('error :: exiting, please fix setting.py')
                print ('error :: IONOSPHERE_LEARN_FOLDER is not set in settings.py')
                invalid_variables = True

            # @added 20160113 - Feature #1858: Ionosphere - autobuild features_profiles dir
            try:
                TEST_IONOSPHERE_AUTOBUILD = settings.IONOSPHERE_AUTOBUILD
            except:
                current_logger.error('error :: IONOSPHERE_AUTOBUILD is not set in settings.py')
                current_logger.info(traceback.format_exc())
                current_logger.error('error :: exiting, please fix setting.py')
                print ('error :: IONOSPHERE_AUTOBUILD is not set in settings.py')
                invalid_variables = True

        # @modified 20170115 - Feature #1854: Ionosphere learn - generations
        # These are now used in a shared context in terms of being required
        # by Panorama and ionosphere/learn
        try:
            TEST_IONOSPHERE_LEARN_DEFAULT_FULL_DURATION_DAYS = 1 + settings.IONOSPHERE_LEARN_DEFAULT_FULL_DURATION_DAYS
        except:
            current_logger.error('error :: IONOSPHERE_LEARN_DEFAULT_FULL_DURATION_DAYS is not set as an int in settings.py')
            current_logger.info(traceback.format_exc())
            current_logger.error('error :: exiting, please fix setting.py')
            print ('error :: IONOSPHERE_LEARN_DEFAULT_FULL_DURATION_DAYS is not set in settings.py')
            invalid_variables = True

        try:
            TEST_IONOSPHERE_LEARN_DEFAULT_VALID_TIMESERIES_OLDER_THAN_SECONDS = 1 + settings.IONOSPHERE_LEARN_DEFAULT_VALID_TIMESERIES_OLDER_THAN_SECONDS
        except:
            current_logger.error('error :: IONOSPHERE_LEARN_DEFAULT_VALID_TIMESERIES_OLDER_THAN_SECONDS is not set as an int in settings.py')
            current_logger.info(traceback.format_exc())
            current_logger.error('error :: exiting, please fix setting.py')
            print ('error :: IONOSPHERE_LEARN_DEFAULT_VALID_TIMESERIES_OLDER_THAN_SECONDS is not set in settings.py')
            invalid_variables = True

        try:
            TEST_IONOSPHERE_LEARN_DEFAULT_MAX_PERCENT_DIFF_FROM_ORIGIN = 1 + settings.IONOSPHERE_LEARN_DEFAULT_MAX_PERCENT_DIFF_FROM_ORIGIN
        except:
            current_logger.error('error :: IONOSPHERE_LEARN_DEFAULT_MAX_PERCENT_DIFF_FROM_ORIGIN is not set as a float or int in settings.py')
            current_logger.info(traceback.format_exc())
            current_logger.error('error :: exiting, please fix setting.py')
            print ('error :: IONOSPHERE_LEARN_DEFAULT_MAX_PERCENT_DIFF_FROM_ORIGIN is not set in settings.py')
            invalid_variables = True

        try:
            TEST_IONOSPHERE_LEARN_NAMESPACE_CONFIG = settings.IONOSPHERE_LEARN_NAMESPACE_CONFIG
        except:
            current_logger.error('error :: IONOSPHERE_LEARN_NAMESPACE_CONFIG is not set as a tuple of tuples in settings.py')
            current_logger.info(traceback.format_exc())
            current_logger.error('error :: exiting, please fix setting.py')
            print ('error :: IONOSPHERE_LEARN_NAMESPACE_CONFIG is not set in settings.py')
            invalid_variables = True

    # @added 20170809 - Task #2132: Optimise Ionosphere DB usage
    try:
        TEST_MEMCACHE_ENABLED = settings.MEMCACHE_ENABLED
    except:
        current_logger.error('error :: MEMCACHE_ENABLED is not set as a boolean in settings.py')
        current_logger.info(traceback.format_exc())
        current_logger.error('error :: exiting, please fix setting.py')
        print ('error :: MEMCACHE_ENABLED is not set in settings.py')
        invalid_variables = True
    try:
        TEST_MEMCACHED_SERVER_IP = settings.MEMCACHED_SERVER_IP
    except:
        current_logger.error('error :: MEMCACHED_SERVER_IP is not set as a str in settings.py')
        current_logger.info(traceback.format_exc())
        current_logger.error('error :: exiting, please fix setting.py')
        print ('error :: MEMCACHED_SERVER_IP is not set in settings.py')
        invalid_variables = True
    try:
        TEST_MEMCACHED_SERVER_PORT = settings.MEMCACHED_SERVER_PORT
    except:
        current_logger.error('error :: MEMCACHED_SERVER_PORT is not set as an int in settings.py')
        current_logger.info(traceback.format_exc())
        current_logger.error('error :: exiting, please fix setting.py')
        print ('error :: MEMCACHED_SERVER_PORT is not set in settings.py')
        invalid_variables = True

    # @added 20180524 - Branch #2270: luminosity
    try:
        TEST_LUMINOL_CROSS_CORRELATION_THRESHOLD = isinstance(settings.LUMINOL_CROSS_CORRELATION_THRESHOLD, float)
    except:
        current_logger.error('error :: LUMINOL_CROSS_CORRELATION_THRESHOLD is not set as a float in settings.py')
        current_logger.info(traceback.format_exc())
        current_logger.error('error :: exiting, please fix setting.py')
        print ('error :: LUMINOL_CROSS_CORRELATION_THRESHOLD is not set as a float in settings.py')
        invalid_variables = True
    if TEST_LUMINOL_CROSS_CORRELATION_THRESHOLD:
        if settings.LUMINOL_CROSS_CORRELATION_THRESHOLD >= 1.0:
            current_logger.error('error :: LUMINOL_CROSS_CORRELATION_THRESHOLD is >= 1.0 in settings.py')
            current_logger.info(traceback.format_exc())
            current_logger.error('error :: exiting, please fix setting.py')
            print ('error :: LUMINOL_CROSS_CORRELATION_THRESHOLD should be a float between 0.0 and 1.0000')
            invalid_variables = True
        if settings.LUMINOL_CROSS_CORRELATION_THRESHOLD <= 0.0:
            current_logger.error('error :: LUMINOL_CROSS_CORRELATION_THRESHOLD is <= 0.0 in settings.py')
            current_logger.info(traceback.format_exc())
            current_logger.error('error :: exiting, please fix setting.py')
            print ('error :: LUMINOL_CROSS_CORRELATION_THRESHOLD should be a float between 0.0 and 1.0000')
            invalid_variables = True

    if invalid_variables:
        return False

    return True
