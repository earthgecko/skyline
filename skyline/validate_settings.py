import logging
import traceback
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
            current_logger.error('error :: IONOSPHERE_FEATURES_PERCENT_SIMILAR is not set as an int in settings.py')
            current_logger.info(traceback.format_exc())
            current_logger.error('error :: exiting, please fix setting.py')
            print ('error :: IONOSPHERE_FEATURES_PERCENT_SIMILAR is not set in settings.py')
            invalid_variables = True

        # @added 20170109 - Feature #1854: Ionosphere learn
        # Added the Ionosphere LEARN related variables
        ionosphere_learning_enabled = False
        try:
            ionosphere_learning_enabled = settings.IONOSPHERE_ENABLE_LEARNING
        except:
            current_logger.error('error :: IONOSPHERE_ENABLE_LEARNING is not set as a boolean in settings.py')
            current_logger.info(traceback.format_exc())
            current_logger.error('error :: exiting, please fix setting.py')
            print ('error :: IONOSPHERE_ENABLE_LEARNING is not set in settings.py')
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

            try:
                TEST_IONOSPHERE_DEFAULT_LEARN_FULL_DURATION_DAYS = 1 + settings.IONOSPHERE_DEFAULT_LEARN_FULL_DURATION_DAYS
            except:
                current_logger.error('error :: IONOSPHERE_DEFAULT_LEARN_FULL_DURATION_DAYS is not set as an int in settings.py')
                current_logger.info(traceback.format_exc())
                current_logger.error('error :: exiting, please fix setting.py')
                print ('error :: IONOSPHERE_DEFAULT_LEARN_FULL_DURATION_DAYS is not set in settings.py')
                invalid_variables = True

            try:
                TEST_IONOSPHERE_DEFAULT_LEARN_VALID_TIMESERIES_OLDER_THAN_SECONDS = 1 + settings.IONOSPHERE_DEFAULT_LEARN_VALID_TIMESERIES_OLDER_THAN_SECONDS
            except:
                current_logger.error('error :: IONOSPHERE_DEFAULT_LEARN_VALID_TIMESERIES_OLDER_THAN_SECONDS is not set as an int in settings.py')
                current_logger.info(traceback.format_exc())
                current_logger.error('error :: exiting, please fix setting.py')
                print ('error :: IONOSPHERE_DEFAULT_LEARN_VALID_TIMESERIES_OLDER_THAN_SECONDS is not set in settings.py')
                invalid_variables = True

            try:
                TEST_IONOSPHERE_LEARNING_NAMESPACE_CONFIG = settings.IONOSPHERE_LEARNING_NAMESPACE_CONFIG
            except:
                current_logger.error('error :: IONOSPHERE_LEARNING_NAMESPACE_CONFIG is not set as a tuple of tuples in settings.py')
                current_logger.info(traceback.format_exc())
                current_logger.error('error :: exiting, please fix setting.py')
                print ('error :: IONOSPHERE_LEARNING_NAMESPACE_CONFIG is not set in settings.py')
                invalid_variables = True

    if invalid_variables:
        return False

    return True
