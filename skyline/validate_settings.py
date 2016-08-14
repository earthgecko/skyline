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
        logger.error('error :: REDIS_SOCKET_PATH is not set in settings.py')
        logger.info(traceback.format_exc())
        logger.error('error :: exiting, please fix setting.py')
        print ('error :: REDIS_SOCKET_PATH is not set in settings.py')
        invalid_variables = True

    try:
        TEST_SKYLINE_TMP_DIR = settings.SKYLINE_TMP_DIR
    except:
        logger.error('error :: SKYLINE_TMP_DIR is not set in settings.py')
        logger.info(traceback.format_exc())
        logger.error('error :: exiting, please fix setting.py')
        print ('error :: SKYLINE_TMP_DIR is not set in settings.py')
        invalid_variables = True

    try:
        TEST_FULL_NAMESPACE_IN_SETTINGS = settings.FULL_NAMESPACE
    except:
        logger.error('error :: FULL_NAMESPACE is not set in settings.py')
        logger.info(traceback.format_exc())
        logger.error('error :: exiting, please fix setting.py')
        print ('error :: FULL_NAMESPACE is not set in settings.py')
        invalid_variables = True

    try:
        TEST_CRUCIBLE_CHECK_PATH = settings.CRUCIBLE_CHECK_PATH
    except:
        logger.error('error :: CRUCIBLE_CHECK_PATH is not set in settings.py')
        logger.info(traceback.format_exc())
        logger.error('error :: exiting, please fix setting.py')
        print ('error :: CRUCIBLE_CHECK_PATH is not set in settings.py')
        valid_variables = True
    try:
        PANORAMA_CHECK_PATH = settings.PANORAMA_CHECK_PATH
    except:
        logger.error('error :: PANORAMA_CHECK_PATH is not set in settings.py')
        logger.info(traceback.format_exc())
        logger.error('error :: exiting, please fix setting.py')
        print ('error :: PANORAMA_CHECK_PATH is not set in settings.py')
        valid_variables = True

    if current_skyline_app in 'analyzer':
        try:
            TEST_ALGORITHMS_IN_SETTINGS = settings.ALGORITHMS
        except:
            logger.error('error :: ALGORITHMS is not set in settings.py')
            logger.info(traceback.format_exc())
            logger.error('error :: exiting, please fix setting.py')
            print ('error :: ALGORITHMS is not set in settings.py')
            invalid_variables = True

        try:
            TEST_ANALYZER_PROCESSES = settings.ANALYZER_PROCESSES + 1
        except:
            logger.error('error :: ANALYZER_PROCESSES is not set to an int in settings.py')
            logger.info(traceback.format_exc())
            logger.error('error :: exiting, please fix setting.py')
            print ('error :: ANALYZER_PROCESSES is not set in settings.py')
            invalid_variables = True

        try:
            TEST_MAX_ANALYZER_PROCESS_RUNTIME = settings.MAX_ANALYZER_PROCESS_RUNTIME + 1
        except:
            logger.error('error :: MAX_ANALYZER_PROCESS_RUNTIME is not set to an int in settings.py')
            logger.info(traceback.format_exc())
            logger.error('error :: exiting, please fix setting.py')
            print ('error :: MAX_ANALYZER_PROCESS_RUNTIME is not set in settings.py')
            invalid_variables = True

        try:
            TEST_ANALYZER_OPTIMUM_RUN_DURATION = settings.ANALYZER_OPTIMUM_RUN_DURATION + 1
        except:
            logger.error('error :: ANALYZER_OPTIMUM_RUN_DURATION is not set to an int in settings.py')
            logger.info(traceback.format_exc())
            logger.error('error :: exiting, please fix setting.py')
            print ('error :: ANALYZER_OPTIMUM_RUN_DURATION is not set in settings.py')
            invalid_variables = True

        try:
            TEST_ANALYZER_CRUCIBLE_ENABLED = settings.ANALYZER_CRUCIBLE_ENABLED
        except:
            logger.error('error :: ANALYZER_CRUCIBLE_ENABLED is not set in settings.py')
            logger.info(traceback.format_exc())
            print ('error :: ANALYZER_CRUCIBLE_ENABLED is not set in settings.py')
            invalid_variables = True

    try:
        TEST_ENABLE_ALERTS = settings.ENABLE_ALERTS
    except:
        logger.error('error :: ENABLE_ALERTS is not set in settings.py')
        logger.info(traceback.format_exc())
        logger.error('error :: exiting, please fix setting.py')
        print ('error :: ENABLE_ALERTS is not set in settings.py')
        invalid_variables = True

    try:
        TEST_MIRAGE_CHECK_PATH = settings.MIRAGE_CHECK_PATH
    except:
        logger.error('error :: MIRAGE_CHECK_PATH is not set in settings.py')
        logger.info(traceback.format_exc())
        logger.error('error :: exiting, please fix setting.py')
        print ('error :: MIRAGE_CHECK_PATH is not set in settings.py')
        invalid_variables = True

    try:
        TEST_ENABLE_CRUCIBLE = settings.ENABLE_CRUCIBLE
    except:
        logger.error('error :: ENABLE_CRUCIBLE is not set in settings.py')
        logger.info(traceback.format_exc())
        logger.error('error :: exiting, please fix setting.py')
        print ('error :: ENABLE_CRUCIBLE is not set in settings.py')
        invalid_variables = True

    try:
        TEST_PANORAMA_ENABLED = settings.PANORAMA_ENABLED
    except:
        logger.error('error :: PANORAMA_ENABLED is not set in settings.py')
        logger.info(traceback.format_exc())
        logger.error('error :: exiting, please fix setting.py')
        print ('error :: PANORAMA_ENABLED is not set in settings.py')
        invalid_variables = True

    if invalid_variables:
        return False

    return True
