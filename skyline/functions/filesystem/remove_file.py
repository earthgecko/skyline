import logging
from os.path import isfile
from os import remove as os_remove

import settings


# @added 20210525 - Branch #1444: thunder
def remove_file(current_skyline_app, file_to_remove):
    """
    Remove a file if it exists AND is in a valid Skyline directory.

    :param current_skyline_app: the Skyline app making the call
    :param file_to_remove: the full path and filename to remove
    :type current_skyline_app: str
    :type file_to_remove: str
    :return: removed_file
    :rtype: boolean

    """

    function_str = 'functions.filessystem.remove_file'
    removed_file = False
    current_logger = None

    if not isinstance(file_to_remove, str):
        current_skyline_app_logger = current_skyline_app + 'Log'
        current_logger = logging.getLogger(current_skyline_app_logger)
        current_logger.error('error :: %s :: file_to_remove not a valid str - %s' % (
            function_str, str(file_to_remove)))
        return removed_file

    if isfile(file_to_remove):

        # Check it is in a valid directory
        valid_dir = False
        settings_variables = [v for v in dir(settings) if v[:2] != '__']
        dir_variables = [v for v in settings_variables if '_DIR' in v or '_PATH' in v and 'WHISPER' not in v and 'LOG_PATH' not in v]
        valid_dirs = []
        for dir_variable in dir_variables:
            valid_dir = getattr(settings, dir_variable)
            valid_dirs.append(valid_dir)
        for v_dir in valid_dirs:
            if v_dir in file_to_remove:
                valid_dir = True
                break
        if not valid_dir:
            current_skyline_app_logger = current_skyline_app + 'Log'
            current_logger = logging.getLogger(current_skyline_app_logger)
            current_logger.error('error :: %s :: file_to_remove is not in a valid Skyline dir - %s' % (
                function_str, str(file_to_remove)))
            return removed_file

        try:
            os_remove(file_to_remove)
            removed_file = True
        except OSError:
            current_skyline_app_logger = current_skyline_app + 'Log'
            current_logger = logging.getLogger(current_skyline_app_logger)
            current_logger.error('error :: %s :: failed to remove %s, continuing' % (
                function_str, file_to_remove))
            return False
        except Exception as e:
            current_skyline_app_logger = current_skyline_app + 'Log'
            current_logger = logging.getLogger(current_skyline_app_logger)
            current_logger.error('error :: %s ::  failed to remove %s, continuing - %s' % (
                function_str, file_to_remove, e))
            return False

    return removed_file
