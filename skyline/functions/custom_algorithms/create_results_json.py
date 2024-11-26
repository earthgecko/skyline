"""
create_results_json.py
"""
import logging
import json
import os
import traceback

from functions.skyline.coerce_dict_to_json_safe import coerce_dict_to_json_safe
from skyline_functions import mkdir_p


# @added 20241121 - Feature #5519: functions.skyline.create_results_json
#                   Bug #5518: custom_algorithms_results - invalid JSON
def create_results_json(current_skyline_app, data_dict, json_file=None):
    """
    A function to create json from custom_algorithms results and write it
    to a file or to a debug txt file as a last resort.  This is required because
    the input to the custom_algorithms results dict is "user" generated and may
    not to coerced properly, but also because things change.  The things which
    custom_algorithms use can change and begin to return typed outputs.  Things
    such as NaNs, np.int64, np.float64, np.bool_, etc. Or at times an int like a
    timestamp or a float will be used as the key/s in a dict, json keys can only
    be strings and will result in errors such as:
    
    TypeError: Object of type int32 is not JSON serializable

    This function coerces known problem strings to valid json types and returns
    the coerced_dict which itself in json safe and creates a valid json the
    `'json_file'` if the parameter is passed.

    The function as logs if coercions were made and writes what was coerced to a
    as plain text to file for debugging purposes to identify what coercions are
    not being handled by the custom_algorithm.

    :param current_skyline_app: the Skyline app.
    :param data_dict: the Python dict to coerce create json fromparent pid which is executing the algorithm, this is
        **required** for error handling and logging.  You do not have to worry
        about handling this argument in the scope of algorithm, but the
        algorithm must accept it as the second argument.
    :param json_file: the file to write the json too, full path and file name.
    :type current_skyline_app: str
    :type data_dict: dict
    :type json_file: str
    :return: coerced_dict, coerced, json_file_created
    :rtype: tuple(dict, dict, str)

    """
    coerced_dict = {}
    coerced = {}
    json_file_created = None

    function_str = 'create_results_json'
    current_skyline_app_logger = current_skyline_app + 'Log'
    current_logger = logging.getLogger(current_skyline_app_logger)
    
    current_logger.info('%s :: coercing dict to json safe dict' % function_str)
    try:
        coerced_dict, coerced = coerce_dict_to_json_safe(current_skyline_app, data_dict)
    except Exception as err:
        current_logger.error('error :: %s :: failed to created coerced_dict, err: %s' % (
            function_str, err))
        return coerced_dict, coerced, json_file_created

    if not coerced_dict:
        current_logger.error('error :: %s :: no coerced_dict interpolated' % (
            function_str))
        return coerced_dict, coerced, json_file_created

    if json_file:
        current_logger.info('%s :: dumping coerced json safe dict to json_file: %s' % (
            function_str, json_file))
        json_file_dir = os.path.dirname(json_file)
        if not os.path.exists(json_file_dir):
            mkdir_p(json_file_dir)
        try:
            with open(json_file, 'w') as f:
                f.write(json.dumps(coerced_dict, skipkeys=True))
            os.chmod(json_file, mode=0o644)
            json_file_created = str(json_file)
            current_logger.info('%s :: saved %s' % (function_str, json_file))
        except Exception as err:
            current_logger.error(traceback.format_exc())
            current_logger.error('error :: failed to save json to %s, err: %s' % (
                json_file, err))
        if not json_file_created:
            # Create as txt file for debugging purposes
            debug_file = str(json_file)
            debug_file = debug_file.replace('.json', '.debug.txt')
            try:
                with open(debug_file, 'w') as f:
                    f.write(str(coerced_dict))
                os.chmod(json_file, mode=0o644)
                json_file_created = str(debug_file)
                current_logger.info('%s :: saved as DEBUG file - %s' % (function_str, json_file_created))
            except Exception as err:
                current_logger.error(traceback.format_exc())
                current_logger.error('error :: failed to save text to %s, err: %s' % (
                    debug_file, err))
        if len(coerced) > 0:
            # Create a text file of the things that required coercion for
            # debugging purposes
            coercion_debug_file = str(json_file)
            coercion_debug_file = coercion_debug_file.replace('.json', '.not_coerced.debug')
            current_logger.info('%s :: creating %s' % (function_str, coercion_debug_file))
            try:
                with open(coercion_debug_file, 'w') as f:
                    f.write(str(coerced))
                os.chmod(coercion_debug_file, mode=0o644)
                current_logger.info('warning :: %s :: the input custom_algorithms dict had things that needed to be coerced, what was coerced is saved for debugging to file: %s' % (
                    function_str, coercion_debug_file))
            except Exception as err:
                current_logger.error(traceback.format_exc())
                current_logger.error('error :: failed to save text to coercion_debug_file: %s, err: %s' % (
                    coercion_debug_file, err))

    return coerced_dict, coerced, json_file_created

