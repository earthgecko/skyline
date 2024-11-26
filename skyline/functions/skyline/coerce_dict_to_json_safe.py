"""
coerce_dict_to_json_safe.py
"""
import logging
import math
import numpy as np
import traceback


# @added 20241121 - Feature #5519: functions.skyline.coerce_to_valid_json
#                   Bug #5518: custom_algorithms_results - invalid JSON
def coerce_dict_to_json_safe(current_skyline_app, data_dict):
    """
    A global function to coerce a dict to value which are json safe.  Coercing
    data to valid json is required because outputs that write to json resources,
    such as custom_algorithm results may not coerce things to json such as NaN,
    numpy.int64, etc, but also because things change.  The things which
    custom_algorithms use can change and begin to return typed outputs.  Things
    such as NaNs, np.int64, np.float64, np.bool_, etc. Or at times an int like a
    timestamp or a float will be used as the key/s in a dict, json keys can only
    be strings and will result in errors such as:
    
    TypeError: Object of type int32 is not JSON serializable

    This function coerces known problem strings to valid json types and returns
    the coerced_dict which itself in json safe and coerced which is a dict that
    is composed of only the things which were coerce.  This coerce dict is used
    to provide debug information to identify what elements need to be coerced
    upstream.

    :param current_skyline_app: the Skyline app.
    :param data_dict: the Python dict to coerce to a json safe format.
    :type current_skyline_app: str
    :type data_dict: dict
    :return: coerced_dict, coerced
    :rtype: tuple(dict, dict)

    """

    coerced_dict = {}
    coerced = {}

    function_str = 'coerce_dict_to_json_safe'
    current_skyline_app_logger = current_skyline_app + 'Log'
    current_logger = logging.getLogger(current_skyline_app_logger)

    def get_coerced(original_data, coerced_data):
        def diff_dict(orig, coer):
            differences = {}
            for key in orig:
                if key in coer:
                    if isinstance(orig[key], dict) and isinstance(coer[key], dict):
                        nested_diff = diff_dict(orig[key], coer[key])
                        # Only add if there are changes
                        if nested_diff:
                            differences[key] = nested_diff
                    elif isinstance(orig[key], list) and isinstance(coer[key], list):
                        list_diff = diff_list(orig[key], coer[key])
                        # Only add if there are changes
                        if list_diff:
                            differences[key] = list_diff
                    elif type(orig[key]) is not type(coer[key]):
                        # Keep the original value with different type
                        differences[key] = orig[key]
            return differences

        def diff_list(orig_list, coer_list):
            differences = []
            for i in range(min(len(orig_list), len(coer_list))):
                if isinstance(orig_list[i], dict) and isinstance(coer_list[i], dict):
                    nested_diff = diff_dict(orig_list[i], coer_list[i])
                    # Only add if there are changes
                    if nested_diff:
                        differences.append(nested_diff)
                elif isinstance(orig_list[i], list) and isinstance(coer_list[i], list):
                    list_diff = diff_list(orig_list[i], coer_list[i])
                    # Only add if there are changes
                    if list_diff:
                        differences.append(list_diff)
                elif type(orig_list[i]) is not type(coer_list[i]):
                    # Keep the original value with different type
                    differences.append(orig_list[i])
            # Handle extra elements in the original list
            if len(orig_list) > len(coer_list):
                differences.extend(orig_list[len(coer_list):])
            return differences

        return diff_dict(original_data, coerced_data)


    def make_json_safe_dict(input_dict):
        def coerce(value):
            # Coerce np.bool_ to a bool and ensure booleans are preserved and
            # not coerced to 0 or 1 when the int coercion is done.
            if isinstance(value, bool):
                return bool(value)
            if isinstance(value, str):
                return value
            # numpy dtypes inherit from their base Python counterparts therefore
            # the base type can be used and will match the np dtype e.g.
            # np.int64(2) IS isinstance int and IS isinstance np.int64
            # Coerce np.int64, np.integer, etc into an int
            if isinstance(value, int):
                return int(value)
            # Coerce np.float64, np.floating, etc into a float and NaN to None
            if isinstance(value, float):  
                return float(value) if not math.isnan(value) else None
            # While np int64, float64 and bool_ inherit int32 and float32 do not
            if isinstance(value, np.integer):
                return int(value)
            if isinstance(value, np.floating):
                return float(value)
            # Handle nested dictionaries
            if isinstance(value, dict):
                return {str(k): coerce(v) for k, v in value.items()}
            # Handle nested lists
            if isinstance(value, list):
                return [coerce(v) for v in value]
            return value

        # Ensure all keys are strings and coerce all values to json safe types
        try:
            coerced_dict = {str(k): coerce(v) for k, v in input_dict.items()}
        except Exception as err:
            current_logger.error(traceback.format_exc())
            current_logger.error('error :: %s :: make_json_safe_dict - failed to created coerced_dict, err: %s' % (
                function_str, err))
        return coerced_dict

    current_logger.info('%s :: coercing data' % function_str)
    try:
        coerced_dict = make_json_safe_dict(data_dict)
    except Exception as err:
        current_logger.error('error :: %s :: failed to created coerced_dict, err: %s' % (
            function_str, err))
        return coerced_dict, coerced

    if coerced_dict:
        # They are compared as strings because if they are compared as dicts
        # if one dict has np.int32(496) and one has 496 they will evaluate in a
        # dict comparison as the same, whereas when converted to string these
        # differences are detected.
        if str(data_dict) != str(coerced_dict):
            current_logger.info('%s :: there was data that needed to be coerced' % function_str)
            current_logger.info('%s :: len(str(data_dict)): %s, len(str(coerced_dict)): %s' % (
                function_str, str(len(str(data_dict))), str(len(str(coerced_dict)))))
            try:
                coerced = get_coerced(data_dict, coerced_dict)
                current_logger.info('%s :: determined what was coerced for debug, len(coerced): %s' % (
                    function_str, str(len(coerced))))
            except Exception as err:
                current_logger.error(traceback.format_exc())
                current_logger.error('error :: %s :: get_coerced - failed to created coerced, err: %s' % (
                    function_str, err))

    if not coerced_dict:
        current_logger.error('error :: %s :: no coerced_dict interpolated' % (
            function_str))

    return coerced_dict, coerced
