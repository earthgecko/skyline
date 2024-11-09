"""
coerce_to_valid_json.py
"""
import logging
import json
import os
import re


# @added 20241025 - Feature #5519: functions.skyline.coerce_to_valid_json
#                   Bug #5518: custom_algorithms_results - invalid JSON
def coerce_to_valid_json(current_skyline_app, json_str=None, json_file=None):
    """
    A global function to coerce invalid json data.  Coercing data to valid json
    is required because outputs that write to json resources, such as
    custom_algorithm results may not coerce things to json such as NaN,
    numpy.int64, etc.
    This function coerces known problem strings to valid json types and either
    returns the new json_str and/or writes the valid json to file.

    """
    json_resource = None
    function_str = 'coerce_to_valid_json'
    current_skyline_app_logger = current_skyline_app + 'Log'
    current_logger = logging.getLogger(current_skyline_app_logger)
    coerce_map = {
        'NaN': 'null',
        'Nan': 'null',
        'nan': 'null',
        'None': 'null',
        'none': 'null',
        'False': 'false',
        'True': 'true',
    }
    save_file = False
    if json_file:
        if os.path.isfile(json_file):
            try:
                with open(json_file, 'r') as f:
                    json_str = f.read()
            except Exception as err:
                current_logger.error('error :: %s :: data from json_file %s could not be loaded, err: %s' % (
                    function_str, str(json_file), err))
                return json_resource
            if len(json_str) > 0:
                save_file = True

    new_json_str = None
    if json_str:
        # coerce_str
        try:
            pattern = re.compile(r'\b(' + '|'.join(coerce_map.keys()) + r')\b')
            new_json_str = pattern.sub(lambda x: coerce_map[x.group()], json_str)
        except Exception as err:
            current_logger.error('error :: %s :: failed to coerce json_str, err: %s' % (
                function_str, err))
            return json_resource
    valid_json = False
    if new_json_str:
        # coerce_str
        try:
            valid_json = json.loads(new_json_str)
        except Exception as err:
            current_logger.error('error :: %s :: failed to load coerced json_str as json, err: %s' % (
                function_str, err))
            return json_resource
    if not valid_json:
        return json_resource
    if valid_json:
        json_resource = str(new_json_str)        
    if save_file and json_file:
        file_saved = False
        # Copy original to .bak
        # Create json_file with valid json

    return json_resource