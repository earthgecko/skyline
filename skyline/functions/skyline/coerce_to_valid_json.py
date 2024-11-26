"""
coerce_to_valid_json.py
"""
import logging
import json
import os
import re
import traceback
from shutil import copyfile


# @added 20241025 - Feature #5519: functions.skyline.coerce_to_valid_json
#                   Bug #5518: custom_algorithms_results - invalid JSON
def coerce_to_valid_json(
        current_skyline_app, json_str=None, save_file=False, json_file=None):
    """
    A global function to coerce invalid json data.  Coercing data to valid json
    is required because outputs that write to json resources, such as
    custom_algorithm results may not coerce things to json such as NaN,
    numpy.int64, etc.
    This function coerces known problem strings to valid json types and either
    returns the valid coerced json as a dict and can writes the valid json to
    file.

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
        
    if new_json_str:
        try:
            # Handle np dtypes - match np types and np.*_ patterns
            pattern = r"np\.(?:int64|uint64|float64|int32|uint32|float32|int16|uint16|float16|int8|uint8|float8)\(([^)]+)\)|np\.(\w+)_"
            # Replace np types with their raw values and np.*_ with their names without np. and _
            new_json_str = re.sub(pattern, lambda m: m.group(1) if m.group(1) else m.group(2).lower(), new_json_str)
        except Exception as err:
            current_logger.error('error :: %s :: failed to coerce np dtypes from new_json_str, err: %s' % (
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
        bak_file = '%s.bak' % json_file    
        try:
            copyfile(json_file, bak_file)
            current_logger.info('%s :: invalid json_file: %s, to backup: %s' % (
                function_str, json_file, bak_file))
        except Exception as err:
            current_logger.error('error :: failed to copy json_file: %s, to backup: %s, err: %s' % (
                json_file, bak_file, err))
        # Create json_file with valid json
        try:
            with open(json_file, 'w') as f:
                f.write(json.dumps(valid_json, skipkeys=True))
            os.chmod(json_file, mode=0o644)
            json_file_created = str(json_file)
            current_logger.info('%s :: valid saved %s' % (function_str, json_file))
        except Exception as err:
            current_logger.error(traceback.format_exc())
            current_logger.error('error :: failed to save json to %s, err: %s' % (
                json_file, err))
        if not json_file_created:
            # Create as txt file for debugging purposes
            debug_file = json_file.replace('json', 'debug.txt')
            try:
                with open(debug_file, 'w') as f:
                    f.write(str(valid_json))
                os.chmod(json_file, mode=0o644)
                json_file_created = str(debug_file)
                current_logger.info('%s :: saved as DEBUG file - %s' % (function_str, json_file_created))
            except Exception as err:
                current_logger.error(traceback.format_exc())
                current_logger.error('error :: failed to save text to %s, err: %s' % (
                    debug_file, err))

    return json_resource