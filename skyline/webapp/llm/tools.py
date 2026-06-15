"""
skyline/webapp/llm/tools.py

Tool execution layer for the Skyline LLM chat integration.

Each function here corresponds to a tool definition in prompts.TOOL_DEFINITIONS.
When the LLM decides to call a tool, the agent loop in chat.py dispatches to
the appropriate function here.

All functions return a dict that is serialisable to JSON and passed back to
the LLM as the tool result.  On error, return {'error': '<message>'} so the
LLM can reason about the failure gracefully.

Placeholder comments mark where existing Skyline DB/Redis functions should
be wired in.  The function signatures and return shapes are fixed - only the
internals need filling in.

# @added 20260228 - Task #5709: POC LLM integration
"""

import json
import logging
import os
import time
import traceback
from datetime import datetime, timezone

# @added 20260308
import pandas as pd

import requests
# @modified 20260308 - added create_engine for read-only enforcement
from sqlalchemy import text, create_engine

# @added 20260308
import zlib

# ---------------------------------------------------------------------------
# Skyline imports - adjust paths to match actual module locations
# ---------------------------------------------------------------------------
import settings

# TODO: import the Skyline logger appropriate to webapp context
# from skyline import logger  (or however webapp acquires its logger)
#from skyline.webapp.webapp import logger

# TODO: import DB connection/query utilities, e.g.
from skyline.database import get_engine

# TODO: import Redis connection utility, e.g.
from skyline_functions import get_redis_conn_decoded

skyline_app = 'webapp'
skyline_app_logger = skyline_app + 'Log'
logger = logging.getLogger(skyline_app_logger)

this_host = str(os.uname()[1])
this_host_as_bytes = str(this_host).encode()
SECRET_STRING_ = zlib.adler32(this_host_as_bytes)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _ts_to_utc(unix_ts):
    """Convert a unix timestamp integer to a UTC ISO 8601 string."""
    try:
        return datetime.fromtimestamp(int(unix_ts), tz=timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')
    except Exception:
        return str(unix_ts)


def _safe_json(obj):
    """Best-effort JSON serialisation for DB row results."""
    try:
        return json.loads(json.dumps(obj, default=str))
    except Exception:
        return str(obj)


# ---------------------------------------------------------------------------
# Tool implementations
# ---------------------------------------------------------------------------

def tool_get_current_time():
    """
    Return the current unix timestamp and a human-readable UTC string.
    No external dependencies - always safe to call.
    """
    now = int(time.time())
    return {
        'unix_timestamp': now,
        'utc_datetime': _ts_to_utc(now),
    }


def tool_query_db(timestamp, llm_model, tool_call_id, sql):
    """
    Execute a read-only SQL SELECT against the Skyline MariaDB database.

    Safety: only SELECT statements are permitted.  Any other statement type
    is rejected before execution.

    :param timestamp: The timestamp float that is the conversation identier.
    :type timestamp: float
    :param llm_model: The llm_model being used.
    :type llm_model: str
    :param tool_call_id: The tool_call_id
    :type tool_call_id: str
    :param sql: SQL SELECT statement string
    :type sql: str
    :return: dict with 'columns' list and 'rows' list of dicts, or 'error'
    :rtype: dict
    """
    logger.info(f"tools.tool_query_db query: {sql}")
    # Basic safety checks
    # Reject anything that does not start with SELECT
    sql_stripped = sql.strip().upper()
    if not sql_stripped.startswith('SELECT'):
        return {'error': 'Only SELECT statements are permitted.'}
    # Do not allow chained statements
    # SELECT 1; DELETE FROM anomalies; 
    if ';' in sql_stripped:
        return {'error': 'Multiple statements are not permitted.'}


    try:
        # @modified 20260308 -  - Task #5709: POC LLM integration
        # Use read-only user and engine
        #engine = get_engine(skyline_app)
        #try:
        #    engine, fail_msg, trace = get_engine(skyline_app)
        #except Exception as err:
        #    trace = traceback.format_exc()
        #    logger.error('%s' % trace)
        #    fail_msg = f"error :: tools.tool_query_db :: failed to get MySQL engine, err: {err}"
        #    logger.error('%s' % fail_msg)
        try:
            engine = create_engine(
                'mysql+mysqlconnector://%s:%s@%s:%s/%s' % (
                    settings.LLM_DBUSER, settings.LLM_DBUSERPASS,
                    settings.PANORAMA_DBHOST, str(settings.PANORAMA_DBPORT),
                    settings.PANORAMA_DATABASE))
            readonly_engine = engine.execution_options(isolation_level='AUTOCOMMIT')
        except Exception as err:
            trace = traceback.format_exc()
            logger.error('%s' % trace)
            fail_msg = f"error :: tools.tool_query_db :: create_engine failed, err: {err}"
            logger.error('%s' % fail_msg)

        with readonly_engine.connect() as conn:
            result = conn.execute(text(sql))
            #columns = list(result.keys())
            #rows = [dict(zip(columns, row)) for row in result.fetchall()]
            results = [dict(row._mapping) for row in result.fetchall()]
        try:
            readonly_engine.dispose()
        except:
            pass

        # @modified 20260308 -  - Task #5709: POC LLM integration
        # Create the dataframe and return df.head(1), df.tail(1) and df.info()
        # This prevents sending all the data (lots and lots of tokens) back to
        # the LLM.  Skyline only sends the smallest possible sample for the LLM
        # to verify that the results are as desired and allow for it to then
        # provide this dataframe as a source data set for the next tool call
        # which can be query and merged with another dataframe created from a
        # different query or source to generate the final result.
        #return {
        #    #'columns': columns,
        #    'rows': _safe_json(rows),
        #    'row_count': len(rows),
        #}
        # Create the results dataframe and save as csv
        json_safe_results = _safe_json(results)
        df = pd.DataFrame(json_safe_results)
        results_csv_filename = '%s.%s.csv' % (str(timestamp), str(tool_call_id))
        if '/' in llm_model:
            llm_model = llm_model.split('/')[-1]
        SECRET_STRING = '%s.llm_chat.%s' % (SECRET_STRING_, llm_model)
        secret_results_csv_filename = '%s.%s' % (
            str(SECRET_STRING), results_csv_filename)
        results_csv = '%s/%s' % (
            settings.SKYLINE_TMP_DIR, secret_results_csv_filename)
        df.to_csv(results_csv, index=False)
        results_count = len(json_safe_results)
        first_row = json_safe_results[0]
        last_row = None
        if results_count > 1:
            last_row = json_safe_results[-1]

        message = f"""
The {results_count} results of the sql query have converted to a dataframe and saved to the local csv file {results_csv_filename}

This is a sample of the returned data for you to ensure the results are as expected.

The first row of the query results is:
{first_row}
The last row is:
{last_row}

"""
        results = {
            'timestamp': timestamp,
            'tool': 'tool_query_db',
            'tool_call_id': tool_call_id,
            'sql': sql,
            'message': message,
            'results_csv': results_csv_filename,
        }
        return results

    except Exception as err:
        logger.error(f"error :: tools.tool_query_db, err: {err}")
        return {'error': 'DB query failed: %s' % str(err)}


def tool_query_dataframes(
        timestamp, llm_model, tool_call_id, dataframe_files, dataframes_query,
        final=False):
    """
    Execute pandas operations on the specified dataframes with the specified
    dataframes_queries.

    :param timestamp: The timestamp float that is the conversation identier.
    :type timestamp: float
    :param llm_model: The llm_model being used.
    :type llm_model: str
    :param tool_call_id: The tool_call_id
    :type tool_call_id: str
    :param dataframe_files: A dict of dataframe indentifiers mapped to the
        dataframe csv file, for each 
    :type dataframe_files: list
    :param dataframes_query: The pandas operation to run on the A dict of queries dataframe indentifiers mapped to the
        dataframe csv file
    :type dataframes_queries: str

    :return: dict with 'columns' list and 'rows' list of dicts, or 'error'
    :rtype: dict
    """
    logger.info('tools.tool_query_db called')

    # Blocklist dangerous builtins and OS access
    BLOCKED_TERMS = [
        '__import__', '__builtins__', '__globals__', '__locals__',
        'exec(', 'eval(', 'open(', 'os.', 'sys.', 'subprocess',
        'importlib', 'shutil', 'pathlib', 'socket', 'requests',
        'globals()', 'locals()', 'vars()', 'dir(',
        'getattr(', 'setattr(', 'delattr(',
    ]

    one_df_operation = False
    if len(dataframe_files) != 2:
        one_df_operation = True
        if 'df1' in dataframes_query:
            if 'df2' in dataframes_query:
                one_df_operation = False
        if not one_df_operation:
            logger.error('error :: tools.tool_query_dataframes - dataframe_files does not specify 2 dataframes')
            return {'error': 'dataframe_files does not specify 2 dataframes'}

    if '/' in llm_model:
        llm_model = llm_model.split('/')[-1]
    SECRET_STRING = '%s.llm_chat.%s' % (SECRET_STRING_, llm_model)
    try:
        # Basic safety gate - reject anything that is not saved with the
        # SECRET_STRING
        bad_llm_file_requests = []
        validated_dataframe_files = []
        for unhashed_csv_file in dataframe_files:
            full_hashed_csv_file = '%s/%s.%s' % (settings.SKYLINE_TMP_DIR, SECRET_STRING, unhashed_csv_file)
            if not os.path.isfile(full_hashed_csv_file):
                # The LLM has been very naughty, bad or made a mistake.
                bad_llm_file_requests.append(unhashed_csv_file)
                continue
            validated_dataframe_files.append(full_hashed_csv_file)
        if len(bad_llm_file_requests) > 0:
            logger.error(f"error :: tools.tool_query_dataframes - bad_llm_file_requests: {bad_llm_file_requests}")
            return {'error': 'These are not the files you are looking for ...'}

        logger.info(f"tools.tool_query_dataframes - validated_dataframe: {validated_dataframe_files}")

        logger.info(f"tools.tool_query_dataframes - dataframes_query: {dataframes_query}")
        if 'df1' not in str(dataframes_query):
            logger.error('error :: tools.tool_query_dataframes - bad dataframes_query no df1 declared')
            return {'error': 'bad dataframes_query'}
        if not one_df_operation:
            if 'df2' not in str(dataframes_query):
                logger.error('error :: tools.tool_query_dataframes - bad dataframes_query no df1 declared')
                return {'error': 'bad dataframes_query'}

        for term in BLOCKED_TERMS:
            if term in dataframes_query:
                logger.error('error :: blocked term in dataframes_query: %s' % term)
                return {'error': 'Query contains disallowed operations'}
    
        dataframes = {}
        for i, full_hashed_csv_file in enumerate(validated_dataframe_files):
            df = None
            try:
                if i == 0:
                    df1 = pd.read_csv(full_hashed_csv_file)
                    dataframes[i] = df1
                else:
                    df2 = pd.read_csv(full_hashed_csv_file)
                    dataframes[i] = df2
            except Exception as err:
                logger.error(f"error :: tools.tool_query_dataframes - pandas failed to load {full_hashed_csv_file}, err: {err}")
        if not one_df_operation:
            if len(dataframes) == 2:
                logger.error('error :: tools.tool_query_dataframes - 2 dataframes were not loaded')
                return {'error': '2 dataframes were not loaded'}

        results = None
        try:
            # Only expose df1, df2 and pandas to the eval context
            # Nothing else from the environment is accessible
            safe_globals = {'__builtins__': {}}
            if one_df_operation:
                safe_locals = {
                    'df1': df1,
                    'pd': pd,
                }
            else:
                safe_locals = {
                    'df1': df1,
                    'df2': df2,
                    'pd': pd,
                }
            results = eval(dataframes_query, safe_globals, safe_locals)
        except Exception as err:
            logger.error(f"error :: tools.tool_query_dataframes - dataframes_query failed, err: {err}")
            return {'error': 'dataframe_query failed: %s' % str(err)}
        if not isinstance(results, pd.DataFrame):
            logger.error('error :: tools.tool_query_dataframes - result is not a DataFrame')
            return {'error': 'query must return a DataFrame'}
        MAX_RESULT_ROWS = 100000
        if len(results) > MAX_RESULT_ROWS:
            logger.warning('tools.tool_query_dataframes - truncating results from %s to %s rows' % (len(results), MAX_RESULT_ROWS))
            results = results.head(MAX_RESULT_ROWS)

        json_safe_results = _safe_json(results)
        df = pd.DataFrame(json_safe_results)
        results_csv_filename = '%s.%s.csv' % (str(timestamp), str(tool_call_id))
        secret_results_csv_filename = '%s.%s' % (
            str(SECRET_STRING), results_csv_filename)
        results_csv = '%s/%s' % (
            settings.SKYLINE_TMP_DIR, secret_results_csv_filename)
        try:
            df.to_csv(results_csv, index=False)
        except Exception as err:
            logger.error(f"error :: tools.tool_query_dataframes - failed to save {results_csv_filename}, err: {err}")
            return {'error': f"failed to save: {results_csv_filename}"}
        first_row = None
        last_row = None
        results_count = len(json_safe_results)
        if results_count:
            first_row = json_safe_results[0]
            if results_count > 1:
                last_row = json_safe_results[-1]

        message = f"""
The {results_count} results of the dataframes_query have been converted to a dataframe and saved to the local csv file {results_csv_filename}

This is a sample of the returned data for you to ensure the results are as expected.

The first row of the query results is:
{first_row}
The last row is:
{last_row}

"""

        results = {
            'timestamp': timestamp,
            'tool': 'tool_query_dataframes',
            'tool_call_id': tool_call_id,
            'dataframes_query': dataframes_query,
            'message': message,
            'results_csv': results_csv_filename,
        }
        return results

    except Exception as err:
        logger.error(f"error :: tools.tool_query_db, err: {err}")
        return {'error': 'DB query failed: %s' % str(err)}

# ---------------------------------------------------------------------------
# Dispatch table - maps tool name strings to functions
# Used by the agent loop in chat.py
# ---------------------------------------------------------------------------
# Only the 3 tools are required.
TOOL_DISPATCH = {
    'get_current_time':         lambda args: tool_get_current_time(),
#    'get_anomalies':            lambda args: tool_get_anomalies(**args),
    'query_db':                 lambda args: tool_query_db(**args),
    'query_dataframes':         lambda args: tool_query_dataframes(**args),
#    'get_anomalies':            lambda args: tool_get_anomalies(**args),
#    'get_metric_anomaly_count': lambda args: tool_get_metric_anomaly_count(**args),
#    'get_correlated_metrics':   lambda args: tool_get_correlated_metrics(**args),
#    'get_illuminance_activity': lambda args: tool_get_illuminance_activity(**args),
#    'get_metric_info':          lambda args: tool_get_metric_info(**args),
#    'get_ionosphere_matches':   lambda args: tool_get_ionosphere_matches(**args),
#    'get_comments':             lambda args: tool_get_comments(**args),
#    'get_related_metrics':      lambda args: tool_get_related_metrics(**args),
}


def execute_tool(tool_name, tool_args):
    """
    Execute a named tool with the provided arguments dict.

    Called by the agent loop.  Returns a JSON-serialisable dict.
    Unknown tool names return an error dict rather than raising.

    :param tool_name: string matching a key in TOOL_DISPATCH
    :param tool_args: dict of keyword arguments for the tool
    :return: tool result dict
    :rtype: dict
    """
    if tool_name not in TOOL_DISPATCH:
        return {'error': 'Unknown tool: %s' % tool_name}
    try:
        logger.info(f"tools.execute_tool calling {tool_name}")
        return TOOL_DISPATCH[tool_name](tool_args)
    except Exception as err:
        logger.error(f"error :: tools.execute_tool calling {tool_name}, err: {err}")
        return {'error': 'Tool %s raised exception: %s' % (tool_name, str(err))}
