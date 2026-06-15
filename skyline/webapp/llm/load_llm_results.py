"""
skyline/webapp/llm/load_llm_results.py

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

import logging
import os

import pandas as pd
import zlib

# ---------------------------------------------------------------------------
# Skyline imports - adjust paths to match actual module locations
# ---------------------------------------------------------------------------
from settings import SKYLINE_TMP_DIR

skyline_app = 'webapp'
skyline_app_logger = skyline_app + 'Log'
logger = logging.getLogger(skyline_app_logger)

this_host = str(os.uname()[1])
this_host_as_bytes = str(this_host).encode()
SECRET_STRING_ = zlib.adler32(this_host_as_bytes)


def load_llm_results(llm_model, csv_file):
    """
    Load the csv_file and return the data as a dict for the jinja template table.
    
    """
    llm_results = {}
    if '/' in llm_model:
        llm_model = llm_model.split('/')[-1]

    SECRET_STRING = '%s.llm_chat.%s' % (SECRET_STRING_, llm_model)

    full_hashed_csv_file = '%s/%s.%s' % (SKYLINE_TMP_DIR, SECRET_STRING, csv_file)
    if not os.path.isfile(full_hashed_csv_file):
        logger.error(f"error :: load_llm_results {full_hashed_csv_file} does not exist")

    MAX_DISPLAY_ROWS = 500  # cap what gets sent to browser
    try:
        df = pd.read_csv(full_hashed_csv_file)
        # List of dicts for Jinja table
        #llm_results = df.to_dict(orient='records')
        truncated = len(df) > MAX_DISPLAY_ROWS
        df_display = df.head(MAX_DISPLAY_ROWS)
        logger.info(f"load_llm_results - loaded {full_hashed_csv_file}")
    except Exception as err:
        logger.error(f"error :: load_llm_results failed to load {full_hashed_csv_file}, err: {err}")

    llm_results = {
        'columns': list(df_display.columns),
        'rows': df_display.to_dict(orient='records'),
        'truncated': truncated,
        'filename': full_hashed_csv_file,
    }
    return llm_results


