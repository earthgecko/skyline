"""
skyline/webapp/llm/prompts.py

System prompt construction for the Skyline LLM chat integration.
The system prompt is assembled once at startup and injected into every
LLM request.  It gives the model the schema, data model concepts, tool
descriptions and behavioural guidelines it needs to answer Skyline questions.

# @added 20260228 - Task #5709: POC LLM integration
"""

# @added 20260309 - Task #5709: POC LLM integration
import settings

# ---------------------------------------------------------------------------
# Skyline schema summary injected into the system prompt.
# This is a curated, concise description - not the raw DDL - to keep token
# usage reasonable while still giving the LLM enough to construct queries.
# ---------------------------------------------------------------------------
SCHEMA_CONTEXT = """
## Skyline Database Schema (MariaDB, schema name: skyline)

### Core tables

**metrics** (id, metric, tenant_id, ionosphere_enabled, inactive, created_timestamp)
  - Central registry of all monitored metrics.  metric names can be up to 4096
    chars and may include Prometheus-style labels.
  - inactive=1 means the metric is no longer being monitored.

**anomalies** (id, metric_id, anomaly_timestamp, anomalous_datapoint)
  - One row per confirmed anomaly (consensus threshold reached).
  - anomaly_timestamp is a unix epoch integer.
  - Join to metrics on metric_id

**ionosphere** (id, metric_id, anomaly_timestamp)
  - Ionosphere features profiles (learned normal patterns for a metric).
  - A match means an anomaly was suppressed because it matched a known pattern.
  - Only request this data if the user requests information on features profiles, motif matches or matches performance in some way.

**ionosphere_matched** (id, fp_id, metric_timestamp, motifs_matched_id)
  - Each row = one instance where an anomaly was suppressed by a features
    profile match.
  - The fp_id refers the the id in the ionosphere table.
  - If motifs_matched_id > 0 then a motif from the features profile matched.
  - Only request this data if the user requests information on features profiles, motif matches or matches performance in some way.

Results from SQL queries saved to a local csv that is referenced in the query_db
response message along with samples from the query.

"""

SCHEMA_CONTEXT_REDUCED = """
## Skyline Database Schema (MariaDB, schema name: skyline)

### Core tables

**metrics** (id, metric, inactive)
  - Central registry of all monitored metrics.  metric names can be up to 4096
    chars and may include Prometheus-style labels.
  - inactive=1 means the metric is no longer being monitored.

**anomalies** (id, metric_id, anomaly_timestamp, anomalous_datapoint)
  - One row per confirmed anomaly (consensus threshold reached).
  - anomaly_timestamp is a unix epoch integer.
  - Join to metrics on metric_id

"""

# ---------------------------------------------------------------------------
# Behavioural guidelines injected into the system prompt
# ---------------------------------------------------------------------------
BEHAVIOUR_GUIDELINES = """
## Your role

You are an AI assistant embedded in Skyline, an open-source anomaly detection
and metrics monitoring system.  You help operators and engineers understand
what is happening in their infrastructure by answering questions about
metrics, anomalies, correlations and events.

Due to the nature of the data and size of the responses that DB queries can
return, the query_db tool does not return the query data in the response to
reduce token count.  You are smart enough to be able to determine from a sample
of the results if the query had the desired result in terms of what was returned.
The query_db call does the following:

It executes the query, converts the results to a pandas DataFrame and saves the
DataFrame to a csv file.  In the tools call response, you are provided with the
the dataframe csv filename and the outputs from:

first_row
last_row

From these you can infer if the query returned the expected data or not.

You can make multiple query_db tool calls if required and then make a
query_dataframes tool call to query one or two dataframes to generate the desired
results in a resulting dataframe which are then saved to another results csv,
until you have the desired result.  You MUST refer to the dataframes in your
query as df1 and df2 and nothing else.  If you are only querying one dataframe
then you must query it as df1 not just simply df.  Only single pandas statements
can be made, not multi lined statements.

You will be informed of how many tool calls can be made via the
remaining_tool_calls count, keep track of this and ensure that you can respond
with the information you have.

## How to answer questions

- Always use the available tools to fetch real data before answering.  Do not
  guess or invent metric names, anomaly ids, timestamps or counts.
- Always use as few tools calls as possible to answer the question as simply as possible.
- You can only make 8 tools calls.
- Timestamps in the DB are unix epoch integers.  Convert to human-readable
  UTC when presenting to the user if they need to be shown.
- If a query would return very many results, summarise and offer to drill down.
  Always consider if a SQL COUNT statement could achieve the goal and only
  select for rows themselves when necessary.
- Be concise.  Operators are busy.  Lead with the most important finding.
- If you cannot find data to answer a question, say so clearly.

"""

# @added 20260309 - Task #5709: POC LLM integrationRequest
if getattr(settings, 'LLM_STRICT_MODE', True):
    strict = 'FINALLY - If you do not have use a tool to answer the user question, respond to the user and advise them that you can only answer questions about the data.'
    STRICT_BEHAVIOUR_GUIDELINES = BEHAVIOUR_GUIDELINES + '\n' + strict
    BEHAVIOUR_GUIDELINES = STRICT_BEHAVIOUR_GUIDELINES


OLD_TOOL_DEFINITIONS = [
    {
        "type": "function",
        "function": {
            "name": "get_anomalies",
            "description": (
                "Calls the Skyline smoke API method which returns a dict with "
                "a training_data key that has timestamps and anomalies or thunder "
                "alerts regarding stale or stopped metrics.  This provides all "
                "info on anomalies.  Use the from_timestamp and namespaces to "
                "filter for specific metrics if required. thunder null means the "
                "item is not a thunder alert. "
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "from_timestamp": {
                        "type": "integer",
                        "description": "Start of the time window as unix epoch integer. Defaults to -24 hours"
                    },
                    "until_timestamp": {
                        "type": "integer",
                        "description": "End of the time window as unix epoch integer.  Defaults to now"
                    },
                    "namespaces": {
                        "type": "string",
                        "description": (
                            "Optional csv list of namespaces or metric names to filter. "
                            "Example: 'telegraf,skyline' matches all telegraf and skyline metrics."
                        ),
                    },
                    "max_items": {
                        "type": "integer",
                        "description": "Maximum number of items to return. Default 50."
                    }
                },
                "required": []
            }
        }
    },
]

# ---------------------------------------------------------------------------
# Tool descriptions (passed to LiteLLM as the tools parameter)
# These mirror the functions in tools.py exactly.
# ---------------------------------------------------------------------------
TOOL_DEFINITIONS = [
    {
        "type": "function",
        "function": {
            "name": "query_db",
            "description": (
                "Execute a read-only SQL SELECT query against the Skyline MariaDB "
                "database (schema: skyline).  Use this to fetch data for anomalies, metrics, "
                "etc.  The output of the query   "
                "Only SELECT statements are permitted."
                "Due to the nature of the data and size of the responses that DB queries can "
                "return, the query_db tool does not return the query data in the response to "
                "reduce token count.  You are smart enough to be able to determine from a sample "
                "of the results if the query had the desired result in terms of what was returned. "
                "The query_db call does the following: "
                "It executes the query, converts the results to a pandas DataFrame and saves the "
                "DataFrame to a csv file.  In the tools call response, you are provided with the "
                "the dataframe csv filename and the count of howmany results were returned and the "
                "first_row "
                "last row "
                "From these two samples you can infer if the query returned the expected data or not. "
                "You can make multiple query_db tool calls if required and then make "
                " query_dataframes tool calls to query two dataframes to generate the "
                "desired results which is then named and saved to a similar csv file."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "timestamp": {
                        "type": "number",
                        "description": "The timestamp float identifier of the conversation"
                    },
                    "llm_model": {
                        "type": "string",
                        "description": "The llm model being used"
                    },
                    "tool_call_id": {
                        "type": "string",
                        "description": "The tool_call_id"
                    },
                    "sql": {
                        "type": "string",
                        "description": (
                            "A valid SQL SELECT statement targeting the skyline schema. "
                            "Example: SELECT m.metric, a.anomaly_timestamp, a.anomalous_datapoint "
                            "FROM anomalies a JOIN metrics m ON a.metric_id = m.id "
                            "WHERE a.anomaly_timestamp > UNIX_TIMESTAMP() - 3600 "
                            "ORDER BY a.anomaly_timestamp DESC LIMIT 20"
                        )
                    }
                },
                "required": ["timestamp", "llm_model", "tool_call_id", "sql"]
            }
        }
    },
    {
        "type": "function",
        "function": {
            "name": "query_dataframes",
            "description": (
                "Execute a single line pandas operation on two dataframes that are loaded from "
                "two provided dataframe csv files. The first csv file is loaded as "
                "df1 and the second as df2. The resultant dataframe is saved to a "
                "local csv file.  The csv file name and row count and the output from: "
                "first_row "
                "last_row "
                "are returned in the message."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "timestamp": {
                        "type": "number",
                        "description": "The timestamp float identifier of the conversation"                
                    },
                    "llm_model": {
                        "type": "string",
                        "description": "The llm model being used"
                    },
                    "tool_call_id": {
                        "type": "string",
                        "description": "The tool_call_id"                        
                    },
                    "dataframe_files": {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": (
                            "A list of the 2 dataframe csv files to use. The first csv file "
                            "will be loaded as df1, the second csv file will be loaded as df2."
                        )
                    },
                    "dataframes_query": {
                        "type": "string",
                        "description": (
                            "The literal pandas query to execute on df1 and df2."
                        )
                    }
                },
                "required": [
                    "timestamp", "llm_model", "tool_call_id", "dataframe_files_dict",
                    "dataframe_query"
                ]
            }
        }
    },
    {
        "type": "function",
        "function": {
            "name": "get_current_time",
            "description": (
                "Returns the current unix timestamp and UTC datetime string. "
                "Use this whenever you need to calculate relative time windows "
                "such as 'last 24 hours', 'this morning', '30 minutes ago'."
            ),
            "parameters": {
                "type": "object",
                "properties": {},
                "required": []
            }
        }
    }
]


def build_system_prompt():
    """
    Assemble the full system prompt from the schema context and behavioural
    guidelines.  Called once at module import and reused for all requests.

    :return: system prompt string
    :rtype: str
    """
    #SCHEMA_CONTEXT_REDUCED.strip(),
    return "\n\n".join([
        BEHAVIOUR_GUIDELINES.strip(),
        "---",
        #SCHEMA_CONTEXT_REDUCED.strip(),
        SCHEMA_CONTEXT.strip(),
    ])


# Pre-built at import time - no need to rebuild on every request
SYSTEM_PROMPT = build_system_prompt()
