"""
skyline/webapp/llm/chat.py

Core orchestration for the Skyline LLM chat integration.

This module owns the agentic loop:
  1. Receive a user message and conversation history
  2. Call the LLM via LiteLLM with the system prompt and tool definitions
  3. If the LLM returns tool calls, execute them and loop back
  4. When the LLM returns a plain text response, return it to the caller

  Is pretty much totally Claude generated, almost.

The Flask route in llm_chat.py calls run_chat_turn() and returns the result
as JSON.  This module has no Flask dependency.

# @added 20260228 - Task #5709: POC LLM integration

"""

import json
import logging
import os
from time import time
import traceback

import litellm

from .prompts import SYSTEM_PROMPT, TOOL_DEFINITIONS
from .tools import execute_tool

skyline_app = 'webapp'
skyline_app_logger = skyline_app + 'Log'
logger = logging.getLogger(skyline_app_logger)

# ---------------------------------------------------------------------------
# LiteLLM configuration
# Applied once at import time from Skyline settings.
# ---------------------------------------------------------------------------

def configure_litellm(settings):
    """
    Apply LiteLLM configuration from Skyline settings.

    Call this once during webapp startup, e.g. in the Flask app factory or
    the llm_chat Blueprint's before_app_first_request handler.

    :param settings: the Skyline settings module
    """
    if getattr(settings, 'LLM_API_KEY', None):
        litellm.api_key = settings.LLM_API_KEY

    if getattr(settings, 'LLM_API_BASE', None):
        litellm.api_base = settings.LLM_API_BASE


    # Suppress LiteLLM's own verbose logging if desired
    # litellm.set_verbose = False


# ---------------------------------------------------------------------------
# Session / conversation history management
# ---------------------------------------------------------------------------

def trim_history(history, max_turns):
    """
    Trim conversation history to at most max_turns complete turns
    (each turn = 1 user message + 1 assistant message).

    Tool call messages (role: tool) are kept with their associated assistant
    turn to avoid breaking the message chain.

    :param history: list of message dicts
    :param max_turns: maximum number of user/assistant turn pairs to retain
    :return: trimmed list of message dicts
    :rtype: list
    """
    if not history:
        return history

    # Count user messages as turn boundaries
    user_indices = [i for i, m in enumerate(history) if m.get('role') == 'user']

    if len(user_indices) <= max_turns:
        return history

    # Drop everything before the (N - max_turns)th user message
    cutoff_index = user_indices[-(max_turns)]
    return history[cutoff_index:]


# ---------------------------------------------------------------------------
# Main agentic loop
# ---------------------------------------------------------------------------

def run_chat_turn(provider_llm_model, llm_model, user_message, history, settings):
    """
    Process one user message, running the agentic tool-call loop until the
    LLM produces a final text response.

    :param user_message: the user's message string
    :param history: list of previous message dicts (role/content pairs).
                    Modified in-place and returned as updated_history.
    :param settings: the Skyline settings module (for model, max_tokens, etc.)
    :return: dict with keys:
               'response'        - the LLM's final text response string
               'updated_history' - the full conversation history including
                                   this turn, ready to store back in session
               'tool_calls_made' - list of tool names called this turn
               'error'           - error message string if something failed,
                                   None otherwise
    :rtype: dict
    """
    #model = getattr(settings, 'LLM_MODEL', None)
    max_tokens = getattr(settings, 'LLM_MAX_TOKENS', 4096)
    max_tool_iterations = getattr(settings, 'LLM_MAX_TOOL_ITERATIONS', 8)
    max_turns = getattr(settings, 'LLM_MAX_CONVERSATION_TURNS', 20)
    if 'bedrock' in provider_llm_model:
        LLM_AWS_BEARER_TOKEN_BEDROCK = None
        try:
            LLM_AWS_BEARER_TOKEN_BEDROCK = getattr(settings, 'LLM_AWS_BEARER_TOKEN_BEDROCK', None)
        except:
            LLM_AWS_BEARER_TOKEN_BEDROCK = None
        if LLM_AWS_BEARER_TOKEN_BEDROCK:
            os.environ['AWS_BEARER_TOKEN_BEDROCK'] = LLM_AWS_BEARER_TOKEN_BEDROCK
            litellm.api_key = None
        LLM_AWS_REGION_NAME = None
        try:
            LLM_AWS_REGION_NAME = getattr(settings, 'LLM_AWS_REGION_NAME', None)
        except:
            LLM_AWS_REGION_NAME = None
        if LLM_AWS_REGION_NAME:
            os.environ['AWS_REGION_NAME'] = LLM_AWS_REGION_NAME

    # @added 20260308 - Task #5709: POC LLM integration
    LLM_MODELS_AVAILABLE = None
    if getattr(settings, 'LLM_MODELS_AVAILABLE', None):
        LLM_MODELS_AVAILABLE = settings.LLM_MODELS_AVAILABLE
    if LLM_MODELS_AVAILABLE:
        set_api_key = False
        llm_api_token = None
        try:
            llm_api_token = LLM_MODELS_AVAILABLE[provider_llm_model]['api_token']
            if llm_api_token:
                set_api_key = True
        except KeyError:
            llm_api_token = None
        if set_api_key:
            litellm.api_key = llm_api_token
            # Clear Bedrock env vars if switching to API key model
            os.environ.pop('AWS_BEARER_TOKEN_BEDROCK', None)
            os.environ.pop('AWS_REGION_NAME', None)

        set_aws_auth = False
        aws_bearer_token_bedrock = None
        aws_region_name = None
        try:
            aws_bearer_token_bedrock = LLM_MODELS_AVAILABLE[provider_llm_model]['aws_bearer_token_bedrock']
            if aws_bearer_token_bedrock:
                aws_region_name = LLM_MODELS_AVAILABLE[provider_llm_model]['aws_region_name']
            if aws_bearer_token_bedrock and aws_region_name:
                set_aws_auth = True
        except KeyError:
            set_aws_auth = None
        if set_aws_auth:
            os.environ['AWS_BEARER_TOKEN_BEDROCK'] = aws_bearer_token_bedrock
            os.environ['AWS_REGION_NAME'] = aws_region_name
            litellm.api_key = None

    if getattr(settings, 'LLM_API_BASE', None):
        litellm.api_base = settings.LLM_API_BASE

    timestamp = time()

    if '/' in llm_model:
        llm_model = provider_llm_model.split('/')[-1]
    logger.info(f"run_chat_turn :: llm_model: {llm_model}, user_message: {user_message}")

    # @added 20260309 - Task #5709: POC LLM integration
    try:
        LLM_STRICT_MODE = settings.LLM_STRICT_MODE
    except AttributeError:
        LLM_STRICT_MODE = True
    if LLM_STRICT_MODE:
        logger.info(f"run_chat_turn :: LLM_STRICT_MODE: {LLM_STRICT_MODE}, no chit chat mode enabled.")

    # Trim history before adding the new user message
    history = trim_history(list(history), max_turns)

    # Append the new user message
    history.append({'role': 'user', 'content': user_message})

    logger.info(f"run_chat_turn :: history: {str(history)}")

    tool_calls_made = []
    final_response = None
    error = None
    results_csv = None
    query_results_csvs = {}

    total_usage = []
    max_tool_iterations = getattr(settings, 'LLM_MAX_TOOL_ITERATIONS', 8)
    #max_tool_iterations = 4

    final_call = False
    remaining_tool_calls = int(max_tool_iterations)
    try:
        for iteration in range(max_tool_iterations + 1):

            # Build the messages list: system prompt + conversation history
            messages = [{'role': 'system', 'content': SYSTEM_PROMPT}] + history

            logger.info(f"run_chat_turn :: calling with max_tokens: {max_tokens}")
            logger.info(f"run_chat_turn :: messages: {messages}")

            # Call the LLM via LiteLLM
            response = litellm.completion(
                model=provider_llm_model,
                messages=messages,
                tools=TOOL_DEFINITIONS,
                tool_choice='auto',
                max_tokens=max_tokens,
            )

            assistant_message = response.choices[0].message
            logger.info(f"run_chat_turn :: response - assistant_message: {assistant_message}")
        
            usage = response.usage
            usage_dict = {
                'prompt_tokens': usage.prompt_tokens,
                'completion_tokens': usage.completion_tokens,
                'total_tokens': usage.total_tokens,
            }
            total_usage.append(usage_dict)
            logger.info(f"run_chat_turn :: response - usage: {usage_dict}")

            # Convert to a plain dict for storage in history
            assistant_dict = {'role': 'assistant', 'content': assistant_message.content}

            # Check if the LLM wants to call tools
            tool_calls = getattr(assistant_message, 'tool_calls', None)

            if not tool_calls:
                # No tool calls - this is the final response
                final_response = assistant_message.content or ''
                final_response = final_response + f"\n\ntoken_usage: {total_usage}"

                # @added 20260309 - Task #5709: POC LLM integration
                if LLM_STRICT_MODE:
                    if len(tool_calls_made) == 0:
                        logger.info('run_chat_turn :: LLM_STRICT_MODE kicked in assistant_message.content suffixed')
                        # Let the LLM think if followed instructions, which it
                        # generally does, but just in case it did not, remind it
                        # via it's own output which then reinforces the system
                        # prompt on the next turn, because the LLM will have no
                        # issue believing that it followed instructions and
                        # behaved as instructed, that is coherent.
                        strict = "\n\nI can only answer questions about the data.  Do you have a question about the data?"
                        strict_assistant_message_content = assistant_message.content + strict
                        assistant_dict = {'role': 'assistant', 'content': strict_assistant_message_content}
                        #final_response = assistant_message.content or ''
                        #final_response = final_response + f"\n{strict}" + f"\n\ntoken_usage: {total_usage}"

                history.append(assistant_dict)

                break

            if final_call:
                history.append(assistant_dict)
                final_response = assistant_message.content
                final_response = final_response + f"\n\ntoken_usage: {total_usage}"
                break

            if iteration == max_tool_iterations:
                # Reached iteration limit - return whatever we have
                final_call = True
                if final_call:
                    content = assistant_message.content + '\nNo more tool calls are available so complete the task with what you know.'
                    assistant_dict = {'role': 'assistant', 'content': content}
                    history.append(assistant_dict)
                    continue
                history.append(assistant_dict)
                final_response = (
                    assistant_message.content or
                    'I reached the maximum number of data lookups for this query. '
                    'Here is what I found so far.'
                )
#                final_response = 'I reached the maximum number of data lookups for this query.\nHere is what I found so far.\n' + assistant_message.content
                final_response = final_response + f"\nusage: {total_usage}"

                break

            # Store the assistant message with tool_calls in history
            # LiteLLM/OpenAI format requires tool_calls in the message dict
            assistant_dict_with_tools = {
                'role': 'assistant',
                'content': assistant_message.content,
                'tool_calls': [
                    {
                        'id': tc.id,
                        'type': 'function',
                        'function': {
                            'name': tc.function.name,
                            'arguments': tc.function.arguments,
                        }
                    }
                    for tc in tool_calls
                ]
            }
            history.append(assistant_dict_with_tools)

            # Execute each tool call and append results to history
            for tool_call in tool_calls:
                tool_name = tool_call.function.name
                tool_call_id = tool_call.id

                try:
                    tool_args = json.loads(tool_call.function.arguments)
                except json.JSONDecodeError as err:
                    tool_args = {}
                    logger.error(f"error :: run_chat_turn tool_call, tool_args error, err: {err}")

                tool_args['tool_call_id'] = tool_call_id
                tool_args['timestamp'] = timestamp
                tool_args['llm_model'] = llm_model

                tool_calls_made.append(tool_name)
                logger.info(f"run_chat_turn :: tool_name: {tool_name}, llm_model: {llm_model}")

                # Execute the tool
                try:
                    tool_result = execute_tool(tool_name, tool_args)
                    remaining_tool_calls -= 1
                except Exception as err:
                    logger.error(f"error :: run_chat_turn execute_tool {tool_name}, err: {err}")

                results_csv = None
                if 'results_csv' in tool_result:
                    try:
                        results_csv = tool_result['results_csv']
                    except KeyError:
                        results_csv = None
                if results_csv:
                    if 'sql' in tool_result:
                        query_results_csvs[results_csv] = {
                            'query': tool_result['sql'], 'csv': results_csv
                        }
                    if 'dataframes_query' in tool_result:
                        query_results_csvs[results_csv] = {
                            'query': tool_result['dataframes_query'],
                            'csv': results_csv
                        }

                if remaining_tool_calls <= 2:
                    if 'message' in tool_result:
                        use_message = str(tool_result['message']) + f"\nThere are only {remaining_tool_calls} tool calls left to be made consider finalising with what you know."
                        tool_result['message'] = use_message

                # Append tool result to history in the format LiteLLM expects
                history.append({
                    'role': 'tool',
                    'tool_call_id': tool_call_id,
                    'name': tool_name,
                    'remaining_tool_calls': remaining_tool_calls,
                    'content': json.dumps(tool_result, default=str),
                })

            # Loop back to call the LLM again with the tool results

    except litellm.exceptions.AuthenticationError as err:
        error = 'LLM authentication failed - check LLM_API_KEY in settings: %s' % str(err)
        logger.error(error)
    except litellm.exceptions.RateLimitError as err:
        error = 'LLM rate limit reached - please try again shortly: %s' % str(err)
        logger.error(error)
    except litellm.exceptions.BadRequestError as err:
        error = 'LLM bad request - possible context length issue: %s' % str(err)
        logger.error(error)
    except Exception as err:
        error = 'LLM chat failed: %s' % str(err)
        logger.error('error :: llm chat failed: %s' % traceback.format_exc())

    return {
        'response': final_response,
        'updated_history': history,
        'tool_calls_made': tool_calls_made,
        'error': error,
        'results_csv': results_csv,
        'query_results_csvs': query_results_csvs,
    }
