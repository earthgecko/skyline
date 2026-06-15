"""
api_skip_analysis.py
"""
import datetime
import logging
from time import time, strftime, gmtime
from flask import request

import settings
from backend import get_cluster_data
from functions.database.queries.get_all_active_db_metric_names import get_all_active_db_metric_names
from matched_or_regexed_in_list import matched_or_regexed_in_list
from skyline_functions import get_redis_conn_decoded
from slack_functions import slack_post_message

# @added 20231016 - Feature #5102: webapp - api skip_analysis
def api_skip_analysis(current_skyline_app, cluster_data=False):
    """
    Add metrics to skip analysis on.

    :param current_skyline_app: the app calling the function
    :param cluster_data: the cluster_data parameter from the request
    :type current_skyline_app: str
    :type cluster_data: bool
    :return: analysed_events
    :rtype: dict

    """
    function_str = 'api_skip_analysis'
    results = {
        'skip_analysis_count': 0,
        'skip_analysis_on': [],
        'expiry': None
    }

    current_skyline_app_logger = current_skyline_app + 'Log'
    current_logger = logging.getLogger(current_skyline_app_logger)

    patterns = []
    patterns_str = ''
    try:
        patterns_str = request.args.get('patterns')
        if patterns_str:
            patterns = patterns_str.split(',')
    except Exception as err:
        current_logger.error('error :: api_skip_analysis patterns argument failed - %s' % err)
        return 400

    expiry = None
    try:
        expiry = int(request.args.get('expiry'))
        results['expiry'] = expiry
    except Exception as err:
        current_logger.error('error :: api_skip_analysis failed to evaluate expiry argument - %s' % err)
        return 400

    all_active_db_metric_names = []
    try:
        all_active_db_metric_names = get_all_active_db_metric_names(current_skyline_app)
    except Exception as err:
        current_logger.error('error :: %s :: get_all_active_db_metric_names failed - %s' % (
            function_str, str(err)))

    expiry_timestamp_str = str(int(time()) + expiry)

    metrics_to_skip_analysis = {}
    for metric in all_active_db_metric_names:
        pattern_match, metric_matched_by = matched_or_regexed_in_list('webapp', metric, patterns)
        if pattern_match:
            metrics_to_skip_analysis[metric] = expiry_timestamp_str

    redis_key = 'metrics_manager.api_skip_analysis'
    if metrics_to_skip_analysis:
        try:
            redis_conn_decoded = get_redis_conn_decoded(current_skyline_app)
        except Exception as err:
            current_logger.error('error :: %s :: get_redis_conn_decoded failed - %s' % (
                function_str, err))
            raise
        try:
            redis_conn_decoded.hset(redis_key, mapping=metrics_to_skip_analysis)
            current_logger.info('%s :: added %s metrics to %s' % (
                function_str, str(len(metrics_to_skip_analysis)), redis_key))
        except Exception as err:
            current_logger.error('error :: %s :: failed to hset mapping for %s Redis hash key - %s' % (
                function_str, redis_key, err))
        try:
            metrics_to_skip = list(metrics_to_skip_analysis.keys())
            redis_conn_decoded.sadd('analyzer.metrics_manager.analyzer_skip', *set(metrics_to_skip))
            current_logger.info('%s :: added %s metrics to analyzer.metrics_manager.analyzer_skip' % (
                function_str, str(len(metrics_to_skip))))
        except Exception as err:
            current_logger.error('error :: %s :: failed to sadd to analyzer.metrics_manager.analyzer_skip - %s' % (
                function_str, err))

    remote_metrics_to_skip_analysis = []
    if settings.REMOTE_SKYLINE_INSTANCES and cluster_data:
        current_logger.info('%s :: calling /skip_analysis on other cluster nodes' % function_str)
        api_uri = 'skip_analysis=true&patterns=%s&expiry=%s&cluster_data=false&cluster_call=true' % (
                patterns_str, str(expiry))
        try:
            remote_metrics_to_skip_analysis = get_cluster_data(api_uri, 'skip_analysis_on')
        except Exception as err:
            current_logger.error('error :: %s :: failed to get skip_analysis from remote instances - %s' % (
                function_str, err))
            raise
        for metric in remote_metrics_to_skip_analysis:
            metrics_to_skip_analysis[metric] = expiry_timestamp_str

    skip_analysis_on = list(metrics_to_skip_analysis.keys())
    results['skip_analysis_count'] = len(skip_analysis_on)
    results['skip_analysis_on'] = skip_analysis_on

    try:
        slack_enabled = len(settings.SLACK_OPTS['bot_user_oauth_access_token'])
    except:
        slack_enabled = False
    if skip_analysis_on and slack_enabled:
        expiry_timestamp = int(expiry_timestamp_str)
        until = datetime.datetime.utcfromtimestamp(expiry_timestamp).strftime('%Y-%m-%d %H:%M:%S')
        try:
            alert_slack_channel = None
            slack_message = '*Skyline - NOTICE* - skipping analysis on %s metrics for %s seconds until %s (%s).  For metric_patterns: %s' % (
                str(len(skip_analysis_on)), str(expiry), str(until), str(settings.SERVER_PYTZ_TIMEZONE), patterns_str)
            slack_post = slack_post_message(current_skyline_app, alert_slack_channel, None, slack_message)
            current_logger.info('%s :: posted notice to slack - %s' % (function_str, slack_message))
        except Exception as err:
            current_logger.error('error :: %s :: slack_post_message failed - %s' % (
                function_str, err))

    return results
