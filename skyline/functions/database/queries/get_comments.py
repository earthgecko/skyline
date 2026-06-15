"""
get_comments.py
"""
import copy
import datetime
import logging
import traceback

from sqlalchemy import select

import settings
from database import get_engine, engine_disposal, comments_table_meta


# @added 20230811 - Feature #5046: comments
def get_comments(
        current_skyline_app, timestamp, metric, metric_id, user_id=None,
        anomaly_id=None, fp_id=None, match_id=None, motif_match_id=None,
        snab_id=None):
    """
    Get comments from the comments table
    """

    function_str = 'database.queries.get_comments'

    comments = {}

    current_skyline_app_logger = current_skyline_app + 'Log'
    current_logger = logging.getLogger(current_skyline_app_logger)

    criteria = {
        'timestamp': timestamp, 'metric_id': metric_id, 'user_id': user_id,
        'anomaly_id': anomaly_id, 'fp_id': fp_id, 'match_id': match_id,
        'motif_match_id': motif_match_id, 'snab_id': snab_id
    }
    current_logger.info('%s :: getting comments for %s with criteria: %s' % (
        function_str, str(metric), str(criteria)))

    try:
        engine, fail_msg, trace = get_engine(current_skyline_app)
    except Exception as err:
        current_logger.error('error :: %s :: could not get a MySQL engine - %s' % (function_str, err))
        return comments

    try:
        comments_table, fail_msg, trace = comments_table_meta(current_skyline_app, engine)
    except Exception as err:
        current_logger.error('error :: %s :: failed to get comments_table meta - %s' % (
            function_str, err))
        if engine:
            engine_disposal(current_skyline_app, engine)
        return comments

    all_comments = {}
    try:
        connection = engine.connect()
        stmt = select([comments_table]).where(comments_table.c.metric_id == int(metric_id))
        results = connection.execute(stmt)
        if results:
            for row in results:
                comment_id = row['id']
                all_comments[comment_id] = dict(row)
        connection.close()
    except Exception as err:
        current_logger.error(traceback.format_exc())
        current_logger.error('error :: %s :: failed to build all_comments dict - %s' % (
            function_str, str(err)))

    if engine:
        engine_disposal(current_skyline_app, engine)

    if all_comments:
        for comment_id in list(all_comments.keys()):
            add_comment = False
            comment_for = 'timestamp: %s' % str(timestamp)
            if all_comments[comment_id]['timestamp'] == int(timestamp):
                add_comment = True
            else:
                continue
            if all_comments[comment_id]['anomaly_id']:
                comment_for = 'anomaly_id: %s' % str(all_comments[comment_id]['anomaly_id'])
            if all_comments[comment_id]['fp_id']:
                comment_for = 'fp_id: %s' % str(all_comments[comment_id]['fp_id'])
            if all_comments[comment_id]['match_id']:
                comment_for = 'match_id: %s' % str(all_comments[comment_id]['match_id'])
            if all_comments[comment_id]['motif_match_id']:
                comment_for = 'motif_match_id: %s' % str(all_comments[comment_id]['motif_match_id'])
            if all_comments[comment_id]['snab_id']:
                comment_for = 'snab_id: %s' % str(all_comments[comment_id]['snab_id'])
            if add_comment:
                all_comments[comment_id]['comment_for'] = comment_for
                all_comments[comment_id]['metric'] = metric
                dt = str(all_comments[comment_id]['created_timestamp'])
                human_date_created = datetime.datetime.strptime(dt, '%Y-%m-%d %H:%M:%S')
                all_comments[comment_id]['added'] = human_date_created
                comments[comment_id] = copy.deepcopy(all_comments[comment_id])

    current_logger.info('%s :: %s comments found for %s for given criteria' % (
        function_str, str(len(comments)), str(metric)))

    return comments
