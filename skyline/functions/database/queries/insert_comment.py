"""
insert_comments.py
"""
import logging
import traceback

from sqlalchemy import select

import settings
from database import get_engine, engine_disposal, comments_table_meta


# @added 20230811 - Feature #5046: comments
def insert_comment(
        current_skyline_app, timestamp, metric, metric_id, user_id, comment_data,
        anomaly_id=None, fp_id=None, match_id=None, motif_match_id=None,
        snab_id=None):
    """
    Insert a new comment into the comments table
    """

    function_str = 'database.queries.insert_comment'

    new_comment_id = None

    current_skyline_app_logger = current_skyline_app + 'Log'
    current_logger = logging.getLogger(current_skyline_app_logger)

    try:
        engine, fail_msg, trace = get_engine(current_skyline_app)
    except Exception as err:
        current_logger.error('error :: %s :: could not get a MySQL engine - %s' % (function_str, err))
        return new_comment_id

    try:
        comments_table, fail_msg, trace = comments_table_meta(current_skyline_app, engine)
    except Exception as err:
        current_logger.error('error :: %s :: failed to get comments_table meta - %s' % (
            function_str, err))
        if engine:
            engine_disposal(current_skyline_app, engine)
        return new_comment_id

    try:
        connection = engine.connect()
        ins = comments_table.insert().values(
            metric_id=int(metric_id),
            timestamp=int(timestamp),
            user_id=int(user_id),
            comment=str(comment_data))
        result = connection.execute(ins)
        connection.close()
        new_comment_id = result.inserted_primary_key[0]
    except Exception as err:
        current_logger.error('error :: %s :: could not insert into comments_table - %s' % (
            function_str, err))

    if new_comment_id:
        current_logger.info('%s :: inserted new comment row with: %s for %s' % (
            function_str, str(new_comment_id), str(metric)))
    else:
        current_logger.error('error :: %s :: failed to inserted comments row' % (
            function_str))

    if anomaly_id and new_comment_id:
        try:
            connection = engine.connect()
            stmt = comments_table.update().values(
                anomaly_id=int(anomaly_id)).\
                where(comments_table.c.id == int(new_comment_id))
            result = connection.execute(stmt)
            if result.rowcount == 1:
                current_logger.info('%s :: updated row %s with anomaly_id: %s' % (
                    function_str, str(new_comment_id), str(anomaly_id)))
            connection.close()
        except Exception as err:
            current_logger.error('error :: %s :: could not update row %s with anomaly_id: %s - %s' % (
                function_str, str(new_comment_id), str(anomaly_id), err))
    if fp_id and new_comment_id:
        try:
            connection = engine.connect()
            stmt = comments_table.update().values(
                fp_id=int(fp_id)).\
                where(comments_table.c.id == int(new_comment_id))
            result = connection.execute(stmt)
            if result.rowcount == 1:
                current_logger.info('%s :: updated row %s with fp_id: %s' % (
                    function_str, str(new_comment_id), str(fp_id)))
            connection.close()
        except Exception as err:
            current_logger.error('error :: %s :: could not update row %s with fp_id: %s - %s' % (
                function_str, str(new_comment_id), str(fp_id), err))
    if match_id and new_comment_id:
        try:
            connection = engine.connect()
            stmt = comments_table.update().values(
                match_id=int(match_id)).\
                where(comments_table.c.id == int(new_comment_id))
            result = connection.execute(stmt)
            if result.rowcount == 1:
                current_logger.info('%s :: updated row %s with match_id: %s' % (
                    function_str, str(new_comment_id), str(match_id)))
            connection.close()
        except Exception as err:
            current_logger.error('error :: %s :: could not update row %s with match_id: %s - %s' % (
                function_str, str(new_comment_id), str(match_id), err))
    if motif_match_id and new_comment_id:
        try:
            connection = engine.connect()
            stmt = comments_table.update().values(
                motif_match_id=int(motif_match_id)).\
                where(comments_table.c.id == int(new_comment_id))
            result = connection.execute(stmt)
            if result.rowcount == 1:
                current_logger.info('%s :: updated row %s with motif_match_id: %s' % (
                    function_str, str(new_comment_id), str(motif_match_id)))
            connection.close()
        except Exception as err:
            current_logger.error('error :: %s :: could not update row %s with motif_match_id: %s - %s' % (
                function_str, str(new_comment_id), str(motif_match_id), err))
    if snab_id and new_comment_id:
        try:
            connection = engine.connect()
            stmt = comments_table.update().values(
                snab_id=int(snab_id)).\
                where(comments_table.c.id == int(new_comment_id))
            result = connection.execute(stmt)
            if result.rowcount == 1:
                current_logger.info('%s :: updated row %s with snab_id: %s' % (
                    function_str, str(new_comment_id), str(snab_id)))
            connection.close()
        except Exception as err:
            current_logger.error('error :: %s :: could not update row %s with snab_id: %s - %s' % (
                function_str, str(new_comment_id), str(snab_id), err))

    if connection:
        try:
            connection.close()
        except:
            pass

    if engine:
        engine_disposal(current_skyline_app, engine)

    return new_comment_id
