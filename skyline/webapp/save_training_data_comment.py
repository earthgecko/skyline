import logging
import traceback

import settings
import skyline_version

from functions.database.queries.insert_comment import insert_comment

skyline_version = skyline_version.__absolute_version__
skyline_app = 'webapp'
skyline_app_logger = '%sLog' % skyline_app
logger = logging.getLogger(skyline_app_logger)
skyline_app_logfile = '%s/%s.log' % (settings.LOG_PATH, skyline_app)
logfile = '%s/%s.log' % (settings.LOG_PATH, skyline_app)

# @added 20230811 - Feature #5046: comments
def save_training_data_comment(
        timestamp, metric, metric_id, user_id, comment_data,
        anomaly_id=None, fp_id=None, match_id=None, motif_match_id=None,
        snab_id=None, ionosphere_match_url=None, motif_match_url=None):
    """
    Insert a comment into the comments table.

    :param timestamp: the timestamp of the event
    :param metric: the metric
    :param metric_id: the metric_id
    :param user_id: the user_id
    :param comment_data: the comment_data
    :param anomaly_id: the anomaly_id
    :param fp_id: the fp_id
    :param match_id: the match_id
    :param motif_match_id: the motif_match_id
    :param snab_id: the snab_id
    :param ionosphere_match_url: the ionosphere_match_url
    :param motif_match_url: the motif_match_url
    :type timestamp: int
    :type metric: str
    :type metric_id: int
    :type user_id: int
    :type comment_data: str
    :type anomaly_id: int
    :type fp_id: int
    :type match_id: int
    :type match_id: int
    :type motif_match_id: int
    :type snab_id: int
    :type ionosphere_match_url: str
    :type motif_match_url: str
    :return: new_comment_id
    :rtype: int

    """

    new_comment_id = None
    parameters = {
        'metric': metric, 'timestamp': timestamp, 'user_id': user_id,
        'anomaly_id': anomaly_id, 'fp_id': fp_id, 'match_id': match_id,
        'motif_match_id': motif_match_id, 'snab_id': snab_id,
        'ionosphere_match_url': ionosphere_match_url,
        'motif_match_url': motif_match_url
    }
    logger.info('save_training_data_comment :: %s' % (
        str(parameters)))

    if ionosphere_match_url:
        comment_data = '%s - ionosphere_match_url: %s' % (comment_data, ionosphere_match_url)

    if ionosphere_match_url:
        comment_data = '%s - ionosphere_match_url: %s' % (comment_data, ionosphere_match_url)

    if motif_match_url:
        comment_data = '%s - motif_match_url: %s' % (comment_data, motif_match_url)

    try:
        new_comment_id = insert_comment(
                            skyline_app, timestamp, metric, metric_id, user_id,
                            comment_data, anomaly_id=anomaly_id, fp_id=fp_id,
                            match_id=match_id, motif_match_id=motif_match_id,
                            snab_id=snab_id)
    except Exception as err:
        trace = traceback.format_exc()
        logger.error(trace)
        logger.error('error :: save_training_data_comment :: insert_comment failed - %s' % str(err))
        raise  # to webapp to return in the UI

    logger.info('save_training_data_comment :: new_comment_id: %s' % str(new_comment_id))

    return new_comment_id
