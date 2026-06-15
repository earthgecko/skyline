"""
manage_echo_fp_ids.py
"""
import logging
import traceback

from functions.database.queries.get_ionosphere_echo_fps import get_ionosphere_echo_fps

skyline_app = 'analyzer'
skyline_app_logger = '%sLog' % skyline_app
logger = logging.getLogger(skyline_app_logger)


# @added 20240201 - Task #5250: Optimise ionosphere_backend.get_fp_matches
# Added a hash of all echo fp ids specifically for webapp
# ionosphere_backend.get_fp_matches so that the /api&anomaly
# request does not need to make a SQL query it get all the echo
# fp ids and it is fast.
def manage_echo_fp_ids(self):
    """

    Create and manage the metrics_manager.echo_fp_ids Redis hash.  This data is
    specifically for webapp ionosphere_backend.get_fp_matches so that the
    /api&anomaly request does not need to make a SQL query it get all the echo
    fp ids and it is fast.

    :param self: the self object
    :type self: object
    :return: new_echo_fps
    :rtype: dict

    """
    echo_fps = {}
    try:
        logger.info('metrics_manager :: manage_echo_fp_ids - managing metrics_manager.echo_fp_ids')
        last_id = 0
        try:
            last_id_str = self.redis_conn_decoded.get('metrics_manager.echo_fp_ids.last_id')
            if last_id_str:
                last_id = int(last_id_str)
        except Exception as err:
            logger.error('error :: metrics_manager :: manage_echo_fp_ids :: failed to get metrics_manager.echo_fp_ids.last_id, err: %s' % (
                err))
        echo_fp_id_strs_dict = {}
        if not last_id:
            try:
                echo_fp_id_strs_dict = self.redis_conn_decoded.hgetall('metrics_manager.echo_fp_ids')
            except Exception as err:
                logger.error('error :: metrics_manager :: manage_echo_fp_ids :: failed to hgetall metrics_manager.echo_fp_ids, err: %s' % (
                    err))
        if echo_fp_id_strs_dict:
            try:
                last_id = max([int(fp_id) for fp_id, metric_id in echo_fp_id_strs_dict.items()])
            except Exception as err:
                logger.error('error :: metrics_manager :: manage_echo_fp_ids :: failed to determine max echo fp id, err: %s' % (
                    err))
        if not last_id:
            last_id = 0
        try:
            echo_fps = get_ionosphere_echo_fps(skyline_app, last_id=last_id)
        except Exception as err:
            logger.error('error :: metrics_manager :: manage_echo_fp_ids :: get_ionosphere_echo_fps failed, err: %s' % (
                err))
        logger.info('Determined %s echo_fps with id > %s' % (
            str(len(echo_fps)), str(last_id)))
        if echo_fps:
            try:
                self.redis_conn_decoded.hset('metrics_manager.echo_fp_ids', mapping=echo_fps)
            except Exception as err:
                logger.error('error :: metrics_manager :: manage_echo_fp_ids :: failed to hset metrics_manager.echo_fp_ids, err: %s' % (
                    err))
            last_id = 0
            try:
                last_id = max([int(fp_id) for fp_id, metric_id in echo_fps.items()])
            except Exception as err:
                logger.error('error :: metrics_manager :: manage_echo_fp_ids :: failed to determine last_id from echo_fps, err: %s' % (
                    err))
                last_id = 0
            if last_id:
                try:
                    self.redis_conn_decoded.set('metrics_manager.echo_fp_ids.last_id', last_id)
                except Exception as err:
                    logger.error('error :: metrics_manager :: manage_echo_fp_ids :: failed to set metrics_manager.echo_fp_ids.last_id, err: %s' % (
                        err))
    except Exception as err:
        logger.error(traceback.format_exc())
        logger.error('error :: metrics_manager :: manage_echo_fp_ids :: failed, err: %s' % (
            err))

    return echo_fps
