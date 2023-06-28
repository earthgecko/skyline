"""
purge_vortex_saved_data.py
"""
import logging
import os
from time import time

from shutil import rmtree

skyline_app = 'analyzer'
skyline_app_logger = '%sLog' % skyline_app
logger = logging.getLogger(skyline_app_logger)


# @added 20221130 - Feature #4734: mirage_vortex
def purge_vortex_saved_data(self, VORTEX_SAVE_RESULTS_FOR, SKYLINE_DIR):
    """

    Purge any entries and directories in the mirage.vortex_saved Redis hash that
    are older than VORTEX_SAVE_RESULTS_FOR.

    :param self: the self object
    :type self: object
    :return: purged
    :rtype: int

    """

    vortex_saved_keys_purged = 0
    function_str = 'metrics_manager :: functions.metrics_manager.purge_vortex_saved_data'

    c_time = int(time())

    logger.info('%s :: purging vortex saved results and dirs older than %s seconds' % (
        function_str, str(VORTEX_SAVE_RESULTS_FOR)))

    vortex_saved_keys = []
    try:
        vortex_saved_keys = self.redis_conn_decoded.hkeys('mirage.vortex_saved')
    except Exception as err:
        logger.error('error :: %s :: failed to hkeys on mirage.vortex_saved - %s' % (
            function_str, str(err)))
    vortex_saved_keys_errors = []
    vortex_saved_keys_purged = 0
    if vortex_saved_keys:
        logger.info('%s :: checking %s mirage.vortex_saved keys for purging saved data dirs' % (
            function_str, str(len(vortex_saved_keys))))
        for vortex_saved_key in vortex_saved_keys:
            remove_saved_dir = False
            try:
                vortex_saved_key_timestamp = int(vortex_saved_key.split('.')[0])
                if c_time > (vortex_saved_key_timestamp + VORTEX_SAVE_RESULTS_FOR):
                    remove_saved_dir = True
            except Exception as err:
                vortex_saved_keys_errors.append([vortex_saved_key, 'failed to determine vortex_saved_key_timestamp older than', err])
            if not remove_saved_dir:
                continue
            vortex_saved_key_path = None
            if remove_saved_dir:
                try:
                    vortex_saved_key_path = self.redis_conn_decoded.hget('mirage.vortex_saved', vortex_saved_key)
                except Exception as err:
                    vortex_saved_keys_errors.append([vortex_saved_key, 'failed to determine vortex_saved_key_path', err])
            if vortex_saved_key_path:
                vortex_save_dir = '%s/flux/vortex/results/%s' % (
                    SKYLINE_DIR, str(vortex_saved_key_path))
                try:
                    rmtree(vortex_save_dir)
                    try:
                        self.redis_conn_decoded.hdel('mirage.vortex_saved', vortex_saved_key)
                    except Exception as err:
                        vortex_saved_keys_errors.append([vortex_saved_key, 'failed to hdel key', err])
                    vortex_saved_keys_purged += 1
                except Exception as err:
                    msg = 'failed to rmtree %s' % vortex_save_dir
                    vortex_saved_keys_errors.append([vortex_saved_key, msg, err])
    if vortex_saved_keys_errors:
        logger.error('error :: %s :: %s error reported, sample of last 5 reported: %s' % (
            function_str, str(len(vortex_saved_keys_errors)),
            str(vortex_saved_keys_errors[-5:])))
    logger.info('%s :: purged %s vortex saved results and dirs' % (
        function_str, str(vortex_saved_keys_purged)))
    # Purge any orphaned timeseries json files
    dir_path = '%s/flux/vortex/data' % SKYLINE_DIR
    remove_files = []
    orphaned_files = []
    for path, folders, files in os.walk(dir_path):
        for i_file in files:
            file_timestamp = int(i_file.split('.')[0])
            if file_timestamp < (c_time - 3700):
                remove_file = '%s/%s' % (path, i_file)
                remove_files.append(remove_file)
                orphaned_files.append(i_file)
    if remove_files:
        logger.info('%s :: removing %s orphaned vortex timeseries json files - %s' % (
            function_str, str(len(remove_files)), str(orphaned_files)))
        removed_files = 0
        for i_file in remove_files:
            if os.path.isfile(i_file):
                try:
                    os.remove(i_file)
                    removed_files += 1
                except Exception as err:
                    logger.error('error :: %s :: failed to remove %s - %s' % (
                        function_str, str(i_file), err))
        logger.info('%s :: removed %s orphaned vortex timeseries json files' % (
            function_str, str(removed_files)))

    return vortex_saved_keys_purged
