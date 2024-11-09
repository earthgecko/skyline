"""
get_failed_checks_to_retry.py
"""
import logging
import os
import traceback
from ast import literal_eval
from time import time
from shutil import copyfile

import settings

skyline_app = 'panorama'
skyline_app_logger = '%sLog' % skyline_app
logger = logging.getLogger(skyline_app_logger)

# @added 20240229 - Feature #5294: panorama - retry failed checks
# At times when there are potential DB issues or network
# partitions from the DB panorama will fail checks.  This adds
# the functionality for panorama to retry checks that have
# recently failed.
def get_failed_checks_to_retry(self):
    """
    Get entries from the panorama.retry_failed_checks Redis hash and remove
    any that have been tried multiple times and are greater than max_age_seconds
    and return any that need to be retried.

    :param self: self object
    :type self: object
    :return: retry_checks
    :rtype: list

    """
    function_str = 'functions.panorama.get_failed_checks_to_retry'
    retry_checks = []
    now = int(time())

    retry_checks_dict = {}
    try:
        retry_checks_dict = self.redis_conn_decoded.hgetall('panorama.retry_failed_checks')
    except Exception as err:
        logger.error('error :: %s :: failed to hgetall panorama.retry_failed_checks, err: %s' % (
            function_str, err))
        return retry_checks

    if not retry_checks:
        logger.info('%s :: there are no failed checks to retry' % function_str)
        return retry_checks

    for key, data_str in retry_checks_dict.items():
        remove_key = False
        succeeded = False
        try:
            data = literal_eval(data_str)
            check_timestamp = data['check_timestamp']
            last_fail_timestamp = data['last_fail_timestamp']
            fail_count = data['fail_count']
            failed_check_file = data['failed_check_file']
            max_age_seconds = data['max_age_seconds']
            try:
                succeeded = data['succeeded']
            except:
                succeeded = False
        except Exception as err:
            logger.error('error :: %s :: failed to literal_eval key data for %s, key will be removed, err: %s' % (
                function_str, key, err))
            remove_key = True

        # Remove fails older than max_age_seconds
        if (now - check_timestamp) > max_age_seconds:
            logger.warning('warning :: %s :: %s key is older than max_age_seconds: %s seconds and will be removed' % (
                function_str, key, str(max_age_seconds)))
            remove_key = True

        if succeeded:
            logger.info('%s :: %s succeeded after %s fails will be removed' % (
                function_str, key, str(fail_count)))
            remove_key = True

        if not remove_key:
            if fail_count > 2:
                if last_fail_timestamp > (now - 20):
                    continue
            if fail_count > 3:
                if last_fail_timestamp > (now - 60):
                    continue

        if remove_key:
            try:
                retry_removed = self.redis_conn_decoded.hdel('panorama.retry_failed_checks', key)
                if retry_removed:
                    logger.info('%s :: failed check removed from panorama.retry_failed_checks after %s fails' % (
                        function_str, str(fail_count)))
            except Exception as err:
                logger.error('error :: %s :: failed to hdel %s key from Redis hash panorama.retry_failed_checks, err: %s' % (
                    function_str, key, err))
            if os.path.isfile(str(failed_check_file)):
                try:
                    os.remove(str(failed_check_file))
                    logger.info('%s :: failed check metric_check_file removed - %s' % (
                        function_str, str(failed_check_file)))
                except OSError:
                    pass
            continue

        # Readd the check
        retry_added = False
        try:
            copyfile(failed_check_file, key)
            logger.info('%s :: readded failed check to %s' % (
                function_str, key))
            retry_checks.append(key)
            retry_added = True
        except Exception as err:
            logger.error('error :: %s :: failed copyfile(%s, %s), err: %s' % (
                function_str, failed_check_file, key, err))
        # Remove the failed_check_file as if it fails again it will be readded
        if retry_added:
            if os.path.isfile(str(failed_check_file)):
                try:
                    os.remove(str(failed_check_file))
                    logger.info('%s :: failed check metric_check_file removed because the check has been readding - %s' % (
                        function_str, str(failed_check_file)))
                except OSError:
                    pass

    return retry_checks
