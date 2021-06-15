import logging
import traceback
from ast import literal_eval
from timeit import default_timer as timer

from pymemcache.client.base import Client as pymemcache_Client

import settings
from functions.database.queries.fp_timeseries import get_db_fp_timeseries

LOCAL_DEBUG = False


# @added 20210424 - Feature #4014: Ionosphere - inference
#                   Task #4030: refactoring
# Add a global method to query mamcache and the DB for a fp timeseries
def get_fp_timeseries(current_skyline_app, metric_id, fp_id, log=True):
    """
    Return the timeseries for the features profile from memcache as a list.
    If not found in memcache, get from database, store in memcache and return
    as a list.

    IMPORTANT NOTE: before using this method, consider that it is quicker to get
    the data directly from the database than it is to fetch it from memcache AND
    then literal_eval the memcache data...

    :param current_skyline_app: the app calling the function
    :param metric_id: the metric id
    :param fp_id: the fp id
    :type current_skyline_app: str
    :type metric_id: int
    :type fp_id: int
    :return: timeseries
    :rtype: list

    """
    function_str = 'functions.memcache.get_fp_timeseries'

    if not log:
        log = True
        # Because the time taken to execute the function was not notably altered
        # by not logging for the worse.  Perhaps an additional ~0.06 s, which
        # could be a bit a bit more if I/O bound.  In fact longer times were
        # still found, but it was not clear if they were related to not finding
        # the data in memcache and having to fetch it from the database, because
        # there was no logging to tell from.

    if log:
        current_skyline_app_logger = current_skyline_app + 'Log'
        current_logger = logging.getLogger(current_skyline_app_logger)
    else:
        current_logger = None

    if settings.MEMCACHE_ENABLED:
        memcache_client = pymemcache_Client((settings.MEMCACHED_SERVER_IP, settings.MEMCACHED_SERVER_PORT), connect_timeout=0.1, timeout=0.2)
    else:
        memcache_client = None

    metric_fp_ts_table = 'z_ts_%s' % str(metric_id)
    fp_id_metric_ts = None

    if memcache_client:
        # @added 20200421 - Task #3304: py3 - handle pymemcache bytes not str
        # Explicitly set the fp_id_metric_ts_object so it
        # always exists to be evaluated
        fp_id_metric_ts_object = None

        fp_id_metric_ts_key = 'fp.%s.%s.ts' % (str(fp_id), str(metric_id))
        try:
            fp_id_metric_ts_object = memcache_client.get(fp_id_metric_ts_key).decode('utf-8')
            # if memcache does not have the key the response to the
            # client is None, it does not except
            if fp_id_metric_ts_object:
                if log:
                    current_logger.info('%s :: got memcache %s key data' % (
                        function_str, fp_id_metric_ts_key))

        except Exception as e:
            # @modified 20200501 - Branch #3262: py3
            # This is not an error if the data does not exist in
            # memcache, it can be expected not to exists in
            # memcache if it has not be used in a while.
            # logger.error('error :: failed to get %s from memcache' % fp_id_metric_ts_key)
            if not log:
                current_skyline_app_logger = current_skyline_app + 'Log'
                current_logger = logging.getLogger(current_skyline_app_logger)
            current_logger.info('%s :: did not get %s from memcache, will query DB - %e' % (
                function_str, fp_id_metric_ts_key, e))
        try:
            memcache_client.close()
        except Exception as e:
            if not log:
                current_skyline_app_logger = current_skyline_app + 'Log'
                current_logger = logging.getLogger(current_skyline_app_logger)
            current_logger.error('error :: %s :: failed to close memcache_client - %s' % (function_str, e))
        if fp_id_metric_ts_object:
            # @modified 20200421 - Task #3304: py3 - handle pymemcache bytes not str
            # Wrapped in try and except
            start_literal_eval = timer()
            try:
                fp_id_metric_ts = literal_eval(fp_id_metric_ts_object)
                # This literal_eval takes probably 80% of the time of this
                # entire function
                if log:
                    end_literal_eval = timer()
                    current_logger.info('%s :: used memcache %s key data to populate fp_id_metric_ts with %s data points (literal_eval took %6f seconds)' % (
                        function_str, fp_id_metric_ts_key, str(len(fp_id_metric_ts)),
                        (end_literal_eval - start_literal_eval)))
            except Exception as e:
                if not log:
                    current_skyline_app_logger = current_skyline_app + 'Log'
                    current_logger = logging.getLogger(current_skyline_app_logger)
                current_logger.error(traceback.format_exc())
                current_logger.error('error :: %s :: failed to literal_eval the fp_id_metric_ts_object - %s' % (
                    function_str, e))
                fp_id_metric_ts = []
        else:
            if log:
                current_logger.info('%s :: no memcache %s key data, will use database' % (
                    function_str, fp_id_metric_ts_key))
    if not fp_id_metric_ts:
        if LOCAL_DEBUG:
            if not log:
                current_skyline_app_logger = current_skyline_app + 'Log'
                current_logger = logging.getLogger(current_skyline_app_logger)
            current_logger.debug('debug :: %s :: getting data from %s database table for fp id %s to populate the fp_id_metric_ts list' % (
                function_str, metric_fp_ts_table, str(fp_id)))
        start_db_query = timer()
        try:
            fp_id_metric_ts = get_db_fp_timeseries(current_skyline_app, int(metric_id), int(fp_id))
            if log and fp_id_metric_ts:
                end_db_query = timer()
                current_logger.info('%s :: used DB data to populate fp_id_metric_ts with %s data points (get_db_fp_timeseries took %6f seconds)' % (
                    function_str, str(len(fp_id_metric_ts)),
                    (end_db_query - start_db_query)))
        except Exception as e:
            if not log:
                current_skyline_app_logger = current_skyline_app + 'Log'
                current_logger = logging.getLogger(current_skyline_app_logger)
            current_logger.error(traceback.format_exc())
            current_logger.error('error :: %s :: could not determine timestamps and values from %s - %s' % (
                function_str, metric_fp_ts_table, e))

        if fp_id_metric_ts and settings.MEMCACHE_ENABLED:
            fp_id_metric_ts_key = 'fp.%s.%s.ts' % (str(fp_id), str(metric_id))
            try:
                memcache_client.set(fp_id_metric_ts_key, fp_id_metric_ts)
                if log:
                    current_logger.info('%s :: populated memcache %s key' % (
                        function_str, fp_id_metric_ts_key))
            except Exception as e:
                if not log:
                    current_skyline_app_logger = current_skyline_app + 'Log'
                    current_logger = logging.getLogger(current_skyline_app_logger)
                current_logger.error('error :: %s :: failed to set %s in memcache - %s' % (
                    function_str, fp_id_metric_ts_key, e))
            try:
                memcache_client.close()
            except Exception as e:
                if not log:
                    current_skyline_app_logger = current_skyline_app + 'Log'
                    current_logger = logging.getLogger(current_skyline_app_logger)
                current_logger.error('error :: %s :: failed to close memcache_client - %s' % (
                    function_str, e))
    if not fp_id_metric_ts:
        if current_skyline_app == 'webapp':
            # Raise to webapp
            raise

    return fp_id_metric_ts
