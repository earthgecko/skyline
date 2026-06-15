"""
cluster_sync_bq_archives.py
"""
import os
import logging
from time import time, sleep

import requests

import settings
from skyline_functions import mkdir_p

skyline_app = 'analyzer'
skyline_app_logger = '%sLog' % skyline_app
logger = logging.getLogger(skyline_app_logger)
this_host = str(os.uname()[1])


# @added 20240610 - Feature #5352: vista - bigquery
def cluster_sync_bq_archives():
    """

    Sync bq_archives from other cluster nodes.

    :param self: the self object
    :type self: object
    :return: batch_processing_namespaces
    :rtype: list

    """

    function_str = 'metrics_manager :: cluster_sync_bq_archives'
    logger.info('%s :: checking bq_archive to sync' % function_str)

    start_sync = time()

    bq_archive_path = '%s/vista/bq_archives' % settings.SKYLINE_DIR
    local_bq_archives = []

    local_bq_archives = []
    if os.path.isdir(bq_archive_path):
        for dir_path, folders, files in os.walk(bq_archive_path):
            try:
                if files:
                    for i in files:
                        path_and_file = '%s/%s' % (dir_path, i)
                        if i.endswith('.csv'):
                            local_bq_archives.append(path_and_file)
            except Exception as err:
                logger.error('error :: %s :: os.walk failed on %s, err: %s' % (
                    function_str, str(bq_archive_path), err))
    logger.info('%s :: %s local bq_archives found' % (function_str, str(len(local_bq_archives))))

    for remote_url, remote_user, remote_password, shostname in settings.REMOTE_SKYLINE_INSTANCES:

        if time() > (start_sync + 30):
            logger.info('%s :: stopping because sync has run for 60 seconds, further files will be synced next run' % (
                function_str))
            break

        remote_bq_archives = []
        if shostname == this_host:
            continue
        r = None
        try:
            url = '%s/api?vista_bq_archives' % remote_url
            # @modified 20241106 - Task #5526: Build v5.0.0 and upgrade deps
            # Add timeout for bandit B113
            #r = requests.get(url, auth=(remote_user, remote_password))
            connect_timeout = 5
            read_timeout = 60
            use_timeout = (int(connect_timeout), int(read_timeout))
            r = requests.get(url, auth=(remote_user, remote_password), timeout=use_timeout)

            logger.info('%s :: %s returned status_code: %s from %s' % (
                function_str, shostname, str(r.status_code), url))
        except Exception as err:
            logger.error('error :: %s :: request failed on %s, err: %s' % (
                function_str, str(url), err))
        if r:
            try:
                response_json = r.json()
                remote_bq_archives = response_json['data']['bq_archives']
            except Exception as err:
                logger.error('error :: %s :: failed to get bq_archives from response, err: %s' % (
                    function_str, err))
                remote_bq_archives = []
        logger.info('%s :: %s remote bq_archives found on %s' % (
            function_str, str(len(remote_bq_archives)), shostname))

        remote_archives_to_sync = []
        for remote_bq_archive in remote_bq_archives:
            if remote_bq_archive not in local_bq_archives:
                remote_archives_to_sync.append(remote_bq_archive)
        logger.info('%s :: %s remote bq_archives from %s need to be synced' % (
            function_str, str(len(remote_archives_to_sync)), shostname))

        synced_archives = 0
        for remote_bq_archive in remote_bq_archives:
            if time() > (start_sync + 30):
                logger.info('%s :: stopping because sync has run for 30 seconds, further files will be synced next run' % (
                    function_str))
                logger.info('%s :: synced %s remote bq_archives from %s' % (function_str, str(len(synced_archives)), shostname))
                break

            if remote_bq_archive not in local_bq_archives:
                bq_archive_filepath = os.path.dirname(remote_bq_archive)
                r = None
                try:
                    url = '%s/vista_bq_archive_file?file=%s' % (remote_url, remote_bq_archive)
                    # @modified 20241106 - Task #5526: Build v5.0.0 and upgrade deps
                    # Add timeout for bandit B113
                    #r = requests.get(url, auth=(remote_user, remote_password))
                    connect_timeout = 5
                    read_timeout = 60
                    use_timeout = (int(connect_timeout), int(read_timeout))
                    r = requests.get(url, auth=(remote_user, remote_password), timeout=use_timeout)
                except Exception as err:
                    logger.error('error :: %s :: request failed on %s, err: %s' % (
                        function_str, str(url), err))
                if r:
                    if r.status_code != 200:
                        logger.error('error :: %s :: request failed on %s with status_code: %s, with reason: %s' % (
                            function_str, str(url), str(r.status_code), str(r.reason)))
                    if r.status_code == 200:
                        if not os.path.isdir(bq_archive_filepath):
                            mkdir_p(bq_archive_filepath)
                        file_saved = False
                        try:
                            open(remote_bq_archive, 'wb').write(r.content)
                            if not os.path.isfile(remote_bq_archive):
                                logger.error('error :: %s :: failed to save_file %s from %s' % (
                                    function_str, str(remote_bq_archive), url))
                            else:
                                file_saved = True
                                synced_archives += 1
                        except Exception as err:
                            logger.error('error :: %s :: request failed on %s, err: %s' % (
                                function_str, str(url), err))
                        if file_saved:
                            local_bq_archives.append(remote_bq_archive)
                            logger.info('%s :: saved %s' % (
                                function_str, remote_bq_archive))
                sleep(0.2)
                if time() > (start_sync + 30):
                    logger.info('%s :: stopping because sync has run for 30 seconds, further files will be synced next run' % (
                        function_str))
                    logger.info('%s :: synced %s remote bq_archives from %s' % (function_str, str(synced_archives), shostname))
                    break
        logger.info('%s :: synced %s remote bq_archives from %s' % (function_str, str(len(synced_archives)), shostname))

    logger.info('metrics_manager :: cluster_sync_bq_archives :: all done')
    return
