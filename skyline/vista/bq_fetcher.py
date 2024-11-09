"""
bq_fetcher.py
"""
import copy
import logging
import os
import sys
import traceback
from multiprocessing import Process
from threading import Thread
from time import time, sleep
from datetime import datetime

import requests

sys.path.insert(0, '/opt/skyline/github/skyline/skyline')
import settings
from functions.settings.get_bq_accounts_settings import get_bq_accounts_settings
from functions.skyline.job_scheduled import job_scheduled
from skyline_functions import get_redis_conn_decoded

try:
    VISTA_BQ_ACCOUNTS = copy.deepcopy(settings.VISTA_BQ_ACCOUNTS)
except:
    VISTA_BQ_ACCOUNTS = {}

try:
    HORIZON_SHARDS = copy.deepcopy(settings.HORIZON_SHARDS)
except:
    HORIZON_SHARDS = {}

this_host = str(os.uname()[1])
HORIZON_SHARD = 0
if HORIZON_SHARDS:
    HORIZON_SHARD = HORIZON_SHARDS[this_host]

parent_skyline_app = 'vista'
child_skyline_app = 'bq_fetcher'
skyline_app_logger = '%sLog' % parent_skyline_app
logger = logging.getLogger(skyline_app_logger)
skyline_app = '%s.%s' % (parent_skyline_app, child_skyline_app)
skyline_app_logfile = '%s/%s.log' % (settings.LOG_PATH, parent_skyline_app)
skyline_app_loglock = '%s.lock' % skyline_app_logfile
skyline_app_logwait = '%s.wait' % skyline_app_logfile

this_host = str(os.uname()[1])

# @added 20240515 - Feature #5352: vista - bigquery
# Checks if there is any bq_backfill jobs and if so adds adds to a redis hash
# and spawns and orphans bq_backfill process
class Bq_Fetcher(Thread):
    """
    The BQFetcher thread checks the BQ accounts schedule and update_schedule and
    submits a bq_backfill or bq_update job as appropriate.
    """
    def __init__(self, parent_pid):
        super().__init__()
        self.parent_pid = parent_pid
        self.daemon = True
        self.redis_conn_decoded = get_redis_conn_decoded(skyline_app)


    def check_if_parent_is_alive(self):
        """
        Self explanatory.
        """
        try:
            os.kill(self.parent_pid, 0)
        except:
            logger.warning('warning :: parent process is dead')
            sys.exit(0)

    def bq_fetch(self, timestamp):

        vista_bq_accounts = {}
        try:
            vista_bq_accounts = get_bq_accounts_settings(parent_skyline_app)
        except Exception as err:
            logger.error('error :: bq_fetcher :: get_bq_accounts_settings failed, err: %s' % err)

        # backfill needs to be offloaded
        # each backfilled entitiy needs a key created to say that backfill is complete
        # When backfill is completed a key needs to be set to say it is filled
        # If a key is not found, the data needs to be checked in the data store to
        # determine 

        bq_backfill_url = '%s/bq_backfill' % (settings.SKYLINE_URL)
        bq_update_url = '%s/bq_update' % (settings.SKYLINE_URL)
        user = str(settings.WEBAPP_AUTH_USER)
        password = str(settings.WEBAPP_AUTH_USER_PASSWORD)

        for vista_bq_account_key, acc_dict in vista_bq_accounts.items():
            scheduled = False
            if 'schedule' in acc_dict.keys():
                try:
                    scheduled = job_scheduled(skyline_app, schedule_dict=acc_dict['schedule'])
                except Exception as err:
                    logger.error('error :: bq_fetcher :: job_scheduled failed for vista_bq_account_keyount: %s, schedule: %s , err: %s' % (
                        str(vista_bq_account_key), str(acc_dict['schedule']), err))
            if scheduled:
                # Submit a bq_backfill job
                try:
                    interval = int(acc_dict['schedule_interval'])
                except Exception as err:
                    logger.error('error :: bq_fetcher :: could not determine interval from %s, err: %s' % (
                        vista_bq_account_key, err))
                    continue
                from_timestamp = int(time()) // interval * interval
                post_data = {}
                try:
                    post_data = {
                        'data': {
                            'bq_backfill': True,
                            'vista_bq_account_key': vista_bq_account_key,
                            'from_timestamp': from_timestamp,
                            # Also use the from_timestamp for the until_timestamp in a backfill job
                            'until_timestamp': from_timestamp,
                            'interval': interval,
                            'date_format': acc_dict['date_format'],
                            'replace_query_string': acc_dict['replace_query_string'],
                            'zero_fill': False,
                            'replace_redis_timeseries': False,
                            'max_creates_per_minute': 1500,
                            'dry_run': False,
                            'flux_test': False,
                            'added_by': 'vista',
                        }
                    }
                except Exception as err:
                    logger.error('error :: bq_fetcher :: failed to build post_data for bq_backfill job, err: %s' % err)
                    continue
                r = None
                try:
                    headers = {"content-type": "application/json"}
                    r = requests.post(bq_backfill_url, auth=(user, password), json=post_data, headers=headers, timeout=5, verify=settings.VERIFY_SSL)
                    logger.info('bq_fetcher :: bq_backfill API request made for %s, returned status_code: %s' % (
                        vista_bq_account_key, str(r.status_code)))
                except Exception as err:
                    logger.error('error :: bq_fetcher :: bq_backfill request failed for %s on %s, err: %s' % (
                        vista_bq_account_key, str(bq_backfill_url), err))
                    continue
                response = None
                if r:
                    try:
                        response = r.json()
                    except Exception as err:
                        logger.error(traceback.format_exc())
                        logger.error('error :: bq_fetcher :: failed to parse json response from %s, err: %s' % (
                            str(bq_backfill_url), err))
                        continue
                if response:
                    logger.info('bq_fetcher :: bq_backfill_job submitted for %s, response: %s' % (
                        vista_bq_account_key, str(response)))
                else:
                    logger.error('error :: bq_fetcher :: bq_backfill job submitted for %s but no response' % (
                        vista_bq_account_key))

            update_scheduled = False
            if 'update_data_schedule' in acc_dict.keys():
                try:
                    update_scheduled = job_scheduled(skyline_app, schedule_dict=acc_dict['update_data_schedule'])
                except Exception as err:
                    logger.error('error :: bq_fetcher :: job_scheduled failed for vista_bq_account_keyount: %s, update_data_schedule: %s , err: %s' % (
                        str(vista_bq_account_key), str(acc_dict['update_data_schedule']), err))
            if update_scheduled:

                # @added 20240703 - Allow for more update checks
                last_bq_update_key = 'vista.bq_update.%s.last_update' % vista_bq_account_key
                last_bq_update_timestamp = None
                try:
                    last_bq_update_timestamp = self.redis_conn_decoded.get(last_bq_update_key)
                except Exception as err:
                    logger.error('error :: bq_fetcher :: failed to determine on last_bq_update_timestamp from %s, err: %s' % (
                        str(last_bq_update_key), err))
                    last_bq_update_timestamp = None
                if last_bq_update_timestamp:
                    logger.info('bq_fetcher :: bq_update is scheduled but %s key exists, skipping' % (
                        last_bq_update_key))
                    continue

                # Submit a bq_update job
                try:
                    interval = int(acc_dict['schedule_interval'])
                except Exception as err:
                    logger.error('error :: bq_fetcher :: could not determine schedule_interval from %s, err: %s' % (
                        vista_bq_account_key, err))
                    continue
                until_timestamp = int(time()) // interval * interval
                try:
                    update_data_period = int(acc_dict['update_data_period'])
                except Exception as err:
                    logger.error('error :: bq_fetcher :: could not determine update_data_period from %s, err: %s' % (
                        vista_bq_account_key, err))
                    continue
                from_timestamp = until_timestamp - update_data_period

                try:
                    resolution = int(acc_dict['resolution'])
                except Exception as err:
                    logger.error('error :: bq_fetcher :: could not determine resolution from %s, err: %s' % (
                        vista_bq_account_key, err))
                    continue

                try:
                    date_field_format = acc_dict['date_field_format']
                except Exception as err:
                    logger.error('error :: bq_fetcher :: could not determine date_field_format from %s, err: %s' % (
                        vista_bq_account_key, err))
                    continue

                intervals_to_fetch_timestamps = {}
                intervals_to_fetch = []
                end_timestamp = int(until_timestamp) + 1
                ctime = int(from_timestamp)
                while ctime < end_timestamp:
                    try:
                        interval_date_fromat = datetime.fromtimestamp(int(ctime)).strftime(date_field_format)
                        intervals_to_fetch.append(interval_date_fromat)
                        intervals_to_fetch_timestamps[interval_date_fromat] = int(ctime)
                    except Exception as err:
                        logger.error('error :: bq_fetcher :: failed to convert %s to date_field_format, err: %s' % (
                            str(ctime), err))
                    ctime = ctime + interval
                    if ctime > end_timestamp:
                        break

                # Search the path for the latest archive for the 
                archive_data_search_path = '%s/vista/bq_archives/%s' % (
                    settings.SKYLINE_DIR, vista_bq_account_key)
                bq_archives = []
                if os.path.isdir(archive_data_search_path):
                    for dir_path, folders, files in os.walk(archive_data_search_path):
                        try:
                            if files:
                                for i in files:
                                    path_and_file = '%s/%s' % (dir_path, i)
                                    if i.endswith('.csv'):
                                        bq_archives.append(path_and_file)
                        except Exception as err:
                            logger.error('error :: bq_fetcher :: os.walk failed on %s, err: %s' % (
                                str(archive_data_search_path), err))
                # Sort latest to oldest
                bq_archives = sorted(bq_archives, reverse=True)
                fetch_interval_archives = {}
                for interval_to_fetch in intervals_to_fetch:
                    for bq_archive in bq_archives:
                        if interval_to_fetch in bq_archive:
                            fetch_interval_archives[interval_to_fetch] = bq_archive
                            break
                for interval_to_fetch in intervals_to_fetch:
                    if interval_to_fetch not in fetch_interval_archives.keys():
                        logger.warning('warning :: bq_fetcher :: no archive found for %s at %s' % (
                            vista_bq_account_key, str(interval_to_fetch)))

                post_data = {}
                try:
                    post_data = {
                        'data': {
                            'bq_update': True,
                            'vista_bq_account_key': vista_bq_account_key,
                            'from_timestamp': from_timestamp,
                            'until_timestamp': until_timestamp,
                            'verify_ssl': settings.VERIFY_SSL,
                            'added_by': 'vista',
                        }
                    }
                except Exception as err:
                    logger.error('error :: bq_fetcher :: failed to build post_data for bq_update job, err: %s' % err)
                    continue
                r = None
                try:
                    headers = {"content-type": "application/json"}
                    r = requests.post(bq_update_url, auth=(user, password), json=post_data, headers=headers, timeout=5, verify=settings.VERIFY_SSL)
                    logger.info('bq_fetcher :: bq_update API request made for %s, returned status_code: %s' % (
                        vista_bq_account_key, str(r.status_code)))
                except Exception as err:
                    logger.error('error :: bq_fetcher :: bq_update request failed for %s on %s, err: %s' % (
                        vista_bq_account_key, str(bq_update_url), err))
                    continue
                response = None
                if r:
                    try:
                        response = r.json()
                    except Exception as err:
                        logger.error(traceback.format_exc())
                        logger.error('error :: bq_fetcher :: failed to parse json response from %s, err: %s' % (
                            str(bq_update_url), err))
                        continue
                if response:
                    logger.info('bq_fetcher :: bq_update_job submitted for %s, response: %s' % (
                        vista_bq_account_key, str(response)))
                else:
                    logger.error('error :: bq_fetcher :: bq_update job submitted for %s but no response' % (
                        vista_bq_account_key))

        logger.info('bq_fetcher :: done')
        return

    def run(self):
        """
        - Called when the process intializes.
        - Check job schedules
        - Wait for the processes to finish.
        """

        # Log management to prevent overwriting
        # Allow the bin/<skyline_app>.d to manage the log
        now = time()
        log_wait_for = now + 5
        while now < log_wait_for:
            if os.path.isfile(skyline_app_loglock):
                sleep(.1)
                now = time()
            else:
                now = log_wait_for + 1

        logger.info('starting %s bq_fetcher run' % skyline_app)

        running = True
        while running:
            begin_fetcher_run = int(time())

            vista_bq_worker = True
            # Only execute on a single node in the cluster
            if HORIZON_SHARDS:
                if HORIZON_SHARD != 0:
                    vista_bq_worker = False
            if vista_bq_worker:
                pids = []
                spawned_pids = []
                now = int(time())
                try:
                    logger.info('bq_fetcher :: spawning a bq_fetch process')
                    p = Process(target=self.bq_fetch, args=(begin_fetcher_run,))
                    pids.append(p)
                    p.start()
                    spawned_pids.append(p.pid)
                    logger.info('bq_fetcher :: spawned a bq_fetch process with pid %s' % str(p.pid))
                except Exception as err:
                    logger.error('error :: fetcher :: failed to spawn bq_fetch, err: %s' % err)

                # Self monitor processes and terminate if any fetch_process has run
                # for longer than VISTA_FETCHER_PROCESS_MAX_RUNTIME seconds
                p_starts = time()
                while time() - p_starts <= 30:
                    if any(p.is_alive() for p in pids):
                        # Just to avoid hogging the CPU
                        sleep(.1)
                    else:
                        # the process is done, break now.
                        time_to_run = time() - p_starts
                        logger.info('bq_fetcher :: bq_fetch completed in %.2f seconds' % time_to_run)
                        break
                else:
                    # We only enter this if we didn't 'break' above.
                    logger.info('bq_fetcher :: timed out, killing bq_fetch')
                    for p in pids:
                        logger.info('bq_fetcher :: killing bq_fetch process')
                        p.terminate()
                        logger.info('bq_fetcher :: killed bq_fetch process')

                for p in pids:
                    if p.is_alive():
                        logger.info('bq_fetcher :: stopping bq_fetch - %s' % (str(p.is_alive())))
                        killing_pid = p.pid
                        logger.info('bq_fetcher :: kill bq_fetch with pid: %s' % (str(killing_pid)))
                        p.terminate()
                        logger.info('bq_fetcher :: killed bq_fetch process with pid: %s' % (str(killing_pid)))

            # Sleep next next run
            process_runtime = int(time()) - begin_fetcher_run
            if int(process_runtime) < 60:
                next_run = int(begin_fetcher_run) + 60
                time_now = int(time())
                sleep_for = next_run - time_now
                logger.info('bq_fetcher :: sleeping for %s seconds until next fetch' % str(sleep_for))
                sleep(sleep_for)
                try:
                    del sleep_for
                except:
                    logger.error('error :: bq_fetcher :: failed to del sleep_for')
                try:
                    del next_run
                except:
                    logger.error('error :: bq_fetcher :: failed to del next_run')
                try:
                    del time_now
                except:
                    logger.error('error :: bq_fetcher :: failed to del time_now')
