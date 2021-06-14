import sys
import os.path
from os import kill
import traceback
from multiprocessing import Process

from time import time
import datetime
from time import sleep
from ast import literal_eval
import socket

# @modified 20200808 - Task #3608: Update Skyline to Python 3.8.3 and deps
# bandit [B403:blacklist] Consider possible security implications associated
# with pickle module.  These have been considered.
import pickle  # nosec

import struct
import shutil
import glob
import gzip
import zipfile
import tarfile
from collections import Counter

# @added 20210407 - Feature #4004: flux - aggregator.py and FLUX_AGGREGATE_NAMESPACES
# Better handle multiple workers
import random

import pandas as pd
import pytz
from timeit import default_timer as timer

from logger import set_up_logging
sys.path.append(os.path.join(os.path.dirname(os.path.realpath(__file__)), os.pardir))
sys.path.insert(0, os.path.dirname(__file__))

if True:
    import settings
    from skyline_functions import (
        get_redis_conn, get_redis_conn_decoded, mkdir_p, sort_timeseries,
        filesafe_metricname, write_data_to_file)

# Consolidate flux logging
logger = set_up_logging(None)

this_host = str(os.uname()[1])

try:
    SERVER_METRIC_PATH = '.%s' % settings.SERVER_METRICS_NAME
    if SERVER_METRIC_PATH == '.':
        SERVER_METRIC_PATH = ''
except:
    SERVER_METRIC_PATH = ''

parent_skyline_app = 'flux'
skyline_app = 'flux'
skyline_app_graphite_namespace = 'skyline.%s%s.uploaded_data_worker' % (parent_skyline_app, SERVER_METRIC_PATH)

LOCAL_DEBUG = False

settings_errors = False
try:
    CARBON_HOST = settings.FLUX_CARBON_HOST
    CARBON_PORT = settings.FLUX_CARBON_PORT
    FLUX_CARBON_PICKLE_PORT = settings.FLUX_CARBON_PICKLE_PORT
    DATA_UPLOADS_PATH = settings.DATA_UPLOADS_PATH
except:
    settings_errors = True

try:
    save_uploads = settings.FLUX_SAVE_UPLOADS
    save_uploads_path = settings.FLUX_SAVE_UPLOADS_PATH
except:
    save_uploads = False

utc_timezones = [
    'Etc/GMT', 'Etc/GMT+0', 'Etc/GMT0', 'Etc/GMT-0', 'Etc/Greenwich', 'Etc/UTC',
    'Etc/Universal', 'Etc/Zulu', 'GMT', 'GMT+0', 'GMT-0', 'GMT0', 'Greenwich',
    'UTC', 'Universal', 'Zulu'
]

ALLOWED_EXTENSIONS = {'json', 'csv', 'xlsx', 'xls'}


class UploadedDataWorker(Process):
    """
    The worker grabs data files from the :mod:`settings.DATA_UPLOADS_PATH` and
    processes the data files and sends to data to Graphite.
    """
    def __init__(self, parent_pid):
        super(UploadedDataWorker, self).__init__()
        self.redis_conn = get_redis_conn(skyline_app)
        self.redis_conn_decoded = get_redis_conn_decoded(skyline_app)

        self.parent_pid = parent_pid
        self.daemon = True

    def check_if_parent_is_alive(self):
        """
        Self explanatory.
        """
        try:
            kill(self.parent_pid, 0)
        except:
            # @added 20201203 - Bug #3856: Handle boring sparsely populated metrics in derivative_metrics
            # Log warning
            logger.warn('warning :: parent process is dead')
            exit(0)

    def run(self):
        """
        Called when the process intializes.
        """

        def pickle_data_to_graphite(data):

            message = None
            try:
                payload = pickle.dumps(data, protocol=2)
                header = struct.pack("!L", len(payload))
                message = header + payload
            except:
                logger.error(traceback.format_exc())
                logger.error('error :: uploaded_data_worker :: failed to pickle to send to Graphite')
                return False
            if message:
                try:
                    sock = socket.socket()
                    sock.connect((CARBON_HOST, FLUX_CARBON_PICKLE_PORT))
                    sock.sendall(message)
                    sock.close()
                except:
                    logger.error(traceback.format_exc())
                    logger.error('error :: uploaded_data_worker :: failed to send pickle data to Graphite')
                    return False
            else:
                logger.error('error :: uploaded_data_worker :: failed to pickle metric data into message')
                return False
            return True

        def remove_redis_set_item(data):
            try:
                self.redis_conn.srem('flux.uploaded_data', str(data))
                logger.info('uploaded_data_worker :: removed upload from the flux.uploaded_data Redis set')
            except:
                logger.error(traceback.format_exc())
                logger.error('error :: uploaded_data_worker :: failed to remove item from the Redis flux.uploaded_data set - %s' % str(data))
                return False
            return True

        def update_redis_upload_status_key(upload_status):
            upload_id_key = None
            for row in upload_status:
                if row[0] == 'upload_id':
                    upload_id = row[1]
                    break
            upload_id_key = upload_id.replace('/', '.')
            try:
                upload_status_redis_key = 'flux.upload_status.%s' % upload_id_key
                upload_status_dict = {}
                for row in upload_status:
                    if row[0] not in upload_status_dict.keys():
                        upload_status_dict[row[0]] = row[1]
                if upload_status_dict:
                    self.redis_conn.setex(upload_status_redis_key, 2592000, str(upload_status_dict))
                    logger.info('uploaded_data_worker :: updated Redis key %s with new status' % upload_status_redis_key)
            except:
                logger.error(traceback.format_exc())
                logger.error('error :: uploaded_data_worker :: failed updated Redis key %s with new status' % upload_status_redis_key)
                return False
            return True

        def new_upload_status(upload_status, key, value):
            new_upload_status = []
            for row in upload_status:
                if row[0] == key:
                    row[1] = value
                new_upload_status.append(row)
            update_redis_upload_status_key(new_upload_status)
            return new_upload_status

        def set_date(self, d):
            try:
                self.date = d.astimezone(pytz.utc)
            except:
                self.date = pytz.utc.localize(d)

        def allowed_file(filename):
            return '.' in filename and \
                   filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS

        logger.info('uploaded_data_worker :: starting worker')

        while True:
            # Make sure Redis is up
            redis_up = False
            while not redis_up:
                try:
                    redis_up = self.redis_conn.ping()
                except:
                    logger.error('uploaded_data_worker :: cannot connect to Redis at socket path %s' % (settings.REDIS_SOCKET_PATH))
                    sleep(2)
                    self.redis_conn = get_redis_conn(skyline_app)
                    self.redis_conn_decoded = get_redis_conn_decoded(skyline_app)

            if settings_errors:
                logger.error('error :: uploaded_data_worker :: there are errors in the settings, cannot run')
                sleep(60)
                continue

            uploads_to_process = []

            # @added 20210407 - Feature #4004: flux - aggregator.py and FLUX_AGGREGATE_NAMESPACES
            # Better handle multiple workers
            sleep_for = random.uniform(0.1, 1.5)
            sleep(sleep_for)

            try:
                # Get uploads to process from the Redis set which the webapp
                # /upload_data populates
                uploads_to_process = list(self.redis_conn_decoded.smembers('flux.uploaded_data'))
                logger.info('uploaded_data_worker :: there are %s uploads to process' % str(len(uploads_to_process)))
            except Exception as e:
                logger.error(traceback.format_exc())
                logger.error('error :: uploaded_data_worker :: failed to query Redis for flux.uploaded_data - %s' % str(e))

            if not uploads_to_process:
                # @modified 20210407 - Feature #4004: flux - aggregator.py and FLUX_AGGREGATE_NAMESPACES
                # Better handle multiple workers
                # logger.info('uploaded_data_worker :: there are no uploads to process, sleeping for 30 seconds')
                # sleep(30)
                sleep_for = random.uniform(0.1, 1.5)
                logger.info('uploaded_data_worker :: there are no uploads to process, sleeping for 30 seconds and some random %s seconds' % str(sleep_for))
                sleep((30 + sleep_for))
                continue

            processing_upload_failed = False
            data_files_uploaded = 0
            data_files_successfully_processed = []
            upload_to_process = None
            dryrun = False
            try:
                uploads_to_process.sort()
                upload_to_process = uploads_to_process[0]
                logger.info('uploaded_data_worker :: new upload to process - %s' % str(upload_to_process))
            except:
                logger.error(traceback.format_exc())
                logger.error('error :: uploaded_data_worker :: failed to determine upload_to_process')
                processing_upload_failed = True
            upload_dict = None
            upload_status = []
            if upload_to_process:
                started_at = int(time())
                started_at = datetime.datetime.fromtimestamp(started_at)
                started_at_date = started_at.strftime('%Y-%m-%d %H:%M:%S')

                start_processing = timer()
                try:
                    upload_dict = literal_eval(upload_to_process)
                except:
                    logger.error(traceback.format_exc())
                    logger.error('error :: uploaded_data_worker :: failed to literal_eval the upload_dict from upload_to_process - %s ' % str(upload_to_process))
                    processing_upload_failed = True

            # @added 20210407 - Feature #4004: flux - aggregator.py and FLUX_AGGREGATE_NAMESPACES
            # Better handle multiple workers, remove the item from the set
            # immediately so another worker does not assign itself to same work
            if upload_dict:
                item_removed = remove_redis_set_item(str(upload_dict))
                if item_removed:
                    logger.info('uploaded_data_worker :: removed failed upload from the flux.uploaded_data Redis set')
                else:
                    logger.error('error :: uploaded_data_worker :: failed to remove item from the Redis flux.uploaded_data set - %s' % str(upload_dict))

            if upload_dict:
                try:
                    parent_metric_namespace = upload_dict['parent_metric_namespace']
                    upload_status.append(['parent_metric_namespace', parent_metric_namespace])
                    timezone = upload_dict['timezone']
                    upload_status.append(['timezone', timezone])
                    upload_id = upload_dict['upload_id']
                    upload_status.append(['upload_id', upload_id])
                    upload_status.append(['status', 'pending'])
                    data_format = upload_dict['format']
                    upload_status.append(['format', data_format])
                    archive = upload_dict['archive']
                    upload_status.append(['archive', archive])
                    data_filename = upload_dict['data_filename']
                    upload_status.append(['data_filename', data_filename])
                    info_filename = upload_dict['info_filename']
                    upload_status.append(['info_filename', info_filename])
                    try:
                        header_row = int(upload_dict['header_row'])
                    except:
                        header_row = None
                    upload_status.append(['header_row', header_row])
                    try:
                        skip_rows = int(upload_dict['skip_rows'])
                    except:
                        skip_rows = None
                    upload_status.append(['skip_rows', skip_rows])
                    try:
                        info_file_in_archive = upload_dict['info_file_in_archive']
                    except:
                        info_file_in_archive = False
                    upload_status.append(['info_file_in_archive', str(info_file_in_archive)])
                    try:
                        resample_method = upload_dict['resample_method']
                    except:
                        resample_method = 'mean'
                    upload_status.append(['resample_method', resample_method])
                    # @added 20200521 - Feature #3538: webapp - upload_data endpoint
                    #                   Feature #3550: flux.uploaded_data_worker
                    # Added the ability to ignore_submitted_timestamps and not
                    # check flux.last metric timestamp
                    try:
                        ignore_submitted_timestamps = upload_dict['ignore_submitted_timestamps']
                    except:
                        ignore_submitted_timestamps = False
                    upload_status.append(['ignore_submitted_timestamps', ignore_submitted_timestamps])
                except:
                    logger.error(traceback.format_exc())
                    logger.error('error :: uploaded_data_worker :: failed to determine required variables for the upload_dict - %s ' % str(upload_dict))
                    processing_upload_failed = True

            if upload_status and upload_id:
                update_redis_upload_status_key(upload_status)
                upload_status.append(['processing started at', started_at_date])
                upload_status = new_upload_status(upload_status, 'processing started at', started_at_date)

            upload_info = None
            info_file = None
            # @added 20210504  - Task #4030: refactoring
            open_info_file = None

            if upload_id and info_filename:
                info_file = '%s/%s/%s' % (DATA_UPLOADS_PATH, upload_id, info_filename)
                if not os.path.isfile(info_file):
                    logger.error('error :: uploaded_data_worker :: failed to find info file - %s' % str(info_file))
                    info_file = None
                    processing_upload_failed = True
                    if upload_status:
                        upload_error = 'info file not found - %s' % info_filename
                        upload_status.append(['error', upload_error])
                        upload_status = new_upload_status(upload_status, 'error', upload_error)
                # @added 20210504  - Task #4030: refactoring
                else:
                    open_info_file = '%s/%s' % (os.path.dirname(info_file), os.path.basename(info_file))

            # @modified 20210504  - Task #4030: refactoring
            # if info_file:
            if open_info_file:
                try:
                    # @modified 20210504  - Task #4030: refactoring
                    # with open(info_file) as f:
                    with open(open_info_file) as f:
                        dict_data_str = f.read()
                    upload_info = literal_eval(dict_data_str)
                except:
                    logger.error(traceback.format_exc())
                    logger.error('error :: uploaded_data_worker :: failed to literal_eval the upload info for - %s' % str(info_file))
                    processing_upload_failed = True
                    if upload_status:
                        upload_error = 'data in info file not in the json correct format - %s' % info_filename
                        upload_status.append(['error', upload_error])
                        upload_status = new_upload_status(upload_status, 'error', upload_error)

            refresh_info = False
            data_file = None
            if processing_upload_failed:
                data_filename = None
            if upload_id and data_filename:
                try:
                    data_file = '%s/%s/%s' % (DATA_UPLOADS_PATH, upload_id, data_filename)
                    if not os.path.isfile(data_file):
                        logger.error('error :: uploaded_data_worker :: failed to find data file - %s' % str(data_file))
                        processing_upload_failed = True
                        data_file = None
                        if upload_status:
                            upload_error = 'data file not found - %s' % data_filename
                            upload_status.append(['error', upload_error])
                            upload_status = new_upload_status(upload_status, 'error', upload_error)
                except:
                    logger.error(traceback.format_exc())
                    logger.error('error :: uploaded_data_worker :: failed to check if data_file exists')
                    processing_upload_failed = True
                    if upload_status:
                        upload_error = 'failed to check if data_file exists'
                        upload_status.append(['error', upload_error])
                        upload_status = new_upload_status(upload_status, 'error', upload_error)
            if data_file:
                if archive != 'none':
                    extracted_data_dir = '%s/%s/extracted' % (DATA_UPLOADS_PATH, upload_id)
                else:
                    extracted_data_dir = '%s/%s' % (DATA_UPLOADS_PATH, upload_id)
                    upload_status.append([data_filename, 'pending'])
                    upload_status = new_upload_status(upload_status, data_filename, 'pending')
                    data_files_uploaded = 1

                if not os.path.exists(extracted_data_dir):
                    mkdir_p(extracted_data_dir)
                if archive == 'gz':
                    logger.info('uploaded_data_worker :: gunzipping - %s' % str(data_filename))
                    try:
                        uncompressed_data_filename = data_filename.replace('.gz', '')
                        uncompressed_data_file = '%s/%s' % (extracted_data_dir, uncompressed_data_filename)
                        with gzip.open(data_file, 'rb') as f_in:
                            with open(uncompressed_data_file, 'wb') as f_out:
                                shutil.copyfileobj(f_in, f_out)
                        data_files_uploaded = 1
                        upload_status.append([uncompressed_data_filename, 'pending'])
                        upload_status = new_upload_status(upload_status, uncompressed_data_filename, 'pending')
                    except:
                        logger.error(traceback.format_exc())
                        logger.error('error :: uploaded_data_worker :: failed to ungzip data archive - %s' % str(data_file))
                        processing_upload_failed = True
                        if upload_status:
                            upload_error = 'failed to ungzip data archive - %s' % data_filename
                            upload_status.append(['error', upload_error])
                            upload_status = new_upload_status(upload_status, 'error', upload_error)

                if archive == 'zip':
                    logger.info('uploaded_data_worker :: unzipping - %s' % str(data_filename))
                    try:
                        with zipfile.ZipFile(data_file, 'r') as zip_ref:
                            zip_ref.extractall(extracted_data_dir)
                        for root, dirs, files in os.walk(extracted_data_dir):
                            for file in files:
                                if file.endswith('info.json'):
                                    if info_file_in_archive:
                                        info_file = file
                                        refresh_info = True
                                if file.endswith(data_format):
                                    data_files_uploaded += 1
                                    upload_status.append([file, 'pending'])
                                    upload_status = new_upload_status(upload_status, file, 'pending')
                    except:
                        logger.error(traceback.format_exc())
                        logger.error('error :: uploaded_data_worker :: failed to unzip data archive - %s' % str(data_file))
                        processing_upload_failed = True
                        if upload_status:
                            upload_error = 'failed to unzip data archive - %s' % data_filename
                            upload_status.append(['error', upload_error])
                            upload_status = new_upload_status(upload_status, 'error', upload_error)

                if archive == 'tar_gz':
                    logger.info('uploaded_data_worker :: tar ungzipping - %s' % str(data_filename))
                    try:
                        tar = tarfile.open(data_file, 'r:gz')
                        for member in tar.getmembers():
                            f = tar.extractfile(member)
                            if f is not None:
                                extracted_data_dir = '%s/%s/extracted' % (DATA_UPLOADS_PATH, upload_id)
                                uncompressed_data_file = '%s/%s' % (extracted_data_dir, str(member))
                                with open(uncompressed_data_file, 'wb') as f_out:
                                    shutil.copyfileobj(f, f_out)
                                if uncompressed_data_file.endswith('info.json'):
                                    if info_file_in_archive:
                                        info_file = uncompressed_data_file
                                        refresh_info = True
                                if uncompressed_data_file.endswith(data_format):
                                    data_files_uploaded += 1
                                    uncompressed_data_filename = os.path.basename(uncompressed_data_file)
                                    upload_status.append([uncompressed_data_filename, 'pending'])
                                    upload_status = new_upload_status(upload_status, uncompressed_data_filename, 'pending')
                    except:
                        logger.error(traceback.format_exc())
                        logger.error('error :: uploaded_data_worker :: failed to untar data archive - %s' % str(data_file))
                        processing_upload_failed = True
                        if upload_status:
                            upload_error = 'failed to untar data archive - %s' % data_filename
                            upload_status.append(['error', upload_error])
                            upload_status = new_upload_status(upload_status, 'error', upload_error)

                if archive != 'none':
                    unallowed_files = []
                    try:
                        for root, dirs, files in os.walk(extracted_data_dir):
                            for file in files:
                                acceptable_file = allowed_file(file)
                                if not acceptable_file:
                                    file_to_delete = '%s/%s' % (extracted_data_dir, str(file))
                                    unallowed_files.append(file_to_delete)
                                    logger.info('uploaded_data_worker :: WARNING unallowed file format - %s' % file)
                    except:
                        logger.error(traceback.format_exc())
                        logger.error('error :: uploaded_data_worker :: failed to determine what files to delete in %s' % extracted_data_dir)
                    if unallowed_files:
                        for unallowed_file in unallowed_files:
                            unallowed_filename = os.path.basename(unallowed_file)
                            try:
                                os.remove(unallowed_file)
                            except:
                                logger.error(traceback.format_exc())
                                logger.error('error :: uploaded_data_worker :: failed to delete unallowed file - %s' % unallowed_file)
                            try:
                                upload_status.append([unallowed_filename, 'failed - DELETED - not an allowed file type'])
                                upload_status = new_upload_status(upload_status, unallowed_filename, 'failed - DELETED - not an allowed file type')
                            except:
                                logger.error(traceback.format_exc())
                                logger.error('error :: uploaded_data_worker :: failed update upload_status for delete unallowed file - %s' % unallowed_filename)

            logger.info('uploaded_data_worker :: %s data files found to process for %s' % (
                str(data_files_uploaded), upload_id))
            if refresh_info:
                logger.info('uploaded_data_worker :: refresh info for the info.json included in the data archive - %s' % info_file)
                try:

                    # @added 20210504  - Task #4030: refactoring
                    open_info_file = '%s/%s' % (os.path.dirname(info_file), os.path.basename(info_file))

                    # @modified 20210504  - Task #4030: refactoring
                    # with open(info_file) as f:
                    with open(open_info_file) as f:
                        dict_data_str = f.read()
                    upload_info = literal_eval(dict_data_str)
                except:
                    logger.error(traceback.format_exc())
                    logger.error('error :: uploaded_data_worker :: failed to literal_eval the upload info for - %s' % str(info_file))
                    processing_upload_failed = True
                    if upload_status:
                        upload_error = 'data in info file not in the json correct format - %s' % info_filename
                        upload_status.append(['error', upload_error])
                        upload_status = new_upload_status(upload_status, 'error', upload_error)

            if upload_dict and processing_upload_failed:
                try:
                    logger.info('uploaded_data_worker :: failed to process upload - %s' % str(upload_dict))
                    upload_status = new_upload_status(upload_status, data_filename, 'failed')
                    upload_status = new_upload_status(upload_status, 'status', 'failed')
                    item_removed = remove_redis_set_item(str(upload_dict))
                    if item_removed:
                        logger.info('uploaded_data_worker :: removed failed upload from the flux.uploaded_data Redis set')
                    else:
                        logger.error('error :: uploaded_data_worker :: failed to remove item from the Redis flux.uploaded_data set - %s' % str(upload_dict))
                    continue
                except:
                    logger.error(traceback.format_exc())
                    logger.error('error :: uploaded_data_worker :: failed to remove_redis_set_item')
                    continue

            if not upload_info:
                upload_info = upload_dict

            if upload_info:
                try:
                    parent_metric_namespace = upload_info['parent_metric_namespace']
                    logger.info('uploaded_data_worker :: determined parent_metric_namespace from the upload_info dict - %s' % str(parent_metric_namespace))
                except:
                    logger.error(traceback.format_exc())
                    logger.error('error :: uploaded_data_worker :: failed to determine parent_metric_namespace from the upload_info dict - %s' % str(upload_info))
                    processing_upload_failed = True
                    if upload_status:
                        upload_error = 'failed to determine parent_metric_namespace from info file - %s' % info_filename
                        upload_status.append(['error', upload_error])
                        upload_status = new_upload_status(upload_status, 'error', upload_error)
                try:
                    timezone = upload_info['timezone']
                    logger.info('uploaded_data_worker :: determined timezone from the upload_info dict - %s' % str(timezone))
                except:
                    logger.error(traceback.format_exc())
                    logger.error('error :: uploaded_data_worker :: failed to determine timezone from the upload_info dict - %s' % str(upload_info))
                    processing_upload_failed = True
                    if upload_status:
                        upload_error = 'failed to determine timezone from info file - %s' % info_filename
                        upload_status.append(['error', upload_error])
                        upload_status = new_upload_status(upload_status, 'error', upload_error)
                try:
                    skip_rows = int(upload_info['skip_rows'])
                    logger.info('uploaded_data_worker :: determined skip_rows from the upload_info dict - %s' % str(skip_rows))
                    if skip_rows == 0:
                        skip_rows = None
                except:
                    skip_rows = None
                    logger.info('uploaded_data_worker :: skip_rows was not passed in the upload info dict using %s' % str(skip_rows))
                try:
                    header_row = int(upload_info['header_row'])
                    logger.info('uploaded_data_worker :: determined header_row from the upload_info dict - %s' % str(header_row))
                except:
                    logger.error(traceback.format_exc())
                    logger.error('error :: uploaded_data_worker :: failed to determine header_row from the upload_info dict - %s' % str(upload_info))
                    processing_upload_failed = True
                    if upload_status:
                        upload_error = 'failed to determine header_row from info file - %s' % info_filename
                        upload_status.append(['error', upload_error])
                        upload_status = new_upload_status(upload_status, 'error', upload_error)
                try:
                    date_orientation = upload_info['date_orientation']
                    logger.info('uploaded_data_worker :: determined date_orientation from the upload_info dict - %s' % str(date_orientation))
                except:
                    logger.error(traceback.format_exc())
                    logger.error('error :: uploaded_data_worker :: failed to determine date_orientation from the upload_info dict - %s' % str(upload_info))
                    processing_upload_failed = True
                    if upload_status:
                        upload_error = 'failed to determine date_orientation from info file - %s' % info_filename
                        upload_status.append(['error', upload_error])
                        upload_status = new_upload_status(upload_status, 'error', upload_error)
                try:
                    columns_to_metrics = upload_info['columns_to_metrics']
                    logger.info('uploaded_data_worker :: determined columns_to_metrics from the upload_info dict - %s' % str(columns_to_metrics))
                except:
                    columns_to_metrics = None
                    logger.info('uploaded_data_worker :: columns_to_metrics was not passed in the upload info dict setting to None')
                try:
                    columns_to_ignore = upload_info['columns_to_ignore']
                    logger.info('uploaded_data_worker :: determined columns_to_ignore from the upload_info dict - %s' % str(columns_to_ignore))
                except:
                    columns_to_ignore = None
                    logger.info('uploaded_data_worker :: columns_to_ignore was not passed in the upload info dict setting to None')
                try:
                    columns_to_process = upload_info['columns_to_process']
                    logger.info('uploaded_data_worker :: determined columns_to_process from the upload_info dict - %s' % str(columns_to_process))
                except:
                    columns_to_process = None
                    logger.info('uploaded_data_worker :: columns_to_process was not passed in the upload info dict setting to None')
                resample_method = 'mean'
                try:
                    resample_method_str = upload_info['resample_method']
                    if resample_method_str in ['mean', 'sum']:
                        resample_method = resample_method_str
                        logger.info('uploaded_data_worker :: determined resample_method from the upload_info dict - %s' % str(resample_method))
                except:
                    resample_method = 'mean'
                    logger.info('uploaded_data_worker :: resample_method was not passed in the upload info dict setting to mean')
                try:
                    debug_enabled_in_info = upload_info['debug']
                    logger.info('uploaded_data_worker :: determined debug from the upload_info dict - %s' % str(debug_enabled_in_info))
                    if debug_enabled_in_info == 'true':
                        debug_enabled_in_info = True
                except:
                    debug_enabled_in_info = False
                try:
                    dryrun = upload_info['dryrun']
                    logger.info('uploaded_data_worker :: determined dryrun from the upload_info dict - %s' % str(dryrun))
                    if dryrun == 'true':
                        dryrun = True
                except:
                    dryrun = False
                try:
                    ignore_submitted_timestamps_str = upload_info['ignore_submitted_timestamps']
                    if ignore_submitted_timestamps_str == 'true':
                        ignore_submitted_timestamps = True
                        logger.info('uploaded_data_worker :: determined ignore_submitted_timestamps from the upload_info dict - %s' % str(ignore_submitted_timestamps))
                except:
                    logger.info('uploaded_data_worker :: ignore_submitted_timestamps was not passed in the upload info')

            if upload_dict and processing_upload_failed:
                logger.info('uploaded_data_worker :: failed to process upload - %s' % str(upload_dict))
                if upload_status:
                    upload_status = new_upload_status(upload_status, 'status', 'failed')
                    upload_status = new_upload_status(upload_status, data_filename, 'failed')

                item_removed = remove_redis_set_item(str(upload_dict))
                if item_removed:
                    logger.info('uploaded_data_worker :: removed failed upload from the flux.uploaded_data Redis set')
                else:
                    logger.error('error :: uploaded_data_worker :: failed to remove item from the Redis flux.uploaded_data set - %s' % str(upload_dict))
                continue

            data_format_extension = '.%s' % str(data_format)
            data_files_to_process = []
            try:
                for root, dirs, files in os.walk(extracted_data_dir):
                    for file in files:
                        if file.endswith(data_format_extension):
                            file_to_process = '%s/%s' % (extracted_data_dir, str(file))
                            data_files_to_process.append(file_to_process)
            except:
                logger.error(traceback.format_exc())
                logger.error('error :: uploaded_data_worker :: failed to determine files to process in %s' % extracted_data_dir)
                if upload_status:
                    upload_error = 'no extracted files found to process'
                    upload_status.append(['error', upload_error])
                    upload_status = new_upload_status(upload_status, 'error', upload_error)
                    upload_status = new_upload_status(upload_status, data_filename, 'failed')

            pytz_tz = 'UTC'
            invalid_timezone = False
            if timezone:
                pytz_tz = str(timezone)
                try:
                    test_tz = pytz.timezone(pytz_tz)
                    logger.info('uploaded_data_worker :: the passed pytz_tz argument is OK - %s - ' % str(test_tz))
                except:
                    logger.error(traceback.format_exc())
                    logger.error('error :: uploaded_data_worker :: the passed pytz_tz argument is not a valid pytz timezone string - %s' % str(timezone))
                    invalid_timezone = True
                    if upload_status:
                        upload_error = 'invalid timezone passed'
                        upload_status.append(['error', upload_error])
                        upload_status = new_upload_status(upload_status, 'error', upload_error)
                        upload_status = new_upload_status(upload_status, data_filename, 'failed')

            data_file_process_failures = 0

            for file_to_process in data_files_to_process:
                successful = True
                df = None
                failure_reason = 'none'
                data_columns = []

                processing_filename = os.path.basename(file_to_process)
                upload_status = new_upload_status(upload_status, processing_filename, 'in progress')

                if invalid_timezone:
                    failure_reason = 'falied - invalid timezone passed'
                    logger.error('error :: uploaded_data_worker :: %s' % failure_reason)
                    successful = False
                    if upload_status:
                        upload_status = new_upload_status(upload_status, processing_filename, failure_reason)

                if successful:
                    if data_format == 'xlsx' or data_format == 'xls':
                        try:
                            if LOCAL_DEBUG or debug_enabled_in_info:
                                logger.info('running - pd.read_excel(' + file_to_process + ', skiprows=' + str(skip_rows) + ', header=' + str(header_row) + ', usecols=' + str(columns_to_process) + ')')
                                # pandas-log is not available in Python 2 with
                                # pip, although Python 2 is no longer supported
                                # this to ensure this is backwards compatible
                                # with any current Python 2 installations
                                if sys.version[0] == 3:
                                    import pandas_log
                                    with pandas_log.enable():
                                        df = pd.read_excel(file_to_process, skiprows=skip_rows, header=header_row, usecols=columns_to_process)
                                else:
                                    df = pd.read_excel(file_to_process, skiprows=skip_rows, header=header_row, usecols=columns_to_process)
                                logger.debug(df.head())
                                logger.debug(df.info())
                            else:
                                df = pd.read_excel(file_to_process, skiprows=skip_rows, header=header_row, usecols=columns_to_process)
                            logger.info('uploaded_data_worker :: pandas dataframe created from %s - %s' % (
                                data_format, str(file_to_process)))
                            # Unfortunately this if df is not a reliable test
                            # if df.info() is None:
                            #     logger.error('error :: uploaded_data_worker :: df.info() returns None')
                            #     df = None
                        except:
                            logger.error(traceback.format_exc())
                            failure_reason = 'failed - pandas failed to parse the %s data file - %s' % (
                                data_format, str(file_to_process))
                            logger.error('error :: uploaded_data_worker :: %s' % failure_reason)
                            successful = False
                            if upload_status:
                                upload_status = new_upload_status(upload_status, processing_filename, failure_reason)
                    if data_format == 'csv':
                        try:
                            if LOCAL_DEBUG or debug_enabled_in_info:
                                logger.info('running - pd.read_csv(' + file_to_process + ', skiprows=' + str(skip_rows) + ', header=' + str(header_row) + ', usecols=' + str(columns_to_process) + ')')
                                if sys.version[0] == 3:
                                    import pandas_log
                                    with pandas_log.enable():
                                        df = pd.read_csv(file_to_process, skiprows=skip_rows, header=header_row, usecols=columns_to_process)
                                else:
                                    df = pd.read_csv(file_to_process, skiprows=skip_rows, header=header_row, usecols=columns_to_process)
                                logger.debug(df.head())
                                logger.debug(df.info())
                            else:
                                df = pd.read_csv(file_to_process, skiprows=skip_rows, header=header_row, usecols=columns_to_process)
                                logger.info('uploaded_data_worker :: pandas dataframe created from csv - %s' % str(file_to_process))
                        except:
                            logger.error(traceback.format_exc())
                            failure_reason = 'pandas failed to parse the csv data file - %s' % str(file_to_process)
                            logger.error('error :: uploaded_data_worker :: %s' % failure_reason)
                            successful = False
                            if upload_status:
                                upload_status = new_upload_status(upload_status, processing_filename, 'failed to read csv data')
                            # Unfortunately this if df is not a reliable test
                            # if df.info() is None:
                            #     logger.error('error :: uploaded_data_worker :: df.info() returns None')
                            #     df = None
                # Unfortunately this if df is not a reliable test
                # if not df:
                #     failure_reason = 'failed - emtpy dataframe'
                #     logger.error('error :: uploaded_data_worker :: %s' % failure_reason)
                #     successful = False
                #     if upload_status:
                #         upload_status = new_upload_status(upload_status, processing_filename, failure_reason)

                if successful:
                    if columns_to_metrics:
                        columns_list = columns_to_metrics.split(',')
                        try:
                            df.columns = columns_list
                            logger.info('uploaded_data_worker :: pandas renamed columns to - %s' % str(columns_list))
                        except:
                            logger.error(traceback.format_exc())
                            failure_reason = 'pandas failed to rename columns to - %s' % str(columns_list)
                            logger.error('error :: uploaded_data_worker :: uploaded_data_worker :: %s' % failure_reason)
                            successful = False
                            if upload_status:
                                upload_status = new_upload_status(upload_status, processing_filename, 'failed to rename columns')
                date_columns = []
                if successful:
                    try:
                        date_columns = [col for col in df.columns if 'datetime64[ns' in str(df[col].dtypes)]
                        logger.info('uploaded_data_worker :: determined date columns, %s' % str(date_columns))
                        if len(date_columns) == 0:
                            logger.info('uploaded_data_worker :: no date column determined trying to convert data column to datetime64')
                            # @added 20200807 - Feature #3550: flux.uploaded_data_worker
                            # Handle unix timestamps
                            first_date = None
                            unix_timestamp = False
                            try:
                                first_date = df['date'].values.tolist()[0]
                                if len(str(first_date)) == 10:
                                    if int(first_date) > 1:
                                        unix_timestamp = True
                            except:
                                logger.error(traceback.format_exc())
                                logger.error('error :: uploaded_data_worker :: failed to determine if date columns values are unix timestamps')
                            try:
                                # @modified 20200807 - Feature #3550: flux.uploaded_data_worker
                                # Handle unix timestamps
                                if unix_timestamp:
                                    df['date'] = pd.to_datetime(df['date'], unit='s')
                                else:
                                    df['date'] = pd.to_datetime(df['date'])
                            except:
                                logger.error(traceback.format_exc())
                                logger.error('error :: uploaded_data_worker :: failed to convert date column to datetime64')
                            date_columns = [col for col in df.columns if 'datetime64[ns' in str(df[col].dtypes)]
                            logger.info('uploaded_data_worker :: date_columns determined to be %s' % str(date_columns))
                    except:
                        logger.error(traceback.format_exc())
                        failure_reason = 'pandas failed to determined date columns'
                        logger.error('error :: uploaded_data_worker :: %s' % failure_reason)
                        successful = False
                        if upload_status:
                            upload_status = new_upload_status(upload_status, processing_filename, 'failed to determine datetime column')
                date_column = None
                if successful:
                    if len(date_columns) != 1:
                        failure_reason = 'the dataframe has no or more than one date columns - %s' % str(date_columns)
                        logger.error('error :: uploaded_data_worker :: %s' % failure_reason)
                        successful = False
                        if upload_status:
                            upload_status = new_upload_status(upload_status, processing_filename, 'failed - more than one date column')
                    else:
                        date_column = date_columns[0]
                        logger.info('uploaded_data_worker :: determined date column as %s' % str(date_column))
                        del date_columns
                if successful and date_column:
                    if pytz_tz not in utc_timezones:
                        try:
                            df[date_column] = df[date_column].dt.tz_localize(pytz_tz)
                            logger.info('uploaded_data_worker :: applied timezone %s to date column, %s' % (
                                str(pytz_tz), str(date_column)))
                        except:
                            logger.error(traceback.format_exc())
                            failure_reason = 'pandas failed to applied timezone %s to date column, %s' % (
                                str(pytz_tz), str(date_column))
                            logger.error('error :: uploaded_data_worker :: %s' % failure_reason)
                            successful = False
                            if upload_status:
                                upload_status = new_upload_status(upload_status, processing_filename, 'failed - to apply timezone')
                dates = None
                timestamps = []
                if successful and date_column:
                    try:
                        dates = df[date_column].tolist()
                        timestamps = []
                        for d in dates:
                            timestamps.append(int(d.strftime('%s')))
                        logger.info('uploaded_data_worker :: created UTC timestamps list from date column %s data' % (
                            str(date_column)))
                        del dates
                    except:
                        logger.error(traceback.format_exc())
                        failure_reason = 'pandas failed to convert datetimes to UTC timestamps'
                        logger.error('error :: uploaded_data_worker :: %s' % failure_reason)
                        successful = False
                        if upload_status:
                            upload_status = new_upload_status(upload_status, processing_filename, 'failed to convert datetimes to UTC timestamps')

                if successful and timestamps:
                    try:
                        df['pandas_utc_timestamp'] = timestamps
                        logger.info('uploaded_data_worker :: added pandas_utc_timestamp column to dataframe')
                        del timestamps
                    except:
                        logger.error(traceback.format_exc())
                        failure_reason = 'failed added skyline_uts_ts column to dataframe'
                        logger.error('error :: uploaded_data_worker :: %s' % failure_reason)
                        successful = False
                        if upload_status:
                            upload_status = new_upload_status(upload_status, processing_filename, 'failed to add UTC timestamp column')

                data_columns = []
                if successful:
                    try:
                        data_columns = [col for col in df.columns if col not in [date_column, 'pandas_utc_timestamp']]
                        logger.info('uploaded_data_worker :: determined %s data columns from the dataframe' % str(len(data_columns)))
                    except:
                        logger.error(traceback.format_exc())
                        failure_reason = 'failed to determine data columns from the dataframe'
                        logger.error('error :: uploaded_data_worker :: %s' % failure_reason)
                        successful = False
                        if upload_status:
                            upload_status = new_upload_status(upload_status, processing_filename, failure_reason)
                    if len(data_columns) == 0:
                        failure_reason = 'there are no data columns in the dataframe'
                        logger.error('error :: uploaded_data_worker :: %s' % failure_reason)
                        successful = False
                        if upload_status:
                            upload_status = new_upload_status(upload_status, processing_filename, failure_reason)
                if not successful:
                    data_file_process_failures += 1
                    logger.error('error :: uploaded_data_worker :: could not process - %s - continuing' % str(file_to_process))
                    try:
                        del df
                    except:
                        pass
                    continue

                successful = True
                data_df_failures = 0
                processed_data_columns = []
                if successful and data_columns:
                    for data_col in data_columns:
                        data_df_successful = True
                        data_df = None
                        data_col_key = '%s (%s)' % (data_col, processing_filename)
                        upload_status.append([data_col_key, 'processing'])
                        try:
                            data_df = df[['pandas_utc_timestamp', data_col]].copy()
                            logger.info('uploaded_data_worker :: created dataframe for %s' % data_col)
                        except:
                            logger.error(traceback.format_exc())
                            failure_reason = 'failed to create dataframe for pandas_utc_timestamp and %s' % data_col
                            logger.error('error :: uploaded_data_worker :: %s' % failure_reason)
                            data_df_successful = False
                            data_df_failures += 1
                            if upload_status:
                                upload_status = new_upload_status(upload_status, data_col_key, failure_reason)
                            try:
                                del data_df
                            except:
                                pass
                            continue
                        timeseries = None
                        original_timeseries_length = 0
                        if data_df_successful:
                            try:
                                data_df['timeseries'] = data_df.apply(lambda x: list([x['pandas_utc_timestamp'], x[data_col]]), axis=1)
                                timeseries = data_df['timeseries'].values.tolist()
                                original_timeseries_length = len(timeseries)
                                logger.info('uploaded_data_worker :: created timeseries for %s with %s timestamps and values' % (
                                    data_col, str(original_timeseries_length)))
                            except:
                                logger.error(traceback.format_exc())
                                failure_reason = 'failed to create timeseries from pandas_utc_timestamp and %s' % data_col
                                logger.error('error :: uploaded_data_worker :: %s' % failure_reason)
                                data_df_successful = False
                                data_df_failures += 1
                                if upload_status:
                                    upload_status = new_upload_status(upload_status, data_col_key, failure_reason)
                                try:
                                    del data_df
                                except:
                                    pass
                                continue
                        original_timeseries = None
                        if data_df_successful and timeseries:
                            original_timeseries = timeseries
                            try:
                                sorted_timeseries = sort_timeseries(timeseries)
                                if sorted_timeseries:
                                    sorted_timeseries_length = len(sorted_timeseries)
                                    timeseries = sorted_timeseries
                                    logger.info('uploaded_data_worker :: sorted timeseries for %s which has %s timestamps and values' % (
                                        data_col, str(sorted_timeseries_length)))
                                    try:
                                        del sorted_timeseries
                                    except:
                                        pass
                                else:
                                    logger.error('error :: uploaded_data_worker :: sorted_timeseries does not exists - %s' % (
                                        str(sorted_timeseries)))
                            except:
                                logger.error(traceback.format_exc())
                                failure_reason = 'failed to sort timeseries of pandas_utc_timestamp and %s' % data_col
                                logger.error('error :: uploaded_data_worker :: %s' % failure_reason)
                                timeseries = original_timeseries

                        metric = None
                        if data_df_successful and timeseries:
                            if original_timeseries:
                                try:
                                    del original_timeseries
                                except:
                                    pass
                            try:
                                full_metric_name = '%s.%s' % (str(parent_metric_namespace), str(data_col))
                                metric = filesafe_metricname(full_metric_name)
                                logger.info('uploaded_data_worker :: interpolated metric name to %s' % metric)
                            except:
                                logger.error(traceback.format_exc())
                                failure_reason = 'failed to interpolated metric name for %s, cannot continue' % data_col
                                logger.error('error :: uploaded_data_worker :: %s' % failure_reason)
                                data_df_successful = False
                                data_df_failures += 1
                                if upload_status:
                                    upload_status = new_upload_status(upload_status, data_col_key, failure_reason)
                                try:
                                    del data_df
                                except:
                                    pass
                                try:
                                    del timeseries
                                except:
                                    pass
                                continue

                        # Best effort to de-duplicate the data sent to Graphite
                        last_flux_timestamp = None
                        if data_df_successful and timeseries and metric:
                            cache_key = 'flux.last.%s' % metric
                            redis_last_metric_data = None
                            # @added 20200521 - Feature #3538: webapp - upload_data endpoint
                            #                   Feature #3550: flux.uploaded_data_worker
                            # Added the ability to ignore_submitted_timestamps and not
                            # check flux.last metric timestamp
                            if not ignore_submitted_timestamps:
                                try:
                                    redis_last_metric_data = self.redis_conn_decoded.get(cache_key)
                                except:
                                    logger.error(traceback.format_exc())
                                    logger.error('error :: uploaded_data_worker :: failed to determine last_flux_timestamp from Redis key %s' % cache_key)
                                    last_flux_timestamp = None
                            if redis_last_metric_data:
                                try:
                                    last_metric_data = literal_eval(redis_last_metric_data)
                                    last_flux_timestamp = int(last_metric_data[0])
                                except:
                                    logger.error(traceback.format_exc())
                                    logger.error('error :: uploaded_data_worker :: failed to determine last_flux_timestamp from Redis key %s' % cache_key)
                                    last_flux_timestamp = None
                        valid_timeseries = []
                        if last_flux_timestamp:
                            try:
                                logger.info('uploaded_data_worker :: determined last timestamp from Redis for %s as %s' % (
                                    metric, str(last_flux_timestamp)))
                                for timestamp, value in timeseries:
                                    if timestamp > last_flux_timestamp:
                                        valid_timeseries.append([timestamp, value])
                                valid_timeseries_length = len(valid_timeseries)
                                logger.info('uploaded_data_worker :: deduplicated timeseries based on last_flux_timestamp for %s which now has %s timestamps and values' % (
                                    data_col, str(valid_timeseries_length)))
                                if valid_timeseries:
                                    timeseries = valid_timeseries
                                else:
                                    newest_data_timestamp = str(timeseries[-1][0])
                                    logger.info('uploaded_data_worker :: none of the timestamps in %s data are older than %s, the newset being %s, nothing to submit continuing' % (
                                        data_col, str(last_flux_timestamp), newest_data_timestamp))
                                    if upload_status:
                                        upload_status = new_upload_status(upload_status, data_col_key, 'processed - no new timestamps, all already known')
                                    try:
                                        del data_df
                                    except:
                                        pass
                                    try:
                                        del timeseries
                                    except:
                                        pass
                                    continue
                            except:
                                logger.error(traceback.format_exc())
                                failure_reason = 'failed to determine if timestamps for %s are newer than the last_flux_timestamp, cannot continue' % data_col
                                logger.error('error :: uploaded_data_worker :: %s' % failure_reason)
                                data_df_successful = False
                                data_df_failures += 1
                                if upload_status:
                                    upload_status = new_upload_status(upload_status, data_col_key, 'failed to determine if timestamps for %s are newer than the last known timestamp, cannot continue')
                                try:
                                    del data_df
                                except:
                                    pass
                                try:
                                    del timeseries
                                except:
                                    pass
                                continue

                        valid_timeseries = []
                        if data_df_successful and timeseries and metric:
                            datapoints_with_no_value = 0
                            for timestamp, value in timeseries:
                                    try:
                                        if value is None:
                                            datapoints_with_no_value += 1
                                            continue
                                        float_value = float(value)
                                        valid_timeseries.append([timestamp, float_value])
                                    except:
                                        datapoints_with_no_value += 1
                                        continue
                            if datapoints_with_no_value > 0:
                                logger.info('uploaded_data_worker :: dropped %s timestamps from %s which have no value' % (
                                    str(datapoints_with_no_value), metric))
                            if valid_timeseries:
                                timeseries = valid_timeseries
                                try:
                                    del valid_timeseries
                                except:
                                    pass
                            else:
                                logger.info('uploaded_data_worker :: none of the timestamps have value data in %s data, nothing to submit continuing' % (
                                    data_col))
                                if upload_status:
                                    upload_status = new_upload_status(upload_status, data_col_key, 'failed - no timestamps have value data')
                                try:
                                    del data_df
                                except:
                                    pass
                                try:
                                    del timeseries
                                except:
                                    pass
                                continue
                        if data_df_successful and timeseries and metric:
                            try:
                                # Deal with lower frequency data
                                # Determine resolution from the last 30 data points
                                resolution_timestamps = []
                                metric_resolution_determined = False
                                for metric_datapoint in timeseries[-30:]:
                                    timestamp = int(metric_datapoint[0])
                                    resolution_timestamps.append(timestamp)
                                timestamp_resolutions = []
                                if resolution_timestamps:
                                    last_timestamp = None
                                    for timestamp in resolution_timestamps:
                                        if last_timestamp:
                                            resolution = timestamp - last_timestamp
                                            timestamp_resolutions.append(resolution)
                                            last_timestamp = timestamp
                                        else:
                                            last_timestamp = timestamp
                                    try:
                                        del resolution_timestamps
                                    except:
                                        pass
                                if timestamp_resolutions:
                                    try:
                                        timestamp_resolutions_count = Counter(timestamp_resolutions)
                                        ordered_timestamp_resolutions_count = timestamp_resolutions_count.most_common()
                                        metric_resolution = int(ordered_timestamp_resolutions_count[0][0])
                                        if metric_resolution > 0:
                                            metric_resolution_determined = True
                                    except:
                                        logger.error(traceback.format_exc())
                                        logger.error('error :: uploaded_data_worker :: failed to determine metric_resolution from timeseries')
                                    try:
                                        del timestamp_resolutions
                                    except:
                                        pass
                                # Resample
                                resample_at = None
                                if metric_resolution_determined and metric_resolution < 60:
                                    resample_at = '1Min'
                                if resample_at:
                                    try:
                                        high_res_df = pd.DataFrame(timeseries)
                                        high_res_df.columns = ['date', data_col]
                                        high_res_df = high_res_df.set_index('date')
                                        high_res_df.index = pd.to_datetime(high_res_df.index, unit='s')
                                        resample_at = '1Min'
                                        if resample_method == 'mean':
                                            resampled_df = high_res_df.resample(resample_at).mean()
                                        else:
                                            resampled_df = high_res_df.resample(resample_at).sum()
                                        logger.info('uploaded_data_worker :: resampled %s data by %s' % (
                                            data_col, resample_method))
                                        try:
                                            del high_res_df
                                        except:
                                            pass
                                        resampled_df.reset_index(level=0, inplace=True)
                                        resampled_df['date'] = resampled_df['date'].dt.tz_localize(pytz_tz)
                                        dates = resampled_df['date'].tolist()
                                        timestamps = []
                                        for d in dates:
                                            timestamps.append(int(d.strftime('%s')))
                                        resampled_df['pandas_utc_timestamp'] = timestamps
                                        resampled_df['timeseries'] = resampled_df.apply(lambda x: list([x['pandas_utc_timestamp'], x[data_col]]), axis=1)
                                        timeseries = resampled_df['timeseries'].values.tolist()
                                        try:
                                            del resampled_df
                                        except:
                                            pass
                                        resampled_timeseries_length = len(timeseries)
                                        logger.info('uploaded_data_worker :: created resampled timeseries for %s with %s timestamps and values' % (
                                            data_col, str(resampled_timeseries_length)))
                                    except:
                                        logger.error(traceback.format_exc())
                                        failure_reason = 'failed to create resampled timeseries from %s' % data_col
                                        logger.error('error :: uploaded_data_worker :: %s' % failure_reason)
                                        data_df_successful = False
                                        data_df_failures += 1
                                        if upload_status:
                                            upload_status = new_upload_status(upload_status, data_col_key, failure_reason)
                                        try:
                                            del data_df
                                        except:
                                            pass
                                        try:
                                            del timeseries
                                        except:
                                            pass
                                        continue
                            except:
                                logger.error(traceback.format_exc())
                                failure_reason = 'failed to resampled timeseries for %s' % data_col
                                logger.error('error :: uploaded_data_worker :: %s' % failure_reason)
                                data_df_successful = False
                                data_df_failures += 1
                                if upload_status:
                                    upload_status = new_upload_status(upload_status, data_col_key, failure_reason)
                                try:
                                    del data_df
                                except:
                                    pass
                                try:
                                    del timeseries
                                except:
                                    pass
                                continue
                        try:
                            del data_df
                        except:
                            pass
                        sent_to_graphite = 0
                        last_timestamp_sent = None
                        last_value_sent = None
                        if data_df_successful and timeseries and metric:
                            timeseries_length = len(timeseries)
                            logger.info('uploaded_data_worker :: after preprocessing there are %s data points to send to Graphite for %s' % (
                                str(timeseries_length), metric))

                            if LOCAL_DEBUG:
                                timeseries_debug_file = '%s/%s.%s.txt' % (settings.SKYLINE_TMP_DIR, str(upload_id), metric)
                                try:
                                    write_data_to_file(
                                        skyline_app, timeseries_debug_file, 'w',
                                        str(timeseries))
                                    logger.debug('debug :: added timeseries_debug_file :: %s' % (timeseries_debug_file))
                                    # @modified 20190522 - Task #3034: Reduce multiprocessing Manager list usage
                                    # Move to Redis set block below
                                    # self.sent_to_panorama.append(base_name)
                                except:
                                    logger.error('error :: failed to add timeseries_debug_file :: %s' % (timeseries_debug_file))
                                    logger.info(traceback.format_exc())

                            timestamp = None
                            value = None
                            start_populating = timer()
                            listOfMetricTuples = []
                            try:
                                for timestamp, value in timeseries:
                                    tuple_data = (metric, (int(timestamp), float(value)))
                                    if LOCAL_DEBUG or debug_enabled_in_info:
                                        logger.debug('debug :: uploaded_data_worker :: sending - %s' % str(tuple_data))
                                    listOfMetricTuples.append(tuple_data)
                                    sent_to_graphite += 1
                                    if value == 0.0:
                                        has_value = True
                                    if value == 0:
                                        has_value = True
                                    if value:
                                        has_value = True
                                    if has_value:
                                        last_timestamp_sent = int(timestamp)
                                        last_value_sent = float(value)
                            except:
                                logger.error(traceback.format_exc())
                                logger.error('error :: uploaded_data_worker :: failed to populate listOfMetricTuples for %s' % str(metric))
                            if listOfMetricTuples:
                                data_points_sent = 0
                                smallListOfMetricTuples = []
                                tuples_added = 0
                                # @added 20200617 - Feature #3550: flux.uploaded_data_worker
                                # Reduce the speed of submissions to Graphite
                                # if there are lots of data points
                                number_of_datapoints = len(listOfMetricTuples)

                                for data in listOfMetricTuples:
                                    smallListOfMetricTuples.append(data)
                                    tuples_added += 1
                                    if tuples_added >= 1000:
                                        if dryrun:
                                            pickle_data_sent = True
                                            logger.info('uploaded_data_worker :: DRYRUN :: faking sending data')
                                        else:
                                            pickle_data_sent = pickle_data_to_graphite(smallListOfMetricTuples)
                                            # @added 20200617 - Feature #3550: flux.uploaded_data_worker
                                            # Reduce the speed of submissions to Graphite
                                            # if there are lots of data points
                                            if number_of_datapoints > 4000:
                                                sleep(0.3)
                                        if pickle_data_sent:
                                            data_points_sent += tuples_added
                                            logger.info('uploaded_data_worker :: sent %s/%s of %s data points to Graphite via pickle for %s' % (
                                                str(tuples_added), str(data_points_sent),
                                                str(timeseries_length), metric))
                                            sent_to_graphite += len(smallListOfMetricTuples)
                                            smallListOfMetricTuples = []
                                            tuples_added = 0
                                        else:
                                            logger.error('error :: uploaded_data_worker :: failed to send %s data points to Graphite via pickle for %s' % (
                                                str(tuples_added), metric))
                                if smallListOfMetricTuples:
                                    tuples_to_send = len(smallListOfMetricTuples)
                                    if dryrun:
                                        pickle_data_sent = True
                                        logger.info('uploaded_data_worker :: DRYRUN :: faking sending data')
                                    else:
                                        pickle_data_sent = pickle_data_to_graphite(smallListOfMetricTuples)
                                    if pickle_data_sent:
                                        data_points_sent += tuples_to_send
                                        logger.info('uploaded_data_worker :: sent the last %s/%s of %s data points to Graphite via pickle for %s' % (
                                            str(tuples_to_send), str(data_points_sent),
                                            str(timeseries_length), metric))
                                    else:
                                        logger.error('error :: uploaded_data_worker :: failed to send the last %s data points to Graphite via pickle for %s' % (
                                            str(tuples_to_send), metric))
                            try:
                                del timeseries
                            except:
                                pass
                            try:
                                del listOfMetricTuples
                            except:
                                pass
                            try:
                                del smallListOfMetricTuples
                            except:
                                pass

                            logger.info('uploaded_data_worker :: sent %s data points to Graphite for %s' % (
                                str(sent_to_graphite), metric))

                            if last_timestamp_sent:
                                try:
                                    # Update Redis flux key
                                    cache_key = 'flux.last.%s' % metric
                                    metric_data = [int(last_timestamp_sent), float(last_value_sent)]
                                    if dryrun:
                                        logger.info('uploaded_data_worker :: DRYRUN :: faking updating %s with %s' % (
                                            cache_key, str(metric_data)))
                                    # @added 20200521 - Feature #3538: webapp - upload_data endpoint
                                    #                   Feature #3550: flux.uploaded_data_worker
                                    # Added the ability to ignore_submitted_timestamps and not
                                    # check flux.last metric timestamp
                                    elif ignore_submitted_timestamps:
                                        logger.info('uploaded_data_worker :: ignore_submitted_timestamps :: not updating %s with %s' % (
                                            cache_key, str(metric_data)))
                                        # @added 20200527 - Feature #3550: flux.uploaded_data_worker
                                        # If submitted timestamps are ignored
                                        # add the the Redis set for analyzer to
                                        # sorted and deduplicated the time
                                        # series data in Redis
                                        self.redis_conn.sadd('flux.sort_and_dedup.metrics', metric)
                                        logger.info('uploaded_data_worker :: added %s to flux.sort_and_dedup.metrics Redis set' % (
                                            metric))
                                    else:
                                        self.redis_conn.set(cache_key, str(metric_data))
                                        logger.info('uploaded_data_worker :: set the metric Redis key - %s - %s' % (
                                            cache_key, str(metric_data)))
                                except:
                                    logger.error(traceback.format_exc())
                                    logger.error('error :: uploaded_data_worker :: failed to set Redis key - %s - %s' % (
                                        cache_key, str(metric_data)))

                            processed_data_columns.append(data_col)
                            end_populating = timer()
                            seconds_to_run = end_populating - start_populating
                            logger.info('uploaded_data_worker :: %s populated to Graphite in %.6f seconds' % (
                                metric, seconds_to_run))
                        if upload_status:
                            new_status = 'processed - %s data points submitted' % str(sent_to_graphite)
                            upload_status = new_upload_status(upload_status, data_col_key, new_status)
                    if upload_status:
                        upload_status = new_upload_status(upload_status, processing_filename, 'complete')
                try:
                    if len(processed_data_columns) == len(data_columns):
                        data_files_successfully_processed.append(processing_filename)
                        new_status = 'completed - processed all %s data columns OK' % str(len(processed_data_columns))
                    else:
                        new_status = 'completed - with some errors processed %s of the %s data columns ' % (
                            str(len(processed_data_columns)), str(len(data_columns)))
                    upload_status = new_upload_status(upload_status, processing_filename, new_status)
                except:
                    logger.error(traceback.format_exc())
                    logger.error('error :: uploaded_data_worker :: failed to determine if processed')

            try:
                data_files_successfully_processed_count = len(data_files_successfully_processed)
                logger.info('uploaded_data_worker :: %s of the %s data files were successfully' % (
                    str(data_files_successfully_processed_count), str(data_files_uploaded)))
                logger.info('uploaded_data_worker :: processed upload - %s' % str(upload_dict))
            except:
                logger.error(traceback.format_exc())
                logger.error('error :: uploaded_data_worker :: failed to determine how many data files were processed')

            all_processed = True
            try:
                if data_files_successfully_processed_count != data_files_uploaded:
                    all_processed = False
            except:
                logger.error(traceback.format_exc())
                logger.error('error :: uploaded_data_worker :: failed to determine if all data files were processed')

            try:
                real_extracted_data_dir = '%s/%s/extracted' % (DATA_UPLOADS_PATH, upload_id)
                if all_processed:
                    try:
                        if os.path.exists(real_extracted_data_dir):
                            shutil.rmtree(real_extracted_data_dir)
                            logger.info('uploaded_data_worker :: removed extracted files directory - %s' % real_extracted_data_dir)
                    except:
                        logger.error('error :: uploaded_data_worker :: failed to rmtree extracted files directory - %s' % real_extracted_data_dir)
                    if upload_status:
                        upload_status = new_upload_status(upload_status, data_filename, 'complete')
                        upload_status = new_upload_status(upload_status, 'status', 'complete')
                else:
                    if upload_status:
                        upload_status = new_upload_status(upload_status, data_filename, 'complete - with caveats')
            except:
                logger.error(traceback.format_exc())
                logger.error('error :: uploaded_data_worker :: failed to determine if extracted dir needs to be removed')

            remove_upload_dir = True
            try:
                upload_data_dir = '%s/%s' % (DATA_UPLOADS_PATH, upload_id)
                if save_uploads and all_processed:
                    save_path = '%s/%s' % (save_uploads_path, upload_id)
                    if not os.path.exists(save_path):
                        mkdir_p(save_path)
                        logger.info('uploaded_data_worker :: created %s' % save_path)
                    logger.info('uploaded_data_worker :: saving uploaded files from %s' % upload_data_dir)
                    data_files = []
                    try:
                        glob_path = '%s/*.*' % upload_data_dir
                        data_files = glob.glob(glob_path)
                    except:
                        logger.error(traceback.format_exc())
                        logger.error('error :: uploaded_data_worker :: glob falied on %s' % upload_data_dir)
                        remove_upload_dir = False
                    for i_file in data_files:
                        try:
                            shutil.copy(i_file, save_path)
                            logger.info('uploaded_data_worker :: data copied to %s/%s' % (save_path, i_file))
                        except shutil.Error as e:
                            logger.error(traceback.format_exc())
                            logger.error('error :: uploaded_data_worker :: shutil error - upload data not copied to %s' % save_path)
                            remove_upload_dir = False
                        # Any error saying that the directory doesn't exist
                        except OSError as e:
                            logger.error(traceback.format_exc())
                            logger.error('error :: uploaded_data_worker :: copying upload data to save_path - %s' % (e))
                            remove_upload_dir = False
                    logger.info('uploaded_data_worker :: upload data copied to %s' % save_path)
            except:
                logger.error(traceback.format_exc())
                logger.error('error :: uploaded_data_worker :: failed to determine if data was saved')

            if remove_upload_dir:
                try:
                    if data_files_successfully_processed_count != data_files_uploaded:
                        logger.info('uploaded_data_worker :: due to their being failures to process some data files, the upload directory is not being removed')
                        remove_upload_dir = False
                        if upload_status:
                            new_status = 'some errors, processed %s of the %s data files successfully' % (
                                str(data_files_successfully_processed_count), str(data_files_uploaded))
                            upload_status = new_upload_status(upload_status, 'final status', new_status)
                except:
                    logger.error(traceback.format_exc())
                    logger.error('error :: uploaded_data_worker :: failed to determine if removed_upload_dir')

            if remove_upload_dir:
                try:
                    if os.path.exists(upload_data_dir):
                        shutil.rmtree(upload_data_dir)
                        logger.info('uploaded_data_worker :: removed upload directory - %s' % upload_data_dir)
                    if upload_status:
                        new_status = 'complete'
                        upload_status.append(['final_status', new_status])
                        upload_status = new_upload_status(upload_status, 'final status', new_status)
                except:
                    logger.error('error :: uploaded_data_worker :: failed to rmtree upload directory - %s' % upload_data_dir)

            try:
                item_removed = remove_redis_set_item(str(upload_dict))
                if item_removed:
                    logger.info('uploaded_data_worker :: removed failed upload from the flux.uploaded_data Redis set')
                else:
                    logger.error('error :: uploaded_data_worker :: failed to remove item from the Redis flux.uploaded_data set - %s' % str(upload_dict))
            except:
                logger.error(traceback.format_exc())
                logger.error('error :: uploaded_data_worker :: failed to remove_redis_set_item')

            try:
                end_processing = timer()
                seconds_to_run = end_processing - start_processing
                logger.info('uploaded_data_worker :: processed upload for %s in %.6f seconds' % (
                    parent_metric_namespace, seconds_to_run))
                if upload_status:
                    time_to_run = '%.6f seconds' % seconds_to_run
                    upload_status.append(['processing_time', time_to_run])
                    upload_status = new_upload_status(upload_status, 'processing_time', time_to_run)
                    completed = int(time())
                    completed_at = datetime.datetime.fromtimestamp(completed)
                    completed_at_date = completed_at.strftime('%Y-%m-%d %H:%M:%S')
                    upload_status.append(['processing completed at', completed_at_date])
                    upload_status = new_upload_status(upload_status, 'processing completed at', completed_at_date)
                    upload_status = new_upload_status(upload_status, 'status', 'complete')
                    try:
                        del upload_status
                    except:
                        pass
            except:
                logger.error(traceback.format_exc())
                logger.error('error :: uploaded_data_worker :: failed to determine time_to_run')
            try:
                del start_processing
            except:
                pass
            try:
                del end_processing
            except:
                pass
            try:
                del df
            except:
                pass
