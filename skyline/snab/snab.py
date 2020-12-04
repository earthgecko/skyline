from __future__ import division
import logging
try:
    from Queue import Empty
except:
    from queue import Empty
from time import time, sleep
from threading import Thread
from collections import defaultdict, Counter
from multiprocessing import Process, Queue
import os
from os import kill, getpid
import traceback
import re
from sys import version_info
import os.path
from ast import literal_eval
import datetime as dt
from time import gmtime, strftime

import settings
from skyline_functions import (
    mkdir_p, get_redis_conn, get_redis_conn_decoded, send_graphite_metric)
from slack_functions import slack_post_message
try:
    from custom_algorithms import run_custom_algorithm_on_timeseries
except:
    run_custom_algorithm_on_timeseries = None

try:
    from settings import DEBUG_CUSTOM_ALGORITHMS
except:
    DEBUG_CUSTOM_ALGORITHMS = False

skyline_app = 'snab'
skyline_app_logger = '%sLog' % skyline_app
logger = logging.getLogger(skyline_app_logger)
skyline_app_logfile = '%s/%s.log' % (settings.LOG_PATH, skyline_app)
skyline_app_loglock = '%s.lock' % skyline_app_logfile
skyline_app_logwait = '%s.wait' % skyline_app_logfile

python_version = int(version_info[0])
this_host = str(os.uname()[1])
snab_work_redis_set = 'snab.work'
snab_work_done_redis_set = 'snab.work.done'

try:
    SERVER_METRIC_PATH = '.%s' % settings.SERVER_METRICS_NAME
    if SERVER_METRIC_PATH == '.':
        SERVER_METRIC_PATH = ''
except:
    SERVER_METRIC_PATH = ''

try:
    KNOWN_NEGATIVE_METRICS = list(settings.KNOWN_NEGATIVE_METRICS)
except:
    KNOWN_NEGATIVE_METRICS = []

try:
    CUSTOM_ALGORITHMS = settings.CUSTOM_ALGORITHMS
except:
    CUSTOM_ALGORITHMS = None
try:
    DEBUG_CUSTOM_ALGORITHMS = settings.DEBUG_CUSTOM_ALGORITHMS
except:
    DEBUG_CUSTOM_ALGORITHMS = False

try:
    SNAB_DATA_DIR = settings.SNAB_DATA_DIR
except:
    SNAB_DATA_DIR = '/opt/skyline/SNAB'
try:
    SNAB_anomalyScore = settings.SNAB_anomalyScore
except:
    SNAB_anomalyScore = {}

skyline_app_graphite_namespace = 'skyline.%s%s' % (skyline_app, SERVER_METRIC_PATH)

LOCAL_DEBUG = False


class SNAB(Thread):
    """
    The SNAB class which controls the snab thread and spawned
    processes.

    """

    def __init__(self, parent_pid):
        """
        Initialize the SNAB

        Create the :obj:`self.exceptions_q` queue
        Create the :obj:`self.anomaly_breakdown_q` queue

        """
        super(SNAB, self).__init__()
        self.redis_conn = get_redis_conn(skyline_app)
        self.redis_conn_decoded = get_redis_conn_decoded(skyline_app)
        self.daemon = True
        self.parent_pid = parent_pid
        self.current_pid = getpid()
        self.exceptions_q = Queue()
        self.anomaly_breakdown_q = Queue()

    def check_if_parent_is_alive(self):
        """
        Self explanatory
        """
        try:
            kill(self.current_pid, 0)
            kill(self.parent_pid, 0)
        except:
            # @added 20201203 - Bug #3856: Handle boring sparsely populated metrics in derivative_metrics
            # Log warning
            logger.warn('warning :: parent or current process dead')
            exit(0)

    def spin_snab_process(self, i, check_details):
        """
        Assign a metric and algorithm for a process to analyze.

        :param i: python process id
        :param run_timestamp: the epoch timestamp at which this process was called
        :param metric_name: the FULL_NAMESPACE metric name as keyed in Redis
        :param last_analyzed_timestamp: the last analysed timestamp as recorded
            in the Redis key last_timestamp.basename key.

        It is possible to want to run a metric through SNAB to check an
        algorithm in the normal anomaly detection workflow, in the algorithm
        testing context and concurrently.  An algorithm could be added as part
        of the main detection process and another algorithm could be simply
        being tested.  Although handling them separately is preferable as any
        testing algorithms will not slow down the real time processing.  However
        having to load and preprocess that data for every algorithm is also
        inefficient, so there must be some trade offs.  Therefore if a metric
        has multiple SNAB configurations, testing and realtime a check item
        must be added for each context.  SNAB propritises realtime over
        testing.

        check_details is a dict with the following structure:
        check_details = {
            'metric': '<base_name>'|str,
            'timestamp': anomaly_timestamp|int,
            'value': datapoint|float,
            'full_duration': full_duration|int,
            'anomaly_data': '<absolute_path/filename>'|str,
            'source': '<skyline_app>'|str,
            'added_at': added_at_timestamp|int,
            'original_added_at': original_added_at|int,
            'context': '<testing|realtime>'|str,
            'algorithm': 'algorithm_1'|str,
            'algorithm_source': '<absolute_path/filename>'|str',
            'algorithm_parameters': {}|dict,
            'max_execution_time': max_seconds|ifloat,
            'debug_logging': False|boolean,
            'analysed': False|boolean,
            'processed': False|boolean,
            'anomalous': None|boolean,
            'anomalyScore': 0.0|float,
            'alert_slack_channel': '<channel_name>|None'|str or boolean,
            'snab_only': False|boolean,
        }

        # @added 20201001 - food for thought
        Walk the dogs...
        Back to making the black hole.
        Does information have weight?
        At what point does the quantity of digital information in a space become to dense or too much?
        How much does a byte weigh?
        What about an EM wave/s?
        Could the pending singularity event not be an AI event, but black hole event?
        Could digital information or signals ever "supernova"?
        What is the equivalent to Hubbles Law for digital information and signals?
        Will all our digital information eventually cross an event horizon and vanish from our perception?

        :return: anomalous
        :rtype: boolean
        """

        def update_check_details(original_check_details, updated_check_details):
            """
            Manage the check_detail a dict in the Redis snab.work set.

            :param original_check_details: the original check_details dict
            :param updated_check_details: the updated check_details dict or the
                string 'remove' to remove the original_check_details

            """
            try:
                self.redis_conn.srem(snab_work_redis_set, str(original_check_details))
                logger.info('removed item from Redis set %s - %s' % (
                    snab_work_redis_set, str(check_details)))
            except:
                logger.error(traceback.format_exc())
                logger.error('error :: failed to remove item from Redis set %s' % (
                    snab_work_redis_set))
            if updated_check_details != 'remove':
                try:
                    self.redis_conn.sadd(snab_work_redis_set, str(updated_check_details))
                    logger.info('added updated check_details item to Redis set %s - %s' % (
                        snab_work_redis_set, str(updated_check_details)))
                except:
                    logger.error(traceback.format_exc())
                    logger.error('error :: failed to remove item from Redis set %s' % (
                        snab_work_redis_set))

        anomalous = None
        anomalyScore = 0

        spin_start = time()
        spin_snab_process_pid = os.getpid()
        logger.info('spin_snab_process - %s, processing check - %s' % (
            str(spin_snab_process_pid), str(check_details)))

        try:
            metric_name = str(check_details['metric'])
            metric_timestamp = int(check_details['timestamp'])
            original_anomaly_timestamp = int(check_details['original_anomaly_timestamp'])
            metric_value = float(check_details['value'])
            anomaly_data = str(check_details['anomaly_data'])
            source = str(check_details['source'])
            algorithm = str(check_details['algorithm'])
            algorithm_source = str(check_details['algorithm_source'])
            algorithm_parameters = check_details['algorithm_parameters'].copy()
            max_execution_time = float(check_details['max_execution_time'])
            algorithm_debug_logging = check_details['debug_logging']
            # @added 20200916 - Branch #3068: SNAB
            #                   Task #3748: POC SNAB
            snab_only = check_details['snab_only']
        except:
            logger.error(traceback.format_exc())
            logger.error('error - spin_snab_process - %s, failed to parse check_details dict' % (
                str(spin_snab_process_pid)))
            return anomalous

        original_check_details = check_details.copy()
        updated_check_details = check_details.copy()
        updated_check_details['processed'] = int(time())
        update_check_details(original_check_details, updated_check_details)
        original_updated_check_details = updated_check_details.copy()

        if algorithm != 'testing':
            if not os.path.isfile(anomaly_data):
                logger.error('error - spin_snab_process - %s, failed to find anomaly_data file - %s' % (
                    str(spin_snab_process_pid), str(anomaly_data)))
                update_check_details(original_updated_check_details, 'remove')
                return anomalous

        snab_do_not_recheck_key = 'snab.do.not.recheck.%s' % metric_name
        snab_do_not_recheck = False
        try:
            snab_do_not_recheck = int(self.redis_conn_decoded.get(snab_do_not_recheck_key))
        except:
            pass
        if snab_do_not_recheck:
            logger.info('%s Redis keys exists, processing check for %s from %s with %s, removing check' % (
                snab_do_not_recheck_key, metric_name, source, algorithm))
            update_check_details(original_updated_check_details, 'remove')
            return anomalous

        logger.info('spin_snab_process - %s, processing check for %s from %s with %s' % (
            str(spin_snab_process_pid), metric_name, source, algorithm))

        # Make process-specific dicts
        anomaly_breakdown = defaultdict(int)

        if metric_name.startswith(settings.FULL_NAMESPACE):
            base_name = metric_name.replace(settings.FULL_NAMESPACE, '', 1)
        else:
            base_name = metric_name

        if anomaly_data.endswith('.json'):
            with open((anomaly_data), 'r') as f:
                raw_timeseries = f.read()
            timeseries_array_str = str(raw_timeseries).replace('(', '[').replace(')', ']')
            timeseries = literal_eval(timeseries_array_str)
        if anomaly_data.endswith('.csv'):
            logger.warn('warning :: spin_snab_process - anomaly_data file is csv format which is not currently handled, skipping %s' % (
                str(anomaly_data)))
            update_check_details(original_updated_check_details, 'remove')
            return anomalous
        if algorithm == 'testing':
            timeseries = [[int(time()), 1.0]]

        if not timeseries:
            logger.error('error - spin_snab_process - no time series data was loaded from anomaly_data file - %s' % (
                str(anomaly_data)))
            update_check_details(original_updated_check_details, 'remove')
            return anomalous

        # @added 20201001 - Branch #3068: SNAB
        #                   Task #3748: POC SNAB
        # Add timings
        analysis_start_time = time()

        if timeseries:
            try:
                if check_details:
                    algorithm_parameters['check_details'] = check_details
                    algorithm_parameters['debug_logging'] = algorithm_debug_logging
                custom_algorithm_dict = {
                    'namespaces': [metric_name],
                    'algorithm_source': algorithm_source,
                    'algorithm_parameters': algorithm_parameters,
                    'max_execution_time': max_execution_time,
                    'consensus': 1,
                    'algorithms_allowed_in_consensus': [algorithm],
                    'run_3sigma_algorithms': False,
                    'use_with': ['snab'],
                    'debug_logging': algorithm_debug_logging,
                }
                anomalous, anomalyScore = run_custom_algorithm_on_timeseries(skyline_app, getpid(), base_name, timeseries, algorithm, custom_algorithm_dict, DEBUG_CUSTOM_ALGORITHMS)
                if anomalous:
                    logger.info('anomaly detected :: with %s on %s with %s at %s' % (
                        algorithm, metric_name, str(timeseries[-1][1]),
                        metric_timestamp))
                else:
                    logger.info('not anomalous :: with %s on %s with %s at %s' % (
                        algorithm, metric_name, str(timeseries[-1][1]),
                        metric_timestamp))
            except:
                logger.error(traceback.format_exc())
                logger.error('error :: failed to run %s on %s' % (
                    algorithm, metric_name))
                anomalous = None
                anomalyScore = None

        # @added 20201001 - Branch #3068: SNAB
        #                   Task #3748: POC SNAB
        # Add timings
        analysis_run_time = time() - analysis_start_time
        logger.info('%s analysis completed in %.2f seconds' % (
            algorithm, analysis_run_time))
        redis_key = 'snab.analysis_run_time.%s.%s.%s' % (algorithm, base_name, str(metric_timestamp))
        try:
            self.redis_conn.setex(redis_key, 120, str(analysis_run_time))
        except:
            logger.error(traceback.format_exc())
            logger.error('error :: failed to add snab analysis_run_time Redis key - %s' % (
                redis_key))

        updated_check_details['analysed'] = int(time())
        updated_check_details['anomalous'] = anomalous
        updated_check_details['anomalyScore'] = anomalyScore

        # Add to the snab.work.done Redis set
        try:
            self.redis_conn.sadd(snab_work_done_redis_set, str(updated_check_details))
            logger.info('added updated check_details item to Redis set %s - %s' % (
                snab_work_done_redis_set, str(updated_check_details)))
        except:
            logger.error(traceback.format_exc())
            logger.error('error :: failed to remove item from Redis set %s' % (
                snab_work_redis_set))

        subsequent_seconds = 0
        try:
            added_at = original_check_details['added_at']
            original_added_at = original_check_details['original_added_at']
            if int(original_added_at) == 0:
                subsequent_seconds = 1
            else:
                subsequent_seconds = added_at - original_added_at
        except:
            pass

        panorama_added_at = int(time())
        slack_post = None
        slack_thread_ts = 0

        try:
            if original_check_details['alert_slack_channel']:
                send_to_slack = True
                if snab_only and not anomalous:
                    send_to_slack = False
                if send_to_slack:

                    # @added 20200928 - Task #3748: POC SNAB
                    #                   Branch #3068: SNAB
                    # When a slack alert goes out a Panorama snab entry must be
                    # created
                    algorithm_group = None
                    if algorithm == 'skyline_matrixprofile':
                        algorithm_group = 'matrixprofile'

                    # @added 20201001 - Branch #3068: SNAB
                    #                      Task #3748: POC SNAB
                    # Added analysis_run_time
                    analysis_run_time = 0
                    redis_key = 'snab.analysis_run_time.%s.%s.%s' % (algorithm, base_name, str(metric_timestamp))
                    try:
                        analysis_run_time = float(self.redis_conn_decoded.get(redis_key))
                    except:
                        logger.error(traceback.format_exc())
                        logger.error('error :: failed to determine analysis_run_time from Redis key - %s' % (
                            redis_key))

                    if snab_only:
                        use_timestamp = original_anomaly_timestamp
                    else:
                        use_timestamp = metric_timestamp

                    snab_panorama_details = {
                        'metric': metric_name,
                        'timestamp': use_timestamp,
                        'anomalyScore': anomalyScore,
                        'analysis_run_time': analysis_run_time,
                        'source': skyline_app,
                        'algorithm_group': algorithm_group,
                        'algorithm': algorithm,
                        'added_at': panorama_added_at,
                    }
                    self.redis_conn.sadd('snab.panorama', str(snab_panorama_details))
                    logger.info('added snab.panorama item - %s' % (
                        str(snab_panorama_details)))

                    # @added 20200929 - Task #3748: POC SNAB
                    #                   Branch #3068: SNAB
                    # Determine the anomaly_id so that the tP, fP, tN, fN links
                    # can be applied to the slack message.
                    snab_slack_comment = None
                    anomaly_id = None
                    anomaly_id_redis_key = 'panorama.anomaly_id.%s.%s' % (
                        str(original_anomaly_timestamp), metric_name)
                    try_get_anomaly_id_redis_key_count = 0
                    while try_get_anomaly_id_redis_key_count < 30:
                        try_get_anomaly_id_redis_key_count += 1
                        try:
                            anomaly_id = int(self.redis_conn_decoded.get(anomaly_id_redis_key))
                            break
                        except:
                            sleep(1)
                    if not anomaly_id:
                        logger.error('error :: failed to determine anomaly_id from Redis key - %s' % anomaly_id_redis_key)
                        snab_slack_comment = '\nThe anomaly id was not determined to generate snab results links'
                    snab_id = None
                    if anomaly_id:
                        snab_id_redis_key = 'snab.id.%s.%s.%s.%s' % (algorithm_group, str(use_timestamp), base_name, panorama_added_at)
                        try_get_snab_id_redis_key_count = 0
                        while try_get_snab_id_redis_key_count < 30:
                            try_get_snab_id_redis_key_count += 1
                            try:
                                snab_id = int(self.redis_conn_decoded.get(snab_id_redis_key))
                                break
                            except:
                                sleep(1)
                        if not snab_id:
                            logger.error('error :: failed to determine snab_id from Redis key - %s' % snab_id_redis_key)
                            snab_slack_comment = '\nThe anomaly id was not determined to generate snab results links'

                    if anomaly_id and snab_id:
                        snab_link = '%s/api?snab=true&snab_id=%s&anomaly_id=%s' % (
                            settings.SKYLINE_URL, str(snab_id), str(anomaly_id))
                        tP_link = '%s&result=tP' % snab_link
                        fP_link = '%s&result=fP' % snab_link
                        tN_link = '%s&result=tN' % snab_link
                        fN_link = '%s&result=fN' % snab_link
                        unsure_link = '%s&result=unsure' % snab_link
                        null_link = '%s&result=NULL' % snab_link
                        snab_slack_comment = '\n<' + tP_link + '|tP - true positive>   ::  <' + fP_link + '|fP - false positive>  ::  <' + null_link + '|NULL - reset to NULL>\n<' + tN_link + '|tN - true negative>  ::  <' + fN_link + '|fN - false negative>  :: <' + unsure_link + '|unsure>'

                    anomaly_status = 'NOT ANOMALOUS'
                    if anomalous:
                        anomaly_status = 'ANOMALOUS'
                        if snab_only:
                            if algorithm == 'skyline_matrixprofile':
                                subsequent_minutes = 0
                                subsequent_units = 'MINUTES'
                                try:
                                    subsequent_minutes = int(subsequent_seconds / 60)
                                except:
                                    subsequent_minutes = 0
                                if subsequent_seconds < 60:
                                    subsequent_minutes = int(subsequent_seconds)
                                    subsequent_units = 'SECONDS'
                                anomaly_status = 'ANOMALOUS - %s %s AFTER three-sigma anomaly' % (str(subsequent_minutes), subsequent_units)
                                # Remove recheck key
                                snab_recheck_key = 'snab.recheck.%s' % base_name
                                try:
                                    self.redis_conn.delete(snab_recheck_key)
                                except:
                                    pass
                                snab_recheck_original_anomaly_timestamp_key = 'snab.recheck.anomaly_timestamp.%s' % base_name
                                try:
                                    self.redis_conn.delete(snab_recheck_original_anomaly_timestamp_key)
                                except:
                                    pass
                                snab_do_not_recheck_key = 'snab.do.not.recheck.%s' % base_name
                                try:
                                    self.redis_conn.setex(snab_do_not_recheck_key, 120, metric_timestamp)
                                except:
                                    pass

                    timezone = strftime("%Z", gmtime())
                    human_anomaly_time = dt.datetime.fromtimestamp(int(timeseries[-1][0])).strftime('%c')
                    slack_time_string = '%s %s' % (human_anomaly_time, timezone)

                    snab_slack_verbose = False
                    if snab_slack_verbose:
                        slack_message = '*Skyline SNAB - %s* check for *%s* with *%s* on %s\n*%s* at %s, anomalyScore: %s\n%s' % (
                            anomaly_status, source, algorithm, metric_name, str(metric_value),
                            slack_time_string, str(anomalyScore), str(updated_check_details))
                    else:
                        slack_message = '*Skyline SNAB - %s* check for *%s* with *%s* on %s\n*%s* at %s, anomalyScore: %s' % (
                            anomaly_status, source, algorithm, metric_name, str(metric_value),
                            slack_time_string, str(anomalyScore))

                    # @added 20200929 - Task #3748: POC SNAB
                    #                   Branch #3068: SNAB
                    # Determine the anomaly_id so that the tP, fP, tN, fN links
                    # can be applied to the slack message.
                    if snab_slack_comment:
                        slack_message = slack_message + snab_slack_comment
                    slack_post = slack_post_message(skyline_app, original_check_details['alert_slack_channel'], None, slack_message)
                    logger.info('posted results to slack - %s' % slack_message)
                    if slack_post:
                        try:
                            slack_thread_ts = slack_post['ts']
                        except:
                            logger.error(traceback.format_exc())
                            logger.error('error :: failed to determine slack_ts[\'ts\'] from slack_post - %s' % (
                                str(slack_post)))

                    # @added 20201002 - Task #3748: POC SNAB
                    #                   Branch #3068: SNAB
                    # Add a Redis key for snab to also update the original anomaly
                    # id slack_thread_ts
                    anomaly_slack_thread_ts = None
                    if anomaly_id:
                        anomaly_id_slack_thread_ts_redis_key = 'panorama.anomaly.id.%s.slack_thread_ts' % (str(anomaly_id))
                        try:
                            anomaly_slack_thread_ts = self.redis_conn_decoded.get(anomaly_id_slack_thread_ts_redis_key)
                            logger.info('determined original anomaly id %s slack_thread_ts - %s' % (
                                str(anomaly_id), str(anomaly_slack_thread_ts)))
                        except:
                            logger.error(traceback.format_exc())
                            logger.error('error :: failed to determine anomaly_slack_thread_ts from Redis key - %s' % (
                                anomaly_id_slack_thread_ts_redis_key))
                            anomaly_slack_thread_ts = None
                    # Disable for now
                    anomaly_slack_thread_ts = None
                    if anomaly_slack_thread_ts:
                        anomaly_slack_post = None
                        try:
                            anomaly_slack_post = slack_post_message(skyline_app, original_check_details['alert_slack_channel'], str(anomaly_slack_thread_ts), slack_message)
                            logger.info('posted results to slack anomaly thread as well')
                        except:
                            logger.error(traceback.format_exc())
                            logger.error('error :: failed to determine post update to slack anomaly thread - %s' % (
                                str(anomaly_slack_thread_ts)))
                        if anomaly_slack_post:
                            logger.info('posted results to slack anomaly thread with result - %s' % str(anomaly_slack_post))
        except:
            logger.error(traceback.format_exc())
            logger.error('error :: failed to run %s on %s' % (
                algorithm, metric_name))

        if slack_thread_ts:
            cache_key = 'panorama.snab.slack_thread_ts.%s.%s' % (str(metric_timestamp), str(snab_id))
            # When there are multiple channels declared Skyline only wants to
            # update the panorama.slack_thread_ts Redis key once for the
            # primary Skyline channel, so if the cache key exists do not update
            cache_key_value = str(slack_thread_ts)
            try:
                self.redis_conn.setex(
                    cache_key, 86400,
                    str(cache_key_value))
                logger.info(
                    'added Panorama panorama.snab.slack_thread_ts Redis key - %s - %s' %
                    (cache_key, str(cache_key_value)))
            except:
                logger.error(traceback.format_exc())
                logger.error(
                    'error :: failed to add Panorama panorama.snab.slack_thread_ts Redis key - %s - %s' %
                    (cache_key, str(cache_key_value)))

        if settings.SNAB_DATA_DIR in anomaly_data:
            if os.path.isfile(anomaly_data):
                os.remove(anomaly_data)
                logger.info('removed check file - %s' % (anomaly_data))
            else:
                logger.info('could not remove check file - %s' % (anomaly_data))

        logger.info('removing check_details from %s Redis set' % snab_work_redis_set)
        update_check_details(updated_check_details, 'remove')
        update_check_details(original_check_details, 'remove')
        update_check_details(original_updated_check_details, 'remove')

        # @added 20200916
        # Due to the manner in which matrixprofile identifies discords a spike
        # or change that 3-sigma detects may only be flagged as a discord a few
        # data points later when the new data changes the profile.  Therefore
        # a recheck Redis key is added for the metric with a TTL of 300 which is
        # checked by Analyzer and if found Analyzer adds the required data to
        # the pipeline to check on each run while the key exists.
        add_recheck_key = False
        if not anomalous:
            if not snab_only:
                if algorithm == 'skyline_matrixprofile':
                    add_recheck_key = True
        if add_recheck_key:
            snab_recheck_key = 'snab.recheck.%s' % metric_name
            snab_recheck_key_exists = False
            try:
                snab_recheck_key_exists = self.redis_conn_decoded.get(snab_recheck_key)
            except:
                logger.error(traceback.format_exc())
                logger.error('error :: failed to get Redis key %s' % (
                    snab_recheck_key))
            if snab_recheck_key_exists:
                logger.info('snab.recheck Redis key exists from %s not adding %s' % (
                    str(snab_recheck_key_exists), snab_recheck_key))
            else:
                try:
                    self.redis_conn.setex(snab_recheck_key, 310, original_added_at)
                    logger.info('added snab.recheck Redis key %s' % snab_recheck_key)
                except:
                    logger.error(traceback.format_exc())
                    logger.error('error :: failed to add snab.recheck Redis key %s' % snab_recheck_key)
                snab_recheck_original_anomaly_timestamp_key = 'snab.recheck.anomaly_timestamp.%s' % metric_name
                try:
                    self.redis_conn.setex(snab_recheck_original_anomaly_timestamp_key, 310, original_anomaly_timestamp)
                    logger.info('added snab.recheck.anomaly_timestamp Redis key %s with original_anomaly_timestamp of %s' % (
                        snab_recheck_original_anomaly_timestamp_key, str(original_anomaly_timestamp)))
                except:
                    logger.error(traceback.format_exc())
                    logger.error('error :: failed to add snab.recheck Redis key %s' % snab_recheck_key)

            metric_timestamp = int(check_details['timestamp'])
            original_anomaly_timestamp = int(check_details['original_anomaly_timestamp'])
            metric_value = float(check_details['value'])
            anomaly_data = str(check_details['anomaly_data'])
            source = str(check_details['source'])
            algorithm = str(check_details['algorithm'])
            algorithm_source = str(check_details['algorithm_source'])
            algorithm_parameters = check_details['algorithm_parameters'].copy()
            max_execution_time = float(check_details['max_execution_time'])
            algorithm_debug_logging = check_details['debug_logging']
            # @added 20200916 - Branch #3068: SNAB
            #                   Task #3748: POC SNAB
            snab_only = check_details['snab_only']

        spin_end = time() - spin_start
        logger.info('spin_snab_process took %.2f seconds' % spin_end)

        return anomalous

    def run(self):
        """
        - Called when the process intializes.

        - Determine if Redis is up and discover checks to run.

        - Divide and assign each process a metric check to analyse and add
          results to source Redis set.

        - Wait for the processes to finish.

        """

        # Log management to prevent overwriting
        # Allow the bin/<skyline_app>.d to manage the log
        if os.path.isfile(skyline_app_logwait):
            try:
                os.remove(skyline_app_logwait)
            except OSError:
                logger.error('error - failed to remove %s, continuing' % skyline_app_logwait)
                pass

        now = time()
        log_wait_for = now + 5
        while now < log_wait_for:
            if os.path.isfile(skyline_app_loglock):
                sleep(.1)
                now = time()
            else:
                now = log_wait_for + 1

        logger.info('starting %s run' % skyline_app)
        if os.path.isfile(skyline_app_loglock):
            logger.error('error - bin/%s.d log management seems to have failed, continuing' % skyline_app)
            try:
                os.remove(skyline_app_loglock)
                logger.info('log lock file removed')
            except OSError:
                logger.error('error - failed to remove %s, continuing' % skyline_app_loglock)
                pass
        else:
            logger.info('bin/%s.d log management done' % skyline_app)

        try:
            SERVER_METRIC_PATH = '.%s' % settings.SERVER_METRICS_NAME
            if SERVER_METRIC_PATH == '.':
                SERVER_METRIC_PATH = ''
            logger.info('SERVER_METRIC_PATH is set from settings.py to %s' % str(SERVER_METRIC_PATH))
        except:
            SERVER_METRIC_PATH = ''
            logger.info('warning :: SERVER_METRIC_PATH is not declared in settings.py, defaults to \'\'')
        logger.info('skyline_app_graphite_namespace is set to %s' % str(skyline_app_graphite_namespace))

        if not os.path.exists(settings.SKYLINE_TMP_DIR):
            try:
                mkdir_p(settings.SKYLINE_TMP_DIR)
            except:
                logger.error(traceback.format_exc())
                logger.error('error :: failed to create %s' % settings.SKYLINE_TMP_DIR)

        last_sent_to_graphite = int(time())
        snab_checks_done = 0
        algorithm_run_times = []
        analyzer_anomalies = 0
        analyzer_falied_checks = 0
        analyzer_realtime_checks = 0
        analyzer_testing_checks = 0
        mirage_anomalies = 0
        mirage_falied_checks = 0
        mirage_realtime_checks = 0
        mirage_testing_checks = 0

        while 1:
            now = time()
            # Make sure Redis is up
            try:
                self.redis_conn.ping()
            except:
                logger.error(traceback.format_exc())
                logger.error('error :: cannot connect to redis at socket path %s' % settings.REDIS_SOCKET_PATH)
                sleep(10)
                try:
                    self.redis_conn = get_redis_conn(skyline_app)
                    logger.info('connected via get_redis_conn')
                except:
                    logger.error(traceback.format_exc())
                    logger.error('error :: not connected via get_redis_conn')
                continue
            try:
                self.redis_conn_decoded.ping()
            except:
                logger.error(traceback.format_exc())
                logger.error('error :: not connected via get_redis_conn_decoded')
                sleep(10)
                try:
                    self.redis_conn_decoded = get_redis_conn_decoded(skyline_app)
                    logger.info('onnected via get_redis_conn_decoded')
                except:
                    logger.error(traceback.format_exc())
                    logger.error('error :: cannot connect to get_redis_conn_decoded')
                continue

            """
            Determine if any metric has been added to process
            """
            while True:

                # Report app up
                try:
                    self.redis_conn.setex(skyline_app, 120, int(now))
                except:
                    logger.error(traceback.format_exc())
                    logger.error('error :: could not update the Redis %s key' % skyline_app)

                current_timestamp = int(time())

                # Determine checks to process and algorithms that are to be run
                snab_work = None
                sorted_snab_work = []
                unsorted_snab_work = []
                try:
                    snab_work = self.redis_conn_decoded.smembers(snab_work_redis_set)
                except Exception as e:
                    logger.error('error :: could not query Redis for set %s - %s' % (snab_work_redis_set, e))
                if snab_work:
                    total_snab_work_item_count = len(snab_work)
                    # If multiple work items exist sort them by oldest timestamp
                    # and process the item with the oldest timestamp first
                    realtime_check_count = 0
                    testing_check_count = 0
                    for index, snab_item in enumerate(snab_work):
                        try:
                            check_details = literal_eval(snab_item)
                            metric_name = check_details['metric']
                            metric_timestamp = check_details['timestamp']
                            source = check_details['source']
                            context = check_details['context']
                            if context == 'realtime':
                                realtime_check_count += 1
                            else:
                                testing_check_count += 1
                            algorithm = check_details['algorithm']
                            processed = check_details['processed']
                            if not processed:
                                unsorted_snab_work.append(check_details)
                        except:
                            logger.error(traceback.format_exc())
                            logger.error('error :: could not determine check_details from snab.work entry')

                    for index, snab_item in enumerate(snab_work):
                        try:
                            check_details = literal_eval(snab_item)
                            processed = check_details['processed']
                            metric_timestamp = check_details['timestamp']
                            use_timestamp = 0
                            if processed:
                                try:
                                    use_timestamp = int(processed)
                                except:
                                    pass
                            if not use_timestamp:
                                use_timestamp = int(metric_timestamp)
                            if use_timestamp:
                                age = current_timestamp - int(use_timestamp)
                                if age > 900:
                                    self.redis_conn.srem(snab_work_redis_set, str(snab_item))
                                    logger.info('removed snab check older than 900 seconds - %s' % (
                                        str(check_details)))
                        except:
                            logger.error(traceback.format_exc())
                            logger.error('error :: could not determine check_details from snab.work entry')

                    if unsorted_snab_work:
                        sorted_snab_work = sorted(unsorted_snab_work, key=lambda i: i['timestamp'])
                    sorted_snab_work_count = len(sorted_snab_work)
                    if sorted_snab_work_count > 0:
                        logger.info('there are %s metric checks to process in the %s Redis set' % (
                            str(sorted_snab_work_count), snab_work_redis_set))
                        break
                    else:
                        logger.info('there are no unprocessed items to process and %s processed items in %s' % (
                            str(total_snab_work_item_count), snab_work_redis_set))
                        if int(time()) >= (last_sent_to_graphite + 60):
                            break
                        sleep(5)
                else:
                    logger.info('there are no %s items to process' % snab_work_redis_set)
                    if int(time()) >= (last_sent_to_graphite + 60):
                        break
                    sleep(5)

            # Determine how many processes SNAB is to run in order to determine
            # what realtime and/or testing checks to run
            snab_work_item_count = len(sorted_snab_work)
            if snab_work_item_count > 1:
                try:
                    SNAB_PROCESSES = int(settings.SNAB_PROCESSES)
                    if len(sorted_snab_work) < SNAB_PROCESSES:
                        SNAB_PROCESSES = len(sorted_snab_work)
                except:
                    SNAB_PROCESSES = 1
            else:
                SNAB_PROCESSES = 1

            checks_assigned = []
            checks_assigned_count = 0
            realtime_checks_assigned = 0
            testing_checks_assigned = 0
            try:
                for check_details in sorted_snab_work:
                    if checks_assigned == SNAB_PROCESSES:
                        break
                    if realtime_checks_assigned < realtime_check_count:
                        if context == 'testing':
                            logger.info('skipping %s check for %s from %s as realtime checking are pending' % (
                                context, metric_name, str(metric_timestamp)))
                            continue
                    processed = check_details['processed']
                    if processed:
                        continue
                    metric_name = check_details['metric']
                    metric_timestamp = check_details['timestamp']
                    context = check_details['context']
                    algorithm = check_details['algorithm']
                    logger.info('assign %s check for %s from %s with algorithm %s' % (
                        context, metric_name, str(metric_timestamp), algorithm))
                    checks_assigned.append(check_details)
                    checks_assigned_count += 1
                    if context == 'realtime':
                        realtime_checks_assigned += 1
                    else:
                        testing_checks_assigned += 1
                    if checks_assigned_count == SNAB_PROCESSES:
                        break
            except:
                logger.error(traceback.format_exc())
                logger.error('error :: failed to determine checks to assign')

            # Skip checks if snab needs to send Graphite metrics.  The checks
            # will be done in the next run
            if int(time()) >= (last_sent_to_graphite + 60):
                checks_assigned_count = None
                logger.info('skipping checks this run to send Graphite metrics')

            if checks_assigned_count:
                # Remove any existing algorithm.error and timing files from any
                # previous runs
                pattern = '%s.*.algorithm.error' % skyline_app
                try:
                    for f in os.listdir(settings.SKYLINE_TMP_DIR):
                        if re.search(pattern, f):
                            try:
                                os.remove(os.path.join(settings.SKYLINE_TMP_DIR, f))
                                logger.info('cleaning up old error file - %s' % (str(f)))
                            except OSError:
                                pass
                except:
                    logger.error('error :: failed to cleanup algorithm.error files')
                    logger.info(traceback.format_exc())
                pattern = '%s.*.algorithm.timings' % skyline_app
                try:
                    for f in os.listdir(settings.SKYLINE_TMP_DIR):
                        if re.search(pattern, f):
                            try:
                                os.remove(os.path.join(settings.SKYLINE_TMP_DIR, f))
                                logger.info('cleaning up old timings file - %s' % (str(f)))
                            except OSError:
                                pass
                except:
                    logger.error('error :: failed to cleanup algorithm.timing files')
                    logger.info(traceback.format_exc())

                logger.info('processing %s checks' % str(checks_assigned_count))

                # Spawn processes
                pids = []
                spawned_pids = []
                pid_count = 0
                for i in range(1, SNAB_PROCESSES + 1):
                    up_to = i - 1
                    check_details = checks_assigned[up_to]
                    metric_name = check_details['metric']
                    metric_timestamp = check_details['timestamp']
                    context = check_details['context']
                    algorithm = check_details['algorithm']
                    logger.info('processing %s check for %s from %s' % (
                        context, metric_name, str(metric_timestamp)))
                    p = Process(target=self.spin_snab_process, args=(i, check_details))
                    pids.append(p)
                    pid_count += 1
                    logger.info('starting 1 of %s spin_snab_process' % (str(pid_count)))
                    p.start()
                    spawned_pids.append(p.pid)
                    snab_checks_done += 1

                # Send wait signal to zombie processes
                # for p in pids:
                #     p.join()
                # Self monitor processes and terminate if any spin_snab_process
                # that has run for longer than 58 seconds
                p_starts = time()
                while time() - p_starts <= 58:
                    if any(p.is_alive() for p in pids):
                        # Just to avoid hogging the CPU
                        sleep(.1)
                    else:
                        # All the processes are done, break now.
                        time_to_run = time() - p_starts
                        logger.info('1 spin_snab_process completed in %.2f seconds' % (time_to_run))
                        break
                else:
                    # We only enter this if we didn't 'break' above.
                    logger.info('timed out, killing all spin_snab_process processes')
                    for p in pids:
                        p.terminate()
                        # p.join()

                for p in pids:
                    if p.is_alive():
                        logger.info('stopping spin_snab_process - %s' % (str(p.is_alive())))
                        p.join()

                # Determine the results of the assigned check from the updated
                # check_details for the purpose of sending snab metrics
                analyzer_check_algorithm_timings = []
                mirage_check_algorithm_timings = []
                try:
                    snab_work = self.redis_conn_decoded.smembers(snab_work_redis_set)
                except Exception as e:
                    logger.error('error :: could not query Redis for set %s - %s' % (snab_work_redis_set, e))
                if snab_work:
                    try:
                        for assigned_check_details in checks_assigned:
                            assigned_metric_name = assigned_check_details['metric']
                            assigned_metric_timestamp = int(assigned_check_details['timestamp'])
                            assigned_source = assigned_check_details['source']
                            assigned_context = assigned_check_details['context']
                            assigned_algorithm = assigned_check_details['algorithm']
                            for index, snab_item in enumerate(snab_work):
                                check_details = literal_eval(snab_item)
                                metric_name = check_details['metric']
                                if metric_name != assigned_metric_name:
                                    continue
                                metric_timestamp = int(check_details['timestamp'])
                                if metric_timestamp != assigned_metric_timestamp:
                                    continue
                                source = check_details['source']
                                if source != assigned_source:
                                    continue
                                context = check_details['context']
                                if context != assigned_context:
                                    continue
                                algorithm = check_details['algorithm']
                                if algorithm != assigned_algorithm:
                                    continue
                                anomalous = check_details['anomalous']
                                analysed = check_details['analysed']
                                if anomalous:
                                    if context == 'analyzer':
                                        analyzer_anomalies += 1
                                    if context == 'mirage':
                                        mirage_anomalies += 1
                                if source == 'analyzer' and context == 'testing':
                                    analyzer_testing_checks += 1
                                if source == 'analyzer' and context == 'realtime':
                                    analyzer_realtime_checks += 1
                                if source == 'mirage' and context == 'testing':
                                    mirage_testing_checks += 1
                                if source == 'mirage' and context == 'realtime':
                                    mirage_realtime_checks += 1
                                if not analysed:
                                    if context == 'analyzer':
                                        analyzer_falied_checks += 1
                                    if context == 'mirage':
                                        mirage_falied_checks += 1
                                else:
                                    try:
                                        processed = check_details['processed']
                                        algorithm_run_time = analysed - processed
                                        if context == 'analyzer':
                                            analyzer_check_algorithm_timings.append([algorithm, algorithm_run_time])
                                        if context == 'mirage':
                                            mirage_check_algorithm_timings.append([algorithm, algorithm_run_time])
                                    except:
                                        logger.error(traceback.format_exc())
                                        logger.error('error :: could not determine algorithm_run_time from check_details in snab.work entry')
                    except:
                        logger.error(traceback.format_exc())
                        logger.error('error :: could not determine check_details from snab.work entry')

                # Grab data from the queue and populate dictionaries
                exceptions = dict()
                anomaly_breakdown = dict()
                while 1:
                    try:
                        key, value = self.anomaly_breakdown_q.get_nowait()
                        if key not in anomaly_breakdown.keys():
                            anomaly_breakdown[key] = value
                        else:
                            anomaly_breakdown[key] += value
                    except Empty:
                        break

                while 1:
                    try:
                        key, value = self.exceptions_q.get_nowait()
                        if key not in exceptions.keys():
                            exceptions[key] = value
                        else:
                            exceptions[key] += value
                    except Empty:
                        break

                # Report any algorithm errors
                pattern = '%s.*.algorithm.error' % skyline_app
                try:
                    for f in os.listdir(settings.SKYLINE_TMP_DIR):
                        if re.search(pattern, f):
                            try:
                                algorithm_error_file = os.path.join(settings.SKYLINE_TMP_DIR, f)
                                if os.path.isfile(algorithm_error_file):
                                    logger.error('error :: error reported in %s' % (
                                        algorithm_error_file))
                                    try:
                                        with open(algorithm_error_file, 'r') as f:
                                            error_string = f.read()
                                        logger.error('%s' % str(error_string))
                                    except:
                                        logger.error('error :: failed to read error file - %s' % algorithm_error_file)
                                    try:
                                        os.remove(algorithm_error_file)
                                    except OSError:
                                        pass
                            except:
                                logger.error(traceback.format_exc())
                                logger.error('error :: failed to check algorithm errors')
                except:
                    logger.error(traceback.format_exc())
                    logger.error('error :: failed to check algorithm errors')

                exceptions_metrics = ['Boring', 'Stale', 'TooShort', 'Other']
                try:
                    for i_exception in exceptions_metrics:
                        if i_exception not in exceptions.keys():
                            exceptions[i_exception] = 0

                    # @added 20200607 - Feature #3566: custom_algorithms
                    anomaly_breakdown_algorithms = []
                    if CUSTOM_ALGORITHMS:
                        for custom_algorithm in settings.CUSTOM_ALGORITHMS:
                            anomaly_breakdown_algorithms.append(custom_algorithm)

                    # @modified 20200607 - Feature #3566: custom_algorithms
                    # for i_anomaly_breakdown in settings.ALGORITHMS:
                    for i_anomaly_breakdown in anomaly_breakdown_algorithms:
                        if i_anomaly_breakdown not in anomaly_breakdown.keys():
                            anomaly_breakdown[i_anomaly_breakdown] = 0

                    exceptions_string = ''
                    for i_exception in exceptions.keys():
                        if exceptions_string == '':
                            exceptions_string = '%s: %s' % (str(i_exception), str(exceptions[i_exception]))
                        else:
                            exceptions_string = '%s, %s: %s' % (exceptions_string, str(i_exception), str(exceptions[i_exception]))
                    logger.info('exceptions - %s' % str(exceptions_string))

                    anomaly_breakdown_string = ''
                    if anomaly_breakdown:
                        for i_anomaly_breakdown in anomaly_breakdown.keys():
                            if anomaly_breakdown_string == '':
                                anomaly_breakdown_string = '%s: %s' % (str(i_anomaly_breakdown), str(anomaly_breakdown[i_anomaly_breakdown]))
                            else:
                                anomaly_breakdown_string = '%s, %s: %s' % (anomaly_breakdown_string, str(i_anomaly_breakdown), str(anomaly_breakdown[i_anomaly_breakdown]))
                        logger.info('anomaly_breakdown - %s' % str(anomaly_breakdown_string))
                    else:
                        logger.info('anomaly_breakdown - none, no anomalies')
                except:
                    logger.error(traceback.format_exc())
                    logger.error('error :: could not exceptions and anomaly_breakdown details')
                try:
                    del exceptions
                except:
                    pass
                try:
                    del anomaly_breakdown
                except:
                    pass
                try:
                    with self.exceptions_q.mutex:
                        self.exceptions_q.queue.clear()
                except:
                    pass
                try:
                    with self.anomaly_breakdown_q.mutex:
                        self.anomaly_breakdown_q.queue.clear()
                except:
                    pass
            else:
                sleep(1)

            # Send snab metrics to Graphite every 60 seconds
            if int(time()) >= (last_sent_to_graphite + 60):
                logger.info('checks.processed          :: %s' % str(snab_checks_done))
                send_metric_name = '%s.checks.processed' % skyline_app_graphite_namespace
                send_graphite_metric(skyline_app, send_metric_name, str(snab_checks_done))
                algorithms_run = []
                for algorithm, run_time in algorithm_run_times:
                    algorithms_run.append(algorithm)
                for algorithm in algorithms_run:
                    avg_time = None
                    timings = []
                    for r_algorithm, run_time in algorithm_run_times:
                        if algorithm == r_algorithm:
                            run_time.append(timings)
                    try:
                        avg_time = sum(timings) / len(timings)
                    except:
                        logger.error(traceback.format_exc())
                        logger.error('error :: could not calculate avg_time for %s' % algorithm)
                    if avg_time or avg_time == 0:
                        logger.info('checks.analyzer.anomalies   :: %s' % str(analyzer_anomalies))
                        send_metric_name = '%s.checks.analyzer.anomalies' % skyline_app_graphite_namespace
                        send_graphite_metric(skyline_app, send_metric_name, str(analyzer_anomalies))

                logger.info('checks.analyzer.anomalies :: %s' % str(analyzer_anomalies))
                send_metric_name = '%s.checks.analyzer.anomalies' % skyline_app_graphite_namespace
                send_graphite_metric(skyline_app, send_metric_name, str(analyzer_anomalies))
                logger.info('checks.analyzer.realtime  :: %s' % str(analyzer_realtime_checks))
                send_metric_name = '%s.checks.analyzer.realtime' % skyline_app_graphite_namespace
                send_graphite_metric(skyline_app, send_metric_name, str(analyzer_realtime_checks))
                logger.info('checks.analyzer.testing   :: %s' % str(analyzer_testing_checks))
                send_metric_name = '%s.checks.analyzer.testing' % skyline_app_graphite_namespace
                send_graphite_metric(skyline_app, send_metric_name, str(analyzer_testing_checks))
                logger.info('checks.analyzer.falied    :: %s' % str(analyzer_falied_checks))
                send_metric_name = '%s.checks.analyzer.falied' % skyline_app_graphite_namespace
                send_graphite_metric(skyline_app, send_metric_name, str(analyzer_falied_checks))
                logger.info('checks.mirage.anomalies   :: %s' % str(mirage_anomalies))
                send_metric_name = '%s.checks.mirage.anomalies' % skyline_app_graphite_namespace
                send_graphite_metric(skyline_app, send_metric_name, str(mirage_anomalies))
                logger.info('checks.mirage.realtime    :: %s' % str(mirage_realtime_checks))
                send_metric_name = '%s.checks.mirage.realtime' % skyline_app_graphite_namespace
                send_graphite_metric(skyline_app, send_metric_name, str(mirage_realtime_checks))
                logger.info('checks.mirage.testing     :: %s' % str(mirage_testing_checks))
                send_metric_name = '%s.checks.mirage.testing' % skyline_app_graphite_namespace
                send_graphite_metric(skyline_app, send_metric_name, str(mirage_testing_checks))
                logger.info('checks.mirage.falied      :: %s' % str(mirage_falied_checks))
                send_metric_name = '%s.checks.mirage.falied' % skyline_app_graphite_namespace
                send_graphite_metric(skyline_app, send_metric_name, str(mirage_falied_checks))

                last_sent_to_graphite = int(time())
                snab_checks_done = 0
                algorithm_run_times = []
                analyzer_anomalies = 0
                analyzer_falied_checks = 0
                analyzer_realtime_checks = 0
                analyzer_testing_checks = 0
                mirage_anomalies = 0
                mirage_falied_checks = 0
                mirage_realtime_checks = 0
                mirage_testing_checks = 0

                # Clean up snab.work.done
                snab_work_done = []
                try:
                    snab_work_done = self.redis_conn_decoded.smembers(snab_work_done_redis_set)
                except Exception as e:
                    logger.error('error :: could not query Redis for set %s - %s' % (snab_work_done_redis_set, e))
                if snab_work_done:
                    current_timestamp = int(time())
                    for snab_item in snab_work_done:
                        metric_timestamp = None
                        try:
                            check_details = literal_eval(snab_item)
                            metric_timestamp = int(check_details['timestamp'])
                        except:
                            logger.error(traceback.format_exc())
                            logger.error('error :: could not determine check_details from snab.work entry')
                        if metric_timestamp:
                            item_age = current_timestamp - metric_timestamp
                            if item_age > 86400:
                                try:
                                    self.redis_conn.srem(snab_work_done_redis_set, str(snab_item))
                                    logger.info('removed item from Redis set %s - %s' % (
                                        snab_work_done_redis_set, str(snab_item)))
                                except:
                                    logger.error(traceback.format_exc())
                                    logger.error('error :: failed to remove item from Redis set %s' % (
                                        snab_work_done_redis_set))
                try:
                    snab_work = self.redis_conn_decoded.smembers(snab_work_redis_set)
                except Exception as e:
                    logger.error('error :: could not query Redis for set %s - %s' % (snab_work_redis_set, e))
