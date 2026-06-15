"""
learn_self_validation.py
"""
import copy
import logging
import os
import gzip
import traceback
import json
from time import time
from ast import literal_eval

import requests

import settings
from functions.database.queries.insert_comment import insert_comment
from functions.database.queries.get_ionosphere_learnt_unvalidated_fps import get_ionosphere_learnt_unvalidated_fps
from functions.database.queries.get_ionosphere_fp_row_from_match_id import get_ionosphere_fp_row_from_match_id
from functions.database.queries.get_ionosphere_fp_row import get_ionosphere_fp_db_row
from skyline_functions import mkdir_p, get_graphite_metric, get_redis_conn_decoded
from functions.metrics.get_base_name_from_labelled_metrics_name import get_base_name_from_labelled_metrics_name
from functions.victoriametrics.get_victoriametrics_metric import get_victoriametrics_metric
from functions.metrics.get_metric_id_from_base_name import get_metric_id_from_base_name
from functions.metrics.get_base_name_from_metric_id import get_base_name_from_metric_id
from functions.timeseries.determine_data_frequency import determine_data_frequency
from functions.timeseries.downsample import downsample_timeseries

skyline_app = 'ionosphere'
skyline_app_logger = '%sLog' % skyline_app
logger = logging.getLogger(skyline_app_logger)
skyline_app_logfile = '%s/%s.log' % (settings.LOG_PATH, skyline_app)
skyline_app_loglock = '%s.lock' % skyline_app_logfile
skyline_app_logwait = '%s.wait' % skyline_app_logfile

this_host = str(os.uname()[1])

# @added 20250728 - Feature #5644: ionosphere.learn_self_validation
def self_validation(last_self_validate_timestamp):
    """
    Called by :class:`~skyline.skyline.Ionosphere.spawn_self_validation` to
    evaluate learnt features profiles that have not been validated after 2 weeks
    and validate them if no anomalies are present in the features profile period
    which is included in a 28 day period.

    :param unvalidated_learnt_fps: the dict of unvalidated_learnt_fps
    :type unvalidated_learnt_fps: dict
    :return: True or False
    :rtype: boolean

    """
    function_str = 'learn_self_validation'
    start = time()
    validated_learnt_fps = {}
    child_process_pid = os.getpid()
    logger.info('learn_self_validation :: child_process_pid - %s' % str(child_process_pid))
    valid_to_run = False
    algorithm = None
    try:
        algorithm = settings.IONOSPHERE_LEARN_SELF_VALIDATE['algorithm']
    except Exception as err:
        logger.error('error :: learn_self_validation :: failed to determine algorithm from settings.IONOSPHERE_LEARN_SELF_VALIDATE, err: %s' % err)
        algorithm = None

    vortex_algorithm_done_key = 'ionosphere.learn_self_validation.%s.fps_done' % str(algorithm)
    common_motifs_done_hash_key = 'ionosphere.learn_self_validation.common_motifs.fps_done'

    try:
        if algorithm in settings.VORTEX_ALGORITHMS:
            valid_to_run = True
    except Exception as err:
        logger.error('error :: learn_self_validation :: %s is not in settings.VORTEX_ALGORITHMS, err: %s' % err)
    common_motifs_method = False
    try:
        common_motifs_method = settings.IONOSPHERE_LEARN_SELF_VALIDATE['common_motifs_method']
    except Exception as err:
        logger.error('error :: learn_self_validation :: failed to determine common_motifs_method from settings.IONOSPHERE_LEARN_SELF_VALIDATE, err: %s' % err)
        common_motifs_method = False
    if common_motifs_method:
        valid_to_run = True

    if not valid_to_run:
        return validated_learnt_fps

    try:
        redis_conn_decoded = get_redis_conn_decoded(skyline_app)
    except Exception as err:
        logger.error('error :: learn_self_validation :: get_redis_conn_decoded failed, err: %s' % err)

    common_motifs_work_done_keys = []
    try:
        common_motifs_work_done_keys = list(redis_conn_decoded.hkeys('ionosphere.common_motifs.learn_self_validation.work_done'))
    except Exception as err:
        logger.error('error :: hkeys failed on ionosphere.common_motifs.learn_self_validation.work_done, err: %s' % err)
    common_motifs_work_done = {}
    #for fp_id_str, raw_dict in common_motifs_work_done_raw.items():
    for fp_id_str in common_motifs_work_done_keys:
        try:
            common_motifs_work_done[int(fp_id_str)] = fp_id_str
        except Exception as err:
            logger.error('error :: failed to add %s to common_motifs_work_done, err: %s' % (
                str(fp_id_str), err))
    logger.info('learn_self_validation :: %s common_motifs_work_done items to process' % str(len(common_motifs_work_done)))

    from_timestamp = int(last_self_validate_timestamp)
    too_old_timestamp = start - (86400 * 365)
    if from_timestamp < too_old_timestamp:
        from_timestamp = int(too_old_timestamp) + 10
    # Validation is only carried out once there is two weeks more data after a
    # features profile is learnt
    # @modified 20250922 - Feature #5644: ionosphere.learn_self_validation
    # Add an offset to skip busy periods.  Many learnt fps may be added during
    # peak periods and the validation jobs should not also be added at peak
    # periods.
    #until_timestamp = int(start) - (86400 * 14)
    until_timestamp = int(start) - (86400 * 14) - 5400

    unvalidated_learnt_fps = {}
    if not common_motifs_work_done:
        try:
            unvalidated_learnt_fps = get_ionosphere_learnt_unvalidated_fps(skyline_app, from_timestamp, until_timestamp)
        except Exception as err:
            logger.error('error :: %s :: get_ionosphere_learnt_unvalidated_fps failed, err: %s' % (
                function_str, err))

    common_motifs_work_done = {}
    #for fp_id_str, raw_dict in common_motifs_work_done_raw.items():
    for fp_id_str in common_motifs_work_done_keys:
        try:
            common_motifs_work_done[int(fp_id_str)] = fp_id_str
        except Exception as err:
            logger.error('error :: failed to add %s to common_motifs_work_done, err: %s' % (
                str(fp_id_str), err))
    logger.info('learn_self_validation :: %s common_motifs_work_done items to process' % str(len(common_motifs_work_done)))

    adhoc_unvalidated_learnt_fp_ids = []
    if not common_motifs_work_done:
        try:
            adhoc_unvalidated_learnt_fp_ids = [int(fp_id) for fp_id in list(redis_conn_decoded.smembers('ionosphere.learn_self_validation.adhoc_fp_ids'))]
        except Exception as err:
            logger.error('error :: smembers failed on ionosphere.learn_self_validation.adhoc_fp_ids, err: %s' % err)
    if adhoc_unvalidated_learnt_fp_ids:
        logger.info('learn_self_validation :: %s adhoc_fp_ids found to process' % str(len(adhoc_unvalidated_learnt_fp_ids)))
        try:
            unvalidated_learnt_fps = get_ionosphere_learnt_unvalidated_fps(skyline_app, from_timestamp, until_timestamp, adhoc_fp_ids=adhoc_unvalidated_learnt_fp_ids)
        except Exception as err:
            logger.error('error :: %s :: get_ionosphere_learnt_unvalidated_fps with adhoc_unvalidated_learnt_fp_ids failed, err: %s' % (
                function_str, err))

    user = None
    password = None
    if settings.WEBAPP_AUTH_ENABLED:
        user = str(settings.WEBAPP_AUTH_USER)
        password = str(settings.WEBAPP_AUTH_USER_PASSWORD)
    # Handle self signed certificate
    verify_ssl = True
    try:
        verify_ssl = settings.VERIFY_SSL
    except:
        verify_ssl = True

    logger.info('learn_self_validation :: %s unvalidated learnt features to process' % str(len(unvalidated_learnt_fps)))

    processed = 0
    validated = 0
    invalidate_fps = {}

    # Only run 3 times in a row and then sleep for 5 minutes to allow for
    # find_repeative_patterns, etc to run
    increment_by = 1
    ttl = 59

    try:
        redis_conn_decoded.incrby('ionosphere.last_self_validate.count', increment_by)
        redis_conn_decoded.expire('ionosphere.last_self_validate.count', ttl)
    except Exception as err:
        logger.error('error :: learn_self_validation :: Redis incr failed, err: %s' % err)

    if not unvalidated_learnt_fps:
        logger.info('learn_self_validation :: no unvalidated learnt features to process, nothing to do')
        #return validated_learnt_fps

    # @added 20250923 - Feature #5644: ionosphere.learn_self_validation
    # Only get the keys not the whole hash which can become quite large
    # with time series data when the queue backs up a bit
    #learn_self_validation_common_motifs_work = redis_conn_decoded.hgetall('ionosphere.learn_self_validation.common_motifs.work')
    learn_self_validation_common_motifs_work_keys = []

    learn_self_validation_common_motifs_work = {}
    try:
        # @modified 20250923 - Feature #5644: ionosphere.learn_self_validation
        # Only get the keys not the whole hash which can become quite large
        # with time series data when the queue backs up a bit
        #learn_self_validation_common_motifs_work = redis_conn_decoded.hgetall('ionosphere.learn_self_validation.common_motifs.work')
        learn_self_validation_common_motifs_work_keys = sorted(list(redis_conn_decoded.hkeys('ionosphere.learn_self_validation.common_motifs.work')))
    except Exception as err:
        logger.error('error :: learn_self_validation :: hkeys failed on ionosphere.learn_self_validation.common_motifs.work, err: %s' % err)

    # @added 20250925 - Feature #5644: ionosphere.learn_self_validation
    # To mitigate any issues that may occur between the states of this host and
    # any remote host declared in IONOSPHERE_EXTERNAL_COMMON_MOTIFS_HOSTS_DICT
    # if external hosts are used then check to determine in the hash_key is in
    # the wind 
    send_via_wind = []
    for i_hash_key in learn_self_validation_common_motifs_work_keys:
        wind_hash_key = 'ionosphere.common_motifs.wind.%s' % str(i_hash_key)
        sent_via_wind = False
        try:
            sent_via_wind = redis_conn_decoded.exists(wind_hash_key)
        except Exception as err:
            logger.error('error :: learn_self_validation :: exists failed on %s, err: %s' % (
                wind_hash_key, err))            
        if not sent_via_wind:
            send_via_wind.append(i_hash_key)

    try:
        # Only get the keys not the whole hash which can become quite large
        # with time series data when the queue backs up a bit
        #learn_self_validation_common_motifs_work = redis_conn_decoded.hgetall('ionosphere.learn_self_validation.common_motifs.work')
        learn_self_validation_common_motifs_work_keys = sorted(list(redis_conn_decoded.hkeys('ionosphere.learn_self_validation.common_motifs.work')))
    except Exception as err:
        logger.error('error :: hkeys failed on ionosphere.learn_self_validation.common_motifs.work, err: %s' % err)

    # @added 20250923 - Feature #5644: ionosphere.learn_self_validation
    if len(learn_self_validation_common_motifs_work_keys) > 4:
        logger.info('learn_self_validation :: %s items already in the common_motifs work queue, not adding additional work' % (
            str(len(learn_self_validation_common_motifs_work_keys))))
        if not common_motifs_work_done:
            return validated_learnt_fps            

    # Only get a single keys not the whole hash
    work_key = None
    if len(learn_self_validation_common_motifs_work_keys):
        try:
            work_key = learn_self_validation_common_motifs_work_keys[0]
        except Exception as err:
            logger.error('error :: failed to determine work key from learn_self_validation_common_motifs_work_keys, err: %s' % err)
    learn_self_validation_common_motifs_work_item = None
    if work_key:
        try:
            learn_self_validation_common_motifs_work_item = redis_conn_decoded.hget('ionosphere.learn_self_validation.common_motifs.work', work_key)
        except Exception as err:
            logger.error('error :: hget failed on %s from ionosphere.learn_self_validation.common_motifs.work, err: %s' % (
                work_key, err))
    if learn_self_validation_common_motifs_work_item:
        learn_self_validation_common_motifs_work[work_key] = learn_self_validation_common_motifs_work_item
        logger.info('learn_self_validation :: added work_key %s to process' % work_key)

    fp_ids_in_common_motifs_work = []
    for i_timestamp, data_str in learn_self_validation_common_motifs_work.items():

        # @added 20250916 - Feature #5644: ionosphere.learn_self_validation
        # Handle nans from VictoriaMetrics, although this was handled in
        # commit_motifs.py when the pw5_timeseries is fetched, it is also added
        # here as a check to handle any that are in the queue
        if 'nan]' in data_str:
            try:
                data_str = data_str.replace('nan]', 'None]')
            except Exception as err:
                logger.error('error :: failed to replace nan in data_str for %s from ionosphere.learn_self_validation.common_motifs.work, err: %s' % (
                    str(i_timestamp), err))

        try:
            i_data = literal_eval(data_str)
        except Exception as err:
            logger.error('error :: failed to literal_eval data_str for %s from ionosphere.learn_self_validation.common_motifs.work, err: %s' % (
                str(i_timestamp), err))
            try:
                redis_conn_decoded.hset('ionosphere.learn_self_validation.common_motifs.work.failed', i_timestamp, data_str)
                redis_conn_decoded.hdel('ionosphere.learn_self_validation.common_motifs.work', i_timestamp)
                logger.info('removed %s from ionosphere.learn_self_validation.common_motifs.work' % (
                    str(i_timestamp)))
            except Exception as err:
                logger.error('error :: failed to remove %s from ionosphere.learn_self_validation.common_motifs.work, err: %s' % (
                    str(i_timestamp), err))
            continue
        try:
            if 'fp_id' in i_data.keys():
                fp_ids_in_common_motifs_work.append(int(i_data['fp_id']))
        except Exception as err:
            logger.error('error :: failed add fp_id to fp_ids_in_common_motifs_work from i_data: %s, err: %s' % (
                str(i_data), err))

    use_data_dict = copy.deepcopy(unvalidated_learnt_fps)
    if common_motifs_work_done:
        use_data_dict = copy.deepcopy(common_motifs_work_done)

    work_queue_count = len(fp_ids_in_common_motifs_work)
    if work_queue_count > 2 and len(common_motifs_work_done) == 0:
        logger.info('learn_self_validation :: %s items already in the common_motifs work queue and no common_motifs_work_done, skipping run' % (
            str(work_queue_count)))
        return validated_learnt_fps

    last_validated_timestamp = 0
    min_last_validated_timestamp = 0
    for fp_id, fp_data in use_data_dict.items():
        now = time()
        runtime = now - start
        if runtime > 55:
            logger.info('learn_self_validation :: stopping as max processing time reached, runtime: %s' % (
                str(runtime)))
            break

        if common_motifs_work_done:
            fp_id_str = str(fp_id)
            fp_id = int(fp_id)

        if fp_id in fp_ids_in_common_motifs_work:
            logger.info('learn_self_validation :: fp_id: %s already queued in common_motifs' % (
                str(fp_id)))
            if fp_id not in common_motifs_work_done.keys():
                continue

        if common_motifs_work_done:
            #fp_data = fp_data['fp_data']
            raw_dict = ''
            try:
                raw_dict = redis_conn_decoded.hget('ionosphere.common_motifs.learn_self_validation.work_done', fp_id_str)
            except Exception as err:
                logger.error('error :: hget failed on ionosphere.common_motifs.learn_self_validation.work_done for %s, err: %s' % (
                    fp_id_str, err))
            fp_data = {}
            try:
                fp_data = literal_eval(raw_dict)
            except Exception as err:
                logger.error('error :: failed to literal_eval data for fp_id_str: %s, err: %s' % (
                    fp_id_str, err))
                try:
                    redis_conn_decoded.hdel('ionosphere.common_motifs.learn_self_validation.work_done', fp_id_str)
                except Exception as err2:
                    logger.error('error :: failed to hdel: %s, err: %s' % (
                        str(fp_id_str), err2))
                continue
            common_motifs_work_done[fp_id] = fp_data

        try:
            processed += 1
            validation_method = None
            metric = str(fp_data['metric'])
            metric_id = fp_data['metric_id']
            graphite_metric = True
            if 'tenant_id' in metric:
                graphite_metric = False

            anomaly_timestamp = int(fp_data['anomaly_timestamp'])
            last_validated_timestamp = int(anomaly_timestamp)

            # @added 20250728 - Feature #5644: ionosphere.learn_self_validation
            adhoc_job = False
            try:
                adhoc_job = fp_data['adhoc']
            except:
                adhoc_job = False

            too_old_timestamp = now - (86400 * 365)
            if not graphite_metric:
                too_old_timestamp = now - (86400 * 16)
            # @modified 20250728 - Feature #5644: ionosphere.learn_self_validation
            #if anomaly_timestamp < too_old_timestamp:
            if anomaly_timestamp < too_old_timestamp and not adhoc_job:
                logger.info('learn_self_validation :: skipping too old to validate - fp_id: %s, metric: %s' % (
                    str(fp_id), metric))
                try:
                    redis_conn_decoded.hset('ionosphere.learn_self_validation.fps_done', fp_id, now)
                except Exception as err:
                    logger.error('error :: %s :: hset failed on ionosphere.learn_self_validation.fps_done for fp_id: %s, err: %s' % (
                        function_str, str(fp_id), err))
                continue

            logger.info('learn_self_validation :: checking fp_id: %s, metric: %s' % (
                str(fp_id), metric))

            if metric == 'unknown':
                fp_dict = {}
                try:
                    fp_dict = get_ionosphere_fp_db_row(skyline_app, int(fp_id))
                except Exception as err:
                    logger.error('error :: %s :: get_ionosphere_fp_db_row failed for fp_id: %s, err: %s' % (
                        function_str, str(fp_id), err))
                if fp_dict:
                    try:
                        metric_id = fp_dict['metric_id']
                        metric = get_base_name_from_metric_id(skyline_app, metric_id)
                    except Exception as err:
                        logger.error('error :: %s :: get_base_name_from_metric_id failed for fp_id: %s and metric_id: %s, err: %s' % (
                            function_str, str(fp_id), str(metric_id), err))

            # Because the common_motifs method is added to a work queue, check
            # if the results are availabe
            common_motifs_results = {}
            if fp_id in common_motifs_work_done.keys():
                try:
                    common_motifs_results = common_motifs_work_done[fp_id]
                except Exception as err:
                    logger.error('error :: %s :: failed to get fp_id: %s from common_motifs_work_done, err: %s' % (
                        function_str, str(fp_id), err))
                if common_motifs_results:
                    logger.info('learn_self_validation :: common_motif results available for fp_id: %s, metric: %s' % (
                        str(fp_id), metric))

            until_timestamp = anomaly_timestamp + (86400 * 14)
            from_timestamp = until_timestamp - (86400 * 28)

            timeseries = []
            if not common_motifs_results:
                if graphite_metric:
                    # Get Graphite data
                    try:
                        timeseries = get_graphite_metric(skyline_app, metric, from_timestamp, until_timestamp, 'list', 'object')
                    except Exception as err:
                        logger.error('error :: learn_self_validation :: get_graphite_metric failed for metric: %s, err: %s' % (
                            metric, err))
                else:
                    # Get Victoriametrics data
                    try:
                        timeseries = get_victoriametrics_metric(skyline_app, metric, from_timestamp, until_timestamp, 'list', 'object')
                    except Exception as err:
                        logger.error('error :: learn_self_validation :: get_victoriametrics_metric failed for metric: %s, err: %s' % (
                            metric, err))

            validated_fp = False
            vortex_data = {}
            validate_fp = False

            if len(timeseries) < 3000 and not common_motifs_results:
                try:
                    redis_conn_decoded.hset(vortex_algorithm_done_key, fp_id, now)
                except Exception as err:
                    logger.error('error :: %s :: hset failed on %s for fp_id: %s, err: %s' % (
                        function_str, vortex_algorithm_done_key, str(fp_id), err))
                try:
                    redis_conn_decoded.hset(common_motifs_done_hash_key, fp_id, now)
                except Exception as err:
                    logger.error('error :: %s :: hset failed on %s for fp_id: %s, err: %s' % (
                        function_str, vortex_algorithm_done_key, str(fp_id), err))
                try:
                    redis_conn_decoded.hset('ionosphere.learn_self_validation.fps_done', fp_id, now)
                except Exception as err:
                    logger.error('error :: %s :: hset failed on ionosphere.learn_self_validation.fps_done for fp_id: %s, err: %s' % (
                        function_str, str(fp_id), err))
                invalidate_fps[fp_id] = 'insuffient_data_to_validate'
                logger.info('learn_self_validation :: insufficient data to validate, skipping - fp_id: %s, metric: %s' % (
                    str(fp_id), metric))

                # @added 20250728 - Feature #5644: ionosphere.learn_self_validation
                if fp_id in adhoc_unvalidated_learnt_fp_ids:
                    logger.info('%s :: removing adhoc fp_id: %s, from ionosphere.learn_self_validation.adhoc_fp_ids' % (
                        function_str, str(fp_id)))
                    try:
                        redis_conn_decoded.srem('ionosphere.learn_self_validation.adhoc_fp_ids', str(fp_id))
                    except Exception as err:
                        logger.error('error :: srem failed on ionosphere.learn_self_validation.adhoc_fp_ids, err: %s' % err)

                continue

            # Using multiple methods of validation, first check if the vortex
            # algorithm check has been completed
            if common_motifs_results:
                algorithm_check_done = True
            else:
                algorithm_check_done = False
                try:
                    algorithm_check_done = redis_conn_decoded.hexists(vortex_algorithm_done_key, str(fp_id))
                except Exception as err:
                    logger.error('error :: %s :: hexist failed on %s for fp_id: %s, err: %s' % (
                        function_str, vortex_algorithm_done_key, str(fp_id), err))

            if algorithm and algorithm_check_done:
                logger.info('learn_self_validation :: algorithm_check_done already done for fp_id: %s' %str(fp_id))

            if algorithm and not algorithm_check_done:
                reference = 'learn_self_validation-%s-%s' % (
                    str(anomaly_timestamp), metric)
                algorithm_dict = {'anomaly_window': len(timeseries)}
                if 'consensus_count' in settings.IONOSPHERE_LEARN_SELF_VALIDATE:
                    algorithm_dict['consensus_count'] = int(settings.IONOSPHERE_LEARN_SELF_VALIDATE['consensus_count'])
                vortex_data = {
                    'key': settings.FLUX_SELF_API_KEY,
                    'metric': metric,
                    'timeout': 60,
                    'return_timeseries': False,
                    'check_all_consensuses': False,
                    'timeseries': timeseries,
                    'algorithms': {
                        algorithm: algorithm_dict,
                    },
                    'consensus': [[algorithm]],
                    'consensus_count': 0,
                    'reference': reference,
                    'adhoc': True,
                    'no_downsample': True,
                    'save_training_data_on_false': False,
                    'send_to_ionosphere': False,
                    'return_image_urls': False,
                    'trigger_anomaly': False,
                    'algorithms_test_only': True,
                    'override_7_day_limit': True,
                    'realtime_analysis': False,
                    'return_results': True,
                }
                headers = {'content-encoding': 'gzip', "content-type": "application/json"}
                url = '%s/flux/vortex' % settings.SKYLINE_URL
                vortex_results = {}

                # @added 20251008 - Feature #5644: ionosphere.learn_self_validation
                # If vortex returns a 400 do not validate
                status_code = 0

                if vortex_data:
                    r = None
                    try:
                        request_body = gzip.compress(json.dumps(vortex_data).encode('utf-8'))
                        r = requests.post(url, data=request_body, headers=headers, timeout=60, verify=settings.VERIFY_SSL)
                        if r.status_code != 200:
                            logger.info('warning :: learn_self_validation :: got response with status code %s and reason %s' % (
                                str(r.status_code), str(r.reason)))
                        else:
                            logger.info('learn_self_validation :: got vortex response')
                        # @added 20251008 - Feature #5644: ionosphere.learn_self_validation
                        # If vortex returns a 400 do not validate
                        status_code = int(r.status_code)
                    except Exception as err:
                        logger.error(traceback.format_exc())
                        logger.error('error :: learn_self_validation :: flux/vortex request failed, err: %s' % (
                            err))
                    if r:
                        try:
                            vortex_results = r.json()
                        except Exception as err:
                            logger.error('error :: %s :: failed to get json response from %s - %s' % (
                                function_str, url, err))

                # @modified 20251008 - Feature #5644: ionosphere.learn_self_validation
                # Set anomalies_in_period to a dict
                #anomalies_in_period = None
                anomalies_in_period = {}

                if vortex_results:
                    try:
                        redis_conn_decoded.hset(vortex_algorithm_done_key, fp_id, now)
                    except Exception as err:
                        logger.error('error :: %s :: hset failed on %s for fp_id: %s, err: %s' % (
                            function_str, vortex_algorithm_done_key, str(fp_id), err))
                    # Determine if any anomalies are triggered in the learnt features
                    # profile period
                    try:
                        anomalies_in_period = {}
                        fp_start_timestamp = anomaly_timestamp - fp_data['full_duration']
                        consensus_results = False
                        if 'results' in vortex_results:
                            if 'consensus_results' in vortex_results['results']:
                                if 'anomalies' in vortex_results['results']['consensus_results']:
                                    consensus_results = True
                        if consensus_results:
                            for a_timestamp_str in vortex_results['results']['consensus_results']['anomalies'].keys():
                                a_timestamp = int(a_timestamp_str)
                                if a_timestamp >= fp_start_timestamp:
                                    if a_timestamp <= anomaly_timestamp:
                                        anomalies_in_period[a_timestamp] = vortex_results['results']['consensus_results']['anomalies'][a_timestamp_str]
                        else:
                            for a_timestamp_str in vortex_results['results']['anomalies'].keys():
                                a_timestamp = int(a_timestamp_str)
                                if a_timestamp >= fp_start_timestamp:
                                    if a_timestamp <= anomaly_timestamp:
                                        anomalies_in_period[a_timestamp] = vortex_results['results']['anomalies'][a_timestamp_str]
                    except Exception as err:
                        logger.error('error :: %s :: failed to determine if any anomalies are in the features profile period, err: %s' % (
                            function_str, err))
                    if isinstance(anomalies_in_period, dict):
                        if len(anomalies_in_period) == 0:
                            validate_fp = True
                            validation_method = algorithm

                # @added 20251008 - Feature #5644: ionosphere.learn_self_validation
                # If vortex returns a 400 do not validate
                if status_code == 400:
                    validate_fp = False

                if not validate_fp:
                    if status_code == 200:
                        logger.info('%s :: not validating fp_id: %s with %s as %s anomalies in features profile period' % (
                            function_str, str(fp_id), algorithm, str(len(anomalies_in_period))))
                    else:
                        logger.info('%s :: not validating fp_id: %s with %s as no result from vortex' % (
                            function_str, str(fp_id), algorithm))

            # Use common_motifs method
            # Preprocess the time series data to a pw5_timeseries to be
            # submitted to ionosphere.common_motifs
            # NOTE:
            # In the context of a learn_self_validation job, the common_motifs
            # method is being used to simply validate that the features profile
            # which was learnt is valid when considered in the context of 1 week
            # before the learnt features profile and 2 weeks after the learnt
            # features profile.  Therefore the pw5_timeseries that is used is
            # actually a preprocessed 4 week time series, where the most current
            # 2 weeks are timeshifted:
            # Consider this time series in terms of weeks as:
            # fp-1week,fp,fp+1week,fp+2week
            # This is timeshift as follows:
            # fp+1week,fp+2week,fp-1week,fp
            # This can be done as the method is only concerned with common motifs,
            # not order, this is simply checking that in the recent 3 weeks, the
            # motifs from the learnt features profile are common.
            if not validate_fp and common_motifs_method:
                if not common_motifs_results:
                    downsampled_timeseries = []
                    resolution = 0
                    try:
                        resolution = determine_data_frequency(skyline_app, timeseries, False)
                    except Exception as err:
                        logger.error('error :: %s :: determine_data_frequency failed, err: %s' % (
                            function_str, err))
                    logger.info('%s :: pw5_timeseries, resolution: %s' % (
                        function_str, str(resolution)))
                    if resolution < 600:
                        logger.info('%s :: downsampling pw5_timeseries resolution to 600' % (
                            function_str))
                        try:
                            downsampled_timeseries = downsample_timeseries(skyline_app, timeseries, resolution, 600, 'mean', 'end')
                        except Exception as err:
                            logger.error('error :: %s :: downsample_timeseries failed, err: %s' % (
                                function_str, err))
                    if downsampled_timeseries:
                        timeseries = list(downsampled_timeseries)
                    pw4_timeseries = list(timeseries)
                    pw5_timeseries = []
                    first_segment = [item for item in timeseries if item[0] > anomaly_timestamp]
                    last_segment = [item for item in timeseries if item[0] <= anomaly_timestamp]
                    timeshift = first_segment[-1][0] - last_segment[0][0] + resolution
                    for t, v in first_segment:
                        pw5_timeseries.append([(t - timeshift), v])
                    pw5_timeseries = pw5_timeseries + last_segment
                    fp_start_timestamp = anomaly_timestamp - fp_data['full_duration']
                    fp_timeseries = [item for item in timeseries if item[0] >= fp_start_timestamp and item[0] <= anomaly_timestamp]

                    key = str(time())
                    adhoc_job = False
                    try:
                        adhoc_job = fp_data['adhoc']
                    except:
                        adhoc_job = False
                    check_data = {
                        'metric_id': metric_id, 'metric': metric,
                        'anomaly_id': 0,
                        'anomaly_timestamp': anomaly_timestamp,
                        'fp_id': fp_id,
                        'fp_data': copy.deepcopy(fp_data),
                        'context': 'learn_self_validation',
                        'pw4_timeseries': pw4_timeseries,
                        'pw5_timeseries': pw5_timeseries,
                        'fp_timeseries': fp_timeseries,
                        'validation_link': fp_data['validation_link'],
                        'adhoc': adhoc_job,
                        'user': user,
                        'password': password,
                    }
                    logger.info('%s :: adding common_motifs work for fp_id: %s' % (
                        function_str, str(fp_id)))
                    try:
                        redis_conn_decoded.hset('ionosphere.learn_self_validation.common_motifs.work', key, str(check_data))
                    except Exception as err:
                        logger.error('error :: %s :: could not add data to Redis hash ionosphere.learn_self_validation.common_motifs.work, err: %s' % (
                            function_str, err))
                    if fp_id in adhoc_unvalidated_learnt_fp_ids:
                        logger.info('%s :: removing adhoc fp_id: %s, from ionosphere.learn_self_validation.adhoc_fp_ids' % (
                            function_str, str(fp_id)))
                        try:
                            redis_conn_decoded.srem('ionosphere.learn_self_validation.adhoc_fp_ids', fp_id)
                        except Exception as err:
                            logger.error('error :: srem failed on ionosphere.learn_self_validation.adhoc_fp_ids, err: %s' % err)

                    if not min_last_validated_timestamp:
                        min_last_validated_timestamp = int(anomaly_timestamp)
                    continue
                else:
                    logger.info('%s :: common_motifs has been completed for fp_id: %s, not adding' % (
                        function_str, str(fp_id)))
            if common_motifs_results:
                validated_by_common_motifs = False
                try:
                    validated_by_common_motifs = common_motifs_results['learn']
                except Exception as err:
                    logger.error('error :: failed to determine common_motifs_results["results"]["learn"] for fp_id: %s, err: %s' % (
                        str(fp_id), err))
                if validated_by_common_motifs:
                    validate_fp = True
                    validation_method = 'common_motifs'
                logger.info('%s :: fp_id: %s, validated_by_common_motifs: %s' % (
                    function_str, str(fp_id), str(validated_by_common_motifs)))
                try:
                    deleted = redis_conn_decoded.hdel('ionosphere.common_motifs.learn_self_validation.work_done', str(fp_id))
                    if deleted:
                        logger.info('%s :: removed fp_id: %s, from ionosphere.common_motifs.learn_self_validation.work_done Redis hash' % (
                            function_str, str(fp_id)))
                except Exception as err:
                    logger.error('error :: %s :: hdel of fp_id: %s, failed on ionosphere.common_motifs.learn_self_validation.work_done, err: %s' % (
                        function_str, str(fp_id), err))
                try:
                    redis_conn_decoded.hset('ionosphere.learn_self_validation.fps_done', fp_id, now)
                except Exception as err:
                    logger.error('error :: %s :: hset failed on ionosphere.learn_self_validation.fps_done for fp_id: %s, err: %s' % (
                        function_str, str(fp_id), err))

            if validate_fp:
                logger.info('%s :: validating fp_id: %s, validation_method: %s' % (
                    function_str, str(fp_id), str(validation_method)))
                validation_link = fp_data['validation_link']
                rv = None
                try:            
                    if user and password:
                        rv = requests.get(validation_link, timeout=60, auth=(user, password), verify=verify_ssl)
                    else:
                        rv = requests.get(validation_link, timeout=60, verify=verify_ssl)
                except Exception as err:
                    logger.error('error :: %s :: failed to get %s, err: %s' % (
                        function_str, validation_link, err))
                rv_dict = {}
                if rv:
                    try:
                        rv_dict = rv.json()
                    except Exception as err:
                        logger.error('error :: %s :: failed to validation response from %s, err: %s' % (
                            function_str, validation_link, err))
                if rv_dict:
                    try:
                        if rv_dict['data']['validated']:
                            validated_fp = True
                    except Exception as err:
                        logger.error('error :: %s :: failed to validation response from %s, err: %s' % (
                            function_str, validation_link, err))
                if validated_fp:
                    logger.info('%s :: validated fp_id: %s' % (
                        function_str, str(fp_id)))
                    validated += 1
                    validated_learnt_fps[fp_id] = copy.deepcopy(fp_data)
                    new_comment_id = None
                    user_id = 0
                    validated_at = int(time())
                    comment_data = '%s - validated at %s with %s' % (
                        function_str, str(validated_at), str(validation_method))
                    validated_learnt_fps[fp_id]['comment'] = comment_data
                    validated_learnt_fps[fp_id]['validated'] = 1
                    try:
                        metric_id = fp_data['metric_id']
                        new_comment_id = insert_comment(
                                            skyline_app, anomaly_timestamp, metric,
                                            metric_id, user_id, comment_data,
                                            anomaly_id=None, fp_id=fp_id,
                                            match_id=None, motif_match_id=None,
                                            snab_id=None)
                    except Exception as err:
                        logger.error('error :: %s :: insert_comment failed, err: %s' % (
                            function_str, err))
        except Exception as err:
            logger.error(traceback.format_exc())
            logger.error('error :: learn_self_validation :: failed to process fp_id: %s, err: %s' % (
                str(fp_id), err))

    # Use the min_last_validated_timestamp so that check queued to common_motifs
    # are used as the ladt timestamp so that the results can be checked when
    # they are complete
    if not min_last_validated_timestamp:
        min_last_validated_timestamp = int(last_validated_timestamp)
    try:
        redis_conn_decoded.set('ionosphere.learn_self_validate.last_timestamp', min_last_validated_timestamp)
    except Exception as err:
        logger.error('error :: learn_self_validation :: redis_conn_decoded failed to set ionosphere.learn_self_validate.last_timestamp, err: %s' % err)
    logger.info('learn_self_validation :: processed: %s, validated: %s' % (
        str(processed), str(validated)))
    now = time()
    runtime = now - start
    logger.info('learn_self_validation :: runtime: %s' % (
        str(runtime)))

    return validated_learnt_fps
