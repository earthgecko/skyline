"""
wind.py
"""
import gzip
import json
import logging
import traceback
from time import time

from flask import request

import settings

from skyline_functions import get_redis_conn_decoded

skyline_app = 'webapp'
skyline_app_logger = '%sLog' % skyline_app
logger = logging.getLogger(skyline_app_logger)
skyline_app_logfile = '%s/%s.log' % (settings.LOG_PATH, skyline_app)
logfile = '%s/%s.log' % (settings.LOG_PATH, skyline_app)


def wind(results):
    """
    Create a job or accept the results of a job and submit them to the
    appropriate work Redis hash.

    wind expects a json payload with the required_keys and will either post to
    results of the job to the originating source node or will accept the
    results from a job and post them to the appropriate work Redis hash.
    If results are present, results_url must be set to None, if this is a job
    being submitted results_url must be set and results must be an empty dict or
    None/null.

    post_data = {
        'source_host': skyline_node,
        'app': 'ionosphere',
        'job': 'common_motifs',
        'redis_work_hash': 'ionosphere.find_repetitive_patterns.common_motifs.work',
        'redis_work_hash_key': '1712230013.1507778',
        'data': {'metric_id': metric_id, 'metric': metric,
            'anomaly_id': anomaly_id,
            'anomaly_timestamp': anomaly_timestamp
        },
        'results_url': 'url_to_post_results_to or None/null',
        'results': results_dict_or_None/null,
    }

    :param results: a dict with status_code, etc to populate and return
    :type results: dict
    :return: results
    :rtype:  dict

    """

    post_data = {}
    content_encoding = None
    try:
        content_encoding = request.headers.get('content-encoding', '')
    except Exception as err:
        logger.error('error :: wind :: an error occurred determining content-encoding, err: %s' % (
            err))
    if content_encoding == 'gzip':
        logger.info('wind :: handling gzip payload')
        post_data_obj = None
        try:
            postData_obj = request.data
            post_data_obj = gzip.decompress(postData_obj)
        except Exception as err:
            logger.error(traceback.format_exc())
            logger.error('error :: wind :: gzip.decompress(postData_obj) failed, err: %s' % err)
        if post_data_obj:
            try:
                post_data = json.loads(post_data_obj)
            except Exception as err:
                logger.error(traceback.format_exc())
                logger.error('error :: wind :: json.loads(post_data_obj) failed, err: %s' % err)

    if not post_data:
        try:
            post_data = request.get_json()
        except Exception as err:
            logger.error(traceback.format_exc())
            logger.error('error :: wind :: no POST data, err: %s' % (
                err))
            logger.info('wind :: return 400 no POST data')
            results['status_code'] = 400
            results['reason'] = 'no POST data'
            return results
    if not post_data:
        logger.error('error :: wind :: no POST data')
        logger.info('wind :: return 400 no POST data')
        results['status_code'] = 400
        results['reason'] = 'no POST data'
        return results

    required_keys = [
        'source_host', 'app', 'job', 'redis_work_hash', 'redis_work_hash_key',
        'data', 'results', 'results_url'
    ]
    valid_jobs = {
        'common_motifs': {
            'app': 'ionosphere',
            'redis_work_hash': 'ionosphere.find_repetitive_patterns.common_motifs.work',
        },
    }
    results_dict = {}
    for key in required_keys:
        if key not in post_data.keys():
            reason = 'missing parameter in POST data'
            logger.error('error :: wind :: no %s in POST data' % (
                key))
            logger.info('wind, return 400')
            results['status_code'] = 400
            results['reason'] = reason
            return results
        if key == 'source_host':
            source_host = post_data[key]
        if key == 'app':
            app = post_data[key]
        if key == 'job':
            job = post_data[key]
            if job not in list(valid_jobs.keys()):
                reason = 'invalid job passed'
                logger.error('error :: wind :: %s, job: %s' % (
                    reason, str(job)))
                logger.info('wind :: return 400')
                results['status_code'] = 400
                results['reason'] = reason
                return results
        if key == 'redis_work_hash':
            redis_work_hash = post_data[key]
        if key == 'redis_work_hash_key':
            redis_work_hash_key = post_data[key]
        if key == 'data':
            data = post_data[key]
            if not isinstance(data, dict):
                reason = 'invalid data passed'
                logger.error('error :: wind :: %s, data: %s' % (
                    reason, str(data)))
                logger.info('wind :: return 400')
                results['status_code'] = 400
                results['reason'] = reason
                return results
        if key == 'results_url':
            results_url = post_data[key]
        work_host = None
        if key == 'results':
            results_dict = post_data[key]
            if isinstance(results_dict, dict):
                if len(results_dict) > 0:
                    results_url = None
                try:
                    work_host = results_dict['work_host']
                except KeyError:
                    work_host = None
                if not work_host:
                    try:
                        work_host = post_data['work_host']
                    except KeyError:
                        work_host = None
    if job == 'common_motifs':
        if 'pw5_timeseries' not in post_data.keys():
            reason = 'invalid data passed'
            logger.error('error :: wind :: no pw5_timeseries passed')
            logger.info('wind :: return 400')
            results['status_code'] = 400
            results['reason'] = reason
            return results

    if results_url:
        logger.info('wind :: accepting %s job from %s' % (
            job, source_host))
    if results_dict:
        logger.info('wind :: accepting %s job results from %s' % (
            job, str(work_host)))

    try:
        redis_conn_decoded = get_redis_conn_decoded(skyline_app)
    except Exception as err:
        logger.error(traceback.format_exc())
        logger.error('error :: wind :: get_redis_conn_decoded failed, err: %s' % (
            err))
        logger.info('wind, return 500')
        results['status_code'] = 500
        results['reason'] = 'Redis failed'
        return results

    added_at = time()
    post_data['added_at'] = added_at
    new_redis_work_hash_key = str(float(redis_work_hash_key) + 1)
    post_data['redis_work_hash_key'] = new_redis_work_hash_key

    if results_dict:

        # @added 20250923 - Feature #5644: ionosphere.learn_self_validation 
        # Set the local key from external results
        if job == 'common_motifs':
            if 'fp_id' in post_data.keys():
                fp_id_str = str(post_data['fp_id'])
                try:
                    redis_conn_decoded.hset('ionosphere.common_motifs.learn_self_validation.work_done', fp_id_str, str(post_data))
                except Exception as err:
                    logger.error('error :: wind :: hset failed on ionosphere.common_motifs.learn_self_validation.work_done, err: %s' % (
                        err))

        try:
            redis_conn_decoded.hset(redis_work_hash, new_redis_work_hash_key, str(post_data))
            results['status_code'] = 200
            results['reason'] = 'OK'
            results['job'] = job
            job_result = 'results submitted for %s to %s' % (redis_work_hash_key, redis_work_hash)
            results['action'] = job_result
            logger.info('wind :: request results: %s' % str(results))
            return results
        except Exception as err:
            logger.error(traceback.format_exc())
            logger.error('error :: wind :: redis_conn_decoded failed to set %s in %s, err: %s' % (
                str(redis_work_hash_key), str(redis_work_hash), err))
            logger.info('wind, return 500')
            results['status_code'] = 500
            results['reason'] = 'Redis failed add results'
            logger.info('wind :: request results: %s' % str(results))
            return results
    if results_url:

        # @added 20250923 - Feature #5644: ionosphere.learn_self_validation 
        # Limit how many items can be added to the Add hash_key to the function
        if job == 'common_motifs':
            work_items_count = 0
            try:
                work_items_count = redis_conn_decoded.hlen(redis_work_hash)
            except Exception as err:
                logger.error('error :: wind :: redis_conn_decoded failed to hlen %s, err: %s' % (
                    str(redis_work_hash), err))
            if work_items_count >= 4:
                results['status_code'] = 204
                results['reason'] = 'full work queue'
                results['job'] = job
                job_result = 'job was not submitted %s' % redis_work_hash
                results['action'] = job_result
                logger.info('wind :: request results: %s' % str(results))
                return results        
        
        try:
            redis_conn_decoded.hset(redis_work_hash, new_redis_work_hash_key, str(post_data))
            results['status_code'] = 200
            results['reason'] = 'OK'
            results['job'] = job
            job_result = 'submitted job for %s to %s' % (new_redis_work_hash_key, redis_work_hash)
            results['action'] = job_result
            logger.info('wind :: request results: %s' % str(results))
            return results        
        except Exception as err:
            logger.error(traceback.format_exc())
            logger.error('error :: wind :: redis_conn_decoded failed to set %s in %s, err: %s' % (
                str(new_redis_work_hash_key), str(redis_work_hash), err))
            logger.info('wind, return 500')
            results['status_code'] = 500
            results['reason'] = 'Redis failed add results'
            logger.info('wind :: request results: %s' % str(results))
            return results

    logger.info('wind :: request results: %s' % str(results))
    return results
