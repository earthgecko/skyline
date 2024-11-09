"""
api_alias_features_profile.py
"""
import copy
import logging

from flask import request

from functions.database.queries.create_alias_fps_for_metrics import create_alias_fps_for_metrics
from functions.database.queries.get_alias_fps_for_metrics import get_alias_fps_for_metrics
from functions.database.queries.get_all_db_metric_names import get_all_db_metric_names
from functions.database.queries.get_fps_for_metrics import get_fps_for_metrics
from matched_or_regexed_in_list import matched_or_regexed_in_list


# @added 20241006 - Feature #5479: ionosphere.alias_features_profile
def api_alias_features_profile(current_skyline_app, user=None, user_id=0):
    """
    Return a dict with the aliased fps for a metric pattern.

    :param current_skyline_app: the app calling the function
    :type current_skyline_app: str
    :return: alias_features_profile_response_dict
    :rtype: dict

    """
    function_str = 'api_alias_features_profile'
    alias_features_profile_response_dict = {'form_data': False, 'response_format': 'json'}

    current_skyline_app_logger = current_skyline_app + 'Log'
    current_logger = logging.getLogger(current_skyline_app_logger)

    steps = [
        'get_candidate_alias_metrics',
        'exclude_candidate_metrics',
        'create_alias_fps',
        'created',
    ]

    def get_target_metrics(pattern, metric_names_with_ids):
        errors = []
        target_metrics = {}
        for base_name, metric_id in metric_names_with_ids.items():
            pattern_match = False
            try:
                pattern_match, matched_by = matched_or_regexed_in_list(current_skyline_app, base_name, [pattern], False)
                if pattern_match:
                    target_metrics[metric_id] = base_name
            except Exception as err:
                errors.append([base_name, metric_id, err])
        return target_metrics

    def get_candidate_metrics(metric_pattern, exclude_patterns, metric_names_with_ids):
        current_logger.info('api_alias_features_profile - get_candidate_metrics running with exclude_patterns: %s' % str(exclude_patterns))
        errors = []
        candidate_metrics = {}
        all_candidate_metrics = {}
        for base_name, metric_id in metric_names_with_ids.items():
            pattern_match = False
            try:
                pattern_match, matched_by = matched_or_regexed_in_list(current_skyline_app, base_name, [metric_pattern], False)
                if pattern_match:
                    if exclude_patterns:
                        exclude_pattern_match = False
                        try:
                            exclude_pattern_match, matched_by = matched_or_regexed_in_list(current_skyline_app, base_name, exclude_patterns, False)
                            if exclude_pattern_match:
                                continue
                        except Exception as err:
                            errors.append([base_name, metric_id, err])
                    all_candidate_metrics[metric_id] = base_name
            except Exception as err:
                errors.append([base_name, metric_id, err])
        all_candidate_metric_ids = list(all_candidate_metrics.keys())
        metric_ids_fps_dict = {}
        try:
            metric_ids_fps_dict = get_fps_for_metrics(current_skyline_app, all_candidate_metric_ids)
        except Exception as err:
            errors.append([base_name, metric_id, err])
        for metric_id, fps_dict in metric_ids_fps_dict.items():
            candidate_metrics[metric_id] = fps_dict
        return candidate_metrics

    # Test whether form or json POST
    form_data = False
    try:
        metric_pattern = request.form['metric_pattern']
        if metric_pattern:
            form_data = True
            alias_features_profile_response_dict['form_data'] = True
            current_logger.info('api_alias_features_profile, form POST')
            current_logger.info('api_alias_features_profile - metric_pattern passed: %s' % str(metric_pattern))
    except:
        current_logger.info('api_alias_features_profile no form data trying json')

    post_data = {}
    if not form_data:
        try:
            post_data = request.get_json()
        except Exception as err:
            current_logger.error('error :: api_alias_features_profile - no POST data, err: %s' % (
                err))
            current_logger.info('api_alias_features_profile, return 400 no POST data')
            alias_features_profile_response_dict['status_code'] = 400
            alias_features_profile_response_dict['error'] = 'no post data'
            return alias_features_profile_response_dict
        try:
            metric_pattern = post_data['data']['metric_pattern']
            if metric_pattern:
                current_logger.info('api_alias_features_profile - metric_pattern passed: %s' % str(metric_pattern))
        except KeyError:
            metric_pattern = None
        except Exception as err:
            metric_pattern = None
            current_logger.error('error :: api_alias_features_profile - evaluation of  post_data[\'data\'][\'metric_pattern\'] failed, err: %s' % (
                err))
    if not metric_pattern:
        current_logger.info('api_alias_features_profile, return 400 no metric_pattern determined')
        alias_features_profile_response_dict['status_code'] = 400
        alias_features_profile_response_dict['error'] = 'no metric_pattern argument'
        return alias_features_profile_response_dict

    alias_features_profile_response_dict['metric_pattern'] = metric_pattern

    step = None
    replace_pattern = None
    candidate_metrics = []
    candidate_metrics_list = []
    exclude_patterns = []
    alias_metrics = None
    create_aliases = False
    label = None
    response_format = 'html'
    dry_run = True

    if form_data:
        all_form_data = request.form
        form_data_dict = {}
        for key, value in all_form_data.items():
            form_data_dict[key] = value
        current_logger.info('api_alias_features_profile, form_data_dict: %s' % str(form_data_dict))
        try:
            step = request.form['step']
        except KeyError:
            step = None
        except Exception as err:
            current_logger.error('api_alias_features_profile no step, err: %s' % err)
        try:
            replace_pattern = request.form['replace_pattern']
        except KeyError:
            replace_pattern = None
        except Exception as err:
            current_logger.error('api_alias_features_profile no replace_pattern, err: %s' % err)
        try:
            candidate_metrics = request.form['candidate_metrics']
        except KeyError:
            candidate_metrics = None
        except Exception as err:
            current_logger.error('api_alias_features_profile no candidate_metrics, err: %s' % err)
        try:
            candidate_metrics_list = request.form['candidate_metrics_list']
        except KeyError:
            candidate_metrics_list = []
        except Exception as err:
            current_logger.error('api_alias_features_profile no candidate_metrics, err: %s' % err)
        try:
            exclude_patterns_str = request.form['exclude_patterns']
            exclude_patterns = []
            if isinstance(exclude_patterns_str, str):
                if ',' in exclude_patterns_str:
                    exclude_patterns = exclude_patterns_str.split(',')
                else:
                    exclude_patterns = [exclude_patterns_str]
            if isinstance(exclude_patterns_str, str):
                if exclude_patterns_str == '':
                    exclude_patterns = []
        except KeyError:
            exclude_patterns = []
        except Exception as err:
            current_logger.error('api_alias_features_profile no exclude_patterns, err: %s' % err)
        try:
            create_aliases = request.form['create_aliases']
        except KeyError:
            create_aliases = False
        except Exception as err:
            current_logger.error('api_alias_features_profile no create_aliases, err: %s' % err)
        if create_aliases:
            if str(create_aliases) == 'false':
                create_aliases = False
            if str(create_aliases) == 'true':
                create_aliases = True
        try:
            label = request.form['label']
        except KeyError:
            label = None
        except Exception as err:
            current_logger.error('api_alias_features_profile no label, err: %s' % err)
        try:
            passed_user = request.form['user']
            if len(passed_user) > 0:
                user = str(passed_user)
        except KeyError:
            passed_user = None
        except Exception as err:
            current_logger.error('api_alias_features_profile no user, err: %s' % err)
        try:
            passed_user_id = request.form['user_id']
            if passed_user_id:
                user_id = int(passed_user_id)
        except KeyError:
            passed_user_id = 0
        except Exception as err:
            current_logger.error('api_alias_features_profile no user_id, err: %s' % err)
        try:
            response_format = request.form['response_format']
        except KeyError:
            response_format = 'html'
        except Exception as err:
            current_logger.error('api_alias_features_profile no response_format, err: %s' % err)

    if not form_data:
        current_logger.info('api_alias_features_profile, post_data_dict: %s' % str(post_data))
        try:
            step = post_data['data']['step']
        except KeyError:
            step = None
        except Exception as err:
            current_logger.error('error :: api_alias_features_profile - evaluation of  post_data[\'data\'][\'step\'] failed, err: %s' % (
                err))
        try:
            replace_pattern = post_data['data']['replace_pattern']
        except KeyError:
            replace_pattern = None
        except Exception as err:
            current_logger.error('error :: api_alias_features_profile - evaluation of  post_data[\'data\'][\'replace_pattern\'] failed, err: %s' % (
                err))
        try:
            candidate_metrics = post_data['data']['candidate_metrics']
        except KeyError:
            candidate_metrics = []
        except Exception as err:
            current_logger.error('error :: api_alias_features_profile - evaluation of  post_data[\'data\'][\'candidate_metrics\'] failed, err: %s' % (
                err))
        try:
            candidate_metrics_list = post_data['data']['candidate_metrics_list']
        except KeyError:
            candidate_metrics_list = []
        except Exception as err:
            current_logger.error('error :: api_alias_features_profile - evaluation of  post_data[\'data\'][\'candidate_metrics\'] failed, err: %s' % (
                err))
        try:
            exclude_patterns = post_data['data']['exclude_patterns']
        except KeyError:
            exclude_patterns = []
        except Exception as err:
            current_logger.error('error :: api_alias_features_profile - evaluation of  post_data[\'data\'][\'exclude_patterns\'] failed, err: %s' % (
                err))
        try:
            create_aliases = post_data['data']['create_aliases']
            if create_aliases:
                if str(create_aliases) == 'false':
                    create_aliases = False
                if str(create_aliases) == 'true':
                    create_aliases = True
        except KeyError:
            create_aliases = False
        except Exception as err:
            current_logger.error('error :: api_alias_features_profile - evaluation of  post_data[\'data\'][\'create_aliases\'] failed, err: %s' % (
                err))
        try:
            label = post_data['data']['label']
        except KeyError:
            label = None
        except Exception as err:
            current_logger.error('error :: api_alias_features_profile - evaluation of  post_data[\'data\'][\'label\'] failed, err: %s' % (
                err))
        try:
            passed_user = post_data['data']['user']
            if passed_user:
                if len(passed_user) > 0:
                    user = str(passed_user)
        except KeyError:
            passed_user = None
        except Exception as err:
            current_logger.error('error :: api_alias_features_profile - evaluation of  post_data[\'data\'][\'user\'] failed, err: %s' % (
                err))
        try:
            passed_user_id = post_data['data']['user_id']
            if passed_user_id:
                user_id = int(passed_user_id)
        except KeyError:
            passed_user_id = None
        except Exception as err:
            current_logger.error('error :: api_alias_features_profile - evaluation of  post_data[\'data\'][\'user_id\'] failed, err: %s' % (
                err))
        try:
            response_format = post_data['data']['response_format']
        except KeyError:
            response_format = 'html'
        except Exception as err:
            current_logger.error('error :: api_alias_features_profile - evaluation of  post_data[\'data\'][\'response_format\'] failed, err: %s' % (
                err))

    if create_aliases:
        dry_run = False
    alias_features_profile_response_dict['dry_run'] = dry_run

    alias_features_profile_response_dict['response_format'] = response_format

    # @added 20241018 - Feature #5481: ionosphere.copy_features_profile
    fps_copied = {}
    alias_features_profile_response_dict['fps_copied'] = fps_copied

    if not step:
        current_logger.info('api_alias_features_profile, return 400 no step determined')
        alias_features_profile_response_dict['status_code'] = 400
        alias_features_profile_response_dict['error'] = 'no step argument'
        return alias_features_profile_response_dict

    if not replace_pattern:
        current_logger.info('api_alias_features_profile, return 400 no replace_pattern determined')
        alias_features_profile_response_dict['status_code'] = 400
        alias_features_profile_response_dict['error'] = 'no replace_pattern argument'
        return alias_features_profile_response_dict

    if replace_pattern:
        current_logger.info('api_alias_features_profile - replace_pattern passed: %s' % str(replace_pattern))

    if len(exclude_patterns) == 0:
        exclude_patterns = []
    if isinstance(exclude_patterns, str):
        if len(exclude_patterns) == 0:
            exclude_patterns = []
    if exclude_patterns == '':
        exclude_patterns = []

    alias_features_profile_response_dict['create_aliases'] = create_aliases
    alias_features_profile_response_dict['replace_pattern'] = replace_pattern
    alias_features_profile_response_dict['candidate_metrics'] = candidate_metrics
    alias_features_profile_response_dict['exclude_patterns'] = exclude_patterns
#    if form_data and exclude_patterns:
#        alias_features_profile_response_dict['exclude_patterns'] = exclude_patterns_str
    alias_features_profile_response_dict['label'] = label
    alias_features_profile_response_dict['user'] = user
    alias_features_profile_response_dict['user_id'] = user_id
    alias_features_profile_response_dict['all_alias_fps_preexist'] = False

    current_logger.debug('debug :: api_alias_features_profile :: pre-populated alias_features_profile_response_dict: %s' % (
        str(alias_features_profile_response_dict)))

    metric_names_with_ids = {}
    with_ids = True
    try:
        metric_names, metric_names_with_ids = get_all_db_metric_names(current_skyline_app, with_ids)
    except Exception as err:
        current_logger.error('error :: api_alias_features_profile :: get_all_db_metric_names failed, err: %s' % (
            err))
    metric_ids_with_names = {}
    for metric_name, metric_id in metric_names_with_ids.items():
        metric_ids_with_names[metric_id] = metric_name

    # The last step first
    if step == 'create_alias_fps':
        # Need:
        # alias_metrics
        # exclude_patterns
        # replace_pattern
        # Returns metric_ids_alias_fps_dict
        current_logger.info('api_alias_features_profile - step: %s' % str(step))
        try:
            candidate_metrics = get_candidate_metrics(metric_pattern, exclude_patterns, metric_names_with_ids)
        except Exception as err:
            current_logger.error('error :: api_alias_features_profile :: get_candidate_metrics failed, err: %s' % err)
            alias_features_profile_response_dict['status_code'] = 500
            alias_features_profile_response_dict['error'] = 'get_candidate_metrics failed'
            return alias_features_profile_response_dict

        candidate_metric_names_and_ids = {}
        candidate_metric_ids = []
        for metric_id in list(candidate_metrics.keys()):
            try:
                candidate_metric_name = metric_ids_with_names[metric_id]
                candidate_metric_names_and_ids[candidate_metric_name] = metric_id
            except:
                pass
        candidate_metric_ids = list(set(list(candidate_metric_names_and_ids.values())))

        candidate_metrics_list = list(candidate_metric_names_and_ids.keys())
        current_logger.info('api_alias_features_profile - len(candidate_metrics_list): %s' % (
            str(len(candidate_metrics_list))))

        target_metric_ids = []
        for candidate_metric in candidate_metrics_list:
            alias_metric_id = 0
            target_metric = candidate_metric.replace(metric_pattern, replace_pattern)
            if target_metric in metric_names_with_ids:
                alias_metric = str(target_metric)
                alias_metric_id = metric_names_with_ids[alias_metric]
                target_metric_ids.append(alias_metric_id)

        alias_fps = {}
        # TODO
        # Determine existing alias fps for idempotent operations
        metric_ids_alias_fps_dict = {}
        if len(candidate_metric_ids) > 0:
            try:
                metric_ids_alias_fps_dict = get_alias_fps_for_metrics(current_skyline_app, target_metric_ids)
            except Exception as err:
                current_logger.error('error :: api_alias_features_profile :: get_alias_fps_for_metrics failed, err: %s' % err)
                alias_features_profile_response_dict['status_code'] = 500
                alias_features_profile_response_dict['error'] = 'get_alias_fps_for_metrics failed'
                return alias_features_profile_response_dict
            current_logger.info('api_alias_features_profile :: %s alias features profiles determined for target_metric_ids' % (
                str(len(metric_ids_alias_fps_dict))))

        orphaned_fps = {}
        existing_alias_fp_ids = []
        for candidate_metric in candidate_metrics_list:
            alias_metric = None
            alias_metric_id = 0
            target_metric = candidate_metric.replace(metric_pattern, replace_pattern)
            if target_metric in metric_names_with_ids:
                alias_metric = str(target_metric)
                alias_metric_id = metric_names_with_ids[alias_metric]
            current_logger.info('api_alias_features_profile :: candidate_metric: %s, target_metric: %s, alias_metric: %s' % (
                candidate_metric, target_metric, str(alias_metric)))
            candidate_metric_id = candidate_metric_names_and_ids[candidate_metric]
            for fp_id in list(candidate_metrics[candidate_metric_id].keys()):
                alias_exists = False
                if alias_metric_id in metric_ids_alias_fps_dict.keys():
                    if fp_id in metric_ids_alias_fps_dict[alias_metric_id].keys():
                        alias_exists = True
                        existing_alias_fp_ids.append(fp_id)
                if alias_metric and alias_metric_id:
                    alias_fps[fp_id] = {
                        'fp_id': fp_id,
                        'metric_id': alias_metric_id,
                        'metric': alias_metric,
                        'original_metric_id': candidate_metric_id,
                        'original_metric': candidate_metric,
                        'exists': alias_exists,
                        'label': label,
                        'user_id': user_id,
                    }
                else:
                    orphaned_fps[fp_id] = {
                        'fp_id': fp_id,
                        'metric_id': candidate_metric_id,
                        'metric': candidate_metric,
                    }

        if len(alias_fps) == len(existing_alias_fp_ids):
            alias_features_profile_response_dict['all_alias_fps_preexist'] = True

        if not create_aliases:
            alias_features_profile_response_dict['candidate_metrics'] = candidate_metrics
            alias_features_profile_response_dict['candidate_metrics_list'] = candidate_metrics_list
            alias_features_profile_response_dict['orphaned_fps'] = orphaned_fps
            alias_features_profile_response_dict['target_alias_fps'] = alias_fps
            current_logger.debug('debug :: api_alias_features_profile :: alias_features_profile_response_dict: %s' % (
                str(alias_features_profile_response_dict)))
            return alias_features_profile_response_dict

        alias_fps_ids = list(alias_fps.keys())
        alias_fps_to_create = {}
        for fp_id in alias_fps_ids:
            if fp_id not in existing_alias_fp_ids:
                alias_fps_to_create[fp_id] = copy.deepcopy(alias_fps[fp_id])
        alias_fps_to_created = {}
        if len(alias_fps_to_create) > 0:
            try:
                # @modified 20241018 - Feature #5481: ionosphere.copy_features_profile
                # Added fps_copied
                alias_fps_to_created, fps_copied = create_alias_fps_for_metrics(current_skyline_app, alias_fps_to_create)
            except Exception as err:
                current_logger.error('error :: api_alias_features_profile :: create_alias_fps_for_metrics failed, err: %s' % err)
                alias_features_profile_response_dict['status_code'] = 500
                alias_features_profile_response_dict['error'] = 'create_alias_fps_for_metrics failed'
                return alias_features_profile_response_dict
            for fp_id in alias_fps_to_created.keys():
                alias_fps[fp_id] = copy.deepcopy(alias_fps_to_created[fp_id])
            
        if len(alias_fps_to_created) > 0:
            alias_features_profile_response_dict['aliased_fps'] = alias_fps_to_created

        if len(alias_fps) == len(existing_alias_fp_ids):
            alias_features_profile_response_dict['all_alias_fps_preexist'] = True

        # @added 20241018 - Feature #5481: ionosphere.copy_features_profile
        alias_features_profile_response_dict['fps_copied'] = fps_copied

        return alias_features_profile_response_dict

    if step == 'get_candidate_alias_metrics':
        # Need:
        # metric_pattern
        # Returns candidate_fp_metrics
        try:
            candidate_metrics = get_candidate_metrics(metric_pattern, exclude_patterns, metric_names_with_ids)
        except Exception as err:
            current_logger.error('error :: api_alias_features_profile :: get_candidate_metrics failed, err: %s' % err)
            alias_features_profile_response_dict['status_code'] = 500
            alias_features_profile_response_dict['error'] = 'get_candidate_metrics failed'
            return alias_features_profile_response_dict
        alias_features_profile_response_dict['candidate_metrics'] = candidate_metrics
        candidate_metrics_list = []
        for metric_id in list(candidate_metrics.keys()):
            try:
                metric_name = metric_ids_with_names[metric_id]
                candidate_metrics_list.append(metric_name)
            except:
                pass
        alias_features_profile_response_dict['candidate_metrics_list'] = list(set(candidate_metrics_list))

        return alias_features_profile_response_dict

    current_logger.info('%s :: %s alias_features_profile returned' % (
        function_str, str(len(alias_features_profile_response_dict))))
    return alias_features_profile_response_dict
