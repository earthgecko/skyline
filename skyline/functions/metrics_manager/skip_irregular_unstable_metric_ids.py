"""
skip_irregular_unstable_metric_ids.py
"""
import logging
import traceback
from time import time

import settings
from matched_or_regexed_in_list import matched_or_regexed_in_list
try:
    SKIP_IRREGULAR_UNSTABLE = settings.SKIP_IRREGULAR_UNSTABLE
except:
    SKIP_IRREGULAR_UNSTABLE = []

skyline_app = 'analyzer'
skyline_app_logger = '%sLog' % skyline_app
logger = logging.getLogger(skyline_app_logger)


# @added 20230505 - Feature #4848: mirage - analyse.irregular.unstable.timeseries.at.30days
def build_skip_irregular_unstable_metric_ids(self, metrics_with_ids, external_settings):
    """
    Determine the list of metric ids that need to be skipped from
    irregular_unstable_timeseries analysis determined from the list
    of active metric base_names and ids, using the list from the
    skip_irregular_unstable key from external_settings namespaces
    and :mod:`settings.SKIP_IRREGULAR_UNSTABLE`.

    :param self: the self object
    :param metrics_with_ids: the active base_names with id dict
    :param external_settings: the external_settings dict
    :type self: object
    :type metrics_with_ids: dict
    :type external_settings: dict
    :return: Count of the metrics to skip
    :rtype: int

    """
    start = time()

    skip_irregular_unstable_metric_ids = []

    active_base_names = list(metrics_with_ids.keys())

    errors = []
    if external_settings:
        for config_id in list(external_settings):
            skip_irregular_unstable = []
            skipping_irregular_unstable = []
            namespace = None
            try:
                namespace = external_settings[config_id]['namespace']
            except KeyError:
                continue
            except Exception as err:
                logger.error(traceback.format_exc())
                logger.error('error :: metrics_manager :: determining namespace from external_setting %s - %s' % (
                    str(config_id), err))
                continue
            if not namespace:
                continue
            dotted_namespace = '%s.' % namespace
            labelled_namespace = '_tenant_id="%s"' % namespace
            namespaces = [dotted_namespace, labelled_namespace]
            namespace_metrics = [base_name for base_name in active_base_names if dotted_namespace in base_name or labelled_namespace in base_name]
            # If the namespace is an external namespace remove the metrics
            # from the active_base_names list so they are not evaluated by
            # the local settings.SKIP_IRREGULAR_UNSTABLE patterns
            for base_name in namespace_metrics:
                active_base_names.remove(base_name)

            try:
                skip_irregular_unstable = external_settings[config_id]['skip_irregular_unstable']
            except KeyError:
                continue
            except Exception as err:
                logger.error(traceback.format_exc())
                logger.error('error :: metrics_manager :: build_skip_irregular_unstable_metric_ids :: determining skip_irregular_unstable from external_setting - %s, err: %s' % (
                    str(config_id), err))
            for base_name in namespace_metrics:
                try:
                    pattern_match, metric_matched_by = matched_or_regexed_in_list(skyline_app, base_name, skip_irregular_unstable)
                    try:
                        del metric_matched_by
                    except:
                        pass
                except Exception as err:
                    errors.append([base_name, 'pattern_match', str(err)])
                    pattern_match = False
                if pattern_match:
                    try:
                        metric_id = int(metrics_with_ids[base_name])
                        skip_irregular_unstable_metric_ids.append(metric_id)
                        skipping_irregular_unstable.append(base_name)
                    except Exception as err:
                        errors.append([base_name, 'failed to determine metric_id', str(err)])
            logger.info('metrics_manager :: build_skip_irregular_unstable_metric_ids :: determined %s irregular_unstable metrics to skip for %s' % (
                str(len(skipping_irregular_unstable)), namespace))
        logger.info('metrics_manager :: build_skip_irregular_unstable_metric_ids :: determined %s irregular_unstable metrics to skip from namespaces in external_settings' % (
            str(len(skip_irregular_unstable_metric_ids))))
        logger.info('metrics_manager :: build_skip_irregular_unstable_metric_ids :: namespaces in external_settings processed in %s seconds' % (
            str((time() - start))))

    if SKIP_IRREGULAR_UNSTABLE:
        start_local = time()
        logger.info('metrics_manager :: build_skip_irregular_unstable_metric_ids :: evaluating %s metrics to skip' % (
            str(len(active_base_names))))
        skipping_irregular_unstable = []
        for base_name in active_base_names:
            try:
                pattern_match, metric_matched_by = matched_or_regexed_in_list(skyline_app, base_name, SKIP_IRREGULAR_UNSTABLE)
                try:
                    del metric_matched_by
                except:
                    pass
            except Exception as err:
                errors.append([base_name, 'pattern_match', str(err)])
                pattern_match = False
            if pattern_match:
                try:
                    metric_id = int(metrics_with_ids[base_name])
                    skip_irregular_unstable_metric_ids.append(metric_id)
                    skipping_irregular_unstable.append(base_name)
                except Exception as err:
                    errors.append([base_name, 'failed to determine metric_id', str(err)])
        logger.info('metrics_manager :: build_skip_irregular_unstable_metric_ids :: determined %s irregular_unstable metrics to skip from settings.SKIP_IRREGULAR_UNSTABLE' % (
            str(len(skipping_irregular_unstable))))
        logger.info('metrics_manager :: build_skip_irregular_unstable_metric_ids :: local namespaces processed in %s seconds' % (
            str((time() - start_local))))

    success = 0
    if skip_irregular_unstable_metric_ids:
        try:
            success = self.redis_conn_decoded.sadd('metrics_manager.skip_irregular_unstable_metric_ids', *set(skip_irregular_unstable_metric_ids))
        except Exception as err:
            logger.error('error :: metrics_manager :: build_skip_irregular_unstable_metric_ids :: failed to create Redis set metrics_manager.skip_irregular_unstable_metric_ids - %s' % (
                err))
        if success:
            try:
                self.redis_conn_decoded.rename('metrics_manager.skip_irregular_unstable_metric_ids', 'aet.metrics_manager.skip_irregular_unstable_metric_ids')
                logger.info('metrics_manager :: build_skip_irregular_unstable_metric_ids :: created aet.metrics_manager.skip_irregular_unstable_metric_ids with %s ids' % (
                    str(success)))
            except:
                pass

    logger.info('metrics_manager :: build_skip_irregular_unstable_metric_ids :: took %s seconds' % (
        str((time() - start))))

    return success