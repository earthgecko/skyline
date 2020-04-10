from __future__ import division
import logging
import time
import re

import traceback

import settings
import skyline_version
from skyline_functions import RepresentsInt, mkdir_p, write_data_to_file
from tsfresh_feature_names import TSFRESH_FEATURES

from database import get_engine, ionosphere_table_meta, metrics_table_meta

skyline_version = skyline_version.__absolute_version__
skyline_app = 'webapp'
skyline_app_logger = '%sLog' % skyline_app
logger = logging.getLogger(skyline_app_logger)
skyline_app_logfile = '%s/%s.log' % (settings.LOG_PATH, skyline_app)
logfile = '%s/%s.log' % (settings.LOG_PATH, skyline_app)
try:
    ENABLE_WEBAPP_DEBUG = settings.ENABLE_WEBAPP_DEBUG
except:
    logger.error('error :: cannot determine ENABLE_WEBAPP_DEBUG from settings')
    ENABLE_WEBAPP_DEBUG = False

try:
    full_duration_seconds = int(settings.FULL_DURATION)
except:
    full_duration_seconds = 86400

full_duration_in_hours = full_duration_seconds / 60 / 60

# @added 20170102 - Feature #1838: utilites - ALERTS matcher
#                   Branch #922: ionosphere
#                   Task #1658: Patterning Skyline Ionosphere


def alerts_matcher(base_name, pattern, alerter, second_order_resolution_hours):

    """
    Get a list of all the metrics that would match an ALERTS pattern

    :param base_name: The metric name
    :param pattern: the ALERT pattern
    :param alerter: the alerter name e.g. smtp, syslog, hipchat, pagerdaty
    :param second_order_resolution_hours: (optional) The number of hours that
        Mirage should surface the metric timeseries for
    :type base_name: str
    :type pattern: str
    :type alerter: str
    :type second_order_resolution_hours: int
    :return: matched_by
    :rtype: str

        ('metric3', 'hipchat', 600),
        # Log all anomalies to syslog
        ('stats.', 'syslog', 1),
        # Wildcard namespaces can be used as well
        ('metric4.thing.*.requests', 'stmp', 900),
        # However beware of wildcards as the above wildcard should really be
        ('metric4.thing\..*.\.requests', 'stmp', 900),

    .. todo: This fully

    """
    logger.info('matching metric to ALERTS pattern :: %s - %s' % (base_name, pattern))

    # alert = ('stats_counts\..*', 'smtp', 3600, 168)
    # alert = ('.*\.mysql\..*', 'smtp', 7200, 168)
    alert = (pattern, alerter, second_order_resolution_hours)
    ALERT_MATCH_PATTERN = alert[0]
    new_base_name = base_name.replace('metrics.', '', 1)
    METRIC_PATTERN = new_base_name
    pattern_match = False
    matched_by = 'not matched'
    try:
        alert_match_pattern = re.compile(ALERT_MATCH_PATTERN)
        pattern_match = alert_match_pattern.match(METRIC_PATTERN)
        if pattern_match:
            matched_by = 'regex'
            # pattern_match = True
            print('matched_by %s' % matched_by)
    except:
        pattern_match = False
        print('not matched by regex')

    if not pattern_match:
        print('not matched by regex')
        if alert[0] in base_name:
            pattern_match = True
            matched_by = 'substring'
            print('%s' % matched_by)
        else:
            print('not matched in substring')

    if not pattern_match:
        print('not matched by %s in %s' % (base_name, alert[0]))
        if base_name == alert[0]:
            pattern_match = True
            matched_by = 'absolute_match'
            print('%s' % matched_by)
        else:
            print('not matched in substring')

    return matched_by
