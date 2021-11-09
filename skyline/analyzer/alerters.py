from __future__ import division, print_function
import logging
import hashlib
from smtplib import SMTP
import alerters
try:
    import urllib2
except ImportError:
    import urllib.request
    import urllib.error

# @added 20191023 - Task #3290: Handle urllib2 in py3
#                   Branch #3262: py3
# Use urlretrieve
try:
    import urllib2 as urllib
except ImportError:
    from urllib import request as urllib

from ast import literal_eval
from requests.utils import quote

# Added for graphs showing Redis data
import traceback
# import redis
from msgpack import Unpacker
import datetime as dt
# @added 20180809 - Bug #2498: Incorrect scale in some graphs
# @modified 20181025 - Feature #2618: alert_slack
# Added gmtime and strftime
# @modified 20191030 - Branch #3262: py3
# from time import (time, gmtime, strftime)
from time import gmtime, strftime, time
from email import charset

# @added 20201127 - Feature #3820: HORIZON_SHARDS
from os import uname

# @added 20200116: Feature #3396: http_alerter
import requests

# @modified 20160820 - Issue #23 Test dependency updates
# Use Agg for matplotlib==1.5.2 upgrade, backwards compatibile
import matplotlib
matplotlib.use('Agg')
# @modified 20161228 - Feature #1828: ionosphere - mirage Redis data features
# Handle flake8 E402
if True:
    import matplotlib.pyplot as plt
    from matplotlib.pylab import rcParams
    from matplotlib.dates import DateFormatter
    # @modified 20191030 - Branch #3262: py3
    # import io
    import numpy as np
    import pandas as pd
    import syslog
    import os.path
    import sys
    import resource

sys.path.append(os.path.join(os.path.dirname(os.path.realpath(__file__)), os.pardir))
sys.path.insert(0, os.path.dirname(__file__))

python_version = int(sys.version_info[0])
if python_version == 2:
    from email.MIMEMultipart import MIMEMultipart
    from email.MIMEText import MIMEText
    from email.MIMEImage import MIMEImage
if python_version == 3:
    from email.mime.multipart import MIMEMultipart
    from email.mime.text import MIMEText
    from email.mime.image import MIMEImage

# @modified 20161228 - Feature #1828: ionosphere - mirage Redis data features
# Handle flake8 E402
if True:
    import settings
    import skyline_version
    from skyline_functions import (
        write_data_to_file, mkdir_p,
        # @added 20170603 - Feature #2034: analyse_derivatives
        # nonNegativeDerivative, in_list,
        nonNegativeDerivative,
        # @added 20191030 - Bug #3266: py3 Redis binary objects not strings
        #                   Branch #3262: py3
        # Added a single functions to deal with Redis connection and the
        # charset='utf-8', decode_responses=True arguments required in py3
        get_redis_conn, is_derivative_metric,
        # @modified 20191105 - Task #3290: Handle urllib2 in py3
        #                      Branch #3262: py3
        get_graphite_graph_image,
        # @modified 20191105 - Branch #3002: docker
        #                      Branch #3262: py3
        get_graphite_port, get_graphite_render_uri, get_graphite_custom_headers,
        # @added 20200116: Feature #3396: http_alerter
        get_redis_conn_decoded,
        # @added 20200507 - Feature #3532: Sort all time series
        sort_timeseries,
        # @added 20200825 - Feature #3704: Add alert to anomalies
        add_panorama_alert,
        # @added 20201013 - Feature #3780: skyline_functions - sanitise_graphite_url
        encode_graphite_metric_name)
    # @added 20210724 - Feature #4196: functions.aws.send_sms
    from functions.aws.send_sms import send_sms
    from functions.settings.get_sms_recipients import get_sms_recipients

# @added 20201127 - Feature #3820: HORIZON_SHARDS
try:
    HORIZON_SHARDS = settings.HORIZON_SHARDS.copy()
except:
    HORIZON_SHARDS = {}
this_host = str(uname()[1])
HORIZON_SHARD = 0
if HORIZON_SHARDS:
    HORIZON_SHARD = HORIZON_SHARDS[this_host]

skyline_app = 'analyzer'
skyline_app_logger = '%sLog' % skyline_app
logger = logging.getLogger(skyline_app_logger)
skyline_app_logfile = '%s/%s.log' % (settings.LOG_PATH, skyline_app)

skyline_version = skyline_version.__absolute_version__

LOCAL_DEBUG = False

"""
Create any alerter you want here. The function will be invoked from
trigger_alert.

Three arguments will be passed, two of them tuples: alert and metric, and context

alert: the tuple specified in your settings:\n
    alert[0]: The matched substring of the anomalous metric\n
    alert[1]: the name of the strategy being used to alert\n
    alert[2]: The timeout of the alert that was triggered\n
        alert[4]: The type [optional for http_alerter only] (dict)\n
                  The snab_details [optional for SNAB and slack only] (list)\n
        alert[5]: The snab_details [optional for SNAB and slack only] (list)\n
                  The anomaly_id [optional for http_alerter only] (list)\n
metric: information about the anomaly itself\n
    metric[0]: the anomalous value\n
    metric[1]: The full name of the anomalous metric\n
    metric[2]: anomaly timestamp\n
context: app name

"""

# FULL_DURATION to hours so that Analyzer surfaces the relevant timeseries data
# in the graph
try:
    full_duration_seconds = int(settings.FULL_DURATION)
except:
    full_duration_seconds = 86400
full_duration_in_hours = full_duration_seconds / 60 / 60

# @added 20190523 - Branch #3002: docker
try:
    DOCKER_FAKE_EMAIL_ALERTS = settings.DOCKER_FAKE_EMAIL_ALERTS
except:
    DOCKER_FAKE_EMAIL_ALERTS = False


def alert_smtp(alert, metric, context):
    """
    Called by :func:`~trigger_alert` and sends an alert via smtp to the
    recipients that are configured for the metric.

    """
    LOCAL_DEBUG = False
    # logger = logging.getLogger(skyline_app_logger)
    if settings.ENABLE_DEBUG or LOCAL_DEBUG:
        logger.info('debug :: alert_smtp - sending smtp alert')
        logger.info('debug :: alert_smtp - Memory usage at start: %s (kb)' % resource.getrusage(resource.RUSAGE_SELF).ru_maxrss)

    # FULL_DURATION to hours so that analyzer surfaces the relevant timeseries data
    # in the graph
    full_duration_in_hours = int(settings.FULL_DURATION) / 3600

    # @added 20161229 - Feature #1830: Ionosphere alerts
    # Added Ionosphere variables

    # @modified 20200728 - Bug #3652: Handle multiple metrics in base_name conversion
    # base_name = str(metric[1]).replace(settings.FULL_NAMESPACE, '', 1)
    metric_name = str(metric[1])
    if metric_name.startswith(settings.FULL_NAMESPACE):
        base_name = metric_name.replace(settings.FULL_NAMESPACE, '', 1)
    else:
        base_name = metric_name

    # @modified 20190520 - Branch #3002: docker
    # wix-playground added a hashlib hexdigest method which has not been
    # verified so using the originaly method useless the base_name is longer
    # than 254 chars
    if len(base_name) > 254:
        base_name = hashlib.sha224(str(metric[1]).replace(
            settings.FULL_NAMESPACE, '', 1)).hexdigest()
    if settings.IONOSPHERE_ENABLED:
        timeseries_dir = base_name.replace('.', '/')
        training_data_dir = '%s/%s/%s' % (
            settings.IONOSPHERE_DATA_FOLDER, str(int(metric[2])),
            timeseries_dir)
        graphite_image_file = '%s/%s.%s.graphite.%sh.png' % (
            training_data_dir, base_name, skyline_app,
            str(int(full_duration_in_hours)))
        json_file = '%s/%s.%s.redis.%sh.json' % (
            training_data_dir, base_name, skyline_app,
            str(int(full_duration_in_hours)))
        training_data_redis_image = '%s/%s.%s.redis.plot.%sh.png' % (
            training_data_dir, base_name, skyline_app,
            str(int(full_duration_in_hours)))
    # @added 20181006 - Feature #2618: alert_slack
    else:
        graphite_image_file = None

    # For backwards compatibility
    if '@' in alert[1]:
        sender = str(settings.ALERT_SENDER)
        recipients = alert[1]
    else:
        sender = settings.SMTP_OPTS['sender']
        # @modified 20160806 - Added default_recipient
        try:
            recipients = settings.SMTP_OPTS['recipients'][alert[0]]
            use_default_recipient = False
        except:
            use_default_recipient = True

        # @added 20200610 - Feature #3560: External alert config
        # If the alert is for an external alerter set to no_email
        if use_default_recipient:
            try:
                if alert[4]['type'] == 'external':
                    recipients = 'no_email'
                    use_default_recipient = False
            except:
                pass

        if use_default_recipient:
            try:
                recipients = settings.SMTP_OPTS['default_recipient']
                logger.info(
                    'alert_smtp - using default_recipient as no recipients are configured for %s' %
                    str(alert[0]))
            except:
                logger.error(
                    'error :: alert_smtp - no known recipient for %s' %
                    str(alert[0]))
                return False

    # Backwards compatibility
    if type(recipients) is str:
        recipients = [recipients]

    # @added 20180524 - Task #2384: Change alerters to cc other recipients
    # The alerters did send an individual email to each recipient. This would be
    # more useful if one email was sent with the first smtp recipient being the
    # to recipient and the subsequent recipients were add in cc.
    if recipients:
        primary_recipient = False
        cc_recipients = False
        for i_recipient in recipients:
            if not primary_recipient:
                primary_recipient = str(i_recipient)
            if primary_recipient != i_recipient:
                if not cc_recipients:
                    cc_recipients = str(i_recipient)
                else:
                    new_cc_recipients = '%s,%s' % (str(cc_recipients), str(i_recipient))
                    cc_recipients = str(new_cc_recipients)
        logger.info(
            'alert_smtp - will send to primary_recipient :: %s, cc_recipients :: %s' %
            (str(primary_recipient), str(cc_recipients)))

    # @modified 20161229 - Feature #1830: Ionosphere alerts
    # Ionosphere alerts
    # @added 20191002 - Feature #3194: Add CUSTOM_ALERT_OPTS to settings
    # Added as it was not interpolated in v1.3.0 and v1.3.1
    try:
        main_alert_title = settings.CUSTOM_ALERT_OPTS['main_alert_title']
    except:
        main_alert_title = 'Skyline'
    alert_context = context
    if context == 'Analyzer':
        try:
            alert_context = settings.CUSTOM_ALERT_OPTS['analyzer_alert_heading']
        except:
            alert_context = 'Ionosphere'
    if context == 'Analyzer':
        try:
            alert_context = settings.CUSTOM_ALERT_OPTS['analyzer_alert_heading']
        except:
            alert_context = 'Analyzer'
    if context == 'Ionosphere':
        try:
            alert_context = settings.CUSTOM_ALERT_OPTS['ionosphere_alert_heading']
        except:
            alert_context = 'Ionosphere'

    # @modified 20190820 - Feature #3194: Add CUSTOM_ALERT_OPTS to settings
    # unencoded_graph_title = 'Skyline %s - ALERT at %s hours - %s' % (
    #     context, str(int(full_duration_in_hours)), str(metric[0]))
    # @modified 20191002 - Feature #3194: Add CUSTOM_ALERT_OPTS to settings
    # Use alert_context
    # unencoded_graph_title = '%s %s - ALERT at %s hours - %s' % (
    #     main_alert_title, context, str(int(full_duration_in_hours)),
    #     str(metric[0]))
    unencoded_graph_title = '%s %s - ALERT at %s hours - %s' % (
        main_alert_title, alert_context, str(int(full_duration_in_hours)),
        str(metric[0]))
    # @modified 20170603 - Feature #2034: analyse_derivatives
    # Added deriative functions to convert the values of metrics strictly
    # increasing monotonically to their deriative products in alert graphs and
    # specify it in the graph_title
    known_derivative_metric = False
    try:
        # @modified 20180519 - Feature #2378: Add redis auth to Skyline and rebrow
        # @modified 20191030 - Bug #3266: py3 Redis binary objects not strings
        #                      Branch #3262: py3
        # Use get_redis_conn_decoded
        # if settings.REDIS_PASSWORD:
        #     REDIS_ALERTER_CONN = redis.StrictRedis(password=settings.REDIS_PASSWORD, unix_socket_path=settings.REDIS_SOCKET_PATH)
        # else:
        #     REDIS_ALERTER_CONN = redis.StrictRedis(unix_socket_path=settings.REDIS_SOCKET_PATH)
        REDIS_ALERTER_CONN = get_redis_conn(skyline_app)
    except:
        logger.error(traceback.format_exc())
        logger.error('error :: alert_smtp - redis connection failed')

    # @modified 20191030 - Bug #3266: py3 Redis binary objects not strings
    #                      Branch #3262: py3
    # Use is_derivative_metric function
    # try:
    #    derivative_metrics = list(REDIS_ALERTER_CONN.smembers('derivative_metrics'))
    # except:
    #     derivative_metrics = []
    # redis_metric_name = '%s%s' % (settings.FULL_NAMESPACE, str(base_name))
    # if redis_metric_name in derivative_metrics:
    #    known_derivative_metric = True
    # if known_derivative_metric:
    #     try:
    #         non_derivative_monotonic_metrics = settings.NON_DERIVATIVE_MONOTONIC_METRICS
    #     except:
    #         non_derivative_monotonic_metrics = []
    #     skip_derivative = in_list(redis_metric_name, non_derivative_monotonic_metrics)
    #     if skip_derivative:
    #         known_derivative_metric = False

    known_derivative_metric = is_derivative_metric(skyline_app, base_name)

    if known_derivative_metric:
        # @modified 20191002 - Feature #3194: Add CUSTOM_ALERT_OPTS to settings
        # Use main_alert_title and alert_context
        # unencoded_graph_title = 'Skyline %s - ALERT at %s hours - derivative graph - %s' % (
        #     context, str(int(full_duration_in_hours)), str(metric[0]))
        unencoded_graph_title = '%s %s - ALERT at %s hours - derivative graph - %s' % (
            main_alert_title, alert_context, str(int(full_duration_in_hours)),
            str(metric[0]))

    # @added 20200907 - Feature #3734: waterfall alerts
    # Add waterfall alert to title
    waterfall_alert_check_string = '%s.%s' % (str(metric[2]), base_name)
    redis_waterfall_alert_key = 'analyzer.waterfall.alert.%s' % waterfall_alert_check_string
    waterfall_alert = False
    try:
        waterfall_alert = REDIS_ALERTER_CONN.get(redis_waterfall_alert_key)
    except:
        logger.info(traceback.format_exc())
        logger.error('error :: failed to add Redis key - %s for waterfall alert' % (
            redis_waterfall_alert_key))
    if waterfall_alert:
        unencoded_graph_title = unencoded_graph_title.replace('ALERT', 'WATERFALL%20ALERT')

    if settings.ENABLE_DEBUG or LOCAL_DEBUG:
        logger.info('debug :: alert_smtp - unencoded_graph_title: %s' % unencoded_graph_title)
    graph_title_string = quote(unencoded_graph_title, safe='')
    graph_title = '&title=%s' % graph_title_string

    # @added 20180809 - Bug #2498: Incorrect scale in some graphs
    # If -xhours is used the scale is incorrect if x hours > than first
    # retention period, passing from and until renders the graph with the
    # correct scale.
    # @modified 20181009 - Feature #2618: alert_slack
    #                      Bug #2498: Incorrect scale in some graphs
    # Corrected the from_timestamp and until_timestamp as they were incorrectly
    # order, however Graphite still rendered the correct graph as it plotted
    # reverse, which is the same.  Also using the metric[0] value instead of
    # time()
    # from_timestamp = int(time())
    # until_timestamp = from_timestamp - full_duration_seconds
    until_timestamp = int(metric[2])
    from_timestamp = until_timestamp - full_duration_seconds

    # @added 20190518 - Branch #3002: docker
    graphite_port = get_graphite_port(skyline_app)
    graphite_render_uri = get_graphite_render_uri(skyline_app)
    graphite_custom_headers = get_graphite_custom_headers(skyline_app)

    graphite_from = dt.datetime.fromtimestamp(int(from_timestamp)).strftime('%H:%M_%Y%m%d')
    logger.info('graphite_from - %s' % str(graphite_from))
    graphite_until = dt.datetime.fromtimestamp(int(until_timestamp)).strftime('%H:%M_%Y%m%d')
    logger.info('graphite_until - %s' % str(graphite_until))

    # @added 20201013 - Feature #3780: skyline_functions - sanitise_graphite_url
    encoded_graphite_metric_name = encode_graphite_metric_name(skyline_app, base_name)

    # @modified 20180809 - Bug #2498: Incorrect scale in some graphs
    # link = '%s://%s:%s/render/?from=-%shours&target=cactiStyle(%s)%s%s&colorList=orange' % (
    #     settings.GRAPHITE_PROTOCOL, settings.GRAPHITE_HOST,
    #     graphite_port, str(int(full_duration_in_hours)), metric[1],
    #     settings.GRAPHITE_GRAPH_SETTINGS, graph_title)
    # @modified 20190518 - Branch #3002: docker
    # Use GRAPHITE_RENDER_URI
    # link = '%s://%s:%s/render/?from=%s&until=%s&target=cactiStyle(%s)%s%s&colorList=orange' % (
    #     settings.GRAPHITE_PROTOCOL, settings.GRAPHITE_HOST, graphite_port,
    #     str(graphite_from), str(graphite_until), metric[1],
    #     settings.GRAPHITE_GRAPH_SETTINGS, graph_title)
    # @modified 20191106 - Task #3294: py3 - handle system parameter in Graphite cactiStyle
    # link = '%s://%s:%s/%s?from=%s&until=%s&target=cactiStyle(%s)%s%s&colorList=orange' % (
    link = '%s://%s:%s/%s?from=%s&until=%s&target=cactiStyle(%s,%%27si%%27)%s%s&colorList=orange' % (
        settings.GRAPHITE_PROTOCOL, settings.GRAPHITE_HOST, graphite_port,
        graphite_render_uri, str(graphite_from), str(graphite_until),
        # @modified 20201013 - Feature #3780: skyline_functions - sanitise_graphite_url
        # metric[1], settings.GRAPHITE_GRAPH_SETTINGS, graph_title)
        encoded_graphite_metric_name, settings.GRAPHITE_GRAPH_SETTINGS, graph_title)

    # @added 20170603 - Feature #2034: analyse_derivatives
    if known_derivative_metric:
        # @modified 20180809 - Bug #2498: Incorrect scale in some graphs
        # link = '%s://%s:%s/render/?from=-%shours&target=cactiStyle(nonNegativeDerivative(%s))%s%s&colorList=orange' % (
        #     settings.GRAPHITE_PROTOCOL, settings.GRAPHITE_HOST,
        #     graphite_port, str(int(full_duration_in_hours)), metric[1],
        #     settings.GRAPHITE_GRAPH_SETTINGS, graph_title)
        # @modified 20190518 - Branch #3002: docker
        # Use GRAPHITE_RENDER_URI
        # link = '%s://%s:%s/render/?from=%s&until=%s&target=cactiStyle(nonNegativeDerivative(%s))%s%s&colorList=orange' % (
        #     settings.GRAPHITE_PROTOCOL, settings.GRAPHITE_HOST,
        #     graphite_port, str(graphite_from), str(graphite_until), metric[1],
        #     settings.GRAPHITE_GRAPH_SETTINGS, graph_title)
        # @modified 20191106 - Task #3294: py3 - handle system parameter in Graphite cactiStyle
        # link = '%s://%s:%s/%s?from=%s&until=%s&target=cactiStyle(nonNegativeDerivative(%s))%s%s&colorList=orange' % (
        link = '%s://%s:%s/%s?from=%s&until=%s&target=cactiStyle(nonNegativeDerivative(%s),%%27si%%27)%s%s&colorList=orange' % (
            settings.GRAPHITE_PROTOCOL, settings.GRAPHITE_HOST, graphite_port,
            graphite_render_uri, str(graphite_from),
            # @modified 20201013 - Feature #3780: skyline_functions - sanitise_graphite_url
            # str(graphite_until), metric[1], settings.GRAPHITE_GRAPH_SETTINGS,
            str(graphite_until), encoded_graphite_metric_name, settings.GRAPHITE_GRAPH_SETTINGS,
            graph_title)

    content_id = metric[1]
    image_data = None
    if settings.SMTP_OPTS.get('embed-images'):
        # @added 20161229 - Feature #1830: Ionosphere alerts
        # Use existing data if files exist
        if os.path.isfile(graphite_image_file):
            try:
                # @added 20191107 - Branch #3262: py3
                # Open in binary mode for py3
                # with open(graphite_image_file, 'r') as f:
                with open(graphite_image_file, 'rb') as f:
                    image_data = f.read()
                logger.info('alert_smtp - using existing png - %s' % graphite_image_file)
            except:
                logger.error(traceback.format_exc())
                logger.error('error :: alert_smtp - failed to read image data from existing png - %s' % graphite_image_file)
                logger.error('error :: alert_smtp - %s' % str(link))
                image_data = None

        # @added 20191105 - Task #3290: Handle urllib2 in py3
        #                   Branch #3262: py3
        if image_data is None:
            get_graphite_graph_image(skyline_app, link, graphite_image_file)
            if os.path.isfile(graphite_image_file):
                try:
                    # @modified 20191107 - Branch #3262: py3
                    # Open in binary mode for py3
                    # with open(graphite_image_file, 'r') as f:
                    with open(graphite_image_file, 'rb') as f:
                        image_data = f.read()
                    logger.info('alert_smtp - using existing png - %s' % graphite_image_file)
                except:
                    logger.error(traceback.format_exc())
                    logger.error('error :: alert_smtp - failed to read image data from existing png - %s' % graphite_image_file)
                    logger.error('error :: alert_smtp - %s' % str(link))
                    image_data = None

        if image_data is None:
            try:
                # @modified 20170913 - Task #2160: Test skyline with bandit
                # Added nosec to exclude from bandit tests
                # @modified 20190520 - Branch #3002: docker
                # image_data = urllib2.urlopen(link).read()  # nosec
                if graphite_custom_headers:
                    # @modified 20191021 - Task #3290: Handle urllib2 in py3
                    #                      Branch #3262: py3
                    # request = urllib2.Request(link, headers=graphite_custom_headers)
                    if python_version == 2:
                        request = urllib.Request(link, headers=graphite_custom_headers)
                    if python_version == 3:
                        request = urllib.request(link, headers=graphite_custom_headers)
                else:
                    # @modified 20191021 - Task #3290: Handle urllib2 in py3
                    #                      Branch #3262: py3
                    # request = urllib2.Request(link)
                    if python_version == 2:
                        # @modified 20200808 - Task #3608: Update Skyline to Python 3.8.3 and deps
                        # Added nosec for bandit [B310:blacklist] Audit url open for
                        # permitted schemes. Allowing use of file:/ or custom schemes is
                        # often unexpected.
                        if link.lower().startswith('http'):
                            request = urllib2.Request(link)  # nosec
                        else:
                            logger.error(
                                'error :: %s :: link does not start with http - %s' % (str(link)))
                    if python_version == 3:
                        request = urllib.request(link)
                # @modified 20191021 - Task #3290: Handle urllib2 in py3
                #                      Branch #3262: py3
                # image_data = urllib2.urlopen(request).read()  # nosec
                if python_version == 2:
                    image_data = urllib2.urlopen(request).read()  # nosec
                if python_version == 3:
                    image_data = urllib.request.urlopen(request).read()  # nosec
                if settings.ENABLE_DEBUG or LOCAL_DEBUG:
                    logger.info('debug :: alert_smtp - image data OK')
            # @modified 20191021 - Task #3290: Handle urllib2 in py3
            #                      Branch #3262: py3
            # except urllib2.URLError:
            except:
                logger.error(traceback.format_exc())
                logger.error('error :: alert_smtp - failed to get image graph')
                logger.error('error :: alert_smtp - %s' % str(link))
                image_data = None
                if settings.ENABLE_DEBUG or LOCAL_DEBUG:
                    logger.info('debug :: alert_smtp - image data None')

    if LOCAL_DEBUG:
        logger.info('debug :: alert_smtp - Memory usage after image_data: %s (kb)' % resource.getrusage(resource.RUSAGE_SELF).ru_maxrss)

    # If we failed to get the image or if it was explicitly disabled,
    # use the image URL instead of the content.
    if image_data is None:
        img_tag = '<img src="%s"/>' % link
    else:
        img_tag = '<img src="cid:%s"/>' % content_id
        if settings.ENABLE_DEBUG or LOCAL_DEBUG:
            logger.info('debug :: alert_smtp - img_tag: %s' % img_tag)

        if settings.IONOSPHERE_ENABLED:
            # Create Ionosphere Graphite image
            # @modified 20161229 - Feature #1830: Ionosphere alerts
            # Only write the data to the file if it does not exist
            if not os.path.isfile(graphite_image_file):
                try:
                    write_data_to_file(skyline_app, graphite_image_file, 'w', image_data)
                    logger.info(
                        'added %s Ionosphere Graphite image :: %s' % (
                            skyline_app, graphite_image_file))
                except:
                    logger.info(traceback.format_exc())
                    logger.error(
                        'error :: failed to add %s Ionosphere Graphite image' % (
                            skyline_app, graphite_image_file))
            else:
                logger.info(
                    '%s Ionosphere Graphite image already exists :: %s' % (
                        skyline_app, graphite_image_file))

    redis_image_data = None
    try:
        plot_redis_data = settings.PLOT_REDIS_DATA
    except:
        plot_redis_data = False

    if settings.SMTP_OPTS.get('embed-images') and plot_redis_data:
        # Create graph from Redis data
        redis_metric_key = '%s%s' % (settings.FULL_NAMESPACE, metric[1])
        try:
            raw_series = REDIS_ALERTER_CONN.get(redis_metric_key)
            if settings.ENABLE_DEBUG or LOCAL_DEBUG:
                logger.info('debug :: alert_smtp - raw_series: %s' % 'OK')
        except:
            if settings.ENABLE_DEBUG or LOCAL_DEBUG:
                logger.info('debug :: alert_smtp - raw_series: %s' % 'FAIL')

        try:
            if LOCAL_DEBUG:
                logger.info('debug :: alert_smtp - Memory usage before get Redis timeseries data: %s (kb)' % resource.getrusage(resource.RUSAGE_SELF).ru_maxrss)
            unpacker = Unpacker(use_list=True)
            unpacker.feed(raw_series)
            timeseries_x = [float(item[0]) for item in unpacker]
            unpacker = Unpacker(use_list=True)
            unpacker.feed(raw_series)
            timeseries_y = [item[1] for item in unpacker]

            unpacker = Unpacker(use_list=False)
            unpacker.feed(raw_series)
            timeseries = list(unpacker)
            if LOCAL_DEBUG:
                logger.info('debug :: alert_smtp - Memory usage after get Redis timeseries data: %s (kb)' % resource.getrusage(resource.RUSAGE_SELF).ru_maxrss)
        except:
            logger.error(traceback.format_exc())
            logger.error('error :: alert_smtp - unpack timeseries failed')
            timeseries = None

        # @added 20200507 - Feature #3532: Sort all time series
        # To ensure that there are no unordered timestamps in the time
        # series which are artefacts of the collector or carbon-relay, sort
        # all time series by timestamp before analysis.
        original_timeseries = timeseries
        if original_timeseries:
            timeseries = sort_timeseries(original_timeseries)
            del original_timeseries

        if settings.IONOSPHERE_ENABLED and timeseries:
            '''
            .. todo: this is possibly to be used to allow the user to submit the
                FULL_DURATION duration data set for the features profile to be
                created against IF it is a Mirage metric.  This would allow for
                additional granularity in Mirage metrics, thereby maintaining
                their seasonality, but allow user and Skyline to analyze the
                anomaly at a FULL_DURATION resolution as well.  Not sure how to
                code that in Ionosphere context yet but could just be additonal
                flag in the Ionosphere record.  In the Ionosphere frontend, the
                user would be given an option to either create the features
                profile on the Mirage timeseries or the redis FULL_DURATION
                timeseries.  It is a little complicated, but doable.
                # @modified 20161229 - Feature #1828: ionosphere - mirage Redis data features
                However that ^^ is UNDESIRABLE in the Mirage/Ionosphere context
                at the moment.  Ionosphere must only profile SECOND_ORDER_RESOLUTION_HOURS
                currently so as to not pollute the seasonality aspect of Mirage
            '''
            # Create Ionosphere redis timeseries json if is does not exist
            # @modified 20161229 - Feature #1830: Ionosphere alerts
            # Only write the data to the file if it does not exist and replace
            # the timeseries object if a json file exists

            # @added 20170920 - Bug #2168: Strange Redis derivative graph
            using_original_redis_json = False

            if not os.path.isfile(json_file):
                timeseries_json = str(timeseries).replace('[', '(').replace(']', ')')
                try:
                    write_data_to_file(skyline_app, json_file, 'w', timeseries_json)
                    logger.info('added %s Ionosphere Redis data timeseries json file :: %s' % (skyline_app, json_file))
                except:
                    logger.info(traceback.format_exc())
                    logger.error('error :: failed to add %s Ionosphere Redis data timeseries json file' % (skyline_app, json_file))
            else:
                # Replace the timeseries object
                logger.info('%s Ionosphere Redis data timeseries json file already exists, using :: %s' % (skyline_app, json_file))
                anomaly_json = json_file
                try:
                    # Read the timeseries json file
                    with open(anomaly_json, 'r') as f:
                        raw_timeseries = f.read()
                    timeseries_array_str = str(raw_timeseries).replace('(', '[').replace(')', ']')
                    timeseries = literal_eval(timeseries_array_str)
                    logger.info('%s Redis timeseries replaced with timeseries from :: %s' % (skyline_app, anomaly_json))
                    timeseries_x = [float(item[0]) for item in timeseries]
                    timeseries_y = [item[1] for item in timeseries]
                    # @added 20170920 - Bug #2168: Strange Redis derivative graph
                    # This already has nonNegativeDerivative applied to it
                    using_original_redis_json = True
                except:
                    logger.error(traceback.format_exc())
                    logger.error(
                        'error :: %s failed to read timeseries data from %s' % (skyline_app, anomaly_json))
                    timeseries = None

                # @added 20200507 - Feature #3532: Sort all time series
                # To ensure that there are no unordered timestamps in the time
                # series which are artefacts of the collector or carbon-relay, sort
                # all time series by timestamp before analysis.
                original_timeseries = timeseries
                if original_timeseries:
                    timeseries = sort_timeseries(original_timeseries)
                    del original_timeseries

        # @added 20170603 - Feature #2034: analyse_derivatives
        if known_derivative_metric:

            # @added 20170920 - Bug #2168: Strange Redis derivative graph
            # If this is the Mirage Redis json it already has
            # nonNegativeDerivative applied to it
            if not using_original_redis_json:
                logger.info('alert_smtp - nonNegativeDerivative being applied')

                try:
                    derivative_timeseries = nonNegativeDerivative(timeseries)
                    timeseries = derivative_timeseries
                    # @added 20170920 - Bug #2168: Strange Redis derivative graph
                    logger.info('alert_smtp - nonNegativeDerivative applied')
                except:
                    logger.error('error :: alert_smtp - nonNegativeDerivative failed')
            else:
                logger.info('alert_smtp - nonNegativeDerivative not being applied, as it will have been applied in the original json')

        # @added 21070726 - Bug #2068: Analyzer smtp alert error on Redis plot with derivative metrics
        # If the nonNegativeDerivative has been calculated we need to reset the
        # x and y as nonNegativeDerivative has to discard the first value as it
        # has no delta for it so the timeseries is 1 item less.
        if timeseries:
            timeseries_x = [float(item[0]) for item in timeseries]
            timeseries_y = [item[1] for item in timeseries]

        pd_series_values = None
        mean_series = None
        if timeseries:
            try:
                if LOCAL_DEBUG:
                    logger.info('debug :: alert_smtp - Memory usage before pd.Series: %s (kb)' % resource.getrusage(resource.RUSAGE_SELF).ru_maxrss)
                values = pd.Series([x[1] for x in timeseries])
                # Because the truth value of a Series is ambiguous
                pd_series_values = True
                if LOCAL_DEBUG:
                    logger.info('debug :: alert_smtp - Memory usage after pd.Series: %s (kb)' % resource.getrusage(resource.RUSAGE_SELF).ru_maxrss)
            except:
                logger.error('error :: alert_smtp - pandas value series on timeseries failed')

        if pd_series_values:
            try:
                array_median = np.median(values)
                if settings.ENABLE_DEBUG or LOCAL_DEBUG:
                    logger.info('debug :: alert_smtp - values median: %s' % str(array_median))

                array_amax = np.amax(values)
                if settings.ENABLE_DEBUG or LOCAL_DEBUG:
                    logger.info('debug :: alert_smtp - array_amax: %s' % str(array_amax))
                array_amin = np.amin(values)
                if settings.ENABLE_DEBUG or LOCAL_DEBUG:
                    logger.info('debug :: alert_smtp - array_amin: %s' % str(array_amin))
                mean = values.mean()
                if settings.ENABLE_DEBUG or LOCAL_DEBUG:
                    logger.info('debug :: alert_smtp - mean: %s' % str(mean))
                stdDev = values.std()
                if settings.ENABLE_DEBUG or LOCAL_DEBUG:
                    logger.info('debug :: alert_smtp - stdDev: %s' % str(stdDev))

                sigma3 = 3 * stdDev
                if settings.ENABLE_DEBUG or LOCAL_DEBUG:
                    logger.info('debug :: alert_smtp - sigma3: %s' % str(sigma3))

                # sigma3_series = [sigma3] * len(values)

                sigma3_upper_bound = mean + sigma3
                try:
                    sigma3_lower_bound = mean - sigma3
                except:
                    sigma3_lower_bound = 0

                sigma3_upper_series = [sigma3_upper_bound] * len(values)
                sigma3_lower_series = [sigma3_lower_bound] * len(values)
                amax_series = [array_amax] * len(values)
                amin_series = [array_amin] * len(values)
                mean_series = [mean] * len(values)
            except:
                logger.error('error :: alert_smtp - numpy ops on series failed')
                mean_series = None

        if mean_series:
            # @modified 20191003 - Feature #3194: Add CUSTOM_ALERT_OPTS to settings
            # Use main_alert_title and alert_context
            # graph_title = 'Skyline %s - ALERT - at %s hours - Redis data\n%s - anomalous value: %s' % (context, str(int(full_duration_in_hours)), metric[1], str(metric[0]))
            graph_title = '%s %s - ALERT - at %s hours - Redis data\n%s - anomalous value: %s' % (main_alert_title, alert_context, str(int(full_duration_in_hours)), metric[1], str(metric[0]))
            # @added 20170603 - Feature #2034: analyse_derivatives
            if known_derivative_metric:
                graph_title = 'Skyline %s - ALERT - at %s hours - Redis data (derivative graph)\n%s - anomalous value: %s' % (context, str(int(full_duration_in_hours)), metric[1], str(metric[0]))

            # @modified 20160814 - Bug #1558: Memory leak in Analyzer
            # I think the buf is causing a memory leak, trying a file
            # if python_version == 3:
            #     buf = io.StringIO()
            # else:
            #     buf = io.BytesIO()
            buf = '%s/%s.%s.%s.png' % (
                settings.SKYLINE_TMP_DIR, skyline_app, str(int(metric[2])), metric[1])

            if LOCAL_DEBUG:
                logger.info('debug :: alert_smtp - Memory usage before plot Redis data: %s (kb)' % resource.getrusage(resource.RUSAGE_SELF).ru_maxrss)

            # Too big
            # rcParams['figure.figsize'] = 12, 6
            rcParams['figure.figsize'] = 8, 4
            try:
                # fig = plt.figure()
                fig = plt.figure(frameon=False)
                ax = fig.add_subplot(111)
                ax.set_title(graph_title, fontsize='small')
                # @modified 20180417 - Bug #2358: set_axis_bgcolor method removed from Matplotlib - Luminosity
                #                      IssueID #49 'AxesSubplot' object has no attribute 'set_axis_bgcolor'
                # ax.set_axis_bgcolor('black')
                if hasattr(ax, 'set_facecolor'):
                    ax.set_facecolor('black')
                else:
                    ax.set_axis_bgcolor('black')

                try:
                    datetimes = [dt.datetime.utcfromtimestamp(ts) for ts in timeseries_x]
                    if settings.ENABLE_DEBUG or LOCAL_DEBUG:
                        logger.info('debug :: alert_smtp - datetimes: %s' % 'OK')
                except:
                    logger.error('error :: alert_smtp - datetimes: %s' % 'FAIL')

                plt.xticks(rotation=0, horizontalalignment='center')
                xfmt = DateFormatter('%a %H:%M')
                plt.gca().xaxis.set_major_formatter(xfmt)

                ax.xaxis.set_major_formatter(xfmt)

                ax.plot(datetimes, timeseries_y, color='orange', lw=0.6, zorder=3)
                ax.tick_params(axis='both', labelsize='xx-small')

                max_value_label = 'max - %s' % str(array_amax)
                ax.plot(datetimes, amax_series, lw=1, label=max_value_label, color='m', ls='--', zorder=4)
                min_value_label = 'min - %s' % str(array_amin)
                ax.plot(datetimes, amin_series, lw=1, label=min_value_label, color='b', ls='--', zorder=4)
                mean_value_label = 'mean - %s' % str(mean)
                ax.plot(datetimes, mean_series, lw=1.5, label=mean_value_label, color='g', ls='--', zorder=4)

                sigma3_text = (r'3$\sigma$')
                # sigma3_label = '%s - %s' % (str(sigma3_text), str(sigma3))

                sigma3_upper_label = '%s upper - %s' % (str(sigma3_text), str(sigma3_upper_bound))
                ax.plot(datetimes, sigma3_upper_series, lw=1, label=sigma3_upper_label, color='r', ls='solid', zorder=4)

                if sigma3_lower_bound > 0:
                    sigma3_lower_label = '%s lower - %s' % (str(sigma3_text), str(sigma3_lower_bound))
                    ax.plot(datetimes, sigma3_lower_series, lw=1, label=sigma3_lower_label, color='r', ls='solid', zorder=4)

                ax.get_yaxis().get_major_formatter().set_useOffset(False)
                ax.get_yaxis().get_major_formatter().set_scientific(False)

                # Shrink current axis's height by 10% on the bottom
                box = ax.get_position()
                ax.set_position([box.x0, box.y0 + box.height * 0.1,
                                 box.width, box.height * 0.9])

                # Put a legend below current axis
                ax.legend(loc='upper center', bbox_to_anchor=(0.5, -0.05),
                          fancybox=True, shadow=True, ncol=4, fontsize='x-small')
                plt.rc('lines', lw=2, color='w')

                plt.grid(True)

                ax.grid(b=True, which='both', axis='both', color='lightgray',
                        linestyle='solid', alpha=0.5, linewidth=0.6)
                # @modified 20180417 - Bug #2358: set_axis_bgcolor method removed from Matplotlib - Luminosity
                #                      IssueID #49 'AxesSubplot' object has no attribute 'set_axis_bgcolor'
                # ax.set_axis_bgcolor('black')
                if hasattr(ax, 'set_facecolor'):
                    ax.set_facecolor('black')
                else:
                    ax.set_axis_bgcolor('black')

                rcParams['xtick.direction'] = 'out'
                rcParams['ytick.direction'] = 'out'
                ax.margins(y=.02, x=.03)
                # tight_layout removes the legend box
                # fig.tight_layout()
                try:
                    if LOCAL_DEBUG:
                        logger.info('debug :: alert_smtp - Memory usage before plt.savefig: %s (kb)' % resource.getrusage(resource.RUSAGE_SELF).ru_maxrss)
                    plt.savefig(buf, format='png')

                    if settings.IONOSPHERE_ENABLED:
                        if not os.path.exists(training_data_dir):
                            mkdir_p(training_data_dir)
                            logger.info('created dir - %s' % training_data_dir)

                        if not os.path.isfile(training_data_redis_image):
                            try:
                                plt.savefig(training_data_redis_image, format='png')
                                logger.info(
                                    'alert_smtp - save Redis training data image - %s' % (
                                        training_data_redis_image))
                            except:
                                logger.info(traceback.format_exc())
                                logger.error(
                                    'error :: alert_smtp - could not save - %s' % (
                                        training_data_redis_image))
                        else:
                            logger.info(
                                'alert_smtp - Redis training data image already exists - %s' % (
                                    training_data_redis_image))

                    # @added 20160814 - Bug #1558: Memory leak in Analyzer
                    # As per http://www.mail-archive.com/matplotlib-users@lists.sourceforge.net/msg13222.html
                    # savefig in the parent process was causing the memory leak
                    # the below fig.clf() and plt.close() did not resolve this
                    # however spawing a multiprocessing process for alert_smtp
                    # does solve this as issue as all memory is freed when the
                    # process terminates.
                    fig.clf()
                    plt.close(fig)
                    redis_graph_content_id = 'redis.%s' % metric[1]
                    redis_image_data = True
                    if settings.ENABLE_DEBUG or LOCAL_DEBUG:
                        logger.info('debug :: alert_smtp - savefig: %s' % 'OK')
                        logger.info('debug :: alert_smtp - Memory usage after plt.savefig: %s (kb)' % resource.getrusage(resource.RUSAGE_SELF).ru_maxrss)
                except:
                    logger.info(traceback.format_exc())
                    logger.error('error :: alert_smtp - plt.savefig: %s' % 'FAIL')
            except:
                logger.error(traceback.format_exc())
                logger.error('error :: alert_smtp - could not build plot')

    if LOCAL_DEBUG:
        logger.info('debug :: alert_smtp - Memory usage before email: %s (kb)' % resource.getrusage(resource.RUSAGE_SELF).ru_maxrss)

    if redis_image_data:
        redis_img_tag = '<img src="cid:%s"/>' % redis_graph_content_id
        if settings.ENABLE_DEBUG or LOCAL_DEBUG:
            logger.info('debug :: alert_smtp - redis_img_tag: %s' % str(redis_img_tag))
    else:
        # @modified 20161229 - Feature #1830: Ionosphere alerts
        # @modified 20170108 - Feature #1852: Ionosphere - features_profile matched graphite graphs
        # Restored the previous redis_img_tag method as some smtp alerts were
        # coming without a Redis graph, not all but some and for some reason,
        # I am pretty certain retrospectively that it was done that way from
        # testing I just wanted to try and be cleaner.
        # The redis_img_tag was changed at
        # https://github.com/earthgecko/skyline/commit/31bcacf3f90f0953ebed0d57260cb937e01f887c#diff-520bf2a218f65074ffead4d8184c138dR489
        redis_img_tag = '<img src="%s"/>' % 'none'
        # redis_img_tag = '<img src="none"/>'

    # @added 20170806 - Feature #1830: Ionosphere alerts
    # Show a human date in alerts
    alerted_at = str(dt.datetime.utcfromtimestamp(int(metric[2])))

    try:
        # @modified 20191002 - Feature #3194: Add CUSTOM_ALERT_OPTS to settings
        # Use main_alert_title
        # body = '<h3><font color="#dd3023">Sky</font><font color="#6698FF">line</font><font color="black"> %s alert</font></h3><br>' % context
        if main_alert_title == 'Skyline':
            body = '<h3><font color="#dd3023">Sky</font><font color="#6698FF">line</font><font color="black"> %s alert</font></h3><br>' % alert_context
        else:
            body = '<h3>%s<font color="black"> %s alert</font></h3><br>' % (main_alert_title, alert_context)
        body += '<font color="black">metric: <b>%s</b></font><br>' % metric[1]
        body += '<font color="black">Anomalous value: %s</font><br>' % str(metric[0])
        body += '<font color="black">Anomaly timestamp: %s</font><br>' % str(int(metric[2]))
        # @added 20170806 - Feature #1830: Ionosphere alerts
        # Show a human date in alerts
        body += '<font color="black">Anomalous at: %s</font><br>' % alerted_at
        body += '<font color="black">At hours: %s</font><br>' % str(int(full_duration_in_hours))
        body += '<font color="black">Next alert in: %s seconds</font><br>' % str(alert[2])
        # @added 20170603 - Feature #2034: analyse_derivatives
        if known_derivative_metric:
            body += '<font color="black">Derivative graph: True</font><br>'

        more_body = ''
        if settings.IONOSPHERE_ENABLED:
            # @modified 20170823 - Bug #2142: 7bit SMTP encoding breaking long urls
            # Broke body into body and more_body to workaround the 990 character
            # limit per line for SMTP
            # @modified 20191002 - Feature #3194: Add CUSTOM_ALERT_OPTS to settings
            # Use main_alert_title
            # more_body += '<h3><font color="#dd3023">Ionosphere :: </font><font color="#6698FF">training data</font><font color="black"></font></h3>'
            if main_alert_title == 'Skyline':
                more_body += '<h3><font color="#dd3023">Ionosphere :: </font><font color="#6698FF">training data</font><font color="black"></font></h3>'
            else:
                more_body += '<h3>%s :: <font color="#6698FF">training data</font><font color="black"></font></h3>' % main_alert_title

            # @added 20191011 - Feature #3194: Add CUSTOM_ALERT_OPTS to settings
            try:
                ionosphere_link_path = settings.CUSTOM_ALERT_OPTS['ionosphere_link_path']
            except:
                ionosphere_link_path = 'ionosphere'

            # @modified 20191011 - Feature #3194: Add CUSTOM_ALERT_OPTS to settings
            # ionosphere_link = '%s/ionosphere?timestamp=%s&metric=%s' % (
            #     settings.SKYLINE_URL, str(int(metric[2])), str(metric[1]))
            ionosphere_link = '%s/%s?timestamp=%s&metric=%s' % (
                settings.SKYLINE_URL, ionosphere_link_path, str(int(metric[2])),
                str(metric[1]))

            # @modified 20191002 - Feature #3194: Add CUSTOM_ALERT_OPTS to settings
            # Use main_alert_title
            # more_body += '<font color="black">To use this timeseries to train Skyline that this is not anomalous manage this training data at:<br>'
            if main_alert_title == 'Skyline':
                more_body += '<font color="black">To use this timeseries to train Skyline that this is not anomalous manage this training data at:<br>'
            else:
                more_body += '<font color="black">To use this timeseries to train %s that this is not anomalous manage this training data at:<br>' % main_alert_title

            more_body += '<a href="%s">%s</a></font>' % (ionosphere_link, ionosphere_link)

            # @added 20201014 - Feature #3734: waterfall alerts
            # Remove ionosphere training data as all the
            # resources required are not available for training
            if waterfall_alert:
                if main_alert_title == 'Skyline':
                    more_body = '<h3><font color="#dd3023">Ionosphere :: </font><font color="#6698FF">no training data for Analyzer waterfall alert</font><font color="black"></font></h3>'
                else:
                    more_body = '<h3>%s :: <font color="#6698FF">no training data for waterfall alert</font><font color="black"></font></h3>' % main_alert_title

        if redis_image_data:
            more_body += '<font color="black">min: %s  | max: %s   | mean: %s <br>' % (
                str(array_amin), str(array_amax), str(mean))
            more_body += '3-sigma: %s <br>' % str(sigma3)
            more_body += '3-sigma upper bound: %s   | 3-sigma lower bound: %s <br></font>' % (
                str(sigma3_upper_bound), str(sigma3_lower_bound))
            more_body += '<h3><font color="black">Redis data at FULL_DURATION</font></h3><br>'
            more_body += '<div dir="ltr">:%s<br></div>' % redis_img_tag
        if image_data:
            more_body += '<h3><font color="black">Graphite data at FULL_DURATION (may be aggregated)</font></h3>'
            more_body += '<div dir="ltr"><a href="%s">%s</a><br></div><br>' % (link, img_tag)
            more_body += '<font color="black">Clicking on the above graph will open to the Graphite graph with current data</font><br>'
        if redis_image_data:
            # @modified 20191002 - Feature #3194: Add CUSTOM_ALERT_OPTS to settings
            # Use if main_alert_title
            if main_alert_title == 'Skyline':
                more_body += '<font color="black">To disable the Redis data graph view, set PLOT_REDIS_DATA to False in your settings.py, if the Graphite graph is sufficient for you,<br>'
                more_body += 'however do note that will remove the 3-sigma and mean value too.</font>'
        more_body += '<br>'
        # @modified 20191002 - Feature #3194: Add CUSTOM_ALERT_OPTS to settings
        # Use main_alert_title
        if main_alert_title == 'Skyline':
            more_body += '<div dir="ltr" align="right"><font color="#dd3023">Sky</font><font color="#6698FF">line</font><font color="black"> version :: %s</font></div><br>' % str(skyline_version)
    except:
        logger.info(traceback.format_exc())
        logger.error('error :: alert_smtp - could not build body')

    # @modified 20180524 - Task #2384: Change alerters to cc other recipients
    # Do not send to each recipient, send to primary_recipient and cc the other
    # recipients, thereby sending only one email
    # for recipient in recipients:
    if primary_recipient:
        try:
            # @modified 20170823 - Bug #2142: 7bit SMTP encoding breaking long urls
            # Broke body into body and more_body to workaround the 990 character
            # limit per line for SMTP, using mixed as alternative indicates that
            # the client should select one of the parts for display and ignore
            # the rest (tripleee - https://stackoverflow.com/a/35115938)
            # msg = MIMEMultipart('alternative')
            msg = MIMEMultipart('mixed')

            # @added 20170812 - Bug #2142: 7bit SMTP encoding breaking long urls
            # set email charset and email encodings
            cs_ = charset.Charset('utf-8')
            cs_.header_encoding = charset.QP
            cs_.body_encoding = charset.QP
            msg.set_charset(cs_)

            # @modified 20191002 - Feature #3194: Add CUSTOM_ALERT_OPTS to settings
            # Use main_alert_title and alert_context
            # msg['Subject'] = '[Skyline alert] - %s ALERT - %s' % (context, metric[1])

            # @modified 20200907 - Feature #3734: waterfall alerts
            # Add waterfall alert to subject
            if waterfall_alert:
                msg['Subject'] = '[%s alert] - %s WATERFALL ALERT - %s' % (main_alert_title, alert_context, metric[1])
            else:
                msg['Subject'] = '[%s alert] - %s ALERT - %s' % (main_alert_title, alert_context, metric[1])

            msg['From'] = sender
            # @modified 20180524 - Task #2384: Change alerters to cc other recipients
            # msg['To'] = recipient
            msg['To'] = primary_recipient

            # @added 20180524 - Task #2384: Change alerters to cc other recipients
            # Added Cc
            if cc_recipients:
                msg['Cc'] = cc_recipients

            msg.attach(MIMEText(body, 'html'))
            # @added 20170823 - Bug #2142: 7bit SMTP encoding breaking long urls
            # Broke body into body and more_body to workaround the 990 character
            # limit per line for SMTP
            msg.replace_header('content-transfer-encoding', 'quoted-printable')
            msg.attach(MIMEText(more_body, 'html'))

            if redis_image_data:
                try:
                    # @modified 20160814 - Bug #1558: Memory leak in Analyzer
                    # I think the buf is causing a memory leak, trying a file
                    # buf.seek(0)
                    # msg_plot_attachment = MIMEImage(buf.read())
                    # msg_plot_attachment = MIMEImage(buf.read())
                    try:
                        # @added 20191107 - Branch #3262: py3
                        # Open in binary mode for py3
                        # with open(buf, 'r') as f:
                        with open(buf, 'rb') as f:
                            plot_image_data = f.read()
                        try:
                            os.remove(buf)
                        except OSError:
                            logger.error(
                                'error :: alert_smtp - failed to remove file - %s' % buf)
                            logger.info(traceback.format_exc())
                            pass
                    except:
                        logger.error('error :: failed to read plot file - %s' % buf)
                        plot_image_data = None

                    # @added 20161124 - Branch #922: ionosphere
                    msg_plot_attachment = MIMEImage(plot_image_data)
                    msg_plot_attachment.add_header('Content-ID', '<%s>' % redis_graph_content_id)
                    msg.attach(msg_plot_attachment)
                    if settings.ENABLE_DEBUG or LOCAL_DEBUG:
                        logger.info('debug :: alert_smtp - msg_plot_attachment - redis data done')
                except:
                    logger.error('error :: alert_smtp - msg_plot_attachment')
                    logger.info(traceback.format_exc())

            msg_attachment = None
            if image_data is not None:
                try:
                    msg_attachment = MIMEImage(image_data)
                    msg_attachment.add_header('Content-ID', '<%s>' % content_id)
                    msg.attach(msg_attachment)
                    if settings.ENABLE_DEBUG or LOCAL_DEBUG:
                        logger.info('debug :: alert_smtp - msg_attachment - Graphite img source done')
                except:
                    logger.error('error :: alert_smtp - msg_attachment')
                    logger.info(traceback.format_exc())
        except:
            logger.error('error :: alert_smtp - could not attach')
            logger.info(traceback.format_exc())

        # @added 20190517 - Branch #3002: docker
        # Do not try to alert if the settings are default
        send_email_alert = True
        if 'your_domain.com' in str(sender):
            logger.info('alert_smtp - sender is not configured, not sending alert')
            send_email_alert = False
        if 'your_domain.com' in str(primary_recipient):
            logger.info('alert_smtp - recipient is not configured, not sending alert')
            send_email_alert = False
        if 'example.com' in str(sender):
            logger.info('alert_smtp - sender is not configured, not sending alert')
            send_email_alert = False
        if 'example.com' in str(primary_recipient):
            logger.info('alert_smtp - recipient is not configured, not sending alert')
            send_email_alert = False
        if DOCKER_FAKE_EMAIL_ALERTS:
            logger.info('alert_smtp - DOCKER_FAKE_EMAIL_ALERTS is set to %s, not executing SMTP command' % str(DOCKER_FAKE_EMAIL_ALERTS))
            send_email_alert = False

        # @added 20200610 - Feature #3560: External alert config
        #                   Feature #3406: Allow for no_email SMTP_OPTS
        # If the alert is for an external alerter set to no_email
        no_email = False
        if str(sender) == 'no_email':
            send_email_alert = False
            no_email = True
        if str(primary_recipient) == 'no_email':
            send_email_alert = False
            no_email = True
        if no_email:
            logger.info('alert_smtp - no_email is set, not executing SMTP command')

        # @modified 20190517 - Branch #3002: docker
        # Wrap the smtp block based on whether to actually send mail or not.
        # This allows for all the steps to be processed in the testing or docker
        # context without actually sending the email.
        if send_email_alert:
            try:
                s = SMTP('127.0.0.1')
                # @modified 20180524 - Task #2384: Change alerters to cc other recipients
                # Send to primary_recipient and cc_recipients
                # s.sendmail(sender, recipient, msg.as_string())
                if cc_recipients:
                    s.sendmail(sender, [primary_recipient, cc_recipients], msg.as_string())
                else:
                    s.sendmail(sender, primary_recipient, msg.as_string())
                if settings.ENABLE_DEBUG or LOCAL_DEBUG:
                    # logger.info('debug :: alert_smtp - message sent to %s OK' % str(recipient))
                    logger.info(
                        'debug :: alert_smtp - message sent OK to primary_recipient :: %s, cc_recipients :: %s' %
                        (str(primary_recipient), str(cc_recipients)))
            except:
                logger.info(traceback.format_exc())
                # logger.error('error :: alert_smtp - could not send email to %s' % str(recipient))
                logger.error(
                    'error :: alert_smtp - could not send email to primary_recipient :: %s, cc_recipients :: %s' %
                    (str(primary_recipient), str(cc_recipients)))
            s.quit()
        else:
            logger.info(
                'alert_smtp - send_email_alert was set to %s message was not sent to primary_recipient :: %s, cc_recipients :: %s' % (
                    str(send_email_alert), str(primary_recipient), str(cc_recipients)))

    # @added 20200825 - Feature #3704: Add alert to anomalies
    if settings.PANORAMA_ENABLED:
        added_panorama_alert_event = add_panorama_alert(skyline_app, int(metric[2]), base_name)
        if not added_panorama_alert_event:
            logger.error(
                'error :: failed to add Panorama alert event - panorama.alert.%s.%s' % (
                    str(metric[2]), base_name))

    if LOCAL_DEBUG:
        logger.info('debug :: alert_smtp - Memory usage after email: %s (kb)' % resource.getrusage(resource.RUSAGE_SELF).ru_maxrss)

    if redis_image_data:
        # buf.seek(0)
        # buf.write('none')
        if LOCAL_DEBUG:
            logger.info('debug :: alert_smtp - Memory usage before del redis_image_data objects: %s (kb)' % resource.getrusage(resource.RUSAGE_SELF).ru_maxrss)
        del raw_series
        del unpacker
        del timeseries[:]
        del timeseries_x[:]
        del timeseries_y[:]
        del values
        del datetimes[:]
        del msg_plot_attachment
        del redis_image_data
        # We del all variables that are floats as they become unique objects and
        # can result in what appears to be a memory leak, but is not, it is
        # just the way Python handles floats
        del mean
        del array_amin
        del array_amax
        del stdDev
        del sigma3
        if LOCAL_DEBUG:
            logger.info('debug :: alert_smtp - Memory usage after del redis_image_data objects: %s (kb)' % resource.getrusage(resource.RUSAGE_SELF).ru_maxrss)
        if LOCAL_DEBUG:
            logger.info('debug :: alert_smtp - Memory usage before del fig object: %s (kb)' % resource.getrusage(resource.RUSAGE_SELF).ru_maxrss)
        # @added 20160814 - Bug #1558: Memory leak in Analyzer
        #                   Issue #21 Memory leak in Analyzer - https://github.com/earthgecko/skyline/issues/21
        # As per http://www.mail-archive.com/matplotlib-users@lists.sourceforge.net/msg13222.html
        fig.clf()
        plt.close(fig)
        del fig
        if LOCAL_DEBUG:
            logger.info('debug :: alert_smtp - Memory usage after del fig object: %s (kb)' % resource.getrusage(resource.RUSAGE_SELF).ru_maxrss)

    if LOCAL_DEBUG:
        logger.info('debug :: alert_smtp - Memory usage before del other objects: %s (kb)' % resource.getrusage(resource.RUSAGE_SELF).ru_maxrss)
    del recipients[:]
    del body
    del msg
    del image_data
    del msg_attachment
    if LOCAL_DEBUG:
        logger.info('debug :: alert_smtp - Memory usage after del other objects: %s (kb)' % resource.getrusage(resource.RUSAGE_SELF).ru_maxrss)

    return


def alert_pagerduty(alert, metric, context):
    """
    Called by :func:`~trigger_alert` and sends an alert via PagerDuty
    """
    if settings.PAGERDUTY_ENABLED:
        import pygerduty
        pager = pygerduty.PagerDuty(settings.PAGERDUTY_OPTS['subdomain'], settings.PAGERDUTY_OPTS['auth_token'])

        # @added 20191008 - Feature #3194: Add CUSTOM_ALERT_OPTS to settings
        try:
            main_alert_title = settings.CUSTOM_ALERT_OPTS['main_alert_title']
        except:
            main_alert_title = 'Skyline'
        alert_context = context
        if context == 'Analyzer':
            try:
                alert_context = settings.CUSTOM_ALERT_OPTS['analyzer_alert_heading']
            except:
                alert_context = 'Analyzer'
        if context == 'Mirage':
            try:
                alert_context = settings.CUSTOM_ALERT_OPTS['mirage_alert_heading']
            except:
                alert_context = 'Mirage'
        if context == 'Ionosphere':
            try:
                alert_context = settings.CUSTOM_ALERT_OPTS['ionosphere_alert_heading']
            except:
                alert_context = 'Ionosphere'

        # @modified 20191008 - Feature #3194: Add CUSTOM_ALERT_OPTS to settings
        # pager.trigger_incident(settings.PAGERDUTY_OPTS['key'], '%s alert - %s - %s' % (context, str(metric[0]), metric[1]))
        pager.trigger_incident(settings.PAGERDUTY_OPTS['key'], '%s - %s alert - %s - %s' % (main_alert_title, alert_context, str(metric[0]), metric[1]))

        # @added 20200825 - Feature #3704: Add alert to anomalies
        if settings.PANORAMA_ENABLED:
            added_panorama_alert_event = add_panorama_alert(skyline_app, int(metric[2]), metric[1])
            if not added_panorama_alert_event:
                logger.error(
                    'error :: failed to add Panorama alert event - panorama.alert.%s.%s' % (
                        str(metric[2]), metric[1]))
    else:
        return


def alert_hipchat(alert, metric, context):
    """
    Called by :func:`~trigger_alert` and sends an alert the hipchat room that is
    configured in settings.py.
    """

    if settings.HIPCHAT_ENABLED:
        sender = settings.HIPCHAT_OPTS['sender']
        import hipchat
        hipster = hipchat.HipChat(token=settings.HIPCHAT_OPTS['auth_token'])
        rooms = settings.HIPCHAT_OPTS['rooms'][alert[0]]
        unencoded_graph_title = 'Skyline %s - ALERT at %s hours - %s' % (
            context, str(int(full_duration_in_hours)), str(metric[0]))
        graph_title_string = quote(unencoded_graph_title, safe='')
        graph_title = '&title=%s' % graph_title_string

        # @added 20180809 - Bug #2498: Incorrect scale in some graphs
        # If -xhours is used the scale is incorrect if x hours > than first
        # retention period, passing from and until renders the graph with the
        # correct scale.
        # @modified 20181009 - Feature #2618: alert_slack
        #                      Bug #2498: Incorrect scale in some graphs
        # Corrected the from_timestamp and until_timestamp as they were incorrectly
        # order, however Graphite still rendered the correct graph as it plotted
        # reverse, which is the same.  Also using the metric[0] value instead of
        # time()
        # from_timestamp = int(time())
        # until_timestamp = from_timestamp - full_duration_seconds
        until_timestamp = int(metric[2])
        from_timestamp = until_timestamp - full_duration_seconds

        graphite_from = dt.datetime.fromtimestamp(int(from_timestamp)).strftime('%H:%M_%Y%m%d')
        logger.info('graphite_from - %s' % str(graphite_from))
        graphite_until = dt.datetime.fromtimestamp(int(until_timestamp)).strftime('%H:%M_%Y%m%d')
        logger.info('graphite_until - %s' % str(graphite_until))

        graphite_port = get_graphite_port(skyline_app)
        graphite_render_uri = get_graphite_render_uri(skyline_app)

        # @modified 20191106 - Task #3294: py3 - handle system parameter in Graphite cactiStyle
        # link = '%s://%s:%s/%s?from=%s&until=%s&target=cactiStyle(%s)%s%s&colorList=orange' % (
        link = '%s://%s:%s/%s?from=%s&until=%s&target=cactiStyle(%s,%%27si%%27)%s%s&colorList=orange' % (
            settings.GRAPHITE_PROTOCOL, settings.GRAPHITE_HOST,
            graphite_port, graphite_render_uri, str(graphite_from), str(graphite_until),
            metric[1], settings.GRAPHITE_GRAPH_SETTINGS, graph_title)
        embed_graph = "<a href='" + link + "'><img height='308' src='" + link + "'>" + metric[1] + "</a>"

        for room in rooms:
            message = '%s - %s - anomalous metric: %s (value: %s) at %s hours %s' % (
                sender, context, metric[1], str(metric[0]), str(int(full_duration_in_hours)), embed_graph)
            hipchat_color = settings.HIPCHAT_OPTS['color']
            hipster.method(
                'rooms/message', method='POST',
                parameters={'room_id': room, 'from': 'Skyline', 'color': hipchat_color, 'message': message})
    else:
        return


def alert_syslog(alert, metric, context):
    """
    Called by :func:`~trigger_alert` and logs anomalies to syslog.

    """
    if settings.SYSLOG_ENABLED:
        syslog_ident = settings.SYSLOG_OPTS['ident']
        message = '%s - Anomalous metric: %s (value: %s)' % (context, metric[1], str(metric[0]))
        if sys.version_info[:2] == (2, 6):
            syslog.openlog(syslog_ident, syslog.LOG_PID, syslog.LOG_LOCAL4)
        elif sys.version_info[:2] == (2, 7):
            syslog.openlog(ident="skyline", logoption=syslog.LOG_PID, facility=syslog.LOG_LOCAL4)
        elif sys.version_info[:1] == (3):
            syslog.openlog(ident="skyline", logoption=syslog.LOG_PID, facility=syslog.LOG_LOCAL4)
        else:
            syslog.openlog(syslog_ident, syslog.LOG_PID, syslog.LOG_LOCAL4)
        syslog.syslog(4, message)
    else:
        return


# @added 20180807 - Feature #2492: alert on stale metrics (github #67)
def alert_stale_digest(alert, metric, context):
    """
    Called by :func:`~trigger_alert` and sends a digest alert via smtp of the
    stale metrics to the default recipient

    """
    LOCAL_DEBUG = False
    if settings.ENABLE_DEBUG or LOCAL_DEBUG:
        logger.info('debug :: alert_stale_digest - sending smtp digest alert of stale metrics')

    # @added 20180828 - Bug #2568: alert on stale metrics not firing
    # Create a new list from the metric[1] tuple item
    logger.info('alert_stale_digest - metric :: %s' % str(metric))
    stale_metrics = metric[1]
    logger.info('alert_stale_digest - stale_metrics :: %s' % str(stale_metrics))

    sender = settings.SMTP_OPTS['sender']
    try:
        # @modified 20180828 - Bug #2568: alert on stale metrics not firing
        # That was a list not a str
        # recipient = settings.SMTP_OPTS['default_recipient']
        recipient = settings.SMTP_OPTS['default_recipient'][0]
        logger.info('alert_stale_digest - sending smtp digest alert to %s' % str(recipient))
    except:
        logger.error('error :: alert_stale_digest - no known default_recipient')
        return False

    try:
        body = '<h3><font color="#dd3023">Sky</font><font color="#6698FF">line</font><font color="black"> %s alert</font></h3><br>' % context
        body += '<font color="black"><b>Stale metrics (no data sent for ~%s seconds):</b></font><br>' % str(settings.ALERT_ON_STALE_PERIOD)
        # @modified 20180828 - Bug #2568: alert on stale metrics not firing
        # for metric_name in stale_metric[1]:
        for metric_name in stale_metrics:
            body += '<font color="black">%s</font><br>' % str(metric_name)
    except:
        logger.info(traceback.format_exc())
        logger.error('error :: alert_stale_digest - could not build body')
    try:
        msg = MIMEMultipart('mixed')
        cs_ = charset.Charset('utf-8')
        cs_.header_encoding = charset.QP
        cs_.body_encoding = charset.QP
        msg.set_charset(cs_)
        msg['Subject'] = '[Skyline alert] - %s ALERT - %s stale metrics' % (context, str(len(stale_metrics)))
        msg['From'] = sender
        msg['To'] = recipient
        msg.attach(MIMEText(body, 'html'))
        msg.replace_header('content-transfer-encoding', 'quoted-printable')
        # msg.attach(MIMEText(body, 'html'))
        logger.info('alert_stale_digest - msg attached')
    except:
        logger.error('error :: alert_smtp - could not attach')
        logger.info(traceback.format_exc())

    # @added 20190517 - Branch #3002: docker
    # Do not try to alert if the settings are default
    send_email_alert = True
    if 'your_domain.com' in str(sender):
        logger.info('alert_smtp - sender is not configured, not sending alert')
        send_email_alert = False
    if 'your_domain.com' in str(recipient):
        logger.info('alert_smtp - recipient is not configured, not sending alert')
        send_email_alert = False
    if 'example.com' in str(sender):
        logger.info('alert_smtp - sender is not configured, not sending alert')
        send_email_alert = False
    if 'example.com' in str(recipient):
        logger.info('alert_smtp - recipient is not configured, not sending alert')
        send_email_alert = False
    if DOCKER_FAKE_EMAIL_ALERTS:
        logger.info('alert_smtp - DOCKER_FAKE_EMAIL_ALERTS is set to %s, not sending email alert' % str(DOCKER_FAKE_EMAIL_ALERTS))
        send_email_alert = False

    # @added 20200122 - Feature #3406: Allow for no_email SMTP_OPTS
    no_email = False
    if str(sender) == 'no_email':
        send_email_alert = False
        no_email = True
    if str(recipient) == 'no_email':
        send_email_alert = False
        no_email = True
    if no_email:
        logger.info('alert_smtp - no_email is set in SMTP_OPTS, not executing SMTP command')

    # @modified 20190517 - Branch #3002: docker
    # Wrap the smtp block based on whether to actually send mail or not.
    # This allows for all the steps to be processed in the testing or docker
    # context without actually sending the email.
    if send_email_alert:
        try:
            s = SMTP('127.0.0.1')
            s.sendmail(sender, recipient, msg.as_string())
            logger.info('alert_stale_digest - sent email to recipient :: %s' % str(recipient))
        except:
            logger.info(traceback.format_exc())
            logger.error('error :: alert_stale_digest - could not send email to recipient :: %s' % str(recipient))
        s.quit()
    else:
        logger.info(
            'alert_smtp - send_email_alert was set to %s message was not sent to recipient :: %s' % (
                str(send_email_alert), str(recipient)))
    return


# @added 20181006 - Feature #2618: alert_slack
def alert_slack(alert, metric, context):

    if not settings.SLACK_ENABLED:
        logger.info('alert_slack - SLACK_ENABLED is False, not slack alerting for anomalous metric :: alert: %s, metric: %s' % (
            str(alert), str(metric)))
        return False

    # @modified 20190517 - Branch #3002: docker
    # Do not try to alert if the settings are default
    try:
        bot_user_oauth_access_token = settings.SLACK_OPTS['bot_user_oauth_access_token']
    except:
        logger.error('error :: alert_slack - could not determine bot_user_oauth_access_token')
        return False
    if bot_user_oauth_access_token == 'YOUR_slack_bot_user_oauth_access_token':
        logger.info('alert_slack - bot_user_oauth_access_token is not configured, not sending alert')
        return False

    # @modified 20200701 - Task #3612: Upgrade to slack v2
    #                      Task #3608: Update Skyline to Python 3.8.3 and deps
    #                      Task #3556: Update deps
    # slackclient v2 has a version function, < v2 does not
    # from slackclient import SlackClient
    try:
        from slack import version as slackVersion
        slack_version = slackVersion.__version__
    except:
        slack_version = '1.3'
    if slack_version == '1.3':
        from slackclient import SlackClient
    else:
        from slack import WebClient

    import simplejson as json

    logger.info('alert_slack - anomalous metric :: alert: %s, metric: %s' % (str(alert), str(metric)))

    # @modified 20200728 - Bug #3652: Handle multiple metrics in base_name conversion
    # base_name = str(metric[1]).replace(settings.FULL_NAMESPACE, '', 1)
    metric_name = str(metric[1])
    if metric_name.startswith(settings.FULL_NAMESPACE):
        base_name = metric_name.replace(settings.FULL_NAMESPACE, '', 1)
    else:
        base_name = metric_name

    # @modified 20190520 - Branch #3002: docker
    # wix-playground added a hashlib hexdigest method which has not been
    # verified so using the original method useless the base_name is longer
    # than 254 chars
    if len(base_name) > 254:
        base_name = hashlib.sha224(str(metric[1]).replace(
            settings.FULL_NAMESPACE, '', 1)).hexdigest()

    full_duration_in_hours = int(settings.FULL_DURATION) / 3600

    # The known_derivative_metric state is determine in case we need to surface
    # the png image from Graphite if the Ionosphere image is not available for
    # some reason.  This will result in Skyline at least still sending an alert
    # to slack, even if some gear fails in Ionosphere or slack alerting is used
    # without Ionosphere enabled. Yes not DRY but multiprocessing and spawn
    # safe.
    known_derivative_metric = False
    try:
        # @modified 20191030 - Bug #3266: py3 Redis binary objects not strings
        #                      Branch #3262: py3
        # Use get_redis_conn_decoded
        # if settings.REDIS_PASSWORD:
        #     REDIS_ALERTER_CONN = redis.StrictRedis(password=settings.REDIS_PASSWORD, unix_socket_path=settings.REDIS_SOCKET_PATH)
        # else:
        #     REDIS_ALERTER_CONN = redis.StrictRedis(unix_socket_path=settings.REDIS_SOCKET_PATH)
        REDIS_ALERTER_CONN = get_redis_conn(skyline_app)
    except:
        logger.error('error :: alert_slack - redis connection failed')

    # try:
    #     derivative_metrics = list(REDIS_ALERTER_CONN.smembers('derivative_metrics'))
    # except:
    #     derivative_metrics = []
    # redis_metric_name = '%s%s' % (settings.FULL_NAMESPACE, str(base_name))
    # if redis_metric_name in derivative_metrics:
    #     known_derivative_metric = True
    # if known_derivative_metric:
    #     try:
    #         non_derivative_monotonic_metrics = settings.NON_DERIVATIVE_MONOTONIC_METRICS
    #     except:
    #         non_derivative_monotonic_metrics = []
    #     skip_derivative = in_list(redis_metric_name, non_derivative_monotonic_metrics)
    #     if skip_derivative:
    #         known_derivative_metric = False

    known_derivative_metric = is_derivative_metric(skyline_app, base_name)

    # @added 20191008 - Feature #3194: Add CUSTOM_ALERT_OPTS to settings
    try:
        main_alert_title = settings.CUSTOM_ALERT_OPTS['main_alert_title']
    except:
        main_alert_title = 'Skyline'
    alert_context = context
    if context == 'Analyzer':
        try:
            alert_context = settings.CUSTOM_ALERT_OPTS['analyzer_alert_heading']
        except:
            alert_context = 'Analyzer'
    if context == 'Mirage':
        try:
            alert_context = settings.CUSTOM_ALERT_OPTS['mirage_alert_heading']
        except:
            alert_context = 'Mirage'
    if context == 'Ionosphere':
        try:
            alert_context = settings.CUSTOM_ALERT_OPTS['ionosphere_alert_heading']
        except:
            alert_context = 'Ionosphere'

    if known_derivative_metric:
        # @modified 20191008 - Feature #3194: Add CUSTOM_ALERT_OPTS to settings
        # unencoded_graph_title = 'Skyline %s - ALERT at %s hours - derivative graph - %s' % (
        #     context, str(int(full_duration_in_hours)), str(metric[0]))
        # slack_title = '*Skyline %s - ALERT* on %s at %s hours - derivative graph - %s' % (
        #     context, str(metric[1]), str(int(full_duration_in_hours)),
        #     str(metric[0]))
        unencoded_graph_title = '%s %s - ALERT at %s hours - derivative graph - %s' % (
            main_alert_title, alert_context, str(int(full_duration_in_hours)),
            str(metric[0]))
        slack_title = '*%s %s - ALERT* on %s at %s hours - derivative graph - %s' % (
            main_alert_title, alert_context, str(metric[1]),
            str(int(full_duration_in_hours)), str(metric[0]))
    else:
        # @modified 20191008 - Feature #3194: Add CUSTOM_ALERT_OPTS to settings
        # unencoded_graph_title = 'Skyline %s - ALERT at %s hours - %s' % (
        #     context, str(int(full_duration_in_hours)),
        #     str(metric[0]))
        # slack_title = '*Skyline %s - ALERT* on %s at %s hours - %s' % (
        #     context, str(metric[1]), str(int(full_duration_in_hours)),
        #     str(metric[0]))
        unencoded_graph_title = '%s %s - ALERT at %s hours - %s' % (
            main_alert_title, alert_context, str(int(full_duration_in_hours)),
            str(metric[0]))
        slack_title = '*%s %s - ALERT* on %s at %s hours - %s' % (
            main_alert_title, alert_context, str(metric[1]),
            str(int(full_duration_in_hours)), str(metric[0]))

    # @added 20200907 - Feature #3734: waterfall alerts
    # Add waterfall alert to title
    waterfall_alert_check_string = '%s.%s' % (str(metric[2]), base_name)
    redis_waterfall_alert_key = 'analyzer.waterfall.alert.%s' % waterfall_alert_check_string
    waterfall_alert = False
    try:
        waterfall_alert = REDIS_ALERTER_CONN.get(redis_waterfall_alert_key)
    except:
        logger.info(traceback.format_exc())
        logger.error('error :: failed to add Redis key - %s for waterfall alert' % (
            redis_waterfall_alert_key))
    if waterfall_alert:
        unencoded_graph_title = unencoded_graph_title.replace('ALERT', 'WATERFALL%20ALERT')
        slack_title = slack_title.replace('ALERT', 'WATERFALL ALERT')

    graph_title_string = quote(unencoded_graph_title, safe='')
    graph_title = '&title=%s' % graph_title_string
    until_timestamp = int(metric[2])
    from_timestamp = until_timestamp - full_duration_seconds
    graphite_from = dt.datetime.fromtimestamp(int(from_timestamp)).strftime('%H:%M_%Y%m%d')
    logger.info('graphite_from - %s' % str(graphite_from))
    graphite_until = dt.datetime.fromtimestamp(int(until_timestamp)).strftime('%H:%M_%Y%m%d')
    logger.info('graphite_until - %s' % str(graphite_until))
    # @added 20181025 - Feature #2618: alert_slack
    # Added date and time info so you do not have to mouseover the slack
    # message to determine the time at which the alert came in
    timezone = strftime("%Z", gmtime())
    # @modified 20181029 - Feature #2618: alert_slack
    # Use the standard UNIX data format
    # human_anomaly_time = dt.datetime.fromtimestamp(int(until_timestamp)).strftime('%Y-%m-%d %H:%M:%S')
    human_anomaly_time = dt.datetime.fromtimestamp(int(until_timestamp)).strftime('%c')
    slack_time_string = '%s %s' % (human_anomaly_time, timezone)

    graphite_port = get_graphite_port(skyline_app)
    graphite_render_uri = get_graphite_render_uri(skyline_app)
    graphite_custom_headers = get_graphite_custom_headers(skyline_app)

    # @added 20201013 - Feature #3780: skyline_functions - sanitise_graphite_url
    encoded_graphite_metric_name = encode_graphite_metric_name(skyline_app, base_name)

    if known_derivative_metric:

        # @modified 20191106 - Task #3294: py3 - handle system parameter in Graphite cactiStyle
        # link = '%s://%s:%s/%s?from=%s&until=%s&target=cactiStyle(nonNegativeDerivative(%s))%s%s&colorList=orange' % (
        link = '%s://%s:%s/%s?from=%s&until=%s&target=cactiStyle(nonNegativeDerivative(%s),%%27si%%27)%s%s&colorList=orange' % (
            settings.GRAPHITE_PROTOCOL, settings.GRAPHITE_HOST,
            graphite_port, graphite_render_uri, str(
                graphite_from), str(graphite_until),
            # @modified 20201013 - Feature #3780: skyline_functions - sanitise_graphite_url
            # metric[1], settings.GRAPHITE_GRAPH_SETTINGS, graph_title)
            encoded_graphite_metric_name, settings.GRAPHITE_GRAPH_SETTINGS, graph_title)
    else:
        # @modified 20191106 - Task #3294: py3 - handle system parameter in Graphite cactiStyle
        # link = '%s://%s:%s/%s?from=%s&until=%s&target=cactiStyle(%s)%s%s&colorList=orange' % (
        link = '%s://%s:%s/%s?from=%s&until=%s&target=cactiStyle(%s,%%27si%%27)%s%s&colorList=orange' % (
            settings.GRAPHITE_PROTOCOL, settings.GRAPHITE_HOST,
            graphite_port, graphite_render_uri, str(
                graphite_from), str(graphite_until),
            # @modified 20201013 - Feature #3780: skyline_functions - sanitise_graphite_url
            # metric[1], settings.GRAPHITE_GRAPH_SETTINGS, graph_title)
            encoded_graphite_metric_name, settings.GRAPHITE_GRAPH_SETTINGS, graph_title)

    # slack does not allow embedded images, nor will it fetch links behind
    # authentication so Skyline uploads a png graphite image with the message
    image_file = None

    # Use the Ionosphere image if it exists
    if settings.IONOSPHERE_ENABLED:
        timeseries_dir = base_name.replace('.', '/')
        training_data_dir = '%s/%s/%s' % (
            settings.IONOSPHERE_DATA_FOLDER, str(int(metric[2])),
            timeseries_dir)
        graphite_image_file = '%s/%s.%s.graphite.%sh.png' % (
            training_data_dir, base_name, skyline_app,
            str(int(full_duration_in_hours)))
        logger.info('alert_slack - interpolated Ionosphere Graphite image :: %s' % (
            graphite_image_file))
    else:
        graphite_image_file = None
        logger.info('alert_slack - no Ionosphere Graphite image interpolated')

    # @added 20191105 - Task #3290: Handle urllib2 in py3
    #                   Branch #3262: py3
    image_data = get_graphite_graph_image(skyline_app, link, graphite_image_file)

    if graphite_image_file:
        if os.path.isfile(graphite_image_file):
            image_file = graphite_image_file
            logger.info('alert_slack - interpolated Ionosphere Graphite image file exists :: %s' % (
                graphite_image_file))
        else:
            logger.error('error :: alert_slack - interpolated Ionosphere Graphite image file not found :: %s' % (
                graphite_image_file))

    if not image_file:
        # Fetch the png from Graphite
        try:
            # @modified 20190520 - Branch #3002: docker
            # image_data = urllib2.urlopen(link).read()  # nosec
            if graphite_custom_headers:
                # @modified 20191021 - Task #3290: Handle urllib2 in py3
                #                      Branch #3262: py3
                # request = urllib2.Request(link, headers=graphite_custom_headers)
                if python_version == 2:
                    # @modified 20200808 - Task #3608: Update Skyline to Python 3.8.3 and deps
                    # Added nosec for bandit [B310:blacklist] Audit url open for
                    # permitted schemes. Allowing use of file:/ or custom schemes is
                    # often unexpected.
                    if link.lower().startswith('http'):
                        request = urllib.Request(link, headers=graphite_custom_headers)  # nosec
                    else:
                        logger.error(
                            'error :: %s :: link does not start with http - %s' % (str(link)))
                if python_version == 3:
                    request = urllib.request(link, headers=graphite_custom_headers)
            else:
                # @modified 20191021 - Task #3290: Handle urllib2 in py3
                #                      Branch #3262: py3
                # request = urllib2.Request(link)
                if python_version == 2:
                    # @modified 20200808 - Task #3608: Update Skyline to Python 3.8.3 and deps
                    # Added nosec for bandit [B310:blacklist] Audit url open for
                    # permitted schemes. Allowing use of file:/ or custom schemes is
                    # often unexpected.
                    if link.lower().startswith('http'):
                        request = urllib2.Request(link)  # nosec
                    else:
                        logger.error(
                            'error :: %s :: link does not start with http - %s' % (str(link)))
                if python_version == 3:
                    request = urllib.request(link)
            # @modified 20191021 - Task #3290: Handle urllib2 in py3
            #                      Branch #3262: py3
            # image_data = urllib2.urlopen(request).read()  # nosec
            if python_version == 2:
                image_data = urllib2.urlopen(request).read()  # nosec
            if python_version == 3:
                image_data = urllib.request.urlopen(request).read()  # nosec
            if settings.ENABLE_DEBUG or LOCAL_DEBUG:
                logger.info('debug :: alert_smtp - image data OK')
        # @modified 20191021 - Task #3290: Handle urllib2 in py3
        #                      Branch #3262: py3
        # except urllib2.URLError:
        except:
            logger.error(traceback.format_exc())
            logger.error('error :: alert_slack - failed to get image graph')
            logger.error('error :: alert_slack - %s' % str(link))
            image_data = None

        if image_data:
            image_file = '%s/%s.%s.graphite.%sh.png' % (
                settings.SKYLINE_TMP_DIR, base_name, skyline_app,
                str(int(full_duration_in_hours)))
            try:
                write_data_to_file(skyline_app, image_file, 'w', image_data)
                logger.info('alert_slack - added Graphite image :: %s' % (
                    image_file))
            except:
                logger.info(traceback.format_exc())
                logger.error(
                    'error :: alert_slack - failed to add %s Graphite image' % (
                        image_file))
                image_file = None
    try:
        filename = os.path.basename(image_file)
    except:
        filename = None

    try:
        bot_user_oauth_access_token = settings.SLACK_OPTS['bot_user_oauth_access_token']
    except:
        logger.error('error :: alert_slack - could not determine bot_user_oauth_access_token')
        return False
    try:
        channels = settings.SLACK_OPTS['channels'][alert[0]]
    except:
        logger.info('alert_slack - could not determine channel for %s' % str(alert[0]))
        # return False
        channels = None

    # @added 20191106 - Branch #3262: py3
    # If no channel could be determined use the default_channel
    if not channels:
        try:
            default_channel = settings.SLACK_OPTS['default_channel']
            channels = [default_channel]
            logger.info('alert_slack - using default_channel %s for %s' % (str(default_channel), str(alert[0])))
        except:
            logger.error('error :: alert_slack - could not determine default_channel')
            return False

    try:
        icon_emoji = settings.SLACK_OPTS['icon_emoji']
    except:
        icon_emoji = ':chart_with_upwards_trend:'

    try:
        # @modified 20200701 - Task #3612: Upgrade to slack v2
        #                      Task #3608: Update Skyline to Python 3.8.3 and deps
        #                      Task #3556: Update deps
        # sc = SlackClient(bot_user_oauth_access_token)
        if slack_version == '1.3':
            sc = SlackClient(bot_user_oauth_access_token)
        else:
            sc = WebClient(bot_user_oauth_access_token, timeout=10)
    except:
        logger.info(traceback.format_exc())
        logger.error('error :: alert_slack - could not initiate slack.RTMClient')
        return False

    # @added 20191011 - Feature #3194: Add CUSTOM_ALERT_OPTS to settings
    try:
        ionosphere_link_path = settings.CUSTOM_ALERT_OPTS['ionosphere_link_path']
    except:
        ionosphere_link_path = 'ionosphere'

    # @modified 20191011 - Feature #3194: Add CUSTOM_ALERT_OPTS to settings
    # ionosphere_link = '%s/ionosphere?timestamp=%s&metric=%s' % (
    #     settings.SKYLINE_URL, str(int(metric[2])), str(metric[1]))
    ionosphere_link = '%s/%s?timestamp=%s&metric=%s' % (
        settings.SKYLINE_URL, ionosphere_link_path, str(int(metric[2])),
        str(metric[1]))

    # This block is not used but left here as it is the pattern for sending
    # messages using the chat.postMessage methods and could possibly be the
    # failover for a files.upload error or future messages.
    message_payload = json.dumps([{
        "fallback": slack_title + ' - ' + link,
        "title": slack_title,
        "title_link": link,
        "image_url": link,
        "text": 'Ionosphere training data :: ' + ionosphere_link,
        "color": "#764FA5"
    }])
    send_slack_message = False

    for channel in channels:
        if send_slack_message:
            try:
                send_message = sc.api_call(
                    'chat.postMessage',
                    channel=channel,
                    icon_emoji=icon_emoji,
                    attachments=message_payload)
                if not send_message['ok']:
                    logger.error('error :: alert_slack - failed to send slack message')
                else:
                    logger.info('alert_slack - sent slack message')
            except:
                logger.info(traceback.format_exc())
                logger.error('error :: alert_slack - could not send_message')
                return False

        if settings.IONOSPHERE_ENABLED:
            # @modified 20181025 - Feature #2618: alert_slack
            # Added date and time info so you do not have to mouseover the slack
            # message to determine the time at which the alert came in
            # initial_comment = slack_title + ' :: <' + link  + '|graphite image link>\n*Ionosphere training dir* :: <' + ionosphere_link + '|training data link>'
            # @modified 20191008 - Feature #3194: Add CUSTOM_ALERT_OPTS to settings
            # initial_comment = slack_title + ' :: <' + link + '|graphite image link>\n*Ionosphere training dir* :: <' + ionosphere_link + '|training data link> :: for anomaly at ' + slack_time_string
            if main_alert_title == 'Skyline':
                initial_comment = slack_title + ' :: <' + link + '|graphite image link>\n*Ionosphere training dir* :: <' + ionosphere_link + '|training data link> :: for anomaly at ' + slack_time_string
            else:
                initial_comment = slack_title + ' :: <' + link + '|graphite image link>\n*Training dir* :: <' + ionosphere_link + '|training data link> :: for anomaly at ' + slack_time_string
            # @added 20201014 - Feature #3734: waterfall alerts
            # Remove ionosphere training data as all the
            # resources required are not available for training
            if waterfall_alert:
                if main_alert_title == 'Skyline':
                    initial_comment = slack_title + ' :: <' + link + '|graphite image link>\nNo training data is available for Analyzer waterfall alerts :: for anomaly at ' + slack_time_string
                else:
                    initial_comment = slack_title + ' :: <' + link + '|graphite image link>\nNo training data is available for waterfall alerts :: for anomaly at ' + slack_time_string

        else:
            # initial_comment = slack_title + ' :: <' + link  + '|graphite image link>'
            initial_comment = slack_title + ' :: <' + link + '|graphite image link>\nFor anomaly at ' + slack_time_string

        # @added 20201127 - Feature #3820: HORIZON_SHARDS
        # Add the origin and shard for debugging purposes
        if HORIZON_SHARDS:
            initial_comment = initial_comment + ' - from ' + this_host + ' (shard ' + str(HORIZON_SHARD) + ')'

        add_to_panorama = True
        try:
            slack_thread_updates = settings.SLACK_OPTS['thread_updates']
        except:
            slack_thread_updates = False
        if not settings.SLACK_ENABLED:
            slack_thread_updates = False
        slack_file_upload = False
        slack_thread_ts = 0

        try:
            # slack does not allow embedded images, nor links behind authentication
            # or color text, so we have jump through all the API hoops to end up
            # having to upload an image with a very basic message.
            if os.path.isfile(image_file):
                # @modified 20200701 - Task #3612: Upgrade to slack v2
                #                      Task #3608: Update Skyline to Python 3.8.3 and deps
                #                      Task #3556: Update deps
                if slack_version == '1.3':
                    slack_file_upload = sc.api_call(
                        'files.upload', filename=filename, channels=channel,
                        initial_comment=initial_comment, file=open(image_file, 'rb'))
                else:
                    slack_file_upload = sc.files_upload(
                        filename=filename, channels=channel,
                        initial_comment=initial_comment, file=open(image_file, 'rb'))
                if not slack_file_upload['ok']:
                    logger.error('error :: alert_slack - failed to send slack message')
                # @added 20181205 - Branch #2646: slack
                # The slack message id needs to be determined here so that it
                # can be recorded against the anamoly id and so that Skyline can
                # notify the message thread when a training_data page is
                # reviewed, a feature profile is created and/or a layers profile
                # is created.
                else:
                    logger.info('alert_slack - sent slack message')
                    if slack_thread_updates:
                        # @added 20190508 - Bug #2986: New slack messaging does not handle public channel
                        #                   Issue #111: New slack messaging does not handle public channel
                        # The sc.api_call 'files.upload' response which generates
                        # slack_file_upload has a different structure depending
                        # on whether a channel is private or public.  That also
                        # goes for free or hosted slack too.
                        slack_group = None
                        slack_group_list = None

                        # This is basically the channel id of your channel, the
                        # name could be used so that if in future it is used or
                        # displayed in a UI
                        # @modified 20190508 - Bug #2986: New slack messaging does not handle public channel
                        #                      Issue #111: New slack messaging does not handle public channel
                        # This block only works for free slack workspace private
                        # channels.  Although this should be handled in the
                        # SLACK_OPTS as slack_account_type: 'free|hosted' and
                        # default_channel_type = 'private|public', it is going
                        # to be handled in the code for the time being so as not
                        # to inconvience users to update their settings.py for
                        # v.1.2.17 # TODO next release with settings change add
                        # these.
                        # try:
                        #     slack_group = slack_file_upload['file']['groups'][0].encode('utf-8')
                        #     slack_group_list = slack_file_upload['file']['shares']['private'][slack_group]
                        #     slack_thread_ts = slack_group_list[0]['ts'].encode('utf-8')
                        #     logger.info('alert_slack - slack group is %s and the slack_thread_ts is %s' % (
                        #         str(slack_group), str(slack_thread_ts)))
                        # except:
                        #     logger.info(traceback.format_exc())
                        #     logger.error('error :: alert_slack - faied to determine slack_thread_ts')

                        slack_group = None
                        slack_group_trace_groups = None
                        slack_group_trace_channels = None
                        try:
                            # @modified 20191106 - Branch #3262: py3
                            # slack_group = slack_file_upload['file']['groups'][0].encode('utf-8')
                            slack_group = slack_file_upload['file']['groups'][0]
                            logger.info('alert_slack - slack group has been set from \'groups\' as %s' % (
                                str(slack_group)))
                            slack_group_list = slack_file_upload['file']['shares']['private'][slack_group]
                            slack_thread_ts = slack_group_list[0]['ts'].encode('utf-8')
                            logger.info('alert_slack - slack group is %s and the slack_thread_ts is %s' % (
                                str(slack_group), str(slack_thread_ts)))
                        except:
                            slack_group_trace_groups = traceback.format_exc()
                            logger.info('alert_slack - failed to determine slack_group using groups')
                        if not slack_group:
                            # Try by channel
                            try:
                                # @modified 20191106 - Branch #3262: py3
                                # slack_group = slack_file_upload['file']['channels'][0].encode('utf-8')
                                slack_group = slack_file_upload['file']['channels'][0]
                                logger.info('alert_slack - slack group has been set from \'channels\' as %s' % (
                                    str(slack_group)))
                            except:
                                slack_group_trace_channels = traceback.format_exc()
                                logger.info('alert_slack - failed to determine slack_group using channels')
                                logger.error('error :: alert_slack - failed to determine slack_group using groups or channels')
                                logger.error('error :: alert_slack - traceback from slack_group_trace_groups follows:')
                                logger.error(str(slack_group_trace_groups))
                                logger.error('error :: alert_slack - traceback from slack_group_trace_channels follows:')
                                logger.error(str(slack_group_trace_channels))
                                logger.error('error :: alert_slack - faied to determine slack_thread_ts')
                        slack_group_list = None
                        if slack_group:
                            slack_group_list_trace_private = None
                            slack_group_list_trace_public = None
                            # Try private channel
                            try:
                                slack_group_list = slack_file_upload['file']['shares']['private'][slack_group]
                                logger.info('alert_slack - slack_group_list determined from private channel and slack_group %s' % (
                                    str(slack_group)))
                            except:
                                slack_group_list_trace_private = traceback.format_exc()
                                logger.info('alert_slack - failed to determine slack_group_list using private channel')
                            if not slack_group_list:
                                # Try public channel
                                try:
                                    slack_group_list = slack_file_upload['file']['shares']['public'][slack_group]
                                    logger.info('alert_slack - slack_group_list determined from public channel and slack_group %s' % (
                                        str(slack_group)))
                                except:
                                    slack_group_list_trace_public = traceback.format_exc()
                                    logger.info('alert_slack - failed to determine slack_group_list using public channel')
                                    logger.info('alert_slack - failed to determine slack_group_list using private or public channel')
                                    logger.error('error :: alert_slack - traceback from slack_group_list_trace_private follows:')
                                    logger.error(str(slack_group_list_trace_private))
                                    logger.error('error :: alert_slack - traceback from slack_group_list_trace_public follows:')
                                    logger.error(str(slack_group_list_trace_public))
                                    logger.error('error :: alert_slack - faied to determine slack_thread_ts')
                        if slack_group_list:
                            try:
                                # @modified 20191106 - Branch #3262: py3
                                # slack_thread_ts = slack_group_list[0]['ts'].encode('utf-8')
                                slack_thread_ts = slack_group_list[0]['ts']
                                logger.info('alert_slack - slack group is %s and the slack_thread_ts is %s' % (
                                    str(slack_group), str(slack_thread_ts)))
                            except:
                                logger.error(traceback.format_exc())
                                logger.info('alert_slack - failed to determine slack_thread_ts')
            else:
                send_text = initial_comment + '  ::  error :: there was no graph image to upload'
                send_message = sc.api_call(
                    'chat.postMessage',
                    channel=channel,
                    icon_emoji=icon_emoji,
                    text=send_text)
                if not send_message['ok']:
                    logger.error('error :: alert_slack - failed to send slack message')
                else:
                    logger.info('alert_slack - sent slack message')
        except:
            logger.info(traceback.format_exc())
            logger.error('error :: alert_slack - could not upload file')
            return False

        # No anomaly_id will be known here so Panorama has to surface the
        # anomalies table anomaly_id that matches base_name, metric_timestamp
        # and update the slack_thread_ts field in the anomalies table db record
        # A Redis key is added here for Panorama
        # [base_name, metric_timestamp, slack_thread_ts]
        if not settings.PANORAMA_ENABLED:
            add_to_panorama = False
        if slack_file_upload:
            if not slack_file_upload['ok']:
                add_to_panorama = False
        else:
            add_to_panorama = False
        if not slack_thread_updates:
            add_to_panorama = False
        if float(slack_thread_ts) == 0:
            add_to_panorama = False
        if add_to_panorama:
            REDIS_ALERTER_CONN = None
            try:
                # @modified 20191106 - Bug #3266: py3 Redis binary objects not strings
                #                      Branch #3262: py3
                # Use get_redis_conn_decoded
                # if settings.REDIS_PASSWORD:
                #     REDIS_ALERTER_CONN = redis.StrictRedis(password=settings.REDIS_PASSWORD, unix_socket_path=settings.REDIS_SOCKET_PATH)
                # else:
                #     REDIS_ALERTER_CONN = redis.StrictRedis(unix_socket_path=settings.REDIS_SOCKET_PATH)
                REDIS_ALERTER_CONN = get_redis_conn(skyline_app)
            except:
                logger.error('error :: alert_slack - redis connection failed')

            metric_timestamp = int(metric[2])
            cache_key = 'panorama.slack_thread_ts.%s.%s' % (str(metric_timestamp), base_name)

            # @added 20190719 - Bug #3110: webapp - slack_response boolean object
            # When there are multiple channels declared Skyline only wants to
            # update the panorama.slack_thread_ts Redis key once for the
            # primary Skyline channel, so if the cache key exists do not update
            key_exists = False
            try:
                key_exists = REDIS_ALERTER_CONN.get(cache_key)
            except Exception as e:
                logger.error('error :: could not query Redis for cache_key: %s' % e)
            if key_exists:
                logger.info('cache key exists for previous channel, not updating %s' % str(cache_key))
            else:
                # @modified 20190719 - Bug #3110: webapp - slack_response boolean object
                # Wrapped in if key_exists, only add a Panorama Redis key if one
                # has not been added.
                # @modified 20191106 - Branch #3262: py3
                # cache_key_value = [base_name, metric_timestamp, slack_thread_ts]
                cache_key_value = [base_name, metric_timestamp, str(slack_thread_ts)]
                try:
                    REDIS_ALERTER_CONN.setex(
                        cache_key, 86400,
                        str(cache_key_value))
                    logger.info(
                        'added Panorama slack_thread_ts Redis key - %s - %s' %
                        (cache_key, str(cache_key_value)))
                except:
                    logger.error(traceback.format_exc())
                    logger.error(
                        'error :: failed to add Panorama slack_thread_ts Redis key - %s - %s' %
                        (cache_key, str(cache_key_value)))

        # @added 20200825 - Feature #3704: Add alert to anomalies
        if settings.PANORAMA_ENABLED:
            metric_timestamp = int(metric[2])
            added_panorama_alert_event = add_panorama_alert(skyline_app, metric_timestamp, base_name)
            if not added_panorama_alert_event:
                logger.error(
                    'error :: failed to add Panorama alert event - panorama.alert.%s.%s' % (
                        str(metric_timestamp), base_name))


# @added 20200116: Feature #3396: http_alerter
def alert_http(alert, metric, context):
    """
    Called by :func:`~trigger_alert` and sends and resend anomalies to a http
    endpoint.

    """
    if settings.HTTP_ALERTERS_ENABLED:
        alerter_name = alert[1]
        alerter_enabled = False
        try:
            alerter_enabled = settings.HTTP_ALERTERS_OPTS[alerter_name]['enabled']
        except:
            logger.error(traceback.format_exc())
            logger.error('error :: alert_http failed to determine the enabled from settings.HTTP_ALERTERS_OPTS for alert - %s and metric %s' % (
                str(alert), str(metric)))
        if not alerter_enabled:
            logger.info('alert_http - %s enabled %s, not alerting' % (
                str(alerter_name), str(alerter_enabled)))
            return
        alerter_endpoint = False
        try:
            alerter_endpoint = settings.HTTP_ALERTERS_OPTS[alerter_name]['endpoint']
        except:
            logger.error(traceback.format_exc())
            logger.error('error :: alert_http failed to determine the endpoint from settings.HTTP_ALERTERS_OPTS for alert - %s and metric %s' % (
                str(alert), str(metric)))
        if not alerter_endpoint:
            logger.error('alert_http - no endpoint set for %s, not alerting' % (
                str(alerter_name)))
            return

        alerter_token = None
        try:
            alerter_token = settings.HTTP_ALERTERS_OPTS[alerter_name]['token']
        except:
            pass

        # Test for mirage second_order_resolution_hours tuple
        try:
            full_duration = int(alert[3]) * 3600
            # @modified 20200610 - Feature #3560: External alert config
            # Wrapped in if so that if alert[3] 0 is
            # also handled in the all_alerts list
            if full_duration == 0:
                full_duration = settings.FULL_DURATION
        except:
            full_duration = settings.FULL_DURATION

        source = context.lower()
        metric_alert_dict = {}
        alert_data_dict = {}
        try:
            metric_name = str(metric[1])
            timestamp = int(metric[2])
            timestamp_str = str(timestamp)
            # @modified 20200611 - Feature #3578: Test alerts
            # Allow a str as well to display TEST
            try:
                value_str = str(float(metric[0]))
            except:
                value_str = str(metric[0])
            full_duration_str = str(int(full_duration))
            expiry_str = str(int(alert[2]))

            # @added 20200624 - Feature #3560: External alert config
            # Add the external alerter id to the metric_alert_dict
            external_alerter_id = None
            try:
                if alert[4]['type'] == 'external':
                    # @modified 20200624 - Feature #3560: External alert config
                    # Set the alert key to the external alerter id
                    # external_alerter_alerter = alert[4]['alerter']
                    external_alerter_id = alert[4]['id'].replace('external-', '')
            except:
                external_alerter_id = None

            # @added 20201008 - Feature #3772: Add the anomaly_id to the http_alerter json
            #                   Feature #3734: waterfall alerts
            #                   Branch #3068: SNAB
            anomaly_id = None
            anomalyScore = 1.0
            try:
                anomaly_id_details = alert[4]
                if anomaly_id_details[0] == 'anomaly_id':
                    anomaly_id = anomaly_id_details[1]
                    anomalyScore = 1.0
            except:
                anomaly_id = None
            if not anomaly_id:
                try:
                    anomaly_id_details = alert[5]
                    if anomaly_id_details[0] == 'anomaly_id':
                        anomaly_id = anomaly_id_details[1]
                        anomalyScore = 1.0
                except:
                    anomaly_id = None
                if not anomaly_id:
                    logger.error('error :: alert_http :: failed to determine anomaly_id from alert[4] or alert[5]')

            # @added 20201111 - Feature #3772: Add the anomaly_id to the http_alerter json
            # Add the real anomalyScore
            anomalyScore = None
            try:
                anomalyScore_details = alert[5]
                if anomalyScore_details[0] == 'anomalyScore':
                    anomalyScore = anomalyScore_details[1]
            except:
                pass
            if not anomalyScore:
                try:
                    anomalyScore_details = alert[6]
                    if anomalyScore_details[0] == 'anomalyScore':
                        anomalyScore = anomalyScore_details[1]
                except:
                    pass
            if not anomalyScore:
                logger.error('error :: alert_http :: failed to determine anomalyScore from alert[5] or alert[6] - set anomalyScore to 1.0')
                anomalyScore = 1.0

            # @added 20201112 - Feature #3772: Add the anomaly_id to the http_alerter json
            # Add the upper and lower 3sigma bounds
            sigma3_upper_bound = 0
            sigma3_lower_bound = 0
            sigma3_real_lower_bound = 0
            anomaly_json = None

            # @added 20201124 - Feature #3772: Add the anomaly_id to the http_alerter json
            # Added yhat values
            yhat_upper = 0
            yhat_lower = 0
            yhat_real_lower = 0

            # @added 20201113 - Feature #3772: Add the anomaly_id to the http_alerter json
            # Add the upper and lower 3sigma bounds, only if training data will
            # exist
            training_data_available_for_sigma3 = False

            if settings.IONOSPHERE_ENABLED:
                # @added 20201113 - Feature #3772: Add the anomaly_id to the http_alerter json
                # Add the upper and lower 3sigma bounds, only if the metric is
                # a smtp_alerter_metrics metric, if not, there will be no
                # training data
                smtp_alerter_metric = False
                try:
                    REDIS_HTTP_ALERTER_CONN_DECODED = get_redis_conn_decoded(skyline_app)
                    redis_set = 'aet.analyzer.smtp_alerter_metrics'
                    try:
                        smtp_alerter_metric = REDIS_HTTP_ALERTER_CONN_DECODED.sismember(redis_set, metric_name)
                    except Exception as e:
                        logger.error('error :: alert_http :: could not query Redis set %s - %s' % (redis_set, e))
                    if REDIS_HTTP_ALERTER_CONN_DECODED:
                        try:
                            del REDIS_HTTP_ALERTER_CONN_DECODED
                        except:
                            pass
                except:
                    logger.error(traceback.format_exc())
                    logger.error('error :: alert_http :: failed to connect to query Redis for aet.smtp_alerter_metrics')
                if smtp_alerter_metric:
                    training_data_available_for_sigma3 = True
                else:
                    logger.info('alert_http :: will not calculated 3sigma upper and lower bounds for %s as it is not a smtp_alerter_metric s no training dat to calculate from' % metric_name)

            if training_data_available_for_sigma3:
                logger.info('alert_http :: calculating 3sigma upper and lower bounds for %s' % metric_name)
                try:
                    timeseries_dir = metric_name.replace('.', '/')
                    training_data_dir = '%s/%s/%s' % (
                        settings.IONOSPHERE_DATA_FOLDER, str(int(metric[2])),
                        timeseries_dir)
                    anomaly_json = '%s/%s.mirage.redis.%sh.json' % (
                        training_data_dir, metric_name,
                        str(int(full_duration_in_hours)))
                except:
                    logger.error(traceback.format_exc())
                    logger.error('error :: alert_http :: failed to determine anomaly_json')
                raw_timeseries = None
                if anomaly_json:
                    try:
                        # Read the timeseries json file
                        with open(anomaly_json, 'r') as f:
                            raw_timeseries = f.read()
                    except:
                        logger.error(traceback.format_exc())
                        logger.error('error :: alert_http :: failed to determine 3sigma upper and lower bounds')
                if raw_timeseries:
                    try:
                        timeseries_array_str = str(raw_timeseries).replace('(', '[').replace(')', ']')
                        timeseries = literal_eval(timeseries_array_str)
                        values = pd.Series([x[1] for x in timeseries])
                        array_amin = np.amin(values)
                        mean = values.mean()
                        stdDev = values.std()
                        sigma3 = 3 * stdDev
                        sigma3_upper_bound = mean + sigma3
                        if array_amin >= 0:
                            try:
                                sigma3_lower_bound = mean - sigma3
                            except:
                                sigma3_lower_bound = 0
                            sigma3_real_lower_bound = sigma3_lower_bound
                        else:
                            sigma3_real_lower_bound = 0
                        if array_amin == 0:
                            sigma3_real_lower_bound = 0
                        if sigma3_real_lower_bound < 0:
                            if array_amin >= 0:
                                sigma3_real_lower_bound = 0
                        logger.info('alert_http :: calculated 3sigma_upper: %s and 3sigma_lower: %s for %s' % (
                            str(sigma3_upper_bound), str(sigma3_lower_bound),
                            metric_name))
                        try:
                            del raw_timeseries
                        except:
                            pass
                        try:
                            del timeseries_array_str
                        except:
                            pass
                        try:
                            del timeseries
                        except:
                            pass
                        try:
                            del values
                        except:
                            pass
                    except:
                        logger.error(traceback.format_exc())
                        logger.error('error :: alert_http :: failed to determine 3sigma upper and lower bounds')
                else:
                    logger.error('error :: alert_http :: failed to load timeseries data to calculate 3sigma bound from %s' % anomaly_json)

            # @added 20201124 - Feature #3772: Add the anomaly_id to the http_alerter json
            # Added yhat values
            if yhat_upper == 0:
                yhat_upper = sigma3_upper_bound
                yhat_lower = sigma3_lower_bound
                yhat_real_lower = sigma3_real_lower_bound

            # @modified 20201008 - Feature #3772: Add the anomaly_id to the http_alerter json
            #                      Feature #3734: waterfall alerts
            #                      Branch #3068: SNAB
            # Added anomaly_id and anomalyScore
            metric_alert_dict = {
                "metric": str(metric[1]),
                "timestamp": timestamp_str,
                "value": value_str,
                "full_duration": full_duration_str,
                "expiry": expiry_str,
                "source": str(source),
                "token": str(alerter_token),
                "id": str(external_alerter_id),
                "anomaly_id": str(anomaly_id),
                "anomalyScore": str(anomalyScore),
                # @added 20201112 - Feature #3772: Add the anomaly_id to the http_alerter json
                # Add the upper and lower 3sigma bounds
                "3sigma_upper": sigma3_upper_bound,
                "3sigma_lower": sigma3_lower_bound,
                "3sigma_real_lower": sigma3_real_lower_bound,
                # @added 20201124 - Feature #3772: Add the anomaly_id to the http_alerter json
                # Added yhat values
                "yhat_upper": yhat_upper,
                "yhat_lower": yhat_lower,
                "yhat_real_lower": yhat_real_lower,
            }
            # @modified 20200302: Feature #3396: http_alerter
            # Add the token as an independent entity from the alert
            # alert_data_dict = {"status": {}, "data": {"alert": metric_alert_dict}}
            alerter_token_str = str(alerter_token)
            # @modified 20201127 - Feature #3820: HORIZON_SHARDS
            # Add the origin and shard to status for debugging purposes
            if not HORIZON_SHARDS:
                alert_data_dict = {"status": {}, "data": {"token": alerter_token_str, "alert": metric_alert_dict}}
            else:
                alert_data_dict = {"status": {"origin": this_host, "shard": HORIZON_SHARD}, "data": {"token": alerter_token_str, "alert": metric_alert_dict}}

            logger.info('alert_http :: alert_data_dict to send - %s' % str(alert_data_dict))
            # Allow requests to send as json
            # alert_data = json.dumps(alert_data_dict)
            # logger.info('alert_http :: alert_data to send - %s' % str(alert_data))
        except:
            logger.error(traceback.format_exc())
            logger.error('error :: alert_http failed to construct the alert data for %s from alert - %s and metric - %s' % (
                str(alerter_name), str(alert), str(metric)))
            return

        # @added 20201012 - Feature #3772: Add the anomaly_id to the http_alerter json
        # If this is an external alerter and there is no anomaly id, log an
        # error and do not send
        if external_alerter_id and not anomaly_id:
            logger.error('error :: alert_http :: failed to determine anomaly_id not sending alert')
            return

        in_resend_queue = False
        redis_set = '%s.http_alerter.queue' % str(source)
        resend_queue = None
        previous_attempts = 0
        REDIS_HTTP_ALERTER_CONN_DECODED = get_redis_conn_decoded(skyline_app)
        try:
            resend_queue = REDIS_HTTP_ALERTER_CONN_DECODED.smembers(redis_set)
        except Exception as e:
            logger.error('error :: alert_http :: could not query Redis for %s - %s' % (redis_set, e))
        if REDIS_HTTP_ALERTER_CONN_DECODED:
            try:
                del REDIS_HTTP_ALERTER_CONN_DECODED
            except:
                pass
        if resend_queue:
            try:
                for index, resend_item in enumerate(resend_queue):
                    resend_item_list = literal_eval(resend_item)
                    # resend_alert = literal_eval(resend_item_list[0])
                    # resend_metric = literal_eval(resend_item_list[1])
                    resend_metric_alert_dict = literal_eval(resend_item_list[2])
                    if resend_metric_alert_dict['metric'] == metric_name:
                        if int(resend_metric_alert_dict['timestamp']) == timestamp:
                            previous_attempts = int(resend_metric_alert_dict['attempts'])
                            in_resend_queue = True
                            break
            except:
                logger.error(traceback.format_exc())
                logger.error('error :: alert_http failed iterate to resend_queue')
        REDIS_HTTP_ALERTER_CONN = None
        # if in_resend_queue:
        #    REDIS_HTTP_ALERTER_CONN = get_redis_conn(skyline_app)
        REDIS_HTTP_ALERTER_CONN = get_redis_conn(skyline_app)

        add_to_resend_queue = False
        fail_alerter = False
        if alert_data_dict and alerter_endpoint:
            # @modified 20200403 - Feature #3396: http_alerter
            # Changed timeouts from 2, 2 to 5, 20
            connect_timeout = 5
            read_timeout = 20
            if requests.__version__ >= '2.4.0':
                use_timeout = (int(connect_timeout), int(read_timeout))
            else:
                use_timeout = int(connect_timeout)
            if settings.ENABLE_DEBUG:
                logger.debug('debug :: use_timeout - %s' % (str(use_timeout)))
            # headers = {'Content-type': 'application/json', 'Accept': 'text/plain'}
            response = None
            try:
                # response = requests.post(alerter_endpoint, data=alert_data, headers=headers, timeout=use_timeout)
                response = requests.post(alerter_endpoint, json=alert_data_dict, timeout=use_timeout)
            except:
                logger.error(traceback.format_exc())
                logger.error('error :: failed to post alert to %s - %s' % (
                    str(alerter_name), str(alert_data_dict)))
                add_to_resend_queue = True
                response = None

            if in_resend_queue:
                try:
                    REDIS_HTTP_ALERTER_CONN.srem(redis_set, str(resend_item))
                    logger.info('alert_http :: alert removed from %s' % (
                        str(redis_set)))
                except:
                    logger.error(traceback.format_exc())
                    logger.error('error :: alert_http :: failed remove %s from Redis set %s' % (
                        str(resend_item), redis_set))

            # @added 20200310 - Feature #3396: http_alerter
            # When the response code is 401 the response object appears to be
            # False, although the response.code and response.reason are set
            try:
                if response.status_code != 200:
                    logger.error('error :: alert_http :: %s %s responded with status code %s and reason %s' % (
                        str(alerter_name), str(alerter_endpoint),
                        str(response.status_code), str(response.reason)))
                    add_to_resend_queue = True
                    fail_alerter = True
            except:
                logger.error(traceback.format_exc())
                logger.error('error :: alert_http :: failed determine response.status_code')

            if response:
                if response.status_code == 200:
                    logger.info('alert_http :: alert sent to %s - %s' % (
                        str(alerter_endpoint), str(alert_data_dict)))
                    if in_resend_queue:
                        logger.info('alert_http :: alert removed from %s after %s attempts to send' % (
                            str(redis_set), str(previous_attempts)))
                    try:
                        del REDIS_HTTP_ALERTER_CONN
                    except:
                        pass

                    # @added 20200825 - Feature #3704: Add alert to anomalies
                    if settings.PANORAMA_ENABLED:
                        added_panorama_alert_event = add_panorama_alert(skyline_app, timestamp, metric_name)
                        if not added_panorama_alert_event:
                            logger.error(
                                'error :: failed to add Panorama alert event - panorama.alert.%s.%s' % (
                                    str(timestamp), metric_name))

                    return
                else:
                    logger.error('error :: alert_http :: %s %s responded with status code %s and reason %s' % (
                        str(alerter_name), str(alerter_endpoint),
                        str(response.status_code), str(response.reason)))
                    add_to_resend_queue = True
                    fail_alerter = True
            else:
                logger.error('error :: alert_http :: %s %s did not respond as expected' % (
                    str(alerter_name), str(alerter_endpoint)))
                add_to_resend_queue = True
                fail_alerter = True

            number_of_send_attempts = previous_attempts + 1
            metric_alert_dict['attempts'] = number_of_send_attempts
            if add_to_resend_queue:
                alert_str = str(alert)
                metric_str = str(metric)
                data = [alert_str, metric_str, str(metric_alert_dict)]
                logger.info('alert_http :: adding alert to %s after %s attempts to send - %s' % (
                    str(redis_set), str(number_of_send_attempts), str(metric_alert_dict)))
                # try:
                #     REDIS_HTTP_ALERTER_CONN
                # except:
                #     REDIS_HTTP_ALERTER_CONN = get_redis_conn(skyline_app)
                try:
                    # redis_conn.sadd(redis_set, str(metric_alert_dict))
                    REDIS_HTTP_ALERTER_CONN.sadd(redis_set, str(data))
                except:
                    logger.error(traceback.format_exc())
                    logger.error('error :: alert_http :: failed to add %s from Redis set %s' % (
                        str(metric_alert_dict), redis_set))

            # Create a Redis if there was a bad or no response from the
            # alerter_endpoint, to ensure that Analyzer does not loop through
            # every alert in the queue for an alerter_endpoint, if the
            # alerter_endpoint is down
            if fail_alerter:
                alerter_endpoint_cache_key = 'http_alerter.down.%s' % str(alerter_name)
                logger.error('error :: alert_http :: alerter_endpoint %s failed adding Redis key %s' % (
                    str(alerter_endpoint), str(alerter_endpoint_cache_key)))
                try:
                    REDIS_HTTP_ALERTER_CONN
                except:
                    try:
                        REDIS_HTTP_ALERTER_CONN = get_redis_conn(skyline_app)
                    except:
                        logger.error(traceback.format_exc())
                        logger.error('error :: alert_http :: failed to get_redis_conn to add key %s' % str(alerter_endpoint_cache_key))
                        REDIS_HTTP_ALERTER_CONN = None
                if REDIS_HTTP_ALERTER_CONN:
                    try:
                        failed_timestamp = int(time())
                        REDIS_HTTP_ALERTER_CONN.setex(alerter_endpoint_cache_key, 60, failed_timestamp)
                        del REDIS_HTTP_ALERTER_CONN
                    except:
                        logger.error(traceback.format_exc())
                        logger.error('error :: failed to set Redis key %s' % alerter_endpoint_cache_key)
        try:
            del REDIS_HTTP_ALERTER_CONN
        except:
            pass
    else:
        logger.info('alert_http :: settings.HTTP_ALERTERS_ENABLED not enabled nothing to do')
        return


# @added 20210724 - Feature #4196: functions.aws.send_sms
def alert_sms(alert, metric, context):
    """
    Called by :func:`~trigger_alert` and sends anomalies to a SMS endpoint.

    """
    if not settings.AWS_SNS_SMS_ALERTS_ENABLED:
        logger.error('error :: alert_sms recieved but settings.AWS_SNS_SMS_ALERTS_ENABLED not enabled for alert - %s and metric %s' % (
            str(alert), str(metric)))
        return
    metric_name = str(metric[1])
    if metric_name.startswith(settings.FULL_NAMESPACE):
        base_name = metric_name.replace(settings.FULL_NAMESPACE, '', 1)
    else:
        base_name = metric_name

    message = '[Skyline alert] Analyzer - %s: %s' % (base_name, str(metric[0]))
    sms_recipients = []
    try:
        sms_recipients = get_sms_recipients(skyline_app, base_name)
    except Exception as e:
        logger.error(traceback.format_exc())
        logger.error('error :: get_sms_recipients failed checking %s - %s' % (
            base_name, e))
    logger.info('sending SMS alert to %s' % str(sms_recipients))
    for sms_number in sms_recipients:
        success = False
        response = None
        try:
            success, response = send_sms(skyline_app, sms_number, message)
        except Exception as e:
            logger.error(traceback.format_exc())
            logger.error('error :: failed to determine number for SMS recipient %s - %s' % (
                sms_number, e))
        if success:
            logger.info('sent SMS alert to %s' % sms_number)
        else:
            logger.warn('warning :: falied to send SMS alert to %s' % sms_number)
    return


def trigger_alert(alert, metric, context):
    """
    Called by :py:func:`skyline.analyzer.Analyzer.spawn_alerter_process
    <analyzer.analyzer.Analyzer.spawn_alerter_process>` to trigger an alert.

    Analyzer passes three arguments, two of them tuples. The alerting strategy
    is determined and the approriate alert def is then called and passed the
    tuples.

    :param alert:
        The alert tuple specified in settings.py e.g. ('stats.*', 'smtp', 3600, 168)\n
        alert[0]: The matched substring of the anomalous metric (str)\n
        alert[1]: the name of the strategy being used to alert (str)\n
        alert[2]: The timeout of the alert that was triggered (int)\n
        alert[3]: The type [optional for http_alerter only] (dict)\n
                  The snab_details [optional for SNAB and slack only] (list)\n
        alert[4]: The snab_details [optional for SNAB and slack only] (list)\n
                  The anomaly_id [optional for http_alerter only] (list)\n
    :param metric:
        The metric tuple e.g. (2.345, 'server-1.cpu.user', 1462172400)\n
        metric[0]: the anomalous value (float)\n
        metric[1]: The base_name of the anomalous metric (str)\n
        metric[2]: anomaly timestamp (float or int)\n
    :param context: app name
    :type context: str

    """

    if '@' in alert[1]:
        strategy = 'alert_smtp'
    # @added 20200116: Feature #3396: http_alerter
    # Added http_alerter
    elif 'http_alerter' in alert[1]:
        strategy = 'alert_http'
    else:
        strategy = 'alert_%s' % alert[1]
    try:
        getattr(alerters, strategy)(alert, metric, context)
    except:
        logger.error('error :: alerters - %s - getattr error' % strategy)
        logger.info(traceback.format_exc())
