"""
    boundary_alerters
"""
from __future__ import division
import logging
import traceback
# @modified 20201207 - Task #3878: Add metric_trigger and alert_threshold to Boundary alerts
# hashlib not used
# import hashlib
from smtplib import SMTP

import re
# from time import time
import datetime
import os.path
import sys

# @added 20181126 - Task #2742: Update Boundary
#                   Feature #2618: alert_slack
# Added dt, redis, gmtime and strftime
# import datetime as dt
# import redis
from time import (time, gmtime, strftime)

# @added 20201127 - Feature #3820: HORIZON_SHARDS
from os import uname

# @modified 20201207 - Task #3878: Add metric_trigger and alert_threshold to Boundary alerts
# charset no longer used
# from email import charset
# @modified 20220111 - Bug #4366: Fix boundary slack channels match
# Linting removed python 2 support
# if python_version == 2:
#     from email.MIMEMultipart import MIMEMultipart
#     from email.MIMEText import MIMEText
#     from email.MIMEImage import MIMEImage
# if python_version == 3:
#     from email.mime.multipart import MIMEMultipart
#     from email.mime.text import MIMEText
#     from email.mime.image import MIMEImage
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.image import MIMEImage

# @added 20200122: Feature #3396: http_alerter
from ast import literal_eval
import requests

import boundary_alerters
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

from requests.utils import quote

python_version = int(sys.version_info[0])

sys.path.append(os.path.join(os.path.dirname(os.path.realpath(__file__)), os.pardir))
sys.path.insert(0, os.path.dirname(__file__))
if True:
    import settings
    # @added 20181126 - Task #2742: Update Boundary
    #                   Feature #2034: analyse_derivatives
    #                   Feature #2618: alert_slack
    from skyline_functions import (
        write_data_to_file, in_list,
        is_derivative_metric, get_graphite_graph_image,
        # @added 20191030 - Bug #3266: py3 Redis binary objects not strings
        #                   Branch #3262: py3
        # Added a single functions to deal with Redis connection and the
        # charset='utf-8', decode_responses=True arguments required in py3
        get_redis_conn_decoded,
        # @modified 20191105 - Branch #3002: docker
        #                      Branch #3262: py3
        get_graphite_port, get_graphite_render_uri, get_graphite_custom_headers,
        # @added 20200122: Feature #3396: http_alerter
        get_redis_conn,
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

skyline_app = 'boundary'
skyline_app_logger = '%sLog' % skyline_app
logger = logging.getLogger(skyline_app_logger)
skyline_app_logfile = '%s/%s.log' % (settings.LOG_PATH, skyline_app)

"""
Create any alerter you want here. The function is invoked from trigger_alert.
7 arguments will be passed in as strings:
alerter, datapoint, metric_name, expiration_time, metric_trigger, algorithm, metric_timestamp
"""

# FULL_DURATION to hours so that Boundary surfaces the relevant timeseries data
# in the graph
try:
    full_duration_seconds = int(settings.FULL_DURATION)
except:
    full_duration_seconds = 86400
full_duration_in_hours = full_duration_seconds / 60 / 60

try:
    graphite_previous_hours = int(settings.BOUNDARY_SMTP_OPTS['graphite_previous_hours'])
except:
    graphite_previous_hours = full_duration_in_hours

try:
    graphite_graph_line_color = int(settings.BOUNDARY_SMTP_OPTS['graphite_graph_line_color'])
except:
    graphite_graph_line_color = 'pink'

# @added 20200122 - Branch #3002: docker
try:
    DOCKER_FAKE_EMAIL_ALERTS = settings.DOCKER_FAKE_EMAIL_ALERTS
except:
    DOCKER_FAKE_EMAIL_ALERTS = False


def alert_smtp(datapoint, metric_name, expiration_time, metric_trigger, algorithm, metric_timestamp, alert_threshold):

    sender = settings.BOUNDARY_SMTP_OPTS['sender']

    matched_namespaces = []
    for namespace in settings.BOUNDARY_SMTP_OPTS['recipients']:
        CHECK_MATCH_PATTERN = namespace
        check_match_pattern = re.compile(CHECK_MATCH_PATTERN)
        pattern_match = check_match_pattern.match(metric_name)
        if pattern_match:
            matched_namespaces.append(namespace)
    matched_recipients = []
    for namespace in matched_namespaces:
        for recipients in settings.BOUNDARY_SMTP_OPTS['recipients'][namespace]:
            matched_recipients.append(recipients)

    def unique_noHash(seq):
        seen = set()
        return [x for x in seq if str(x) not in seen and not seen.add(str(x))]

    recipients = unique_noHash(matched_recipients)

    # Backwards compatibility
    # @modified 20220111 - Bug #4366: Fix boundary slack channels match
    # Linting
    # if type(recipients) is str:
    if isinstance(recipients, str):
        recipients = [recipients]

    # @added 20180524 - Task #2384: Change alerters to cc other recipients
    # The alerters did send an individual email to each recipient. This would be
    # more useful if one email was sent with the first smtp recipient being the
    # to recipient and the subsequent recipients were add in cc.
    primary_recipient = False
    cc_recipients = False
    if recipients:
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

    alert_algo = str(algorithm)
    alert_context = alert_algo.upper()

    # @added 20191008 - Feature #3194: Add CUSTOM_ALERT_OPTS to settings
    try:
        main_alert_title = settings.CUSTOM_ALERT_OPTS['main_alert_title']
    except:
        main_alert_title = 'Skyline'
    try:
        app_alert_context = settings.CUSTOM_ALERT_OPTS['boundary_alert_heading']
    except:
        app_alert_context = 'Boundary'

    # @modified 20191002 - Feature #3194: Add CUSTOM_ALERT_OPTS to settings
    # Use alert_context
    # unencoded_graph_title = 'Skyline Boundary - %s at %s hours - %s - %s' % (
    #     alert_context, graphite_previous_hours, metric_name, datapoint)
    # @modified 20201207 - Task #3878: Add metric_trigger and alert_threshold to Boundary alerts
    # unencoded_graph_title = '%s %s - %s at %s hours - %s - %s' % (
    #     main_alert_title, app_alert_context, alert_context, graphite_previous_hours, metric_name, datapoint)
    unencoded_graph_title = '%s %s - %s %s %s times - %s' % (
        main_alert_title, app_alert_context, alert_context, str(metric_trigger),
        str(alert_threshold), str(datapoint))

    # @added 20181126 - Task #2742: Update Boundary
    #                   Feature #2034: analyse_derivatives
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
        #     # @modified 20191022 - Bug #3266: py3 Redis binary objects not strings
        #     #                      Branch #3262: py3
        #     # REDIS_ALERTER_CONN = redis.StrictRedis(password=settings.REDIS_PASSWORD, unix_socket_path=settings.REDIS_SOCKET_PATH)
        #     REDIS_ALERTER_CONN = redis.StrictRedis(password=settings.REDIS_PASSWORD, unix_socket_path=settings.REDIS_SOCKET_PATH, charset='utf-8', decode_responses=True)
        # else:
        #     # REDIS_ALERTER_CONN = redis.StrictRedis(unix_socket_path=settings.REDIS_SOCKET_PATH)
        #     REDIS_ALERTER_CONN = redis.StrictRedis(unix_socket_path=settings.REDIS_SOCKET_PATH, charset='utf-8', decode_responses=True)
        REDIS_ALERTER_CONN = get_redis_conn_decoded(skyline_app)
    except:
        logger.error('error :: alert_smtp - redis connection failed')

    # @modified 20191022 - Bug #3266: py3 Redis binary objects not strings
    #                      Branch #3262: py3
    try:
        # @modified 20211012 - Feature #4280: aet.metrics_manager.derivative_metrics Redis hash
        # derivative_metrics = list(REDIS_ALERTER_CONN.smembers('derivative_metrics'))
        derivative_metrics = list(REDIS_ALERTER_CONN.smembers('aet.metrics_manager.derivative_metrics'))
    except:
        derivative_metrics = []
    redis_metric_name = '%s%s' % (settings.FULL_NAMESPACE, str(metric_name))
    if redis_metric_name in derivative_metrics:
        known_derivative_metric = True
    if known_derivative_metric:
        try:
            non_derivative_monotonic_metrics = settings.NON_DERIVATIVE_MONOTONIC_METRICS
        except:
            non_derivative_monotonic_metrics = []
        skip_derivative = in_list(redis_metric_name, non_derivative_monotonic_metrics)
        if skip_derivative:
            known_derivative_metric = False

    known_derivative_metric = is_derivative_metric(skyline_app, metric_name)

    if known_derivative_metric:
        # @modified 20191002 - Feature #3194: Add CUSTOM_ALERT_OPTS to settings
        # unencoded_graph_title = 'Skyline Boundary - %s at %s hours - derivative graph - %s - %s' % (
        #     alert_context, graphite_previous_hours, metric_name, datapoint)
        # @modified 20201207 - Task #3878: Add metric_trigger and alert_threshold to Boundary alerts
        # unencoded_graph_title = '%s %s - %s at %s hours - derivative graph - %s - %s' % (
        #     main_alert_title, app_alert_context, alert_context, graphite_previous_hours, metric_name, datapoint)
        unencoded_graph_title = '%s %s - %s %s %s times - derivative graph - %s' % (
            main_alert_title, app_alert_context, alert_context, str(metric_trigger),
            str(alert_threshold), str(datapoint))

    graph_title_string = quote(unencoded_graph_title, safe='')
    graph_title = '&title=%s' % graph_title_string

    # @added 20181126 - Bug #2498: Incorrect scale in some graphs
    #                   Task #2742: Update Boundary
    # If -xhours is used the scale is incorrect if x hours > than first
    # retention period, passing from and until renders the graph with the
    # correct scale.
    graphite_port = '80'
    if settings.GRAPHITE_PORT != '':
        graphite_port = str(settings.GRAPHITE_PORT)
    until_timestamp = int(time())
    from_seconds_ago = graphite_previous_hours * 3600
    from_timestamp = until_timestamp - from_seconds_ago
    graphite_from = datetime.datetime.fromtimestamp(int(from_timestamp)).strftime('%H:%M_%Y%m%d')
    logger.info('graphite_from - %s' % str(graphite_from))
    graphite_until = datetime.datetime.fromtimestamp(int(until_timestamp)).strftime('%H:%M_%Y%m%d')
    logger.info('graphite_until - %s' % str(graphite_until))
    # @modified 20191022 - Task #3294: py3 - handle system parameter in Graphite cactiStyle
    # graphite_target = 'target=cactiStyle(%s)'

    # @added 20201013 - Feature #3780: skyline_functions - sanitise_graphite_url
    encoded_graphite_metric_name = encode_graphite_metric_name(skyline_app, metric_name)

    # @modified 20201013 - Feature #3780: skyline_functions - sanitise_graphite_url
    # graphite_target = 'target=cactiStyle(%s,%%27si%%27)' % metric_name
    graphite_target = 'target=cactiStyle(%s,%%27si%%27)' % encoded_graphite_metric_name

    if known_derivative_metric:
        # @modified 20191022 - Task #3294: py3 - handle system parameter in Graphite cactiStyle
        # graphite_target = 'target=cactiStyle(nonNegativeDerivative(%s))'
        # @modified 20201013 - Feature #3780: skyline_functions - sanitise_graphite_url
        # graphite_target = 'target=cactiStyle(nonNegativeDerivative(%s),%%27si%%27)' % metric_name
        graphite_target = 'target=cactiStyle(nonNegativeDerivative(%s),%%27si%%27)' % encoded_graphite_metric_name

    # @modified 20190520 - Branch #3002: docker
    # Use GRAPHITE_RENDER_URI
    # link = '%s://%s:%s/render/?from=%s&until=%s&%s%s%s&colorList=%s' % (
    #     settings.GRAPHITE_PROTOCOL, settings.GRAPHITE_HOST, graphite_port,
    #     str(graphite_from), str(graphite_until), graphite_target,
    #     settings.GRAPHITE_GRAPH_SETTINGS, graph_title,
    #     graphite_graph_line_color)
    link = '%s://%s:%s/%s/?from=%s&until=%s&%s%s%s&colorList=%s' % (
        settings.GRAPHITE_PROTOCOL, settings.GRAPHITE_HOST,
        graphite_port, settings.GRAPHITE_RENDER_URI, str(graphite_from),
        str(graphite_until), graphite_target, settings.GRAPHITE_GRAPH_SETTINGS,
        graph_title, graphite_graph_line_color)

    content_id = metric_name
    image_data = None

    image_file = '%s/%s.%s.%s.alert_smtp.png' % (
        settings.SKYLINE_TMP_DIR, skyline_app, str(until_timestamp),
        metric_name)
    if settings.BOUNDARY_SMTP_OPTS.get('embed-images'):
        image_data = get_graphite_graph_image(skyline_app, link, image_file)

    if settings.BOUNDARY_SMTP_OPTS.get('embed-images_disabled3290'):
        # @modified 20191021 - Task #3290: Handle urllib2 in py3
        #                      Branch #3262: py3
        if python_version == 2:
            try:
                # @modified 20170913 - Task #2160: Test skyline with bandit
                # Added nosec to exclude from bandit tests
                # image_data = urllib2.urlopen(link).read()  # nosec
                image_data = None
            except urllib2.URLError:
                image_data = None
        if python_version == 3:
            try:
                # image_data = urllib.request.urlopen(link).read()  # nosec
                image_data = None
            except:
                logger.error(traceback.format_exc())
                logger.error('error :: boundary_alerters :: alert_smtp :: failed to urlopen %s' % str(link))
                image_data = None

    # If we failed to get the image or if it was explicitly disabled,
    # use the image URL instead of the content.
    if image_data is None:
        img_tag = '<img src="%s"/>' % link
    else:
        img_tag = '<img src="cid:%s"/>' % content_id

    # @modified 20191002 - Feature #3194: Add CUSTOM_ALERT_OPTS to settings
    # body = '%s :: %s <br> Next alert in: %s seconds <br> skyline Boundary alert - %s <br><a href="%s">%s</a>' % (
    #     datapoint, metric_name, expiration_time, alert_context, link, img_tag)
    body = '%s :: %s <br> Next alert in: %s seconds <br> %s %s alert - %s <br><a href="%s">%s</a>' % (
        main_alert_title, app_alert_context, expiration_time, datapoint, metric_name, alert_context, link, img_tag)

    # @added 20200122 - Branch #3002: docker
    # Do not try to alert if the settings are default
    send_email_alert = True
    if 'your_domain.com' in str(sender):
        logger.info('alert_smtp - sender is not configured, not sending alert')
        send_email_alert = False
    if 'your_domain.com' in str(primary_recipient):
        logger.info('alert_smtp - sender is not configured, not sending alert')
        send_email_alert = False
    if 'example.com' in str(sender):
        logger.info('alert_smtp - sender is not configured, not sending alert')
        send_email_alert = False
    if 'example.com' in str(primary_recipient):
        logger.info('alert_smtp - sender is not configured, not sending alert')
        send_email_alert = False
    if DOCKER_FAKE_EMAIL_ALERTS:
        logger.info('alert_smtp - DOCKER_FAKE_EMAIL_ALERTS is set to %s, not executing SMTP command' % str(DOCKER_FAKE_EMAIL_ALERTS))
        send_email_alert = False

    # @added 20200122 - Feature #3406: Allow for no_email SMTP_OPTS
    no_email = False
    if str(sender) == 'no_email':
        send_email_alert = False
        no_email = True
    if str(primary_recipient) == 'no_email':
        send_email_alert = False
        no_email = True
    if no_email:
        logger.info('alert_smtp - no_email is set in BOUNDARY_SMTP_OPTS, not executing SMTP command')

    # @modified 20180524 - Task #2384: Change alerters to cc other recipients
    # Do not send to each recipient, send to primary_recipient and cc the other
    # recipients, thereby sending only one email
    # for recipient in recipients:
    # @modified 20200122 - Feature #3406: Allow for no_email SMTP_OPTS
    # if primary_recipient:
    if primary_recipient and send_email_alert:
        logger.info(
            'alert_smtp - will send to primary_recipient :: %s, cc_recipients :: %s' %
            (str(primary_recipient), str(cc_recipients)))

        msg = MIMEMultipart('alternative')
        # @modified 20191002 - Feature #3194: Add CUSTOM_ALERT_OPTS to settings
        # msg['Subject'] = '[Skyline alert] ' + 'Boundary ALERT - ' + alert_context + ' - ' + datapoint + ' - ' + metric_name
        # @modified 20201207 - Task #3878: Add metric_trigger and alert_threshold to Boundary alerts
        # msg['Subject'] = '[' + main_alert_title + ' alert] ' + app_alert_context + ' ALERT - ' + alert_context + ' - ' + datapoint + ' - ' + metric_name
        email_subject = '[%s alert] %s ALERT - %s - %s' % (
            main_alert_title, app_alert_context, alert_context, metric_name)
        msg['Subject'] = email_subject
        msg['From'] = sender
        # @modified 20180524 - Task #2384: Change alerters to cc other recipients
        # msg['To'] = recipient
        msg['To'] = primary_recipient

        # @added 20180524 - Task #2384: Change alerters to cc other recipients
        # Added Cc
        if cc_recipients:
            msg['Cc'] = cc_recipients

        msg.attach(MIMEText(body, 'html'))
        if image_data is not None:

            # msg_attachment = MIMEImage(image_data)
            fp = open(image_file, 'rb')
            msg_attachment = MIMEImage(fp.read())
            fp.close()

            msg_attachment.add_header('Content-ID', '<%s>' % content_id)
            msg.attach(msg_attachment)

        s = SMTP('127.0.0.1')
        # @modified 20180524 - Task #2384: Change alerters to cc other recipients
        # Send to primary_recipient and cc_recipients
        # s.sendmail(sender, recipient, msg.as_string())
        try:
            if cc_recipients:
                s.sendmail(sender, [primary_recipient, cc_recipients], msg.as_string())
            else:
                s.sendmail(sender, primary_recipient, msg.as_string())
        except:
            logger.error(traceback.format_exc())
            logger.error(
                'error :: alert_smtp - could not send email to primary_recipient :: %s, cc_recipients :: %s' %
                (str(primary_recipient), str(cc_recipients)))
        s.quit()
        # @added 20200825 - Feature #3704: Add alert to anomalies
        if settings.PANORAMA_ENABLED:
            added_panorama_alert_event = add_panorama_alert(skyline_app, int(metric_timestamp), metric_name)
            if not added_panorama_alert_event:
                logger.error(
                    'error :: failed to add Panorama alert event - panorama.alert.%s.%s' % (
                        str(metric_timestamp), metric_name))


def alert_pagerduty(datapoint, metric_name, expiration_time, metric_trigger, algorithm, metric_timestamp, alert_threshold):
    if settings.PAGERDUTY_ENABLED:
        import pygerduty
        pager = pygerduty.PagerDuty(settings.BOUNDARY_PAGERDUTY_OPTS['subdomain'], settings.BOUNDARY_PAGERDUTY_OPTS['auth_token'])
        # @modified 20201207 - Task #3878: Add metric_trigger and alert_threshold to Boundary alerts
        # pager.trigger_incident(settings.BOUNDARY_PAGERDUTY_OPTS['key'], 'Anomalous metric: %s (value: %s) - %s' % (metric_name, datapoint, algorithm))
        pager.trigger_incident(settings.BOUNDARY_PAGERDUTY_OPTS['key'], 'Anomalous metric: %s (value: %s) - %s %s %s times' % (
            metric_name, str(datapoint), algorithm, str(metric_trigger),
            str(alert_threshold)))
        # @added 20200825 - Feature #3704: Add alert to anomalies
        if settings.PANORAMA_ENABLED:
            added_panorama_alert_event = add_panorama_alert(skyline_app, int(metric_timestamp), metric_name)
            if not added_panorama_alert_event:
                logger.error(
                    'error :: failed to add Panorama alert event - panorama.alert.%s.%s' % (
                        str(metric_timestamp), metric_name))
    else:
        return False


def alert_hipchat(datapoint, metric_name, expiration_time, metric_trigger, algorithm, metric_timestamp):

    if settings.HIPCHAT_ENABLED:
        sender = settings.BOUNDARY_HIPCHAT_OPTS['sender']
        import hipchat
        hipster = hipchat.HipChat(token=settings.BOUNDARY_HIPCHAT_OPTS['auth_token'])

        # Allow for absolute path metric namespaces but also allow for and match
        # match wildcard namepaces if there is not an absolute path metric namespace
        rooms = 'unknown'
        notify_rooms = []
        matched_rooms = []
        try:
            rooms = settings.BOUNDARY_HIPCHAT_OPTS['rooms'][metric_name]
            notify_rooms.append(rooms)
        except:
            for room in settings.BOUNDARY_HIPCHAT_OPTS['rooms']:
                CHECK_MATCH_PATTERN = room
                check_match_pattern = re.compile(CHECK_MATCH_PATTERN)
                pattern_match = check_match_pattern.match(metric_name)
                if pattern_match:
                    matched_rooms.append(room)

        if matched_rooms != []:
            for i_metric_name in matched_rooms:
                rooms = settings.BOUNDARY_HIPCHAT_OPTS['rooms'][i_metric_name]
                notify_rooms.append(rooms)

        alert_algo = str(algorithm)
        alert_context = alert_algo.upper()

        unencoded_graph_title = 'Skyline Boundary - %s at %s hours - %s - %s' % (
            alert_context, graphite_previous_hours, metric_name, datapoint)
        graph_title_string = quote(unencoded_graph_title, safe='')
        graph_title = '&title=%s' % graph_title_string

        # @modified 20170706 - Support #2072: Make Boundary hipchat alerts show fixed timeframe
        graphite_now = int(time())
        target_seconds = int((graphite_previous_hours * 60) * 60)
        from_timestamp = str(graphite_now - target_seconds)
        until_timestamp = str(graphite_now)
        graphite_from = datetime.datetime.fromtimestamp(int(from_timestamp)).strftime('%H:%M_%Y%m%d')
        graphite_until = datetime.datetime.fromtimestamp(int(until_timestamp)).strftime('%H:%M_%Y%m%d')

        if settings.GRAPHITE_PORT != '':
            # link = '%s://%s:%s/render/?from=-%shours&target=cactiStyle(%s)%s%s&colorList=%s' % (
                # settings.GRAPHITE_PROTOCOL, settings.GRAPHITE_HOST, settings.GRAPHITE_PORT,
                # graphite_previous_hours, metric_name, settings.GRAPHITE_GRAPH_SETTINGS,
            # @modified 20190520 - Branch #3002: docker
            # Use GRAPHITE_RENDER_URI
            # link = '%s://%s:%s/render/?from=%s&until=%s&target=cactiStyle(%s)%s%s&colorList=%s' % (
            #     settings.GRAPHITE_PROTOCOL, settings.GRAPHITE_HOST, settings.GRAPHITE_PORT,
            #     graphite_from, graphite_until, metric_name, settings.GRAPHITE_GRAPH_SETTINGS,
            #     graph_title, graphite_graph_line_color)
            # @modified 20200417 - Task #3294: py3 - handle system parameter in Graphite cactiStyle
            # link = '%s://%s:%s/%s/?from=%s&until=%s&target=cactiStyle(%s)%s%s&colorList=%s' % (
            link = '%s://%s:%s/%s/?from=%s&until=%s&target=cactiStyle(%s,%%27si%%27)%s%s&colorList=%s' % (
                settings.GRAPHITE_PROTOCOL, settings.GRAPHITE_HOST, settings.GRAPHITE_PORT,
                settings.GRAPHITE_RENDER_URI, graphite_from, graphite_until,
                metric_name, settings.GRAPHITE_GRAPH_SETTINGS, graph_title,
                graphite_graph_line_color)
        else:
            # link = '%s://%s/render/?from=-%shour&target=cactiStyle(%s)%s%s&colorList=%s' % (
                # settings.GRAPHITE_PROTOCOL, settings.GRAPHITE_HOST, graphite_previous_hours,
            # @modified 20190520 - Branch #3002: docker
            # Use GRAPHITE_RENDER_URI
            # link = '%s://%s/render/?from=%s&until=%s&target=cactiStyle(%s)%s%s&colorList=%s' % (
            #     settings.GRAPHITE_PROTOCOL, settings.GRAPHITE_HOST, graphite_from, graphite_until,
            #     metric_name, settings.GRAPHITE_GRAPH_SETTINGS, graph_title,
            #     graphite_graph_line_color)
            # @modified 20200417 - Task #3294: py3 - handle system parameter in Graphite cactiStyle
            # link = '%s://%s/%s/?from=%s&until=%s&target=cactiStyle(%s)%s%s&colorList=%s' % (
            link = '%s://%s/%s/?from=%s&until=%s&target=cactiStyle(%s,%%27si%%27)%s%s&colorList=%s' % (
                settings.GRAPHITE_PROTOCOL, settings.GRAPHITE_HOST,
                settings.GRAPHITE_RENDER_URI, graphite_from, graphite_until,
                metric_name, settings.GRAPHITE_GRAPH_SETTINGS, graph_title,
                graphite_graph_line_color)

        embed_graph = "<a href='" + link + "'><img height='308' src='" + link + "'>" + metric_name + "</a>"

        for rooms in notify_rooms:
            for room in rooms:
                hipster.method('rooms/message', method='POST', parameters={'room_id': room, 'from': 'skyline', 'color': settings.BOUNDARY_HIPCHAT_OPTS['color'], 'message': '%s - Boundary - %s - Anomalous metric: %s (value: %s) at %s hours %s' % (sender, algorithm, metric_name, datapoint, graphite_previous_hours, embed_graph)})
    else:
        return False


def alert_syslog(datapoint, metric_name, expiration_time, metric_trigger, algorithm, metric_timestamp, alert_threshold):
    if settings.SYSLOG_ENABLED:
        import sys
        import syslog
        syslog_ident = settings.SYSLOG_OPTS['ident']
        # @modified 20201207 - Task #3878: Add metric_trigger and alert_threshold to Boundary alerts
        # message = str('Boundary - Anomalous metric: %s (value: %s) - %s' % (metric_name, datapoint, algorithm))
        message = 'Boundary - Anomalous metric: %s (value: %s) - %s with %s %s times' % (
            metric_name, str(datapoint), algorithm, str(metric_trigger),
            str(alert_threshold))
        if sys.version_info[:2] == (2, 6):
            syslog.openlog(syslog_ident, syslog.LOG_PID, syslog.LOG_LOCAL4)
        elif sys.version_info[:2] == (2, 7):
            syslog.openlog(ident='skyline', logoption=syslog.LOG_PID, facility=syslog.LOG_LOCAL4)
        elif sys.version_info[:1] == (3):
            syslog.openlog(ident='skyline', logoption=syslog.LOG_PID, facility=syslog.LOG_LOCAL4)
        else:
            syslog.openlog(syslog_ident, syslog.LOG_PID, syslog.LOG_LOCAL4)
        syslog.syslog(4, message)
    else:
        return False


# @added 20181126 - Task #2742: Update Boundary
#                   Feature #2618: alert_slack
def alert_slack(datapoint, metric_name, expiration_time, metric_trigger, algorithm, metric_timestamp, alert_threshold):

    if not settings.SLACK_ENABLED:
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

    metric = metric_name
    logger.info('alert_slack - anomalous metric :: metric: %s - %s' % (metric, algorithm))
    base_name = metric
    alert_algo = str(algorithm)
    alert_context = alert_algo.upper()

    # The known_derivative_metric state is determine in case we need to surface
    # the png image from Graphite if the Ionosphere image is not available for
    # some reason.  This will result in Skyline at least still sending an alert
    # to slack, even if some gear fails in Ionosphere or slack alerting is used
    # without Ionosphere enabled. Yes not DRY but multiprocessing and spawn
    # safe.
    known_derivative_metric = False

#    try:
#        if settings.REDIS_PASSWORD:
#            # @modified 20191022 - Bug #3266: py3 Redis binary objects not strings
#            #                      Branch #3262: py3
#            # REDIS_ALERTER_CONN = redis.StrictRedis(password=settings.REDIS_PASSWORD, unix_socket_path=settings.REDIS_SOCKET_PATH)
#            REDIS_ALERTER_CONN = redis.StrictRedis(password=settings.REDIS_PASSWORD, unix_socket_path=settings.REDIS_SOCKET_PATH, charset='utf-8', decode_responses=True)
#        else:
#            # REDIS_ALERTER_CONN = redis.StrictRedis(unix_socket_path=settings.REDIS_SOCKET_PATH)
#            REDIS_ALERTER_CONN = redis.StrictRedis(unix_socket_path=settings.REDIS_SOCKET_PATH, charset='utf-8', decode_responses=True)
#    except:
#        logger.error('error :: alert_slack - redis connection failed')
#    try:
#        derivative_metrics = list(REDIS_ALERTER_CONN.smembers('derivative_metrics'))
#    except:
#        derivative_metrics = []

    # @modified 20201207 - Task #3878: Add metric_trigger and alert_threshold to Boundary alerts
    # redis_metric_name not used
    # redis_metric_name = '%s%s' % (settings.FULL_NAMESPACE, str(base_name))

#    if redis_metric_name in derivative_metrics:
#        known_derivative_metric = True
    known_derivative_metric = is_derivative_metric(skyline_app, str(base_name))

    # if known_derivative_metric:
    #     try:
    #         non_derivative_monotonic_metrics = settings.NON_DERIVATIVE_MONOTONIC_METRICS
    #     except:
    #         non_derivative_monotonic_metrics = []
    #     skip_derivative = in_list(redis_metric_name, non_derivative_monotonic_metrics)
    #     if skip_derivative:
    #         known_derivative_metric = False

    # @added 20191008 - Feature #3194: Add CUSTOM_ALERT_OPTS to settings
    try:
        main_alert_title = settings.CUSTOM_ALERT_OPTS['main_alert_title']
    except:
        main_alert_title = 'Skyline'
    try:
        app_alert_context = settings.CUSTOM_ALERT_OPTS['boundary_alert_heading']
    except:
        app_alert_context = 'Boundary'

    if known_derivative_metric:
        # @modified 20191008 - Feature #3194: Add CUSTOM_ALERT_OPTS to settings
        # unencoded_graph_title = 'Skyline Boundary - ALERT %s at %s hours - derivative graph - %s' % (
        #     alert_context, str(graphite_previous_hours), metric)
        # slack_title = '*Skyline Boundary - ALERT* %s on %s at %s hours - derivative graph - %s' % (
        #     alert_context, metric, str(graphite_previous_hours), datapoint)
        # @modified 20201207 - Task #3878: Add metric_trigger and alert_threshold to Boundary alerts
        # unencoded_graph_title = '%s %s - ALERT %s at %s hours - derivative graph - %s' % (
        #     main_alert_title, app_alert_context, alert_context, str(graphite_previous_hours), metric)
        # slack_title = '*%s %s - ALERT* %s on %s at %s hours - derivative graph - %s' % (
        #     main_alert_title, app_alert_context, alert_context, metric, str(graphite_previous_hours), datapoint)
        unencoded_graph_title = '%s %s - ALERT %s %s %s times - derivative graph - %s' % (
            main_alert_title, app_alert_context, alert_context,
            str(metric_trigger), str(alert_threshold), metric)
        slack_title = '*%s %s - ALERT* %s %s %s times on %s - derivative graph - %s' % (
            main_alert_title, app_alert_context, alert_context,
            str(metric_trigger), str(alert_threshold), metric, str(datapoint))
    else:
        # unencoded_graph_title = 'Skyline Boundary - ALERT %s at %s hours - %s' % (
        #     alert_context, str(graphite_previous_hours), metric)
        # slack_title = '*Skyline Boundary - ALERT* %s on %s at %s hours - %s' % (
        #     alert_context, metric, str(graphite_previous_hours), datapoint)
        # @modified 20201207 - Task #3878: Add metric_trigger and alert_threshold to Boundary alerts
        # unencoded_graph_title = '%s %s - ALERT %s at %s hours - %s' % (
        #     main_alert_title, app_alert_context, alert_context, str(graphite_previous_hours), metric)
        # slack_title = '*%s %s - ALERT* %s on %s at %s hours - %s' % (
        #     main_alert_title, app_alert_context, alert_context, metric, str(graphite_previous_hours), datapoint)
        unencoded_graph_title = '%s %s - ALERT %s %s %s times - %s' % (
            main_alert_title, app_alert_context, alert_context,
            str(metric_trigger), str(alert_threshold), metric)
        slack_title = '*%s %s - ALERT* %s %s %s times on %s - %s' % (
            main_alert_title, app_alert_context, alert_context,
            str(metric_trigger), str(alert_threshold), metric, str(datapoint))

    graph_title_string = quote(unencoded_graph_title, safe='')
    graph_title = '&title=%s' % graph_title_string

    until_timestamp = int(time())
    target_seconds = int((graphite_previous_hours * 60) * 60)
    from_timestamp = str(until_timestamp - target_seconds)

    graphite_from = datetime.datetime.fromtimestamp(int(from_timestamp)).strftime('%H:%M_%Y%m%d')
    logger.info('graphite_from - %s' % str(graphite_from))
    graphite_until = datetime.datetime.fromtimestamp(int(until_timestamp)).strftime('%H:%M_%Y%m%d')
    logger.info('graphite_until - %s' % str(graphite_until))
    # @added 20181025 - Feature #2618: alert_slack
    # Added date and time info so you do not have to mouseover the slack
    # message to determine the time at which the alert came in
    timezone = strftime("%Z", gmtime())
    # @modified 20181029 - Feature #2618: alert_slack
    # Use the standard UNIX data format
    # human_anomaly_time = dt.datetime.fromtimestamp(int(until_timestamp)).strftime('%Y-%m-%d %H:%M:%S')
    human_anomaly_time = datetime.datetime.fromtimestamp(int(until_timestamp)).strftime('%c')
    slack_time_string = '%s %s' % (human_anomaly_time, timezone)

    # @added 20191106 - Branch #3262: py3
    #                   Branch #3002: docker
    graphite_port = get_graphite_port(skyline_app)
    graphite_render_uri = get_graphite_render_uri(skyline_app)
    graphite_custom_headers = get_graphite_custom_headers(skyline_app)

    # @added 20201013 - Feature #3780: skyline_functions - sanitise_graphite_url
    encoded_graphite_metric_name = encode_graphite_metric_name(skyline_app, metric_name)

    if settings.GRAPHITE_PORT != '':
        if known_derivative_metric:
            # @modified 20190520 - Branch #3002: docker
            # Use GRAPHITE_RENDER_URI
            # link = '%s://%s:%s/render/?from=%s&until=%s&target=cactiStyle(nonNegativeDerivative(%s))%s%s&colorList=orange' % (
            #     settings.GRAPHITE_PROTOCOL, settings.GRAPHITE_HOST,
            #     settings.GRAPHITE_PORT, str(graphite_from), str(graphite_until),
            #     metric, settings.GRAPHITE_GRAPH_SETTINGS, graph_title)
            # @modified 20191022 - Task #3294: py3 - handle system parameter in Graphite cactiStyle
            # link = '%s://%s:%s/%s/?from=%s&until=%s&target=cactiStyle(nonNegativeDerivative(%s))%s%s&colorList=orange' % (
            link = '%s://%s:%s/%s/?from=%s&until=%s&target=cactiStyle(nonNegativeDerivative(%s),%%27si%%27)%s%s&colorList=orange' % (
                settings.GRAPHITE_PROTOCOL, settings.GRAPHITE_HOST,
                # @modified 20201207 - Task #3878: Add metric_trigger and alert_threshold to Boundary alerts
                #                      Branch #3262: py3
                #                      Branch #3002: docker
                # settings.GRAPHITE_PORT, settings.GRAPHITE_RENDER_URI,
                graphite_port, graphite_render_uri,
                str(graphite_from), str(graphite_until),
                # @modified 20201013 - Feature #3780: skyline_functions - sanitise_graphite_url
                # metric, settings.GRAPHITE_GRAPH_SETTINGS, graph_title)
                encoded_graphite_metric_name, settings.GRAPHITE_GRAPH_SETTINGS, graph_title)
        else:
            # @modified 20190520 - Branch #3002: docker
            # Use GRAPHITE_RENDER_URI
            # link = '%s://%s:%s/render/?from=%s&until=%s&target=cactiStyle(%s)%s%s&colorList=orange' % (
            #     settings.GRAPHITE_PROTOCOL, settings.GRAPHITE_HOST,
            #     settings.GRAPHITE_PORT, str(graphite_from), str(graphite_until),
            #     metric, settings.GRAPHITE_GRAPH_SETTINGS, graph_title)
            # @modified 20191022 - Task #3294: py3 - handle system parameter in Graphite cactiStyle
            # link = '%s://%s:%s/%s/?from=%s&until=%s&target=cactiStyle(%s)%s%s&colorList=orange' % (
            link = '%s://%s:%s/%s/?from=%s&until=%s&target=cactiStyle(%s,%%27si%%27)%s%s&colorList=orange' % (
                settings.GRAPHITE_PROTOCOL, settings.GRAPHITE_HOST,
                # @modified 20201207 - Task #3878: Add metric_trigger and alert_threshold to Boundary alerts
                #                      Branch #3262: py3
                #                      Branch #3002: docker
                # settings.GRAPHITE_PORT, settings.GRAPHITE_RENDER_URI,
                graphite_port, graphite_render_uri,
                # str(graphite_from), str(graphite_until), metric,
                # @modified 20201013 - Feature #3780: skyline_functions - sanitise_graphite_url
                str(graphite_from), str(graphite_until), encoded_graphite_metric_name,
                settings.GRAPHITE_GRAPH_SETTINGS, graph_title)
    else:
        if known_derivative_metric:
            # @modified 20190520 - Branch #3002: docker
            # Use GRAPHITE_RENDER_URI
            # link = '%s://%s/render/?from=%s&until=%s&target=cactiStyle(nonNegativeDerivative(%s))%s%s&colorList=orange' % (
            #     settings.GRAPHITE_PROTOCOL, settings.GRAPHITE_HOST,
            #     str(graphite_from), str(graphite_until), metric,
            #     settings.GRAPHITE_GRAPH_SETTINGS, graph_title)
            # @modified 20191022 - Task #3294: py3 - handle system parameter in Graphite cactiStyle
            # link = '%s://%s/%s/?from=%s&until=%s&target=cactiStyle(nonNegativeDerivative(%s))%s%s&colorList=orange' % (
            link = '%s://%s/%s/?from=%s&until=%s&target=cactiStyle(nonNegativeDerivative(%s),%%27si%%27)%s%s&colorList=orange' % (
                settings.GRAPHITE_PROTOCOL, settings.GRAPHITE_HOST,
                settings.GRAPHITE_RENDER_URI, str(graphite_from),
                # @modified 20201013 - Feature #3780: skyline_functions - sanitise_graphite_url
                # str(graphite_until), metric, settings.GRAPHITE_GRAPH_SETTINGS,
                str(graphite_until), encoded_graphite_metric_name, settings.GRAPHITE_GRAPH_SETTINGS,
                graph_title)
        else:
            # @modified 20190520 - Branch #3002: docker
            # Use GRAPHITE_RENDER_URI
            # link = '%s://%s/render/?from=%s&until=%s&target=cactiStyle(%s)%s%s&colorList=orange' % (
            #     settings.GRAPHITE_PROTOCOL, settings.GRAPHITE_HOST,
            #     str(graphite_from), str(graphite_until), metric,
            #     settings.GRAPHITE_GRAPH_SETTINGS, graph_title)
            # @modified 20191022 - Task #3294: py3 - handle system parameter in Graphite cactiStyle
            # link = '%s://%s/%s/?from=%s&until=%s&target=cactiStyle(%s)%s%s&colorList=orange' % (
            link = '%s://%s/%s/?from=%s&until=%s&target=cactiStyle(%s,%%27si%%27)%s%s&colorList=orange' % (
                settings.GRAPHITE_PROTOCOL, settings.GRAPHITE_HOST,
                settings.GRAPHITE_RENDER_URI, str(graphite_from),
                # @modified 20201013 - Feature #3780: skyline_functions - sanitise_graphite_url
                # str(graphite_until), metric, settings.GRAPHITE_GRAPH_SETTINGS,
                str(graphite_until), encoded_graphite_metric_name, settings.GRAPHITE_GRAPH_SETTINGS,
                graph_title)

    # slack does not allow embedded images, nor will it fetch links behind
    # authentication so Skyline uploads a png graphite image with the message
    image_file = None

    # Fetch the png from Graphite
    # @modified 20191021 - Task #3290: Handle urllib2 in py3
    #                      Branch #3262: py3
    image_file = '%s/%s.%s.graphite.%sh.png' % (
        settings.SKYLINE_TMP_DIR, base_name, skyline_app,
        str(int(graphite_previous_hours)))

    if python_version == 22:
        try:
            # image_data = urllib2.urlopen(link).read()  # nosec
            image_data = None
        # except urllib2.URLError:
        except:
            logger.error(traceback.format_exc())
            logger.error('error :: alert_slack - failed to get image graph')
            logger.error('error :: alert_slack - %s' % str(link))
            image_data = None
    if python_version == 33:
        try:
            image_file = '%s/%s.%s.graphite.%sh.png' % (
                settings.SKYLINE_TMP_DIR, base_name, skyline_app,
                str(int(graphite_previous_hours)))
#            urllib.request.urlretrieve(link, image_file)
            image_data = 'retrieved'
            image_data = None
        except:
            try:
                # @added 20191022 - Task #3294: py3 - handle system parameter in Graphite cactiStyle
                image_data = None
                original_traceback = traceback.format_exc()
                if 'cactiStyle' in link:
                    metric_replace = '%s,%%27si%%27' % metric
                    original_link = link
                    link = link.replace(metric, metric_replace)
                    logger.info('link replaced with cactiStyle system parameter added - %s' % str(link))
                    urllib.request.urlretrieve(link, image_file)
                    image_data = 'retrieved'
            except:
                new_trackback = traceback.format_exc()
                logger.error(original_traceback)
                logger.error('error :: boundary_alerters :: alert_slack :: failed to urlopen %s' % str(original_link))
                logger.error(new_trackback)
                logger.error('error :: boundary_alerters :: alert_slack :: failed to urlopen with system parameter added %s' % str(link))
                image_data = None

    # @added 20191025 -
    image_data = get_graphite_graph_image(skyline_app, link, image_file)

    if image_data == 'disabled_for_testing':
        image_file = '%s/%s.%s.graphite.%sh.png' % (
            settings.SKYLINE_TMP_DIR, base_name, skyline_app,
            str(int(graphite_previous_hours)))
        if image_data != 'retrieved':
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
        bot_user_oauth_access_token = settings.BOUNDARY_SLACK_OPTS['bot_user_oauth_access_token']
    except:
        logger.error('error :: alert_slack - could not determine bot_user_oauth_access_token')
        return False

    # Allow for absolute path metric namespaces but also allow for and match
    # match wildcard namepaces if there is not an absolute path metric namespace
    channels = 'unknown'
    notify_channels = []
    matched_channels = []
    try:
        channels = settings.BOUNDARY_SLACK_OPTS['channels'][metric_name]
        notify_channels.append(channels)
    except:
        for channel in settings.BOUNDARY_SLACK_OPTS['channels']:
            CHECK_MATCH_PATTERN = channel
            check_match_pattern = re.compile(CHECK_MATCH_PATTERN)
            pattern_match = check_match_pattern.match(metric_name)
            if pattern_match:
                # @modified 20220114 - Bug #4366: Fix boundary slack channels match
                # Handle multiple channels in the tuple
                # matched_channels.append(channel)
                for i_channel in settings.BOUNDARY_SLACK_OPTS['channels'][channel]:
                    matched_channels.append(i_channel)

    if matched_channels != []:
        # @modified 20220111 - Bug #4366: Fix boundary slack channels match
        # for i_metric_name in matched_channels:
        #     channels = settings.BOUNDARY_SLACK_OPTS['channels'][i_metric_name]
        # @modified 20220114 - Bug #4366: Fix boundary slack channels match
        # De-duplicate channels
        # for i_channel in matched_channels:
        #     notify_channels.append(i_channel)
        notify_channels = list(set(matched_channels))

    if not notify_channels:
        logger.error('error :: alert_slack - could not determine channel')
        return False

    channels = notify_channels

    try:
        icon_emoji = settings.BOUNDARY_SLACK_OPTS['icon_emoji']
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
        logger.error('error :: alert_slack - could not initiate SlackClient')
        return False

    # @added 20200815 - Bug #3676: Boundary slack alert errors
    #                   Task #3608: Update Skyline to Python 3.8.3 and deps
    #                   Task #3612: Upgrade to slack v2
    # Strange only Boundary slack messages are erroring on a tuple or part
    # thereof, mirage_alerters using the same method are fine???
    # The server responded with: {'ok': False, 'error': 'invalid_channel', 'channel': "('#skyline'"}
    # This fix handles converting tuple items into list items where the channel
    # is a tuple.
    channels_list = []
    for channel in channels:
        # @modified 20220111 - Bug #4366: Fix boundary slack channels match
        # if type(channel) == tuple:
        if isinstance(channel, tuple):
            for ichannel in channel:
                channels_list.append(str(ichannel))
        else:
            channels_list.append(str(channel))
    if channels_list:
        channels = channels_list

    for channel in channels:
        initial_comment = slack_title + ' :: <' + link + '|graphite image link>\nFor anomaly at ' + slack_time_string

        # @added 20201127 - Feature #3820: HORIZON_SHARDS
        # Add the origin and shard for debugging purposes
        if HORIZON_SHARDS:
            initial_comment = initial_comment + ' - from ' + this_host + ' (shard ' + str(HORIZON_SHARD) + ')'

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
                    logger.error('error :: alert_slack - failed to send slack message with file upload')
                    logger.error('error :: alert_slack - slack_file_upload - %s' % str(slack_file_upload))
                # @modified 20210708 - Bug #4166: Allow boundary to send same image to multiple slack channels
                # If there are multiple slack channels for an alert, the
                # image_file is removed after posting to the first channel and
                # subsequent channel posts fail.  Moved to outside the loop.
                # try:
                #     os.remove(image_file)
                # except OSError:
                #     logger.error('error - failed to remove %s, continuing' % image_file)
                #     pass
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
        # @added 20200825 - Feature #3704: Add alert to anomalies
        if settings.PANORAMA_ENABLED:
            added_panorama_alert_event = add_panorama_alert(skyline_app, int(metric_timestamp), metric_name)
            if not added_panorama_alert_event:
                logger.error(
                    'error :: failed to add Panorama alert event - panorama.alert.%s.%s' % (
                        str(metric_timestamp), metric_name))

    # @added 20210708 - Bug #4166: Allow boundary to send same image to multiple slack channels
    # Move from inside the channels loop above.
    try:
        os.remove(image_file)
    except OSError:
        logger.error('error - failed to remove %s, continuing' % image_file)


# @added 20200122: Feature #3396: http_alerter
def alert_http(alerter, datapoint, metric_name, expiration_time, metric_trigger, algorithm, metric_timestamp, alert_threshold):
    """
    Called by :func:`~trigger_alert` and sends and resend anomalies to a http
    endpoint.

    """
    if settings.HTTP_ALERTERS_ENABLED:
        alerter_name = alerter
        alerter_enabled = False
        try:
            alerter_enabled = settings.HTTP_ALERTERS_OPTS[alerter_name]['enabled']
        except:
            logger.error(traceback.format_exc())
            logger.error('error :: alert_http failed to determine the enabled from settings.HTTP_ALERTERS_OPTS for alerter - %s and metric %s with algorithm %s' % (
                str(alerter), str(metric_name), algorithm))
        if not alerter_enabled:
            logger.info('alert_http - %s enabled %s, not alerting' % (
                str(alerter_name), str(alerter_enabled)))
            return
        alerter_endpoint = False
        try:
            alerter_endpoint = settings.HTTP_ALERTERS_OPTS[alerter_name]['endpoint']
        except:
            logger.error(traceback.format_exc())
            logger.error('error :: alert_http failed to determine the endpoint from settings.HTTP_ALERTERS_OPTS for alert - %s and metric %s with algorithm %s' % (
                str(alerter), str(metric_name), algorithm))
        if not alerter_endpoint:
            logger.error('alert_http - no endpoint set for %s, not alerting' % (
                str(alerter_name)))
            return

        alerter_token = None
        try:
            alerter_token = settings.HTTP_ALERTERS_OPTS[alerter_name]['token']
        except:
            pass

        source = 'boundary'
        metric_alert_dict = {}
        alert_data_dict = {}
        try:
            timestamp_str = str(metric_timestamp)
            value_str = str(datapoint)
            full_duration_str = str(int(full_duration_seconds))
            expiry_str = str(expiration_time)
            metric_alert_dict = {
                "metric": metric_name,
                "algorithm": algorithm,
                "timestamp": timestamp_str,
                "value": value_str,
                "full_duration": full_duration_str,
                "expiry": expiry_str,
                # @added 20201207 - Task #3878: Add metric_trigger and alert_threshold to Boundary alerts
                "metric_trigger": metric_trigger,
                "alert_threshold": alert_threshold,
                "source": str(source),
                "token": str(alerter_token)
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
        except:
            logger.error(traceback.format_exc())
            logger.error('error :: alert_http failed to construct the alert data for %s from alert - %s and metric - %s' % (
                str(alerter_name), str(algorithm), str(metric_name)))
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

        item_to_resend = None
        if resend_queue:
            try:
                for index, resend_item in enumerate(resend_queue):
                    resend_item_list = literal_eval(resend_item)
                    # resend_alert = literal_eval(resend_item_list[0])
                    # resend_metric = literal_eval(resend_item_list[1])
                    resend_metric_alert_dict = literal_eval(resend_item_list[2])
                    if resend_metric_alert_dict['metric'] == metric_name:
                        if int(resend_metric_alert_dict['timestamp']) == int(metric_timestamp):
                            previous_attempts = int(resend_metric_alert_dict['attempts'])
                            in_resend_queue = True
                            item_to_resend = resend_item
                            break
            except:
                logger.error(traceback.format_exc())
                logger.error('error :: alert_http failed iterate to resend_queue')
        # REDIS_HTTP_ALERTER_CONN = None
        # if in_resend_queue:
        #     REDIS_HTTP_ALERTER_CONN = get_redis_conn(skyline_app)
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
            response = None
            try:
                response = requests.post(alerter_endpoint, json=alert_data_dict, timeout=use_timeout)
            except:
                logger.error(traceback.format_exc())
                logger.error('error :: failed to post alert to %s - %s' % (
                    str(alerter_name), str(alert_data_dict)))
                add_to_resend_queue = True
                response = None

            if in_resend_queue:
                try:
                    # REDIS_HTTP_ALERTER_CONN.srem(redis_set, str(resend_item))
                    REDIS_HTTP_ALERTER_CONN.srem(redis_set, str(item_to_resend))
                    logger.info('alert_http :: alert removed from %s' % (
                        str(redis_set)))
                except:
                    logger.error(traceback.format_exc())
                    logger.error('error :: alert_http :: failed remove %s from Redis set %s' % (
                        # str(resend_item), redis_set))
                        str(item_to_resend), redis_set))

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
                        added_panorama_alert_event = add_panorama_alert(skyline_app, int(metric_timestamp), metric_name)
                        if not added_panorama_alert_event:
                            logger.error(
                                'error :: failed to add Panorama alert event - panorama.alert.%s.%s' % (
                                    str(metric_timestamp), metric_name))
                    return
                logger.error('error :: alert_http :: %s %s responded with status code %s and reason %s' % (
                    str(alerter_name), str(alerter_endpoint),
                    str(response.status_code), str(response.reason)))
                add_to_resend_queue = True
                fail_alerter = True
            else:
                logger.error('error :: alert_http :: %s %s did not respond' % (
                    str(alerter_name), str(alerter_endpoint)))
                add_to_resend_queue = True
                fail_alerter = True

            number_of_send_attempts = previous_attempts + 1
            metric_alert_dict['attempts'] = number_of_send_attempts
            if add_to_resend_queue:
                data = [alerter, datapoint, metric_name, expiration_time, metric_trigger, algorithm, metric_timestamp, str(metric_alert_dict)]
                logger.info('alert_http :: adding alert to %s after %s attempts to send - %s' % (
                    str(redis_set), str(number_of_send_attempts), str(metric_alert_dict)))
                try:
                    # redis_conn.sadd(redis_set, str(metric_alert_dict))
                    REDIS_HTTP_ALERTER_CONN.sadd(redis_set, str(data))
                except:
                    logger.error(traceback.format_exc())
                    logger.error('error :: alert_http :: failed to add %s from Redis set %s' % (
                        str(metric_alert_dict), redis_set))

            # Create a Redis if there was a bad or no response from the
            # alerter_endpoint, to ensure that Boundary does not loop through
            # every alert in the queue for an alerter_endpoint, if the
            # alerter_endpoint is down
            if fail_alerter:
                alerter_endpoint_cache_key = 'http_alerter.down.%s' % str(alerter_name)
                logger.error('error :: alert_http :: alerter_endpoint %s failed adding Redis key %s' % (
                    str(alerter_endpoint), str(alerter_endpoint_cache_key)))
                if REDIS_HTTP_ALERTER_CONN:
                    try:
                        failed_timestamp = int(time())
                        REDIS_HTTP_ALERTER_CONN.setex(alerter_endpoint_cache_key, 60, failed_timestamp)
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
def alert_sms(datapoint, metric_name, expiration_time, metric_trigger, algorithm, metric_timestamp, alert_threshold):
    """
    Called by :func:`~trigger_alert` and sends anomalies to a SMS endpoint.

    """
    if not settings.AWS_SNS_SMS_ALERTS_ENABLED:
        logger.error('error :: alert_sms recieved but settings.AWS_SNS_SMS_ALERTS_ENABLED not enabled - metric %s' % (
            str(metric_name)))
        return
    if metric_name.startswith(settings.FULL_NAMESPACE):
        base_name = metric_name.replace(settings.FULL_NAMESPACE, '', 1)
    else:
        base_name = metric_name
    if alert_threshold == 0:
        alert_threshold = 1
    message = '[Skyline alert] Boundary - %s: %s triggered %s %s times' % (
        base_name, str(datapoint), algorithm, str(alert_threshold))
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
        except Exception as err:
            logger.error(traceback.format_exc())
            logger.error('error :: failed to determine number for SMS recipient %s - %s' % (
                sms_number, err))
        if success:
            logger.info('sent SMS alert to %s' % sms_number)
        else:
            logger.warning('warning :: failed to send SMS alert to %s' % sms_number)


# @modified 20201207 - Task #3878: Add metric_trigger and alert_threshold to Boundary alerts
# def trigger_alert(alerter, datapoint, metric_name, expiration_time, metric_trigger, algorithm, metric_timestamp):
def trigger_alert(alerter, datapoint, metric_name, expiration_time, metric_trigger, algorithm, metric_timestamp, alert_threshold):

    if alerter == 'smtp':
        strategy = 'alert_smtp'
    # @added 20200122: Feature #3396: http_alerter
    # Added http_alerter
    elif 'http_alerter' in alerter:
        strategy = 'alert_http'
    else:
        strategy = 'alert_%s' % alerter

    try:
        if strategy == 'alert_http':
            # @modified 20201207 - Task #3878: Add metric_trigger and alert_threshold to Boundary alerts
            getattr(boundary_alerters, strategy)(alerter, datapoint, metric_name, expiration_time, metric_trigger, algorithm, metric_timestamp, alert_threshold)
        else:
            getattr(boundary_alerters, strategy)(datapoint, metric_name, expiration_time, metric_trigger, algorithm, metric_timestamp, alert_threshold)
    except:
        logger.error(traceback.format_exc())
        logger.error('error :: alerters - %s - getattr error' % strategy)
