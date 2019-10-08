from __future__ import division
import logging
import traceback
import hashlib
from smtplib import SMTP
import boundary_alerters
try:
    import urllib2
except ImportError:
    import urllib.request
    import urllib.error
import re
from requests.utils import quote
from time import time
import datetime
import os.path
import sys

# @added 20181126 - Task #2742: Update Boundary
#                   Feature #2618: alert_slack
# Added dt, redis, gmtime and strftime
import datetime as dt
import redis
from time import (gmtime, strftime)

python_version = int(sys.version_info[0])
if python_version == 2:
    from email.MIMEMultipart import MIMEMultipart
    from email.MIMEText import MIMEText
    from email.MIMEImage import MIMEImage
    from email import charset
if python_version == 3:
    from email.mime.multipart import MIMEMultipart
    from email.mime.text import MIMEText
    from email.mime.image import MIMEImage

sys.path.append(os.path.join(os.path.dirname(os.path.realpath(__file__)), os.pardir))
sys.path.insert(0, os.path.dirname(__file__))
import settings
# @added 20181126 - Task #2742: Update Boundary
#                   Feature #2034: analyse_derivatives
#                   Feature #2618: alert_slack
from skyline_functions import (write_data_to_file, in_list)

skyline_app = 'boundary'
skyline_app_logger = '%sLog' % skyline_app
logger = logging.getLogger(skyline_app_logger)
skyline_app_logfile = '%s/%s.log' % (settings.LOG_PATH, skyline_app)

"""
Create any alerter you want here. The function is invoked from trigger_alert.
4 arguments will be passed in as strings:
datapoint, metric_name, expiration_time, algorithm
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


def alert_smtp(datapoint, metric_name, expiration_time, metric_trigger, algorithm):

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
    if type(recipients) is str:
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
    unencoded_graph_title = '%s %s - %s at %s hours - %s - %s' % (
        main_alert_title, app_alert_context, alert_context, graphite_previous_hours, metric_name, datapoint)

    # @added 20181126 - Task #2742: Update Boundary
    #                   Feature #2034: analyse_derivatives
    # Added deriative functions to convert the values of metrics strictly
    # increasing monotonically to their deriative products in alert graphs and
    # specify it in the graph_title
    known_derivative_metric = False
    try:
        # @modified 20180519 - Feature #2378: Add redis auth to Skyline and rebrow
        if settings.REDIS_PASSWORD:
            REDIS_ALERTER_CONN = redis.StrictRedis(password=settings.REDIS_PASSWORD, unix_socket_path=settings.REDIS_SOCKET_PATH)
        else:
            REDIS_ALERTER_CONN = redis.StrictRedis(unix_socket_path=settings.REDIS_SOCKET_PATH)
    except:
        logger.error('error :: alert_smtp - redis connection failed')
    try:
        derivative_metrics = list(REDIS_ALERTER_CONN.smembers('derivative_metrics'))
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
    if known_derivative_metric:
        # @modified 20191002 - Feature #3194: Add CUSTOM_ALERT_OPTS to settings
        # unencoded_graph_title = 'Skyline Boundary - %s at %s hours - derivative graph - %s - %s' % (
        #     alert_context, graphite_previous_hours, metric_name, datapoint)
        unencoded_graph_title = '%s %s - %s at %s hours - derivative graph - %s - %s' % (
            main_alert_title, app_alert_context, alert_context, graphite_previous_hours, metric_name, datapoint)

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
    graphite_from = dt.datetime.fromtimestamp(int(from_timestamp)).strftime('%H:%M_%Y%m%d')
    logger.info('graphite_from - %s' % str(graphite_from))
    graphite_until = dt.datetime.fromtimestamp(int(until_timestamp)).strftime('%H:%M_%Y%m%d')
    logger.info('graphite_until - %s' % str(graphite_until))
    graphite_target = 'target=cactiStyle(%s)'
    if known_derivative_metric:
        graphite_target = 'target=cactiStyle(nonNegativeDerivative(%s))'
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
    if settings.BOUNDARY_SMTP_OPTS.get('embed-images'):
        try:
            # @modified 20170913 - Task #2160: Test skyline with bandit
            # Added nosec to exclude from bandit tests
            image_data = urllib2.urlopen(link).read()  # nosec
        except urllib2.URLError:
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
        main_alert_title, app_alert_context, datapoint, metric_name, expiration_time, alert_context, link, img_tag)

    # @modified 20180524 - Task #2384: Change alerters to cc other recipients
    # Do not send to each recipient, send to primary_recipient and cc the other
    # recipients, thereby sending only one email
    # for recipient in recipients:
    if primary_recipient:
        logger.info(
            'alert_smtp - will send to primary_recipient :: %s, cc_recipients :: %s' %
            (str(primary_recipient), str(cc_recipients)))

        msg = MIMEMultipart('alternative')
        # @modified 20191002 - Feature #3194: Add CUSTOM_ALERT_OPTS to settings
        # msg['Subject'] = '[Skyline alert] ' + 'Boundary ALERT - ' + alert_context + ' - ' + datapoint + ' - ' + metric_name
        msg['Subject'] = '[' + main_alert_title + ' alert] ' + app_alert_context + ' ALERT - ' + alert_context + ' - ' + datapoint + ' - ' + metric_name
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
            msg_attachment = MIMEImage(image_data)
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
            logger.info(traceback.format_exc())
            logger.error(
                'error :: alert_smtp - could not send email to primary_recipient :: %s, cc_recipients :: %s' %
                (str(primary_recipient), str(cc_recipients)))
        s.quit()


def alert_pagerduty(datapoint, metric_name, expiration_time, metric_trigger, algorithm):
    if settings.PAGERDUTY_ENABLED:
        import pygerduty
        pager = pygerduty.PagerDuty(settings.BOUNDARY_PAGERDUTY_OPTS['subdomain'], settings.BOUNDARY_PAGERDUTY_OPTS['auth_token'])
        pager.trigger_incident(settings.BOUNDARY_PAGERDUTY_OPTS['key'], 'Anomalous metric: %s (value: %s) - %s' % (metric_name, datapoint, algorithm))
    else:
        return False


def alert_hipchat(datapoint, metric_name, expiration_time, metric_trigger, algorithm):

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
            link = '%s://%s:%s/%s/?from=%s&until=%s&target=cactiStyle(%s)%s%s&colorList=%s' % (
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
            link = '%s://%s/%s/?from=%s&until=%s&target=cactiStyle(%s)%s%s&colorList=%s' % (
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


def alert_syslog(datapoint, metric_name, expiration_time, metric_trigger, algorithm):
    if settings.SYSLOG_ENABLED:
        import sys
        import syslog
        syslog_ident = settings.SYSLOG_OPTS['ident']
        message = str('Boundary - Anomalous metric: %s (value: %s) - %s' % (metric_name, datapoint, algorithm))
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
def alert_slack(datapoint, metric_name, expiration_time, metric_trigger, algorithm):

    if not settings.SLACK_ENABLED:
        return False

    from slackclient import SlackClient
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
    try:
        if settings.REDIS_PASSWORD:
            REDIS_ALERTER_CONN = redis.StrictRedis(password=settings.REDIS_PASSWORD, unix_socket_path=settings.REDIS_SOCKET_PATH)
        else:
            REDIS_ALERTER_CONN = redis.StrictRedis(unix_socket_path=settings.REDIS_SOCKET_PATH)
    except:
        logger.error('error :: alert_slack - redis connection failed')
    try:
        derivative_metrics = list(REDIS_ALERTER_CONN.smembers('derivative_metrics'))
    except:
        derivative_metrics = []
    redis_metric_name = '%s%s' % (settings.FULL_NAMESPACE, str(base_name))
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
        unencoded_graph_title = '%s %s - ALERT %s at %s hours - derivative graph - %s' % (
            main_alert_title, app_alert_context, alert_context, str(graphite_previous_hours), metric)
        slack_title = '*%s %s - ALERT* %s on %s at %s hours - derivative graph - %s' % (
            main_alert_title, app_alert_context, alert_context, metric, str(graphite_previous_hours), datapoint)
    else:
        # unencoded_graph_title = 'Skyline Boundary - ALERT %s at %s hours - %s' % (
        #     alert_context, str(graphite_previous_hours), metric)
        # slack_title = '*Skyline Boundary - ALERT* %s on %s at %s hours - %s' % (
        #     alert_context, metric, str(graphite_previous_hours), datapoint)
        unencoded_graph_title = '%s %s - ALERT %s at %s hours - %s' % (
            main_alert_title, app_alert_context, alert_context, str(graphite_previous_hours), metric)
        slack_title = '*%s %s - ALERT* %s on %s at %s hours - %s' % (
            main_alert_title, app_alert_context, alert_context, metric, str(graphite_previous_hours), datapoint)

    graph_title_string = quote(unencoded_graph_title, safe='')
    graph_title = '&title=%s' % graph_title_string

    until_timestamp = int(time())
    target_seconds = int((graphite_previous_hours * 60) * 60)
    from_timestamp = str(until_timestamp - target_seconds)

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

    if settings.GRAPHITE_PORT != '':
        if known_derivative_metric:
            # @modified 20190520 - Branch #3002: docker
            # Use GRAPHITE_RENDER_URI
            # link = '%s://%s:%s/render/?from=%s&until=%s&target=cactiStyle(nonNegativeDerivative(%s))%s%s&colorList=orange' % (
            #     settings.GRAPHITE_PROTOCOL, settings.GRAPHITE_HOST,
            #     settings.GRAPHITE_PORT, str(graphite_from), str(graphite_until),
            #     metric, settings.GRAPHITE_GRAPH_SETTINGS, graph_title)
            link = '%s://%s:%s/%s/?from=%s&until=%s&target=cactiStyle(nonNegativeDerivative(%s))%s%s&colorList=orange' % (
                settings.GRAPHITE_PROTOCOL, settings.GRAPHITE_HOST,
                settings.GRAPHITE_PORT, settings.GRAPHITE_RENDER_URI,
                str(graphite_from), str(graphite_until),
                metric, settings.GRAPHITE_GRAPH_SETTINGS, graph_title)
        else:
            # @modified 20190520 - Branch #3002: docker
            # Use GRAPHITE_RENDER_URI
            # link = '%s://%s:%s/render/?from=%s&until=%s&target=cactiStyle(%s)%s%s&colorList=orange' % (
            #     settings.GRAPHITE_PROTOCOL, settings.GRAPHITE_HOST,
            #     settings.GRAPHITE_PORT, str(graphite_from), str(graphite_until),
            #     metric, settings.GRAPHITE_GRAPH_SETTINGS, graph_title)
            link = '%s://%s:%s/%s/?from=%s&until=%s&target=cactiStyle(%s)%s%s&colorList=orange' % (
                settings.GRAPHITE_PROTOCOL, settings.GRAPHITE_HOST,
                settings.GRAPHITE_PORT, settings.GRAPHITE_RENDER_URI,
                str(graphite_from), str(graphite_until), metric,
                settings.GRAPHITE_GRAPH_SETTINGS, graph_title)
    else:
        if known_derivative_metric:
            # @modified 20190520 - Branch #3002: docker
            # Use GRAPHITE_RENDER_URI
            # link = '%s://%s/render/?from=%s&until=%s&target=cactiStyle(nonNegativeDerivative(%s))%s%s&colorList=orange' % (
            #     settings.GRAPHITE_PROTOCOL, settings.GRAPHITE_HOST,
            #     str(graphite_from), str(graphite_until), metric,
            #     settings.GRAPHITE_GRAPH_SETTINGS, graph_title)
            link = '%s://%s/%s/?from=%s&until=%s&target=cactiStyle(nonNegativeDerivative(%s))%s%s&colorList=orange' % (
                settings.GRAPHITE_PROTOCOL, settings.GRAPHITE_HOST,
                settings.GRAPHITE_RENDER_URI, str(graphite_from),
                str(graphite_until), metric, settings.GRAPHITE_GRAPH_SETTINGS,
                graph_title)
        else:
            # @modified 20190520 - Branch #3002: docker
            # Use GRAPHITE_RENDER_URI
            # link = '%s://%s/render/?from=%s&until=%s&target=cactiStyle(%s)%s%s&colorList=orange' % (
            #     settings.GRAPHITE_PROTOCOL, settings.GRAPHITE_HOST,
            #     str(graphite_from), str(graphite_until), metric,
            #     settings.GRAPHITE_GRAPH_SETTINGS, graph_title)
            link = '%s://%s/%s/?from=%s&until=%s&target=cactiStyle(%s)%s%s&colorList=orange' % (
                settings.GRAPHITE_PROTOCOL, settings.GRAPHITE_HOST,
                settings.GRAPHITE_RENDER_URI, str(graphite_from),
                str(graphite_until), metric, settings.GRAPHITE_GRAPH_SETTINGS,
                graph_title)

    # slack does not allow embedded images, nor will it fetch links behind
    # authentication so Skyline uploads a png graphite image with the message
    image_file = None

    # Fetch the png from Graphite
    try:
        image_data = urllib2.urlopen(link).read()  # nosec
    except urllib2.URLError:
        logger.error(traceback.format_exc())
        logger.error('error :: alert_slack - failed to get image graph')
        logger.error('error :: alert_slack - %s' % str(link))
        image_data = None
    if image_data:
        image_file = '%s/%s.%s.graphite.%sh.png' % (
            settings.SKYLINE_TMP_DIR, base_name, skyline_app,
            str(int(graphite_previous_hours)))
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
                matched_channels.append(channel)

    if matched_channels != []:
        for i_metric_name in matched_channels:
            channels = settings.BOUNDARY_SLACK_OPTS['channels'][i_metric_name]
            notify_channels.append(channels)

    if not notify_channels:
        logger.error('error :: alert_slack - could not determine channel')
        return False
    else:
        channels = notify_channels

    try:
        icon_emoji = settings.BOUNDARY_SLACK_OPTS['icon_emoji']
    except:
        icon_emoji = ':chart_with_upwards_trend:'

    try:
        sc = SlackClient(bot_user_oauth_access_token)
    except:
        logger.info(traceback.format_exc())
        logger.error('error :: alert_slack - could not initiate SlackClient')
        return False

    for channel in channels:
        initial_comment = slack_title + ' :: <' + link + '|graphite image link>\nFor anomaly at ' + slack_time_string
        try:
            # slack does not allow embedded images, nor links behind authentication
            # or color text, so we have jump through all the API hoops to end up
            # having to upload an image with a very basic message.
            if os.path.isfile(image_file):
                slack_file_upload = sc.api_call(
                    'files.upload', filename=filename, channels=channel,
                    initial_comment=initial_comment, file=open(image_file, 'rb'))
                if not slack_file_upload['ok']:
                    logger.error('error :: alert_slack - failed to send slack message')
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


def trigger_alert(alerter, datapoint, metric_name, expiration_time, metric_trigger, algorithm):

    if alerter == 'smtp':
        strategy = 'alert_smtp'
    else:
        strategy = 'alert_%s' % alerter

    try:
        getattr(boundary_alerters, strategy)(datapoint, metric_name, expiration_time, metric_trigger, algorithm)
    except:
        logger.info(traceback.format_exc())
        logger.error('error :: alerters - %s - getattr error' % strategy)
