from __future__ import division
import logging
import traceback
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
python_version = int(sys.version_info[0])
if python_version == 2:
    from email.MIMEMultipart import MIMEMultipart
    from email.MIMEText import MIMEText
    from email.MIMEImage import MIMEImage
if python_version == 3:
    from email.mime.multipart import MIMEMultipart
    from email.mime.text import MIMEText
    from email.mime.image import MIMEImage

sys.path.append(os.path.join(os.path.dirname(os.path.realpath(__file__)), os.pardir))
sys.path.insert(0, os.path.dirname(__file__))
import settings

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

    alert_algo = str(algorithm)
    alert_context = alert_algo.upper()

    unencoded_graph_title = 'Skyline Boundary - %s at %s hours - %s - %s' % (
        alert_context, graphite_previous_hours, metric_name, datapoint)
    graph_title_string = quote(unencoded_graph_title, safe='')
    graph_title = '&title=%s' % graph_title_string

    if settings.GRAPHITE_PORT != '':
        link = '%s://%s:%s/render/?from=-%shour&target=cactiStyle(%s)%s%s&colorList=%s' % (
            settings.GRAPHITE_PROTOCOL, settings.GRAPHITE_HOST, settings.GRAPHITE_PORT,
            graphite_previous_hours, metric_name, settings.GRAPHITE_GRAPH_SETTINGS,
            graph_title, graphite_graph_line_color)
    else:
        link = '%s://%s/render/?from=-%shour&target=cactiStyle(%s)%s%s&colorList=%s' % (
            settings.GRAPHITE_PROTOCOL, settings.GRAPHITE_HOST, graphite_previous_hours,
            metric_name, settings.GRAPHITE_GRAPH_SETTINGS, graph_title,
            graphite_graph_line_color)

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

    body = '%s :: %s <br> Next alert in: %s seconds <br> skyline Boundary alert - %s <br><a href="%s">%s</a>' % (
        datapoint, metric_name, expiration_time, alert_context, link, img_tag)

    for recipient in recipients:
        msg = MIMEMultipart('alternative')
        msg['Subject'] = '[Skyline alert] ' + 'Boundary ALERT - ' + alert_context + ' - ' + datapoint + ' - ' + metric_name
        msg['From'] = sender
        msg['To'] = recipient

        msg.attach(MIMEText(body, 'html'))
        if image_data is not None:
            msg_attachment = MIMEImage(image_data)
            msg_attachment.add_header('Content-ID', '<%s>' % content_id)
            msg.attach(msg_attachment)

        s = SMTP('127.0.0.1')
        s.sendmail(sender, recipient, msg.as_string())
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
                print(room)
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
            link = '%s://%s:%s/render/?from=%s&until=%s&target=cactiStyle(%s)%s%s&colorList=%s' % (
                settings.GRAPHITE_PROTOCOL, settings.GRAPHITE_HOST, settings.GRAPHITE_PORT,
                graphite_from, graphite_until, metric_name, settings.GRAPHITE_GRAPH_SETTINGS,
                graph_title, graphite_graph_line_color)
        else:
            # link = '%s://%s/render/?from=-%shour&target=cactiStyle(%s)%s%s&colorList=%s' % (
                # settings.GRAPHITE_PROTOCOL, settings.GRAPHITE_HOST, graphite_previous_hours,
            link = '%s://%s/render/?from=%s&until=%s&target=cactiStyle(%s)%s%s&colorList=%s' % (
                settings.GRAPHITE_PROTOCOL, settings.GRAPHITE_HOST, graphite_from, graphite_until,
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


def trigger_alert(alerter, datapoint, metric_name, expiration_time, metric_trigger, algorithm):

    if alerter == 'smtp':
        strategy = 'alert_smtp'
    else:
        strategy = 'alert_%s' % alerter

    try:
        getattr(boundary_alerters, strategy)(datapoint, metric_name, expiration_time, metric_trigger, algorithm)
    except:
        logger.error('error :: alerters - %s - getattr error' % strategy)
        logger.info(traceback.format_exc())
