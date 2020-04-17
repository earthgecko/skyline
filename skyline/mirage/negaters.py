from __future__ import division
from smtplib import SMTP
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
import negaters
try:
    import urllib2
except ImportError:
    import urllib.request
    import urllib.error

import os.path
sys.path.append(os.path.join(os.path.dirname(os.path.realpath(__file__)), os.pardir))
sys.path.insert(0, os.path.dirname(__file__))
import settings

# from skyline import settings

"""
Create any alerter you want here. The function will be invoked from trigger_alert.
Two arguments will be passed, both of them tuples: alert and metric.

alert: the tuple specified in your settings:
    alert[0]: The matched substring of the anomalous metric
    alert[1]: the name of the strategy being used to alert
    alert[2]: The timeout of the alert that was triggered
    alert[3]: The SECOND_ORDER_RESOLUTION_HOURS
metric: information about the anomaly itself
    metric[0]: the anomalous value
    metric[1]: The full name of the anomalous metric
"""


def negate_analyzer_alert(alert, metric, second_order_resolution_seconds, metric_value):

    # FULL_DURATION to hours so that mirage can surface the relevant timeseries data
    # for analyzer comparison in the graph
    full_duration_in_hours = int(settings.FULL_DURATION) / 3600

    # SECOND_ORDER_RESOLUTION_SECONDS to hours so that mirage surfaces the
    # relevant timeseries data in the graph
    second_order_resolution_in_hours = int(second_order_resolution_seconds) / 3600

    # For backwards compatibility
    if '@' in alert[1]:
        sender = settings.ALERT_SENDER
        recipient = alert[1]
    else:
        sender = settings.SMTP_OPTS['sender']
        recipients = settings.SMTP_OPTS['recipients'][alert[0]]

    # Backwards compatibility
    if type(recipients) is str:
        recipients = [recipients]

    graph_title = '&title=skyline%%20mirage%%20negation%%20at%%20%s%%20hours%%0A%s%%20-%%20%s' % (second_order_resolution_in_hours, metric[1], metric[0])
    analyzer_graph_title = '&title=skyline%%20analyzer%%20alert%%20at%%20%s%%20hours%%0A%s%%20-%%20%s' % (full_duration_in_hours, metric[1], metric_value)

    if settings.GRAPHITE_PORT != '':

        # @modified 20200417 - Task #3294: py3 - handle system parameter in Graphite cactiStyle
        # link = '%s://%s:%s/render/?from=-%shour&target=cactiStyle(%s)%s%s&colorList=purple' % (settings.GRAPHITE_PROTOCOL, settings.GRAPHITE_HOST, settings.GRAPHITE_PORT, second_order_resolution_in_hours, metric[1], settings.GRAPHITE_GRAPH_SETTINGS, graph_title)
        # analyzer_link = '%s://%s:%s/render/?from=-%shour&target=cactiStyle(%s)%s%s&colorList=orange' % (settings.GRAPHITE_PROTOCOL, settings.GRAPHITE_HOST, settings.GRAPHITE_PORT, full_duration_in_hours, metric[1], settings.GRAPHITE_GRAPH_SETTINGS, analyzer_graph_title)
        link = '%s://%s:%s/render/?from=-%shour&target=cactiStyle(%s,%%27si%%27)%s%s&colorList=purple' % (settings.GRAPHITE_PROTOCOL, settings.GRAPHITE_HOST, settings.GRAPHITE_PORT, second_order_resolution_in_hours, metric[1], settings.GRAPHITE_GRAPH_SETTINGS, graph_title)
        analyzer_link = '%s://%s:%s/render/?from=-%shour&target=cactiStyle(%s,%%27si%%27)%s%s&colorList=orange' % (settings.GRAPHITE_PROTOCOL, settings.GRAPHITE_HOST, settings.GRAPHITE_PORT, full_duration_in_hours, metric[1], settings.GRAPHITE_GRAPH_SETTINGS, analyzer_graph_title)
    else:
        # @modified 20200417 - Task #3294: py3 - handle system parameter in Graphite cactiStyle
        # link = '%s://%s/render/?from=-%shour&target=cactiStyle(%s)%s%s&colorList=purple' % (settings.GRAPHITE_PROTOCOL, settings.GRAPHITE_HOST, second_order_resolution_in_hours, metric[1], settings.GRAPHITE_GRAPH_SETTINGS, graph_title)
        # analyzer_link = '%s://%s/render/?from=-%shour&target=cactiStyle(%s)%s%s&colorList=orange' % (settings.GRAPHITE_PROTOCOL, settings.GRAPHITE_HOST, full_duration_in_hours, metric[1], settings.GRAPHITE_GRAPH_SETTINGS, analyzer_graph_title)
        link = '%s://%s/render/?from=-%shour&target=cactiStyle(%s,%%27si%%27)%s%s&colorList=purple' % (settings.GRAPHITE_PROTOCOL, settings.GRAPHITE_HOST, second_order_resolution_in_hours, metric[1], settings.GRAPHITE_GRAPH_SETTINGS, graph_title)
        analyzer_link = '%s://%s/render/?from=-%shour&target=cactiStyle(%s,%%27si%%27)%s%s&colorList=orange' % (settings.GRAPHITE_PROTOCOL, settings.GRAPHITE_HOST, full_duration_in_hours, metric[1], settings.GRAPHITE_GRAPH_SETTINGS, analyzer_graph_title)

    content_id = metric[1]
    image_data = None
    if settings.SMTP_OPTS.get('embed-images'):
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

    analyzer_content_id = 'analyzer'
    analyzer_image_data = None
    if settings.SMTP_OPTS.get('embed-images'):
        try:
            # @modified 20170913 - Task #2160: Test skyline with bandit
            # Added nosec to exclude from bandit tests
            analyzer_image_data = urllib2.urlopen(analyzer_link).read()  # nosec
        except urllib2.URLError:
            analyzer_image_data = None

    # If we failed to get the image or if it was explicitly disabled,
    # use the image URL instead of the content.
    if analyzer_image_data is None:
        analyzer_img_tag = '<img src="%s"/>' % analyzer_link
    else:
        analyzer_img_tag = '<img src="cid:%s"/>' % analyzer_content_id

    mirage_body = 'mirage alert NEGATION <br> analyzer reported %s as anomalous at %s over %s hours <br> <br> This metric is NOT anomalous at %s hours <br> <a href="%s">%s</a>' % (metric[1], metric_value, full_duration_in_hours, second_order_resolution_in_hours, link, img_tag)
    analyzer_body = ' <br> <br> <br> Analyzer graph at %s hours for anomalous value: %s<br> <a href="%s">%s</a>' % (full_duration_in_hours, metric_value, analyzer_link, analyzer_img_tag)
    if analyzer_image_data is not None:
        body = mirage_body + analyzer_body
    else:
        body = mirage_body

    for recipient in recipients:
        msg = MIMEMultipart('alternative')
        msg['Subject'] = 'mirage alert NEGATION - ' + metric[1]
        msg['From'] = sender
        msg['To'] = recipient

        msg.attach(MIMEText(body, 'html'))
        if image_data is not None:
            msg_attachment = MIMEImage(image_data)
            msg_attachment.add_header('Content-ID', '<%s>' % content_id)
            msg.attach(msg_attachment)
        if analyzer_image_data is not None:
            analyzer_msg_attachment = MIMEImage(analyzer_image_data)
            analyzer_msg_attachment.add_header('Content-ID', '<%s>' % analyzer_content_id)
            msg.attach(analyzer_msg_attachment)

        s = SMTP('127.0.0.1')
        s.sendmail(sender, recipient, msg.as_string())
        s.quit()


def negate_hipchat(alert, metric, second_order_resolution_seconds, metric_value):

    # SECOND_ORDER_RESOLUTION_SECONDS to hours so that mirage surfaces the
    # relevant timeseries data in the graph
    second_order_resolution_in_hours = int(second_order_resolution_seconds) / 3600
    # not really required


def negate_syslog(alert, metric, second_order_resolution_seconds, metric_value):
    import sys
    import syslog

    # FULL_DURATION to hours so that mirage can surface the relevant timeseries data
    # for analyzer comparison in the graph
    full_duration_in_hours = int(settings.FULL_DURATION) / 3600

    # SECOND_ORDER_RESOLUTION_SECONDS to hours so that mirage surfaces the
    # relevant timeseries data in the graph
    second_order_resolution_in_hours = int(second_order_resolution_seconds) / 3600

    syslog_ident = settings.SYSLOG_OPTS['ident'] + '-mirage'
    message = str("not anomalous at %s hours: %s (%s hours value: %s) - (%s hours value: %s)" % (second_order_resolution_in_hours, metric[1], full_duration_in_hours, metric_value, second_order_resolution_in_hours, metric[0]))
    if sys.version_info[:2] == (2, 6):
        syslog.openlog(syslog_ident, syslog.LOG_PID, syslog.LOG_LOCAL4)
    elif sys.version_info[:2] == (2, 7):
        syslog.openlog(ident="skyline", logoption=syslog.LOG_PID, facility=syslog.LOG_LOCAL4)
    elif sys.version_info[:1] == (3):
        syslog.openlog(ident="skyline", logoption=syslog.LOG_PID, facility=syslog.LOG_LOCAL4)
    else:
        syslog.openlog(syslog_ident, syslog.LOG_PID, syslog.LOG_LOCAL4)
    syslog.syslog(4, message)


def trigger_negater(alert, metric, second_order_resolution_seconds, metric_value):

    if alert[1] == 'smtp':
        strategy = 'negate_analyzer_alert'
    else:
        strategy = 'negate_' + alert[1]

    getattr(negaters, strategy)(alert, metric, second_order_resolution_seconds, metric_value)
