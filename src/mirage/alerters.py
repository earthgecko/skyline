from email.MIMEMultipart import MIMEMultipart
from email.MIMEText import MIMEText
from email.MIMEImage import MIMEImage
from smtplib import SMTP
import alerters
import settings
import urllib2

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

# FULL_DURATION to hours so that mirage can surface the relevant timeseries data
# for analyzer comparison in the graph
full_duration_in_hours = int(settings.FULL_DURATION) / 60 / 60

def alert_smtp(alert, metric, second_order_resolution_seconds):

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

    graph_title = '&title=skyline%%20mirage%%20ALERT%%20at%%20%s%%20hours%%0A%s%%20-%%20%s' % (second_order_resolution_in_hours, metric[1], metric[0])

    if settings.GRAPHITE_PORT != '':
        link = '%s://%s:%s/render/?from=-%shour&target=cactiStyle(%s)%s%s&colorList=orange' % (settings.GRAPHITE_PROTOCOL, settings.GRAPHITE_HOST, settings.GRAPHITE_PORT, second_order_resolution_in_hours, metric[1], settings.GRAPHITE_GRAPH_SETTINGS, graph_title)
    else:
        link = '%s://%s/render/?from=-%shour&target=cactiStyle(%s)%s%s&colorList=orange' % (settings.GRAPHITE_PROTOCOL, settings.GRAPHITE_HOST, second_order_resolution_in_hours, metric[1], settings.GRAPHITE_GRAPH_SETTINGS, graph_title)

    content_id = metric[1]
    image_data = None
    if settings.SMTP_OPTS.get('embed-images'):
        try:
            image_data = urllib2.urlopen(link).read()
        except urllib2.URLError:
            image_data = None

    # If we failed to get the image or if it was explicitly disabled,
    # use the image URL instead of the content.
    if image_data is None:
        img_tag = '<img src="%s"/>' % link
    else:
        img_tag = '<img src="cid:%s"/>' % content_id

    body = 'mirage ALERT <br> Anomalous metric: %s <br> value: %s at %s hours <br> Next alert in: %s seconds <br> <a href="%s">%s</a>' % (metric[1], metric[0], second_order_resolution_in_hours, alert[2], link, img_tag)

    for recipient in recipients:
        msg = MIMEMultipart('alternative')
        msg['Subject'] = 'mirage ALERT - ' + metric[1]
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


def alert_pagerduty(alert, metric, second_order_resolution_seconds):
    import pygerduty
    pager = pygerduty.PagerDuty(settings.PAGERDUTY_OPTS['subdomain'], settings.PAGERDUTY_OPTS['auth_token'])
    pager.trigger_incident(settings.PAGERDUTY_OPTS['key'], "Mirage alert - %s - %s" % (metric[0], metric[1]))


def alert_hipchat(alert, metric, second_order_resolution_seconds):
    import hipchat
    hipster = hipchat.HipChat(token=settings.HIPCHAT_OPTS['auth_token'])
    rooms = settings.HIPCHAT_OPTS['rooms'][alert[0]]
    link = settings.GRAPH_URL % (metric[1])

    for room in rooms:
        hipster.method('rooms/message', method='POST', parameters={'room_id': room, 'from': 'Skyline - mirage', 'color': settings.HIPCHAT_OPTS['color'], 'message': 'Anomalous metric: %s (value: %s) - %s' % (metric[1], metric[0], link, metric[1])})


def alert_syslog(alert, metric, second_order_resolution_seconds):
    import sys
    import syslog
    syslog_ident = settings.SYSLOG_OPTS['ident'] + '-mirage'
    message = str("Anomalous metric: %s (value: %s)" % (metric[1], metric[0]))
    if sys.version_info[:2] == (2, 6):
        syslog.openlog(syslog_ident, syslog.LOG_PID, syslog.LOG_LOCAL4)
    elif sys.version_info[:2] == (2, 7):
        syslog.openlog(ident="skyline", logoption=syslog.LOG_PID, facility=syslog.LOG_LOCAL4)
    elif sys.version_info[:1] == (3):
        syslog.openlog(ident="skyline", logoption=syslog.LOG_PID, facility=syslog.LOG_LOCAL4)
    else:
        syslog.openlog(syslog_ident, syslog.LOG_PID, syslog.LOG_LOCAL4)
    syslog.syslog(4, message)


def trigger_alert(alert, metric, second_order_resolution_seconds):

    if '@' in alert[1]:
        strategy = 'alert_smtp'
    else:
        strategy = 'alert_' + alert[1]

    getattr(alerters, strategy)(alert, metric, second_order_resolution_seconds)
