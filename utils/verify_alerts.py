import os
import sys
from os.path import dirname, join, realpath
from optparse import OptionParser
from time import sleep, time
import logging

# Get the current working directory of this file.
# http://stackoverflow.com/a/4060259/120999
__location__ = realpath(join(os.getcwd(), dirname(__file__)))

# Add the shared settings file to namespace.
sys.path.insert(0, join(__location__, '..', 'skyline'))
import settings
settings.LOG_PATH = '/tmp'
logging.basicConfig()

parser = OptionParser()
parser.add_option("-t", "--trigger", dest="trigger", default=False,
                  help="Actually trigger the appropriate alerts (default is False), e.g. stmp, slack, pagerduty or hipchat")

parser.add_option("-m", "--metric", dest="metric", default='skyline.horizon.queue_size',
                  help="Pass the metric to test (default is skyline.horizon.queue_size)")

(options, args) = parser.parse_args()

# Analyzer alerts
sys.path.insert(0, join(__location__, '..', 'skyline', 'analyzer'))
from alerters import trigger_alert
try:
    alerts_enabled = settings.ENABLE_ALERTS
    alerts = settings.ALERTS
except:
    print('Exception: Check your settings file for the existence of ENABLE_ALERTS and ALERTS')
    sys.exit()

try:
    syslog_enabled = settings.SYSLOG_ENABLED
except:
    syslog_enabled = False

print('Verifying alerts for: "' + options.metric + '"')

# Send alerts
context = 'Analyzer'
if alerts_enabled:
    print('Testing Analyzer alerting on ' + options.metric)
    for alert in alerts:
        if alert[0] == options.metric:
            if options.trigger in alert[1]:
                print('    Testing Analyzer alerting - against "' + alert[0] + '" to send via ' + alert[1] + "...triggered")
                metric = (0, options.metric, 12345)
                trigger_alert(alert, metric, context)
                if syslog_enabled:
                    print('    Testing Analyzer alerting - against "' + alert[0] + '" to send via syslog ' + "...triggered")
                    alert = (alert[0], 'syslog')
                    trigger_alert(alert, metric, context)
else:
    print('Alerts are disabled')

# Mirage alerts
try:
    mirage_enabled = settings.ENABLE_MIRAGE
    mirage_alerts_enabled = settings.MIRAGE_ENABLE_ALERTS
except:
    mirage_alerts_enabled = False

if mirage_alerts_enabled:
    sleep(2)
    print('Testing Mirage alerting on ' + options.metric)
    sys.path.insert(0, join(__location__, '..', 'skyline', 'mirage'))
#    import mirage_alerters
    from mirage_alerters import trigger_alert
    for alert in alerts:
        if alert[0] == options.metric:
            if options.trigger in alert[1]:
                print('    Testing Mirage alerting - against "' + alert[0] + '" to send via ' + alert[1] + "...triggered")
                metric = (0, options.metric, 12345)
                trigger_alert(alert, metric, 86400, 'Mirage')
                if syslog_enabled:
                    print('    Testing Mirage alerting - against "' + alert[0] + '" to send via syslog ' + "...triggered")
                    alert = (alert[0], 'syslog')
                    trigger_alert(alert, metric, 86400, context)
else:
    print('Mirage alerts are disabled')

# Boundary alerts
try:
    boundary_alerts_enabled = settings.BOUNDARY_ENABLE_ALERTS
    boundary_alerts = settings.BOUNDARY_METRICS
except:
    boundary_alerts_enabled = False

if boundary_alerts_enabled:
    sleep(2)
    print('Testing Boundary alerting on ' + options.metric)
    sys.path.insert(0, join(__location__, '..', 'skyline', 'boundary'))
    from boundary_alerters import trigger_alert
    for alert in boundary_alerts:
        if alert[0] == options.metric:
            if options.trigger in alert[7]:
                print('    Testing Boundary alerting against "' + alert[0] + '" to send for ' + alert[7] + ' via ' + options.trigger)
                trigger_alert(options.trigger, '0', alert[0], '1', '1', 'test', int(time()))
else:
    print('Boundary alerts are disabled')
