#!/usr/bin/env python

import os
import sys
from os.path import dirname, join, realpath
from optparse import OptionParser

# Get the current working directory of this file.
# http://stackoverflow.com/a/4060259/120999
__location__ = realpath(join(os.getcwd(), dirname(__file__)))

# Add the shared settings file to namespace.
sys.path.insert(0, join(__location__, '..', 'skyline'))
import settings

parser = OptionParser()
parser.add_option("-t", "--trigger", dest="trigger", default=False,
                  help="Actually trigger the appropriate alerts (default is False)")

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
    print "Exception: Check your settings file for the existence of ENABLE_ALERTS and ALERTS"
    sys.exit()

try:
    syslog_enabled = settings.SYSLOG_ENABLED
except:
    syslog_enabled = False

print 'Verifying alerts for: "' + options.metric + '"'

# Send alerts
if alerts_enabled:
    for alert in alerts:
        if alert[0] in options.metric:
            print '    Testing Analyzer alerting - against "' + alert[0] + '" to send via ' + alert[1] + "...triggered"
            if options.trigger:
                metric = (0, options.metric)
                trigger_alert(alert, metric)
                if syslog_enabled:
                    print '    Testing Analyzer alerting - against "' + alert[0] + '" to send via syslog ' + "...triggered"
                    alert = (alert[0], 'syslog')
                    trigger_alert(alert, metric)
        else:
            print '    Testing Analyzer alerting - against "' + alert[0] + '" to send via ' + alert[1] + "..."
else:
    print 'Alerts are disabled'

# Mirage alerts
try:
    mirage_enabled = settings.ENABLE_MIRAGE
    mirage_alerts_enabled = settings.MIRAGE_ENABLE_ALERTS
except:
    mirage_alerts_enabled = False

if mirage_alerts_enabled:
    sys.path.insert(0, join(__location__, '..', 'skyline', 'mirage'))
    import mirage_alerters
    from mirage_alerters import trigger_alert
    if mirage_alerts_enabled:
        for alert in alerts:
            if alert[0] in options.metric:
                print '    Testing Mirage alerting - against "' + alert[0] + '" to send via ' + alert[1] + "...triggered"
                if options.trigger:
                    metric = (0, options.metric)
                    trigger_alert(alert, metric, 86400)
                if syslog_enabled:
                    print '    Testing Mirage alerting - against "' + alert[0] + '" to send via syslog ' + "...triggered"
                    alert = (alert[0], 'syslog')
                    trigger_alert(alert, metric, 86400)
            else:
                print '    Testing Mirage alerting - against "' + alert[0] + '" to send via ' + alert[1] + "..."
else:
    print 'Mirage alerts are disabled'

# Boundary alerts
try:
    boundary_alerts_enabled = settings.BOUNDARY_ENABLE_ALERTS
    boundary_alerts = settings.ALERTS
except:
    boundary_alerts_enabled = False

if boundary_alerts_enabled:
    sys.path.insert(0, join(__location__, '..', 'skyline', 'boundary'))
    from boundary_alerters import trigger_alert
    if boundary_alerts_enabled:
        for alert in alerts:
            if alert[0] in options.metric:
                print '    Testing against "' + alert[0] + '" ...triggered'
                if options.trigger:
                    print '    Testing against "' + alert[0] + '" to send via smtp'
                    trigger_alert('smtp', '0', alert[0], '1', '1', 'greater_than')
                    print '    Testing against "' + alert[0] + '" to send via hipchat'
                    trigger_alert('hipchat', '0', alert[0], '1', '1', 'greater_than')
                    print '    Testing against "' + alert[0] + '" to send via pagerduty'
                    trigger_alert('pagerduty', '0', alert[0], '1', '1', 'greater_than')
                    print '    Testing against "' + alert[0] + '" to send via syslog'
                    trigger_alert('syslog', '0', alert[0], '1', '1', 'greater_than')
else:
    print 'Boundary alerts are disabled'
