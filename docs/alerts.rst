======
Alerts
======

Alerts in Skyline are configured via multiple sets of settings.  There are the
Analyzer (Mirage and Ionosphere) related settings and the Boundary related
alert settings.  This is due to the two classes of alerts being different,
with Analyzer, Mirage and Ionosphere alerts being related to anomalies and
Boundary alerts being related to breaches of the static and dynamic thresholds
defined for Boundary metrics.  Further to this there are alert related settings
for each alert output route, namely smtp, slack, pagerduty and sms.

Required smtp alerter for Analyzer and Mirage metrics
=====================================================

Skyline was initially driven by SMTP alerts and as such the analysis pipeline is
tightly coupled to SMTP alerts.  Many of the resources required for further
analysis and for other alerters are created during the SMTP alert process.
Further to this, the full spectrum of analysis is only done on metrics that have
a SMTP alert tuple configured for the namespace in :mod:`settings.ALERTS`.  This
is because the more extensive analysis is only done on the metrics that you care
about, your KPI metrics.  Doing the extended analysis beyond the 3sigma related
analysis of metrics that have no alerts configured for them makes no sense as no
one wants to know.  Therefore only metrics in namespaces that are defined with a
stmp alerter in :mod:`settings.ALERTS` get analysed by Mirage and Ionosphere.
It is via the smtp alert tuple that metrics get configured to be Mirage metrics
by declaring the SECOND_ORDER_RESOLUTION_HOURS in the tuple.

However if you do not want to be SMTP alerted, you can set the
:mod:`settings.SMTP_OPTS` to `'no_email'` as shown in an example below, but you
must still declare the namespace with a SMTP alert tuple in
:mod:`settings.ALERTS`

The following example, we want to alert via Slack, your :mod:`settings.ALERTS`
and :mod:`settings.SMTP_OPTS` would need to look like this.

.. code-block:: python

  ALERTS = (
    ('carbon', 'smtp', 3600),  # Analyzer metric, only analysed at FULL_DURATION - Mirage NOT enabled
    ('skyline', 'smtp', 3600, 168),  # Mirage enabled as 168 is passed as SECOND_ORDER_RESOLUTION_HOURS
    ('stats', 'smtp', 1800, 168),  # Mirage enabled as 168 is passed as SECOND_ORDER_RESOLUTION_HOURS
    ('telegraf', 'smtp', 3600, 168),  # Mirage enabled as 168 is passed as SECOND_ORDER_RESOLUTION_HOURS
    # smtp alert tuples MUST be declared first
    ('carbon', 'slack', 3600),
    ('skyline', 'slack', 3600),
    ('stats', 'slack', 1800),
    ('telegraf', 'slack', 3600),
  )
  SMTP_OPTS = {
      # This specifies the sender of email alerts.
      'sender': 'no_email',
      # recipients is a dictionary mapping metric names
      # (exactly matching those listed in ALERTS) to an array of e-mail addresses
      'recipients': {
          'carbon': ['no_email'],
          'skyline': ['no_email'],
          'stats': ['no_email'],
          'telegraf': ['no_email'],
      },
      # This is the default recipient which acts as a catchall for alert tuples
      # that do not have a matching namespace defined in recipients
      'default_recipient': ['no_email'],
      'embed-images': True,
    # You can specify the address of an SMTP server if you do not want to or
    # cannot use a local SMTP server on 127.0.0.1
    'smtp_server': {
        'host': '127.0.0.1',
        'port': 25,
        # Whether to use SSL, not required, optional
        'ssl': False,
        # A SMTP username, not required, optional
        'user': None,
        # A SMTP password, not required, optional
        'password': None,
    },
  }

Smart alerting
==============

Due to the nature of analysing lots of time series, there are times when LOTS
of events do happen, something changes and causes lots of change, things like
cloud provider failures or reboots or a bad deployment, these things happen.
When they do lots and lots of metrics can be submitted for further analysis,
although Skyline is fast, getting through lots of complex analysis on 100s of
metrics takes the time it takes.  At times like these Skyline starts waterfall
alerting, this means if Analyzer sent 100 metrics to Mirage to check and after
5 minutes there are still 30 pending, Analyzer will remove the items from the
Mirage queue and just alert on them.  The same is true for checks submitted to
Ionosphere by Mirage, any check sent upstream will be alerted on by the parent
app if the result of further analysis is not available after a defined period.
This way alerts are not missed, although under these conditions, there will be
some false positives.

Alert settings
==============

For each 3rd party alert service e.g. Slack, PagerDuty, http_alerters, there is
a setting to enable the specific alerter which must be set to `True` to enable
the alerter:

- :mod:`settings.SYSLOG_ENABLED`
- :mod:`settings.PAGERDUTY_ENABLED`
- :mod:`settings.SLACK_ENABLED`
- :mod:`settings.HTTP_ALERTERS_ENABLED`
- :mod:`settings.AWS_SNS_SMS_ALERTS_ENABLED`
- :mod:`settings.SMS_ALERT_OPTS`

Analyzer, Mirage and Ionosphere related alert settings (anomaly detection) are:

- :mod:`settings.ENABLE_ALERTS` - must be set to `True` to enable alerting
- :mod:`settings.ENABLE_FULL_DURATION_ALERTS` - should be set to `False` if
  enable Mirage is enabled.  If this is set to ``True`` Analyzer will alert
  on all checks sent to Mirage, even if Mirage does not find them anomalous,
  this is mainly for testing.
- :mod:`settings.ALERTS` - must be defined to enable alerts via Analyzer and
  Mirage
- :mod:`settings.SMTP_OPTS` - must be defined
- :mod:`settings.SLACK_OPTS` - must be defined if you want to alert via Slack
- :mod:`settings.PAGERDUTY_OPTS` - must be defined if you want to alert via
  Pagerduty
- :mod:`settings.SYSLOG_OPTS` - can be used to change syslog settings
- :mod:`settings.HTTP_ALERTERS_OPTS` - must be defined if you want to push
  alerts to a http endpoint
- :mod:`settings.MIRAGE_ENABLE_ALERTS` - must be set to `True` to enable alerts
  from Mirage
- :mod:`settings.AWS_SNS_SMS_ALERTS_ENABLED` - must be set to `True` if you want
  to send alerts via SMS.  boto3 also needs to be set up and AWS/IAM resource
  that boto3 uses needs permissions to publish to AWS SNS.  See boto3
  documentation - https://github.com/boto/boto3)
- :mod:`settings.SMS_ALERT_OPTS` - must be defined if you want to send SMS
  alerts.

Boundary related alert settings (static and dynamic thresholds) are:

- :mod:`settings.BOUNDARY_ENABLE_ALERTS` - must be set to `True` to enable
  alerting
- :mod:`settings.BOUNDARY_METRICS` - must be defined to enable checks and alerts
  for Boundary
- :mod:`settings.BOUNDARY_ALERTER_OPTS` - can be used to change the Boundary
  alert rate limits
- :mod:`settings.BOUNDARY_SMTP_OPTS` - must be defined if you want to send SMTP
  alerts. Note that Boundary will use the same `smtp_server` as defined in
  :mod:`settings.SMTP_OPTS`
- :mod:`settings.BOUNDARY_PAGERDUTY_OPTS` - must be defined if you want to alert
  via Pagerduty
- :mod:`settings.BOUNDARY_SLACK_OPTS` - must be defined if you want to alert via
  Slack
- :mod:`settings.BOUNDARY_HTTP_ALERTERS_OPTS` - must be defined if you want to
  push alerts to a http endpoint
- :mod:`settings.AWS_SNS_SMS_ALERTS_ENABLED` - must be set to `True` if you want
  to send alerts via SMS.  boto3 also needs to be set up and AWS/IAM resource
  that boto3 uses needs permissions to publish to AWS SNS.  See boto3
  documentation - https://github.com/boto/boto3)
- :mod:`settings.SMS_ALERT_OPTS` - must be defined if you want to send SMS
  alerts.

SMS alerts
==========

Skyline can send SMS via AWS SNS.  The set up of AWS SNS is beyond the scope of
this documentation, see AWS SNS documentation (
https://docs.aws.amazon.com/sns/latest/dg/sms_publish-to-phone.html).

http_alerter alerts
===================

The http_alerter alert type, enables Skyline to send alerts to a HTTP endpoint.
http_alerters are configured under the following settings.

In :mod:`settings.ALERTS` and :mod:`settings.BOUNDARY_METRICS` you defined the
namespace and http_alerter and expiry (how long to not send an alert after an
alert has been sent).

.. code-block:: python

  # For Analzyer, Mirage and Ionosphere
  ALERTS = (
      ...
      ('stats', 'http_alerter-external_endpoint', 30),
  )

  # For Boundary
  BOUNDARY_METRICS = (
      # ('metric', 'algorithm', EXPIRATION_TIME, MIN_AVERAGE, MIN_AVERAGE_SECONDS, TRIGGER_VALUE, ALERT_THRESHOLD, 'ALERT_VIAS'),
      ('stats.rpm.total', 'detect_drop_off_cliff', 1800, 50, 3600, 0, 2, 'slack|pagerduty|http_alerter_external_endpoint'),
      ('stats.rpm.total', 'less_than', 3600, 0, 0, 15, 2, 'http_alerter_external_endpoint'),
      ('stats.rpm.total', 'greater_than', 3600, 0, 0, 10000, 1, 'http_alerter_external_endpoint'),
  )

Each http_alerter can be defined in a common :mod:`settings.HTTP_ALERTERS_OPTS`
which all the apps refer to.

.. code-block:: python

  HTTP_ALERTERS_OPTS = {
      'http_alerter-external_endpoint': {
          'enabled': True,
          'endpoint': 'https://http-alerter.example.org/alert_reciever',
          'token': None
      },
      'http_alerter-otherapp': {
          'enabled': False,
          'endpoint': 'https://other-http-alerter.example.org/alerts',
          'token': '1234567890abcdefg'
      },
  }


The http_alerter will post an alert json object to an HTTP endpoint.  Here is
an example of the alert json POST data:

.. code-block:: json

  {
    "status": {},
    "data": {
      "alert": {
        "timestamp": "1579638755",
        "metric": "stats.sites.graphite_access_log.httpd.rpm.total",
        "value": "75.0",
        "expiry": "30",
        "source": "ionosphere",
        "token": "None",
        "full_duration": "604800"
      }
    }
  }

Failures
--------

If the HTTP endpoint does not respond with a 200, the alert item will be added
to the Redis set (queue) for that alert to be resent.  Each relevant app has a
Redis set (queue):
- analyzer.http_alerter.queue
- mirage.http_alerter.queue
- ionosphere.http_alerter.queue
- boundary.http_alerter.queue

Mirage sends the initial HTTP alert for Mirage and Mirage/Ionosphere metrics, if
an alert fails to be sent it is added to the resend queue.

Analyzer handles *all* the resend queues and resends for Analyzer, Mirage and
Ionosphere.  There are checks to determine whether the endpoint is healthy to
prevent Analyzer from repeatedly attempting to resend all alerts to a
http_alerter endpoint that is down.

Boundary handles the boundary resend queue and resends for boundary alerts.
Boundary uses the same checks to determine whether the endpoint is healthy to
prevent Boundary from attempting to repeatedly resend all alerts to a
http_alerter endpoint that is down.

External alert configs
======================

Skyline can fetch alert configs from external sources

- **Example**

.. code-block:: python

    EXTERNAL_ALERTS = {
        'test_alert_config': {
            'url': 'http://127.0.0.1:1500/mock_api?test_alert_config',
            'method': 'GET',
        },
        'metric_collector_1': {
            'url': 'https://example.org/alert_config',
            'method': 'POST',
            'data': {
                'token': '1234567890123456'
            },
        },
        'client_app': {
            'url': 'https://username:password@app.example.org/alerts?token=3456abcde&metrics=all',
            'method': 'GET',
        },
    }

- The external source endpoint must return a json response with a ``data`` item
  which contains named alert items with the following keys and values:

.. code-block:: json

    {"data": {
      "1": {"alerter": "http_alerter-example.org",
       "namespace_prefix": "vista.remote_hosts.example.org",
       "namespace": "nginx.GET",
       "expiration": "3600",
       "second_order_resolution": "604800",
       "learn_days": "30",
       "inactive_after": "1800"},
      "nginx_errors": {"alerter": "http_alerter-example.org",
       "namespace_prefix": "vista.remote_hosts.example.org",
       "namespace": "nginx.errors",
       "expiration": "900",
       "second_order_resolution": "86400",
       "learn_days": "30",
       "inactive_after": "1800"}
      }
    }
