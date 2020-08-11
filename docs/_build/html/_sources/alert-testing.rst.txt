#############
Alert testing
#############

The settings.py has a default non-existent metric namespace for testing your
alerters, in the ``skyline_test.alerters.test`` :mod:`settings.ALERTS` tuple.

If you want to test your email, Slack or Pagerduty OPTS are correctly configured
and working, do the following.

- Ensure that all your email, slack and/or pagerduty options are all configured
  correctly with your strings/tokens/etc in settings.py
- Ensure the relevant ``skyline_test.alerters.test`` instances in settings.py
  are configured to your desired outputs
- Once again using the Python-3.8.3 virtualenv and documentation PATHs as an
  example:

.. code-block:: bash

  PYTHON_VIRTUALENV_DIR="/opt/python_virtualenv"
  PROJECT="skyline-py383"

  cd "${PYTHON_VIRTUALENV_DIR}/projects/${PROJECT}"
  source bin/activate
  # To test email alert
  bin/python3.8 /opt/skyline/github/skyline/utils/verify_alerts.py --trigger smtp --metric 'skyline_test.alerters.test'
  # To test slack alert
  bin/python3.8 /opt/skyline/github/skyline/utils/verify_alerts.py --trigger slack --metric 'skyline_test.alerters.test'
  # To test pagerduty alert
  bin/python3.8 /opt/skyline/github/skyline/utils/verify_alerts.py --trigger pagerduty --metric 'skyline_test.alerters.test'
  deactivate

.. note:: You should get an alert on each alerter that you have enabled and
  tested.  Do note that the graphs sent with the alerts will have **no data** in
  them.

.. note:: the pagerduty alert_testing trigger has not been tested for a long
  time in the verify_alerts.py context.

You should get an alert on each alerter that you have enabled and tested.  Do
note that the graphs sent with the alerts will have **no data** in them.

You can expect output such as this, **there will be errors**, these are expected,
the alerts still get sent.

.. code-block:: bash

  (skyline-py383) [skyline@skyline skyline-py383] bin/python3.8 /opt/skyline/github/skyline/utils/verify_alerts.py --trigger smtp --metric 'skyline_test.alerters.test'
  Verifying alerts for: "skyline_test.alerters.test"
  Testing Analyzer alerting on skyline_test.alerters.test
      Testing Analyzer alerting - against "skyline_test.alerters.test" to send via smtp...triggered
  ERROR:analyzerLog:error :: alerters - alert_smtp - getattr error
      Testing Analyzer alerting - against "skyline_test.alerters.test" to send via syslog ...triggered
  Testing Mirage alerting on skyline_test.alerters.test
      Testing Mirage alerting - against "skyline_test.alerters.test" to send via smtp...triggered
  ERROR:mirageLog:error :: alert_smtp - unpack timeseries failed
  ERROR:mirageLog:error :: alerters - alert_smtp - getattr error
      Testing Mirage alerting - against "skyline_test.alerters.test" to send via syslog ...triggered
  Testing Boundary alerting on skyline_test.alerters.test
      Testing Boundary alerting against "skyline_test.alerters.test" to send for smtp|slack via smtp
  (skyline-py383) [skyline@skyline skyline-py383] bin/python3.8 /opt/skyline/github/skyline/utils/verify_alerts.py --trigger slack --metric 'skyline_test.alerters.test'
  Verifying alerts for: "skyline_test.alerters.test"
  Testing Analyzer alerting on skyline_test.alerters.test
      Testing Analyzer alerting - against "skyline_test.alerters.test" to send via slack...triggered
      Testing Analyzer alerting - against "skyline_test.alerters.test" to send via syslog ...triggered
  Testing Mirage alerting on skyline_test.alerters.test
      Testing Mirage alerting - against "skyline_test.alerters.test" to send via slack...triggered
      Testing Mirage alerting - against "skyline_test.alerters.test" to send via syslog ...triggered
  Testing Boundary alerting on skyline_test.alerters.test
      Testing Boundary alerting against "skyline_test.alerters.test" to send for smtp|slack via slack
  (skyline-py383) [skyline@skyline skyline-py383]
