#############
Alert testing
#############

The settings.py has a default non-existent metric namespace for testing your
alerters, in the ``skyline_test.alerters.test`` :mod:`settings.ALERTS` tuple.

If you want to test your email, Hipchat room and Pagerduty OPTS are correctly
configured and working, do the following, once again using the Python-2.7.11
virtualenv and documentation PATHs as an example:

.. code-block:: bash

  PYTHON_VIRTUALENV_DIR="/opt/python_virtualenv"
  PROJECT="skyline2711"

  cd "${PYTHON_VIRTUALENV_DIR}/projects/${PROJECT}"
  source bin/activate
  python /opt/skyline/github/skyline/utils/verify_alerts.py --trigger True --metric 'skyline_test.alerters.test'
  deactivate

You should get an alert on each alerter that you have enabled.  Do note that the
graphs sent with the alerts will have **no data** in them.
