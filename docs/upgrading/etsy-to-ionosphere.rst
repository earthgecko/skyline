==============================
Upgrading - Etsy to Ionosphere
==============================

First review `Upgrading from the Etsy version of Skyline
<index.html>`__ section.

Upgrade steps
=============

These step describe an in-situ manual upgrade process (use sudo where
appropriate for your environment).

Due to virtualenv being the default and recommended way to now run Skyline, in
order to upgrade most of the installation steps in the Installation
documentation are still appropriate.

- Setup a Python virtualenv for Skyline - see `Running in Python
  virtualenv - HOWTO <../running-in-python-virtualenv.html#howto-python-virtualenv-skyline>`__
- Go through the installation process, ignoring the Redis install part and
  breaking out of installation where instructed to and returning to here.

- Stop all your original Skyline apps, ensuring that something like monit or
  supervisord, etc does not start them again.
- At this point you can either move your current Skyline directory and replace
  it with the new Skyline directory **or** just use the new path and just start
  the apps from their new location.
- If you want to move it, this is the PATH to **your** current Skyline, which
  contains:

::

    skyline/bin
    skyline/src
    # etc

- We are moving your skyline/ directory here.

.. code-block:: bash

    SKYLINE_DIR=<YOUR_SKYLINE_DIR>
    mv "$SKYLINE_DIR" "${SKYLINE_DIR}.etsy.master"
    mv /opt/skyline/github/skyline "$SKYLINE_DIR"

- Copy or move your existing Skyline whisper files to their new
  namespaces on your Graphite server/s (as mentioned above.) Or use
  Graphite's whisper-fill.py to populate the new metric namespaces from
  the old ones after starting the new Skyline apps.
- Start the Skyline apps, either from your path if moved or from the new
  location, whichever you used

.. code-block:: bash

    "$SKYLINE_DIR"/bin/horizon.d start
    "$SKYLINE_DIR"/bin/analyzer.d start
    "$SKYLINE_DIR"/bin/mirage.d start
    "$SKYLINE_DIR"/bin/boundary.d start
    "$SKYLINE_DIR"/bin/ionosphere.d start
    "$SKYLINE_DIR"/bin/pnorama.d start  # if you have the MySQL DB set up
    "$SKYLINE_DIR"/bin/webapp.d start

- Check the log files to ensure things started OK and are running and there are
  no errors.

.. note:: When checking a log make sure you check the log for the appropriate
  time, Skyline can log lots fast, so short tails may miss some event you
  expect between that restart and tail.

.. code-block:: bash

    # Check what the logs reported when the apps started
    head -n 20 /var/log/skyline/*.log

    # How are they running
    tail -n 20 /var/log/skyline/*.log

    # Any errors - each app
    find /var/log/skyline -type f -name "*.log" | while read skyline_logfile
    do
      echo "#####
    # Checking for errors in $skyline_logfile"
      cat "$skyline_logfile" | grep -B2 -A10 -i "error ::\|traceback" | tail -n 60
      echo ""
      echo ""
    done

- If you added the new ``skyline_test.alerters.test`` alerts tuples to your
  ``settings.py`` you can test them now, see `Alert testing <../alert-testing.html>`__
- Look at implementing the other new features at your leisure
- Panorama is probably the quickest win if you opted to not install it
- Boundary, Mirage and Ionosphere will take a little assessment over time to see
  what metrics you want to configure them to monitor.  You cannot rush timeseries.
