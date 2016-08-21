=========
Upgrading
=========

This section covers the steps involved in upgrading an existing Skyline
implementation. For the sake of fullness this describes the changes from
the last
`github/etsy/skyline <https://github.com/etsy/skyline/commit/22ae09da716267a65835472da89ac31cc5cc5192>`__
commit to date.

Things to note
==============

This new version of Skyline sees a lot of changes, however although
certain things have changed and much has been added, whether these
changes are backwards incompatible in terms of functionality is
debatable. That said, there is a lot of change.  Please review the following
key changes relating to upgrading.

Directory structure changes
~~~~~~~~~~~~~~~~~~~~~~~~~~~

In order to bring Skyline in line with a more standard Python package
structure the directory structure has had to changed to accommodate
sphinx autodoc and the setuptools framework.

settings.py
~~~~~~~~~~~

The ``settings.py`` has had a face lift too. This will probably be the
largest initial change that any users upgrading from Skyline < 1.0.0
will find.

The format is much less friendly to the eye as it now has all the
comments in Python docstrings rather than just plain "#"ed lines.
Apologies for this, but for complete autodoc coverage and referencing it
is a necessary change.

Analyzer optimizations
~~~~~~~~~~~~~~~~~~~~~~

There has been some fairly substantial performance improvements that may affect
your Analyzer settings.  The optimizations should benefit any deployment,
however they will benefit smaller Skyline deployments more than very large
Skyline deployments.  Finding the optimum settings for your Analyzer deployment
will require some evaluation of your Analyzer run_time, total_metrics and
:mod:`settings.ANALYZER_PROCESSES`.

See `Analyzer Optimizations <analyzer-optimizations.html>`__ and regardless of
whether your deployment is small or large the new
:mod:`settings.RUN_OPTIMIZED_WORKFLOW` timeseries analysis will improve
performance and benefit all deployments.

Skyline graphite namespace for metrics
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The original metrics namespace for skyline was ``skyline.analyzer``,
``skyline.horizon``, etc. Skyline now shards metrics by the Skyline
server host name. So before you start the new Skyline apps when
referenced below in the Upgrade steps, ensure you move/copy your whisper
files related to Skyline to their new namespaces, which are
``skyline.<hostname>.analyzer``, ``skyline.<hostname>.horizon``, etc. Or
use Graphite's whisper-fill.py to populate the new metric namespaces
from the old ones.

Logging
~~~~~~~

There have been changes made in the logging methodology, see
`Logging <logging.html>`__.

Clone and have a look
~~~~~~~~~~~~~~~~~~~~~

If you are quite familiar with your Skyline setup it would be useful to
clone the new Skyline and first have a look at the new structure and
assess how this impacts any of your deployment patterns, init,
supervisord, etc and et al.  Diff the new settings.py and your existing
settings.py to see the additions and all the changes you need to make.

Upgrade steps
=============

These step describe an in-situ manual upgrade process (use sudo where
appropriate for your environment).

Due to virtualenv being the default and recommended way to now run Skyline, in
order to upgrade most of the installation steps in the Installation
documentation are still appropriate.

- Setup a Python virtualenv for Skyline - see `Running in Python
  virtualenv - HOWTO <running-in-python-virtualenv.html#howto-python-virtualenv-skyline>`__
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
    "$SKYLINE_DIR"/bin/webapp.d start
    "$SKYLINE_DIR"/bin/pnorama.d start  # if you have the MySQL DB set up

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
    cat /var/log/skyline/horizon.log | grep -B1 -A10 -i "error ::\|traceback" | tail -n 60
    cat /var/log/skyline/analyzer.log | grep -B1 -A10 -i "error ::\|traceback" | tail -n 60
    cat /var/log/skyline/webapp.log | grep -B1 -A10 -i "error ::\|traceback" | tail -n 60
    cat /var/log/skyline/panorama.log | grep -B1 -A10 -i "error ::\|traceback" | tail -n 60

    # If you have the other services running
    cat /var/log/skyline/mirage.log | grep -B1 -A10 -i "error ::\|traceback" | tail -n 60
    cat /var/log/skyline/boundary.log | grep -B1 -A10 -i "error ::\|traceback" | tail -n 60
    cat /var/log/skyline/crucible.log | grep -B1 -A10 -i "error ::\|traceback" | tail -n 60

- If you added the new ``skyline_test.alerters.test`` alerts tuples to your
  ``settings.py`` you can test them now, see `Alert testing <alert-testing.html>`__
- Look at implementing the other new features at your leisure
- Panorama is probably the quickest win if you opted to not install it
- Boundary and Mirage will take a little assessment to see what metrics
  you want to configure them for.
