tsfresh
=======

The Ionosphere branch introduced tsfresh to the Skyline stack to enable the
creation of feature profiles for time series that the user deems to be not
anomalous. https://github.com/blue-yonder/tsfresh/

See `Development - Ionosphere <development/ionosphere.html>`__ for the long
trail that lead to tsfresh.

Skyline tsfresh fork
--------------------

Due to the addition of new algorithms/features and modifications to tsfresh, the
original blue-yonder/tsfresh package can no longer be run on any of the tsfresh
releases since v0.5.0 with Skyline to achieve consistent results on features
extraction.  Therefore Skyline runs a modified fork of the tsfresh.  This
modified fork maintains the features extracted at v0.4.0 but moves this tsfresh
forked version forward in line with blue-yonder/tsfresh in terms of tsfresh
internals and dependencies, etc.

https://github.com/earthgecko/tsfresh

In this fork:

- New features added to blue-yonder/tsfresh are disabled
- Original methods for features are maintained even if they are changed in
  blue-yonder/tsfresh

.. note:: these branches/versions are tested against the
  tests/baseline/tsfresh_features_test.py, which was removed from
  blue-yonder/tsfresh in v0.7.0 but has been readded to this fork. These
  branches/versions are only tested via the Skyline build tests, they are not
  tested against the tsfresh tests. Seeing as this fork follows the
  blue-yonder/tsfresh versions and retrospectively makes backwards compatible
  changes to the settings and feature_calculators.py which work with the Skyline
  tests. Therefore these changes are not currently backported to the tsfresh
  tests themselves and the tsfresh tests will fail if run against any of theses
  branches.

tsfresh and Graphite integration
--------------------------------

Skyline needs to tie Graphite, Redis and tsfresh together.  However these is
fairly straight forward really, but to save any others having to reverse
engineer the process the skyline/tsfresh_features/scripts are written is a
generic type of way that does not require downloading Skyline, they should run
standalone so that others can use them if they want some simple Graphite ->
tsfresh feature extraction capabilities.

See:

- skyline/tsfresh_features/scripts/tsfresh_graphite_csv.py
- skyline/tsfresh_features/scripts/tsfresh_graphite_csv.requirements.txt

skyline/tsfresh_features/scripts/tsfresh_graphite_csv.py
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Assign a Graphite single tiemseries metric csv file to tsfresh to process and
calculate the features for.

:param path_to_your_graphite_csv: the full path and filename to your Graphite
    single metric time series file saved from a Graphite request with &format=csv
:type path_to_your_graphite_csv: str
:param pytz_tz: [OPTIONAL] defaults to UTC or pass as your pytz timezone string.
    For a list of all pytz timezone strings see
    https://github.com/earthgecko/skyline/blob/ionosphere/docs/development/pytz.rst
    and find yours.
:type string: str

Run the script with, a virtualenv example is shown but you can run just with
Python-3.8 from wherever you save the script:

.. code-block:: bash

    cd "${PYTHON_VIRTUALENV_DIR}/projects/${PROJECT}"
    source bin/activate
    bin/python3.8 tsfresh_features/scripts/tsfresh_graphite_csv path_to_your_graphite_csv [pytz_timezone]
    deactivate

Where path_to_your_graphite_csv.csv is a single metric time series that has been
from retrieved from Graphite with the &format=csv request parameter and saved to
a file.

The single metric time series could be the result of a graphite function on
multiple time series, as long as it is a single time series.  This does not handle
multiple time series data, meaning a Graphite csv with more than one data set
will not be suitable for this script.

This will output 2 files:

- path_to_your_graphite_csv.features.csv (default tsfresh column wise format)
- path_to_your_graphite_csv.features.transposed.csv (human friendly row wise
  format) you look at this csv :)

Your time series features.

.. warning:: Please note that if your time series are recorded in a daylight
    savings time zone, this has not been tested with DST changes.
