tsfresh
=======

The Ionosphere branch introduced tsfresh to the Skyline stack to enable the
creation of feature profiles for time series that the user deems to be not
anomalous.

https://github.com/blue-yonder/tsfresh/

See `Development - Ionosphere <development/ionosphere.html>`__ for the long
trail that lead to tsfresh.

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
Python-2.7 from wherever you save the script:

.. code-block:: bash

    cd "${PYTHON_VIRTUALENV_DIR}/projects/${PROJECT}"
    source bin/activate
    bin/python2.7 tsfresh_features/scripts/tsfresh_graphite_csv path_to_your_graphite_csv [pytz_timezone]
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
