tsfresh
=======

EXPERIMENTAL

The Ionosphere branch introduces tsfresh to the Skyline stack.

https://github.com/blue-yonder/tsfresh/

See `Development - Ionosphere <development/ionosphere.html>`__ for the long
trail that lead to tsfresh.

Graphite integration
--------------------

Skyline needs to tie Graphite, Redis and tsfresh together.  However these is
fairly straight forward really, but to save any others having to reverse
engineer the process the skyline/tsfresh/scripts are written is a generic type
of way that does not require downloading Skyline, they should run standalone
so that others can use them if they want some simple Graphite -> tsfresh
capabilities.

See:

- skyline/tsfresh/scripts/tsfresh_graphite_csv.py
- skyline/tsfresh/scripts/tsfresh_graphite_csv.requirements.txt


skyline/tsfresh/scripts/tsfresh_graphite_csv.py
-----------------------------------------------

Assign a Graphite single tiemseries metric csv file to tsfresh to process and
calculate the features for.

:param path_to_your_graphite_csv: the full path and filename to your Graphite
    single metric timeseries file saved from a Graphite request with &format=csv
:type path_to_your_graphite_csv: str
:param pytz_tz: [OPTIONAL] defaults to UTC or pass as your pytz timezone string.
    For a list of all pytz timezone strings see
    https://github.com/earthgecko/skyline/blob/ionosphere/docs/development/pytz.rst
    and find yours.
:type string: str

Run the script with:

```
python tsfresh/scripts/tsfresh_graphite_csv path_to_your_graphite_csv [pytz_timezone]
```

Where path_to_your_graphite_csv.csv is a single metric timeseries that has been
from retrieved from Graphite with the &format=csv request parameter and saved to
a file.

The single metric timeseries could be the result of a graphite function on
multiple timeseries, as long as it is a single timeseries.  This does not handle
multiple timeseries data, meaning a Graphite csv with more than one data set
will not be suitable for this script.

This will output 2 files:

- path_to_your_graphite_csv.features.csv (default tsfresh column wise format)
- path_to_your_graphite_csv.features.transposed.csv (human friendly row wise
  format) you look at this csv :)

Your timeseries features.

.. warning:: Please note that if your timeseries are recorded in a daylight savings
    timezone, this has not been tested the DST changes.
