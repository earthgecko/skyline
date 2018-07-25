.. role:: skyblue
.. role:: red
.. role:: brow

Running multiple Skyline instances
==================================

It is possible to run Skyline in a somewhat distributed manner in terms of
running multiple Skyline instances and those Skyline instances sharing a MySQL
database but analysing different metrics and querying the Redis remotely where
required, e.g. Luminosity.

Some organisations have multiple Graphite instances for sharding, geographic or
latency reasons.  In such a case it is possible that each Graphite instance
would pickle to a Skyline instance and for there to be multiple Skyline
instances.

The following settings pertain to running multiple Skyline instances:

- :mod:`settings.ALTERNATIVE_SKYLINE_URLS`
- :mod:`settings.REMOTE_SKYLINE_INSTANCES`

With the introduction of Luminosity a requirement for Skyline to pull the time
series data from remote Skyline instances was added to allow for cross
correlation of all the metrics in the population.  Skyline does this via a api
endpoint and preprocesses the time series data for Luminosity on the remote
Skyline instance and returns only the fragments (gzipped) of time series
required for analysis, by default the previous 12 minutes, to minimise bandwidth
and ensure maintain performance.

Webapp UI
---------

Please note that not all the functionality in the Webapp UI is compatible or
multiple instance aware.  However where any "not in Redis" UI errors are found,
accessing the Skyine Webapp UI on which the metric resides in Redis will resolve
the issue.
