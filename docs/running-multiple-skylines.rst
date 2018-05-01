Running multiple Skylines
=========================

It is possible to run Skyline in a somewhat distributed manner in terms of
running multiple Skyline instances and those Skyline instances sharing a MySQL
but analysing different metrics and querying each other where required.

Some organisations have multiple Graphite instances for sharding, geographic or
latency reasons.  In such a case it is possible that each Graphite instance
would pickle to a Skyline instance and for there to be multiple Skyline
instances.

The following settings pertain to running multiple Skyline instances:

- :mod:`settings.OTHER_SKYLINE_REDIS_INSTANCES`
- :mod:`settings.ALTERNATIVE_SKYLINE_URLS`
