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

- :mod:`settings.OTHER_SKYLINE_REDIS_INSTANCES`
- :mod:`settings.ALTERNATIVE_SKYLINE_URLS`

Up until the introduction of Luminosity, Skyline's Redis requirement was
localhost only, it was bound to 127.0.0.1 and was accessed by the Skyline apps
via socket only, apart from :red:`re`:brow:`brow` which connected on localhost.
If Skyline is run on multiple instances, Luminosity now adds a requirement for
Redis to be accessed remotely in order for Luminosity to cross correlate all
metrics.  This requires additional configuration and set up steps to properly
secure the Redis instances.

Skyline's preferred method of handling remote connections to Redis is via a
SSL tunnel using stunnel.  Although Redis recommends spiped, stunnel performance
is much better and stunnel is easier to implement as there are distro packages
for stunnel.

Setting up stunnel
------------------

docs TDB
