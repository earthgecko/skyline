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
instances.  However...

Running Skyline in a distributed, HA manner is mostly related to running the
components of Skyline in this fashion, for example using Galera cluster to
provide MariaDB replication.  Each of the components have their own high
availability and clustering methods in most cases and the addition of haproxy,
mysqlproxy, load balanced or round robin DNS can achieve a lot of redundancy.
Defining how Skyline can be run in HA or clustered is beyond the scope of
Skyline itself, it is more an exercise of operations and distributed systems
which is beyond the scope of this documentation.

However one word of caution, **do not cluster Redis**.  Although some sharded
configuration may work, it is simpler just to use local Redis data.  Due to
the metric time series constantly changing in Redis clustering or slaving
results in most the entire Redis data store being shipped constantly, not
desired.

That said the actual Skyline modules have certain settings and configurations
that are HA or distributed _aware_ to allow Skyline itself to provide the
ability to use HA/clustered/distributed components.  This section deals with
these.

The following settings pertain to running multiple Skyline instances:

- :mod:`settings.ALTERNATIVE_SKYLINE_URLS` [required]
- :mod:`settings.REMOTE_SKYLINE_INSTANCES` [required]
- :mod:`settings.HORIZON_SHARDS` [optional]

With the introduction of Luminosity a requirement for Skyline to pull the time
series data from remote Skyline instances was added to allow for cross
correlation of all the metrics in the population.  Skyline does this via a api
endpoint and preprocesses the time series data for Luminosity on the remote
Skyline instance and returns only the fragments (gzipped) of time series
required for analysis, by default the previous 12 minutes, to minimise bandwidth
and ensure performance is maintained.

Running Skyline in any form of clustered configuration requires that each
Skyline instance know about the other instances and has access to them via the
appropriate firewall or network rules and via the reverse proxy configuration
(Apache or nginx).

:mod:`settings.ALTERNATIVE_SKYLINE_URLS` is a reequired list of alternative URLs
for the other nodes in the Skyline cluster, so that if a request is made to the
Skyline webapp for a resource it does not have, it can return the other URLs to
the client.

:mod:`settings.REMOTE_SKYLINE_INSTANCES` is similar but is this is used by
Skyline internally to request resources from other Skyline instances to:

1. Retrieve time series data and general data for metrics served by the other
  Skyline instance/s.
2. To retrieve resources for certain client and API requests to respond with
  all the data for the cluster, in terms of unique_metrics, alerting_metrics,
  etc.

Read about :mod:`settings.HORIZON_SHARDS` see
`HORIZON_SHARDS <horizon.html#HORIZON_SHARDS>`__ section on the Horizon page.

Hot standby configuration
-------------------------

Although there are many possible methods and configurations to ensure that
single points of failure are mitigated in infrastructure, this can be difficult
to achieve with both Graphite and Skyline.  This is due to the nature and volume
of the data being dealt with, especially if you are interested in ensuring
that you have redundant storage for disaster recovery.

With Graphite it is difficult to ensure the whisper data is redundant, due to
volume and real time nature of the whisper data files.

With Skyline it is difficult to ensure the real time Redis data is redundant
given that you DO NOT want to run a Redis slave as the ENTIRE key store
constantly changes.  Slaving a Skyline Redis instance is not an option
as it will use mountains of network bandwidth and just would not work.

One possible configuration to achieve redundancy of Graphite and Skyline data is
to run a Graphite and a Skyline instance as hot standbys in a different data
center.  Where the primary Graphite is pickling to a primary Skyline instance
and a standby Graphite instance.  With the standby Graphite instance pickling
data to the standby Skyline instance.

.. code-block::

                        graphite-1
                            |
                       carbon-relay
          __________________|____
          |            |         |
      carbon-cache   pickle    pickle
                       |         |
                       |         +-->--> data-center-2
                       |                      |
                   skyline-1              graphite-2
                                              |
                                         carbon-relay
                                        ______|______
                                        |            |
                                  carbon-cache     pickle
                                                     |
                                                  skyline-2

In terms of the Skyline configuration of the hot standby you configure skyline-2
the same as skyline-1 in terms of alerts, etc, but you set
:mod:`settings.ANALYZER_ENABLED` and :mod:`settings.LUMINOSITY_ENABLED` to
`False`.

In the event of a failure of graphite-1 you reconfigure your things to send
their metrics to graphite-2 and set skyline-2 :mod:`settings.ANALYZER_ENABLED`
and :mod:`settings.LUMINOSITY_ENABLED` to `True`.

In the event of a failure of skyline-1 you set skyline-2
:mod:`settings.ANALYZER_ENABLED` and :mod:`settings.LUMINOSITY_ENABLED` to
`True`.

The setting up of a hot standby Graphite instance requires pickling AND periodic
flock rsyncing of all the whisper files from graphite-1 to graphite-2 to ensure
that any data that graphite-2 may have been lost in any `fullQueueDrops`
experienced with the pickle from graphite-1 to graphite-2 due to network
partitioning, etc, are updated.  flock rsyncing all the whisper files daily
mostly handles this and ensures that you have no gaps in the whisper data on
your backup Graphite instance.

Webapp UI
---------

In terms of the functionality in webapp, the webapp is multiple instance aware.
Where any "not in Redis" UI errors are found, webapp responds to the request
with a 301 redirect to the alternate Skyline URL.
