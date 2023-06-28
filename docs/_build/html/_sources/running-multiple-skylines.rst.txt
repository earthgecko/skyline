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
instances.  Or you might want to run mulitple Skyline instances due to the
number of metrics being handled.  However...

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
results in mostly the entire Redis data store being shipped constantly, not
desired.

That said the actual Skyline modules have certain settings and configurations
that are HA or distributed _aware_ to allow Skyline itself to provide the
ability to use HA/clustered/distributed components.  This section deals with
these.

The following settings pertain to running multiple Skyline instances:

- :mod:`settings.REMOTE_SKYLINE_INSTANCES`
- :mod:`settings.HORIZON_SHARDS`
- :mod:`settings.SYNC_CLUSTER_FILES`

With the introduction of Luminosity a requirement for Skyline to pull the time
series data from remote Skyline instances was added to allow for cross
correlation of all the metrics in the population.  Skyline does this via a api
endpoint and preprocesses the time series data for Luminosity on the remote
Skyline instance and returns only the fragments (gzipped) of time series
required for analysis, by default the previous 12 minutes, to minimise bandwidth
and ensure performance is maintained.

Running Skyline in any form of clustered configuration requires that each
Skyline instance know about the other instances and has access to them via the
webapp therefore appropriate firewall, network rules and reverse proxy
configuration (Apache or nginx) needs to allow this.

:mod:`settings.REMOTE_SKYLINE_INSTANCES` is a required list of alternative URLs,
username, password and hostname for the other instances in the Skyline cluster
so that if a request is made to the Skyline webapp for a resource it does not
have, it can return the other URLs to the client.  This is also used in
:mod:`settings.SYNC_CLUSTER_FILES` so that each instance in the cluster can
sync relevant Ionosphere training data and features profiles data to itself.

It is used by Skyline internally to request resources from other Skyline
instances to:

- Retrieve time series data and general data for metrics served by the other
  Skyline instance/s.
- To retrieve resources for certain client and API requests to respond with
  all the data for the cluster, in terms of unique_metrics, alerting_metrics, etc.
- To sync Ionosphere data between the cluster instances.


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

With the addition of labelled_metrics, one can use a VictoriaMetrics cluster
to achieve HA of the TSDB data for labelled_metrics.

Webapp UI
---------

In terms of the functionality in webapp, the webapp is multiple instance aware.
Where any "not in Redis" UI errors are found, webapp responds to the request
with a 302 redirect to the remote Skyline instance that is assigned the metric.

Cluster sync
------------

Cluster nodes will sync training data and features profiles data between themselves,
however currently saved training is not synced between cluster nodes any **user**
saved training data will only be available on the cluster node on which it was saved.
Therefore each cluster node has its own saved training data pages.  This only
relates to training data that is specifically saved by the user and not normal
operational training data that is generated for Ionosphere.
