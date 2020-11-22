=======
Horizon
=======

The Horizon service is responsible for collecting, cleaning, and formatting
incoming data. It consists of Listeners, Workers, and Roombas. Horizon can be
started, stopped, and restarted with the `bin/horizon.d` script.

Listeners
=========

Listeners are responsible for listening to incoming data. There are currently
two types: a TCP-pickle listener on port 2024, and a UDP-messagepack listener
on port 2025. The Listeners are easily extendible. Once they read a metric from
their respective sockets, they put it on a shared queue that the Workers read
from. For more on the Listeners, see [Getting Data Into Skyline](getting-data-into-skyline.html).

Workers
=======

The workers are responsible for processing metrics off the queue and inserting
them into Redis. They work by popping metrics off of the queue, encoding them
into Messagepack, and appending them onto the respective Redis key of the metric.

Roombas
=======

The Roombas are responsible for trimming and cleaning the data in Redis. You
will only have a finite amount of memory on your server, and so if you just let
the Workers append all day long, you would run out of memory. The Roomba cycles
through each metric in Redis and cuts it down so it is as long as
`settings.FULL_DURATION`. It also dedupes and purges old metrics.

HORIZON_SHARDS
==============

This is an advanced feature related to running multiple, distributed Skyline
servers (and Graphite instances) in a replicated fashion.  This allows for all
the Skyline servers to receiver all metrics but only analyze those metrics that
are assigned to the specific server (shard).  This enables all Skyline servers
that are running Horizon to receive the entire metric population stream from
multiple Graphite carbon-relays and have Horizon drop (not submit to their Redis
instance) any metrics that do not belong to their shard.

In order to achieve this replicated configuration, Graphite also needs to be
configured and run appropriately to allow the metrics to be forwarded to the
remote Graphite servers.  To achieve this an additional carbon-relay-b instance
needs to be run on each Graphite server.  This additional carbon-relay-b
receives metrics from all the other Graphite servers and only relays them to the
Horizon shard listen process (default port 2026) and the local Graphite
carbon-cache.  The primary Graphite carbon-relay process on each Graphite server
relays metrics to the normal horizon listen process (default port 2024), to the
local carbon-cache and to each remote carbon-relay-b instance.

The carbon-relay-b only relays metrics locally.

This Graphite/Horizon mesh configuration achieves:

- all metrics on all Graphite servers
- each Skyline server receiving all metrics
- each Skyline server Horizon only submitting the metrics assigned to its shard
  for analysis

This allows the cluster to be reconfigured to should any server or service fail.
If a Skyline server/shard is lost/fails, it can be removed from the
:mod:`settings.HORIZON_SHARDS` and the metric population will be resharded to
only the remaining online shards.  Although the Skyline shard server will not
have the metrics from the other shard in Redis, with the
:mod:`settings.MIRAGE_AUTOFILL_TOOSHORT` feature enabled, the Skyline Analyzer
process will automatically initiate population of the FULL_DURATION data into
its Redis datastore by adding the newly assigned metrics to mirage.populate_redis
Redis set (queue).  Mirage will then populate Redis with the FULL_DURATION data
from Graphite.

When the shards reassignment is in process, the Skyline cluster should be
considered to be running in a degraded state until the failed shard is brought
back online and all shard reassignment is complete.  For this configuration to
work in times of failure each Skyline server in the cluster must be able to
handle (number_of_metrics_per_shard + (100/number_of_horizon_shards)) to handle
a failure and shard reassignment.  Using other appropriate load management
Skyline configuration options like :mod:`settings.ANALYZER_DYNAMICALLY_ANALYZE_LOW_PRIORITY_METRICS`
and possibly adjusting the namespaces in :mod:`settings.SKYLINE_FEEDBACK_NAMESPACES`,
etc allows Skyline to manage the load in a best effort manage until the cluster
has been restored to normal configuration.

There are many other different configuration strategies that can be employed run
achieve HA and clustering with Skyline and Graphite which can augment the
management of load and failover such as haproxy, MariaDB Galera clustering, etc.
However, the Skyline/Graphite clustering design outlined here is aimed at
ensuring that each Skyline/Graphite server has all the data and can handle any
shard assignment.

HORIZON_SHARDS related settings.

.. code-block:: python

  HORIZON_SHARDS = {
      'skyline-server-1': 0,
      'skyline-server-2': 1,
      'skyline-server-3': 2,
  }
  HORIZON_SHARD_PICKLE_PORT = 2026

Shards are 0 indexed.

If enabled the metric population is divided between the ``number_of_horizon_shards``
declared in :mod:`settings.HORIZON_SHARDS` by calculating the ``zlib.alder32``
hash value (an integer) and applying a modulo to it to determine if the metric
belongs to the Horizon shard number assigned to that server.

Although ``zlib.alder32`` is not a cryptographic hash it is **fast** and
computes the same value across all Python version and platforms so it does the
job.  Further the distribution of metrics is not an exact equal division of the
metrics, but it is good enough.
