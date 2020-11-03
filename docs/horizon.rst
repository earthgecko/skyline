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
that are running Horizon to receiver the entire metric population stream from
multiple Graphite carbon-relays and drop (not submit to their Redis instance)
any metrics that do not belong to their shard.

.. code-block:: python

  HORIZON_SHARDS = {
      'skyline-server-1': 0,
      'skyline-server-2': 1,
      'skyline-server-3': 2,
  }

Shards are 0 indexed.

If enabled the metric population is divided between the ``number_of_horizon_shards``
declared in :mod:`settings.HORIZON_SHARDS` by calculating the ``zlib.alder32``
hash value (an integer) and applying a modulo to it to determine if the metric
belongs to the Horizon shard number assigned to that server.

Although ``zlib.alder32`` is not a cryptographic hash it is **fast** and
computes the same value across all Python version and platforms so it does the
job.  Further the distribution of metrics is not an exact equal division of the
metrics, but it is good enough.
