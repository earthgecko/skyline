.. role:: skyblue
.. role:: red
.. role:: brow

Redis integration
=================

Redis Security
--------------

First please take the time to read and understand Redis security as described
at https://redis.io/topics/security

It is important that you understand factors relating to the default Redis
security model.

.. warning:: With the introduction of :red:`re`:brow:`brow` and Luminosity it
  is very important that Redis and your environment are configured correctly in
  terms of securing Redis and access to Redis.  Please ensure that you implement
  the recommendations described on redis.io security page and the Redis related
  Skyline recommendations described on the installations page in this
  documentation and ``settings.py``.

Types of Skyline and Redis implementations
------------------------------------------

There are two different types of Skyline implementations:

- A single Skyline instance, Redis only needs to bind to 127.0.0.1
- Multiple Skyline instances running in a distributed fashion Redis still
  only needs to bind to 127.0.0.1 but stunnel need to be implmented to allow
  each Skyline server to connect to other Redis instances on Skyline servers via
  a stunnel SSL encrypted connection.  See `Running multiple Skyline instances
  <running-multiple-skylines.html>`__

How are time series stored in Redis?
------------------------------------

Part of the Horizon service's job is to input data into Redis. Skyline uses
MessagePack to store data in Redis. When a data point comes in, a
Horizon worker will pack the data point with the schema ``[timestamp, value]``
into a MessagePack-encoded binary string and make a ``redis.append()`` call to
append this string to the appropriate metric key. This way, Horizon can very
easily make many updates to Redis at once, and this is how Skyline is able to
support a very large firehose of metrics.

One downside to this scheme is that once time series in Redis start to get very
long, Redis' performance suffers. Redis was not designed to contain very large
strings. We may one day switch to an alternative storage design as proposed by
Antirez - see https://github.com/antirez/redis-timeseries - but for now, this is
how it is. And given that the last update to that repo was in 10 Oct 2012, it is
highly probable that normal Redis will be used for the forseenable future.

Using the default mod:`settings.FULL_DURATION` of 86400 (24 hours) ensures that
Redis does not hit performance issues in most cases.  Now where Skyline needs
greater than mod:`settings.FULL_DURATION` data to analyse in Mirage and
Ionosphere, Skyline fetches that data from Graphite directly when required.
This overcomes any Redis performance issues and the use of Graphite data removes
the limitation of Skyline only being able to analyse data and be useful in the
24 hour time window.

What other data are stored in Redis?
------------------------------------

In addition to all the metrics, there are a number of specialized Redis sets and
keys:

- ``metrics.unique_metrics`` and ``mini.unique_metrics`` These contain every key
  that exists in Redis, and they are used by the Analyzer to make it easier for
  the Analyzer to know what to mget from Redis (as opposed to doing a very
  expensive keys * query)
- analyzer.boring - a set of all metrics deemed Boring by Analyzer
- analyzer.stale - a set of all metrics deemed Stale by Analyzer
- analyzer.too_short - a set of all metrics deemed TooShort by Analyzer
- derivative_metrics - a set of all metrics that have been defined as
  metrics to analyse as derivatives by Analyzer.
- non_derivative_metrics - a set of all metrics that have been defined as
  to opposite of derivative_metrics by Analyzer.
- ionosphere.learn.work - a set of metric anomalies with details that require
  learning.
- ionosphere.training_data.* keys containing anomalies that need to be learnt e.g.
  ``[['metric_timestamp', 1526755861], ['base_name', 'carbon.relays.graphite-a.metricsReceived'], ['timeseries_dir', 'carbon/relays/graphite-a/metricsReceived'], ['added_by', 'analyzer']]``
- ionosphere.unique_metrics set
- <skyline_app>.last_alert.<alerter>.<metric> keys
- panorama.mysql_ids.* keys
- rebrow.client.key.* keys - expiring rebrow Redis password JWT encoded tokens
- etc, etc, feel free to browser Redis with :red:`re`:brow:`brow` and take a
  look and please free feel to PR update this doc if you find some that are
  missing here.

What's with the 'mini' and 'metrics' namespaces?
------------------------------------------------

The 'mini' namespace is used by the Webapp to display a `settings.MINI_DURATION`
seconds long view.
