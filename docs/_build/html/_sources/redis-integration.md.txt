## Redis integration

Part of the Horizon service's job is to input data into Redis.

### How are timeseries stored in Redis?

Skyline uses MessagePack to store data in Redis. When a data point comes in, a
Horizon worker will pack the datapoint with the schema [timestamp, value] into a
MessagePack-encoded binary string and make a redis.append() call to append this
string to the appropriate metric key. This way, we can make very easily make
many updates to Redis at once, and this is how Skyline is able to support a very
large firehose of metrics.

One downside to this scheme is that once timeseries in Redis start to get very
long, Redis' performance suffers. Redis was not designed to contain very large
strings. We may one day switch to an alternative storage design as proposed by
Antirez - see https://github.com/antirez/redis-timeseries - but for now, this is
how it is.

In addition to all the metrics, there are two specialized Redis sets:
'metrics.unique_metrics' and 'mini.unique_metrics.' These contain every key that
exists in Redis, and they are used by the Analyzer to make it easier for the
Analyzer to know what to mget from Redis (as opposed to doing a very expensive
keys * query)

### What's with the 'mini' and 'metrics' namespaces?
The 'mini' namespace currently has two uses: it helps performance-wise for
Oculus, if you choose to use it, and it also is used by the webapp to display a
`settings.MINI_DURATION` seconds long view.
