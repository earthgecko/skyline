## Horizon

The Horizon service is responsible for collecting, cleaning, and formatting
incoming data. It consists of Listeners, Workers, and Roombas. Horizon can be
started, stopped, and restarted with the `bin/horizon.d` script.

### Listeners

Listeners are responsible for listening to incoming data. There are currently
two types: a TCP-pickle listener on port 2024, and a UDP-messagepack listener
on port 2025. The Listeners are easily extendible. Once they read a metric from
their respective sockets, they put it on a shared queue that the Workers read
from. For more on the Listeners, see [Getting Data Into Skyline](getting-data-into-skyline.html).

### Workers

The workers are responsible for processing metrics off the queue and inserting
them into Redis. They work by popping metrics off of the queue, encoding them
into Messagepack, and appending them onto the respective Redis key of the metric.

### Roombas

The Roombas are responsible for trimming and cleaning the data in Redis. You
will only have a finite amount of memory on your server, and so if you just let
the Workers append all day long, you would run out of memory. The Roomba cycles
through each metric in Redis and cuts it down so it is as long as
`settings.FULL_DURATION`. It also dedupes and purges old metrics.

