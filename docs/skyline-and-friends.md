# Skyline and friends

Skyline has some close relationships to a number of metric pipelining things.

## Graphite - a close relationship

Anyone having used Skyline may have wondered in the past why Skyline sent metrics
to [Graphite](https://github.com/graphite-project).  One may have also wondered
why there was never a Statsd option, why just Graphite?

It seems natural that Etsy might have had Skyline feed it metrics to Statsd as
an option at least.  However, there never was a `STATSD_HOST` setting and this
is quite fortunate.

The relationship between Graphite and Skyline is very close, as in they can
monitor each other through a direct feedback loop or interaction.  If Statsd
was ever an option, it would add a degree of separation between the 2 which is
not required or desirable, although it would work.

### Feedback loop

Skyline's own metrics really are an important aspect of Skyline's operations
over time, in terms of:

- monitoring Skyline
- monitoring performance in terms of:

  - Skyline's own running times, load, algorithm performance, etc
  - being able monitoring the overall performance of your "things" over time

- To the new user these things may seem uninteresting, probably never to be
  looked metrics, however over time they will describe your ups and downs,
  your highs and lows and hopefully add to your understanding of your "things"

## Statsd

[Statsd](https://github.com/etsy/stats) feeds graphite so it is quite handy.
Use TCP, not UDP

## BuckyServer

Before tcp transport was added to Statsd was
[BuckyServer](https://github.com/HubSpot/BuckyServer) for long haul TCP
transport of your metrics to Statsd -> Graphite.

### Some advantages to using Buckyserver

Although the introduction of another node app may seem like too much effort or
additional complexity, using Buckyserver also has additional advantages.
Although Statd does allow for Multi-Metric Packets the total length of the
payload must be within the MTU of all your hops, a very hard variable to know
and quite a hassle to calculate dynamic in scripts/apps.

Buckysever has the advantage of being able to ship lots of metrics with a single
POST, which is useful for shipping custom or app metrics and has no size limits.

Using Statsd and Buckyserver also allows one to describe 2 methods of shipping
data in scripts/apps, via Buckyserver and via Statsd.  This allows you to ship
via the Buckyserver method and if that fails failover to the Statsd method,
where each metric can be iterated through in your script/app and sent
individually, if you do not want the hassle of dealing with Multi-Metric Packet
payload size.  Running Buckyserver and Statsd on the same instance allows for
these types of advanced usage and has proved to be very reliable.

## Sensu

[Sensu](https://sensuapp.org/) can feed Graphite like collectd but uses TCP not
UDP for transport - [Sensu on github](https://github.com/sensu/sensu)

## Riemann

[Riemann.io](http://riemann.io) can feed Graphite -
[Riemann on github](https://github.com/riemann/riemann)

## Logstash

[Logstash](https://www.elastic.co/products/logstash) can feed Graphite -
[Logstash on github](https://github.com/elastic/logstash)

## Many more

There are a great deal of apps that can feed Skyline, this is just mentioning
a few.  However bear in mind, that whatever you use, try and ensure that it is
TCP and not UDP based.
