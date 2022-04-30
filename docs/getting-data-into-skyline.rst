=========================
Getting data into Skyline
=========================

Firstly a note on time snyc
~~~~~~~~~~~~~~~~~~~~~~~~~~~

Although it may seems obvious, it is important to note that any metrics
coming into Graphite and Skyline should come from synchronised sources.
If there is more than 60 seconds (or highest resolution metric), certain
things in Skyline will start to become less predictable, in terms of the
functioning of certain algorithms which expect very recent datapoints.
Time drift does decrease the accuracy and effectiveness of some
algorithms. In terms of machine related metrics, normal production grade
time synchronisation will suffice.

Secondly a note on the reliability of metric data
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

There are many ways to get data to Graphite and Skyline, however some are better
than others.  The first and most important point is that your metric pipeline
should be transported via TCP, from source, to Graphite, to Skyline.  Although
the original Skyline set up in the days of statsd UDP only and where UDP
transport was seen as acceptable (and it possibly is in a LAN environment).
Data analysis with metrics shipped via any UDP in a distributed and/or cloud
environments is not as effective in terms of analysis as TCP shipped metrics.

For Skyline to do the full spectrum of analysis both in the real time and
historic data contexts, it needs **reliable** data, with as few missing data
points as possible.

Although collectd is great it ships via UDP, which is not great.  So ensure that
your metric pipeline is fully TCP transported.  statsd now has a TCP listener,
there is telegraf, sensu, etc there are lots of options.

Now getting the data in
~~~~~~~~~~~~~~~~~~~~~~~

You currently have a number of options to get data into Skyline, via the
Horizon, Vista and Flux services and via file upload:

Horizon - TCP pickles
=====================

Horizon was designed to support a stream of pickles from the Graphite
carbon-relay service, over port 2024 by default. Carbon relay is a
feature of Graphite that immediately forwards all incoming metrics to
another Graphite instance, for redundancy. In order to access this
stream, you simply need to point the carbon relay service to the box
where Horizon is running. In this way, Carbon-relay just thinks it's
relaying to another Graphite instance. In reality, it's relaying to
Skyline.

Here are example Carbon configuration snippets:

relay-rules.conf:

::

    [all]
    pattern = .*
    destinations = 127.0.0.1:2004, <YOUR_SKYLINE_HOST>:2024

    [default]
    default = true
    destinations = 127.0.0.1:2004:a, <YOUR_SKYLINE_HOST>:2024:a

carbon.conf:

::

    [relay]
    RELAY_METHOD = rules
    DESTINATIONS = 127.0.0.1:2004, <YOUR_SKYLINE_HOST>:2024
    USE_FLOW_CONTROL = False
    MAX_QUEUE_SIZE = 5000

A quick note about the carbon agents: Carbon-relay is meant to be the
primary metrics listener. The 127.0.0.1 destinations in the settings
tell it to relay all metrics locally to a carbon-cache instance that is
presumably running **and** the Skyline host. If you are currently running
carbon-cache as your primary listener, you will need to switch it so
carbon-relay is primary listener and ensure that your metrics are being sent to
the Graphite carbon-relay instance and port and **not** directly to the
carbon-cache instance and port because carbon-cache **does not** forward them to
Skyline.  This will mean reconfiguring your collector (e.g. telegraf, sensu, etc)
to send to the carbon-relay instance and port.

Note the small MAX\_QUEUE\_SIZE - in older versions of Graphite, issues
can arise when a relayed host goes down. The queue will fill up, and
then when the relayed host starts listening again, Carbon will attempt
to flush the entire queue. This can block the event loop and crash
Carbon. A small queue size prevents this behaviour.

See `the
docs <http://graphite.readthedocs.org/en/latest/carbon-daemons.html>`__
for a primer on Carbon relay.

Of course, you don't need Graphite to use this listener - as long as you
pack and pickle your data correctly (you'll need to look at the source
code for the exact protocol), you'll be able to stream to this listener.

Horizon - UDP messagepack
=========================

Generally do not use this.  It is UDP, but has not been removed.

Horizon also accepts metrics in the form of messagepack encoded strings
over UDP, on port 2025. The format is
``[<metric name>, [<timestamp>, <value>]]``. Simply encode your metrics
as messagepack and send them on their way.

However a quick note, on the transport any metrics data over UDP....
sorry if did you not get that.

Telegraf
========

Skyline Flux can ingest data from the influxdata Telegraf collector, see Flux
below, see the `Flux <flux.html>`__ page

Flux
====

Metrics can be submitted to Flux via HTTP/S which feeds Graphite with pickles to
Skyline, see the `Flux <flux.html>`__ page.

upload_data to Flux
~~~~~~~~~~~~~~~~~~~

See the `upload_data to Flux <upload-data-to-flux.html>`__ page.

Vista
=====

Metrics to be fetched by Vista which submits to Flux, see the
`Vista <vista.html>`__ page.

Adding a Listener
=================

If none of these methods are acceptable, it's easy enough to extend
them. Add a method in listen.py and add a line in the horizon-agent that
points to your new listener.

:mod:`settings.FULL_DURATION`
=============================

Once you get real data flowing through your system, the Analyzer will be
able start analysing for anomalies.

.. note:: Do not expect to see anomalies or anything in the Webapp immediately
  after starting the Skyline services. Realistically :mod:`settings.FULL_DURATION`
  should have been passed, before you begin to assess any triggered anomalies,
  after all :mod:`settings.FULL_DURATION` is the baseline.  Although not all
  algorithms utilize all the :mod:`settings.FULL_DURATION` data points, some do
  and some use only 1 hour's worth.  However the Analyzer log should still report
  values in the exception stats, reporting how many metrics were boring, too
  short, etc as soon as it is getting data for metrics that Horizon is populating
  into Redis.
