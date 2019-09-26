.. role:: skyblue
.. role:: red

Vista
=====

Vista is the Skyline service that enables Skyline to fetch metrics from Graphite
and Prometheus servers and populate your Skyline Graphite instance with
them or derivatives of those metrics, via Skyline Flux, so they can be pickled
from Graphite to Skyline for analysis in near real time and the Skyline related
Graphite can be used as the metric source for seasonal analysis and
visualisations.

One example use case would be to say that you wanted to monitor some metrics
from a partner, another environment or a 3rd party with your own Skyline and
Graphite set up.  Vista can be used to fetch those metrics and feed directly to
your Graphite via Skyline Flux and Graphite will pickle them as normal to
Skyline for analysis.

Another example use case is that this allows for Skyline to create new metrics
from existing metrics on a remote or the local Graphite instance.  There are
often some metrics that would suit being analysed by Skyline as sumSeries,
average or some function/s applied to a set of metrics.  For example say you
have a number of servers and you want to monitor some specific metrics like
procs_running or cpu.user collectively for the group of servers by applying a
function, e.g. `sumSeries(stats.*.procs_running)`.  Vista can be used to fetch
the metrics with the function/s applied and submit the resulting time series to
Graphite as a new metric which Skyline can analyse, for example the settings
fetch tuple would take `sumSeries(stats.*.procs_running)` and create the
`vista.stats.cumulative.procs_running` metric.

.. code-block:: python

    ..., 'sumSeries(stats.*.procs_running)', 'vista.stats.cumulative.procs_running', ...

This will result in a new metric in Graphite named
`vista.stats.cumulative.procs_running`, that will have its own time series data
for seasonal analysis, correlation and visualisation.

Important note - Intended use
-----------------------------

Vista is intended to be used to fetch tens to hundreds of metrics, it is not
intended to fetch thousands of metrics.  Although theoretically it could
probably fetch thousands of metrics, it is not intended for this purpose, that
type of implementation would much better suited to a Graphite to Graphite pickle
or Prometheus `remote_storage_adapter` to Graphite.

Very important note - Variations in data and graphs
---------------------------------------------------

Variations in the remote and Graphite data SHOULD be expected.  This applies to
both:
- Prometheus <- Vista -> Graphite and;
- Graphite <- Vista -> Graphite

Anyone with any experience with Graphite, Prometheus or other time series
databases will be aware of a number of slightly confusing concepts that they all
uniquely have.  Whether that be aggregations, averages used in graphs when there
are more data points than pixels, the understanding the `step=` concept, etc,
etc.

When considering populating one time series database from another, all these
factors come into play and combine to increase their complexity and as these
things normally do, create some confusion as there will be slight mismatches
periodically.

Some people may expect that is it data and numbers and computers, so it should
be perfect, all of the time.  Those people have unrealistic expectations and
probably a limited understanding of the time series data stores in question.

There will always be the potential for slight differences.  Usefulness outweighs
perfection and given the variables, perfection cannot be achieved.

Prometheus vs Graphite graphs and data sets
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Be aware that graphs rendered by Prometheus and Graphite may present different
values in terms of max and min values.  Th raw data points in both Prometheus at
`step=60s` and in Graphite in its first retention for a specific time period
should be the same (or very nearly the same, see below).  This can be verified
by rendering the raw data as json for each over the same time period.  The
differences in the rendering of graphs is usually because each render data
somewhat differently, in terms of how each average the data points to best fit
the graph in terms of available pixels, `step=` averaging, etc.

Considering the small differences between Graphite and Prometheus slight
variances in the data should always be expected.  However, these differences
should always be very slight.

Take for example following two time series snippets, one from Prometheus and the
other from Graphite that was populated by Vista from the Prometheus
`query_range` API method.

.. code-block:: python

  # Graphite time series
  [ [1568691720, 1.28],
    [1568691780, 1.17],
    [1568691840, 1.04],
    [1568691900, 1.04],
    [1568691960, 1.15]]

.. code-block:: bash

  [skyline@skyline ~] cat /var/log/skyline/flux.log | grep 1568691840 | grep node_load1
  2019-09-17 03:46:00 :: 7818 :: listen :: GET request - metric=vista.demo_prometheus.prometheus.node_load1&value=1.04&timestamp=1568691840&key=xxxx
  2019-09-17 03:46:00 :: 7818 :: listen :: GET request data added to flux.httpMetricDataQueue - ['vista.demo_prometheus.prometheus.node_load1', 1.04, 1568691840]
  [skyline@skyline ~]

.. code-block:: python

  # Prometheus time series - called later to verify the data via the query_range API method
  [ [1568691720, 1.28],
    [1568691780, 1.17],
    [1568691840, 1.1],
    [1568691900, 1.04],
    [1568691960, 1.15]]

The above example shows that for the 1568691840 data point at the time Vista
requested the data point from the Prometheus query_range API method, the result
returned was a slight bit different to what the Prometheus result was when the
same query_range and time period was issued few hours later.

Another example at 1568693160 shows these artefacts of averages can be expected.

.. code-block:: python

  # graphite time series
  # [[1568693040, 1.01],
  # [1568693100, 1.0],
  # [1568693160, 1.02],
  # [1568693220, 1.02],
  # [1568693280, 1.05]]

  # prometheus time series
  # [[1568693040, 1.01],
  # [1568693100, 1.0],
  # [1568693160, 1.05],
  # [1568693220, 1.02],
  # [1568693280, 1.05]]

Small variations such as these SHOULD be expected, the data sets will probably
never be identical, but they should be very similar.

Important note - Graphite aggregations
--------------------------------------

Be aware that Vista is intended to fetch only the latest data points from the
remote endpoint, at their highest resolution or relevant step.  In terms of the
metric data being aggregated on your Graphite instance, the fetched metrics
submitted to your Graphite will be aggregated as per your own Graphite
storage-schemas.conf.   This is important to understand, should you wish to use
the same retentions as the remote host, you will have to specify the same
retention configuration for the fetched metric namespaces as the remote host in
your Graphite configuration.  If you do not know and cannot calculate the remote
retentions/aggregations, over time at lower resolutions, you may find that your
Graphite data and graphs differ slightly from the remote source.

This also pertains the pre-population of metric data to Graphite, see the below
section on Pre-populating metrics with historic data.

How Vista works
---------------

A key thing to understand is that metrics fetched by Vista are near real time.
Vista will always be submitting data for the previous completed minute and never
the current minute, therefore Vista related metrics are always being analysed by
the other Skyline apps with some lag of a minute or two.  The reasons for this
are detailed below.

Vista has a fetcher process and a single or multiple worker processes. The
fetcher determines what metrics need to be fetched, fetches them and adds the
metric and time series response data as a json object to a queue for the
worker/s process.

The worker reads the json items off the queue and processes each.  The worker
ensures that the metric data points are valid by checking the last timestamp
that has been submitted to Graphite via flux.  This is done by checking the
`flux.last.<metric>` Redis key, which flux updates when data is submitted to
Graphite.  This provides de-duplication of data and ensures that data is only
submitted to Graphite once.

Vista does not submit any data points to flux that have a timestamp that falls
in the current minute period.  Vista will only submit data points to flux that
have timestamps that are less than that of the end of the last minute.  This
ensures that no partially populated 60 seconds periods are sent to Graphite and
pickled to Skyline, which could result in false positives and result is data
points in Graphite being overwritten.  So it is important to understand that
Skyline will always be analysing Vista based metrics for the previous minute,
not the current real time values.

When Vista submits the metric data to flux, the normal Graphite to Skyline
pickle will push the data to Skyline for analysis as per usual.

Vista can also pre-populate Graphite with metric data when new metrics are
added to Vista, see the below section on Pre-populating metrics with historic
data.

Graphite and Prometheus metrics
-------------------------------

Although Graphite and Prometheus are somewhat similar in nature, they have
subtle differences.  Seeing as Skyline is already very tightly integrated with
Graphite, in order to integrate the analysis of Prometheus metrics into Skyline,
it made sense in the context of Skyline to simply populate Graphite with
Prometheus metrics.

Although this may seem somewhat redundant as Skyline could use Prometheus as a
data source, there are a number of reasons why initially this method has been
implemented.

- Firstly, Skyline should not be run against #allthethings, it should be run
  against key metrics.  Although there are a number of ways for Prometheus to
  export metrics to Graphite, given that Skyline is focused on monitoring key
  metrics, Skyline is not aiming to fetch all Prometheus metrics to Graphite.
  If you want to write all your Prometheus metrics to Graphite, then Vista is
  not for you, rather use the Prometheus `remote_storage_adapter` and send the
  data from Prometheus directly to Graphite.
- Secondly, Skyline makes good use of historic data (up to 30 days is useful in
  most cases) and Prometheus is not aimed at long term storage of time series
  data.  Storing key Prometheus metrics in Graphite gives the user the
  opportunity to store a much larger data set which is useful for analysis at
  much greater durations and identifying trends over months and inflection
  points in the entire life time of the metric.
- Thirdly, Skyline does not handle labelled/tagged/function based metric
  namespaces per se, therefore labelled/tagged/function based metric
  namespaces are converted into and stored as Graphite absolute namespaces for
  Skyline to analyse.

Configuration
-------------

The Vista configuration variables are in `settings.py` under the Vista settings
block and they are all prefixed with `VISTA_`.  All the settings variables have
docstrings documenting their use and values, the main user setting is further
described below.

settings.VISTA_FETCH_METRICS
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The metrics you want Vista to fetch from remote sources are defined in the
:mod:`settings.VISTA_FETCH_METRICS` tuple.

The setting consists of a tuple of tuples, like the Analyzer
:mod:`settings.ALERTS` tuple.

.. code-block:: python

    VISTA_FETCH_METRICS = (
        # (remote_host, remote_host_type, frequency, remote_target, graphite_target, uri, namespace_prefix, api_key, token, user, password, (populate_at_resolution_1, populate_at_resolution_2, ...)),
        # Example with no authentication
        ('https://graphite.example.org', 'graphite', 60, 'stats.web01.cpu.user', 'stats.web01.cpu.user', '/render/?from=-10minutes&format=json&target=', 'vista.graphite_example_org', None, None, None, None, ('90days', '7days', '24hours', '6hours')),
        ('https://graphite.example.org', 'graphite', 60, 'sumSeries(stats.*.cpu.user)', 'stats.cumulative.cpu.user', '/render/?from=-10minutes&format=json&target=', 'vista.graphite_example_org', None, None, None, None, ('90days', '7days', '24hours', '6hours')),
        ('https://graphite.example.org', 'graphite', 3600, 'swell.tar.hm0', 'swell.tar.hm0', '/render/?from=-120minutes&format=json&target=', 'graphite_example_org', None, None, None, None, ('90days', '7days', '24hours', '6hours')),
        ('http://prometheus.example.org:9090', 'prometheus', 60, 'node_load1', 'node_load1', 'default', 'vista.prometheus_example_org', None, None, None, None, , ('15d')),
        ('http://prometheus.example.org:9090', 'prometheus', 60, 'node_network_transmit_bytes_total{device="eth0"}', 'node_network_transmit_bytes_total.eth0', 'default', 'vista.prometheus_example_org', None, None, None, None, , ('15d',)),
        ('http://prometheus.example.org:9090', 'prometheus', 60, 'prometheus_http_requests_total{code="200",handler="/api/v1/query_range",instance="prometheus.example.org:9090",job="prometheus"}', 'prometheus_http_requests_total.code.200.handler.api.v1.query_range', 'default', 'vista.prometheus_example_org', None, None, None, None, , ('15d',)),
    )

Each of the tuple parameters are documented directly in the docstrings
documentation (:mod:`settings.VISTA_FETCH_GRAPHITE_METRICS`), however some of
these settings are further described below in more detail to clarify how some of
them work.

.. note:: No authentication methods have been added to Vista yet although the
  they are defined in the fetch tuples.

remote_target
~~~~~~~~~~~~~

.. warning:: Only absolute name spaces must be declared in the tuple, wildcard
  metric name spaces are NOT be handled, unless the wildcard namespace is
  declared in a function that results in a single time series.

frequency
~~~~~~~~~

How often to fetch new data from the remote host, under normal circumstances
this would generally be 60 seconds, however it can be set to any frequency.  It
must be noted that the metric in the fetch tuple is bound by frequency.  As
shown in the example above, there are two fetch tuples for graphite.example.org
one that specifies metrics at 60 seconds and one for 3600 seconds (hourly).

uri
~~~

**Graphite**

The uri parameter is currently always applied to Graphite sources.  The time frame
you use in the uri should be short, this results in less bandwidth and less load
on the remote source.  Fetching the last 10 minutes of data from the remote
Graphite should be reasonable in most cases.  It is important to note that Vista
will automatically determine if there is missing data and adjust the from or
start parameter as appropriate if required.

**Prometheus**

For Prometheus metrics due to the `query_range` API method requiring a timestamp
for the start and end parameters, the uri 'default' is hard coded and in most
cases probably sufficient.  The default uri uses the `query_range` API method
and requests the last 5 minutes of data with a step of 60 seconds.  Both Vista
and the flux/populate_metric endpoint generate the Prometheus URIs dynamically,
with url encoding being applied to the `remote_target` and interpolating the
start and end parameters based on the current_unix_timestamp:
.. code-block::

  /api/v1/query_range?query=<URLENCODED_TARGET>&start=<(current_unix_timestamp-300)>&end=<current_unix_timestamp>&step=60s

Any `remote_target` metric name that is in a tagged or function format, must
have a `graphite_target` defined in the tuple.  As in the example above, metrics
that have a function applied to them can be retrieved as well, but they too need
to be converted to an absolute `graphite_target`.

Should you not wish to use the 'default' uri, you can experiment with passing a
uri for the metric with the appropriate url encoding of the `remote_target`,
should it require escaping, and url encoding any of the function related strings
for example:

.. code-block:: python

        ('http://prometheus.example.org:9090', 'prometheus', 60, 'node_network_receive_bytes_total{device="eth0"}', 'node_network_receive_bytes_total.eth0', '/api/v1/query?query=node_network_transmit_bytes_total%7Bdevice=%22eth0%22%7D%5B5m%5D', 'vista.prometheus_example_org', None, None, None, None, , ('15d',)),

The uri above is passed as:
::

  /api/v1/query?query=node_network_transmit_bytes_total%7Bdevice=%22eth0%22%7D%5B5m%5D

and declares the query API method and `[5m]` which is the last 5 minutes
of raw data and url encodes the necessary parts.  You would not use this, it is
just an example.

namespace_prefix
~~~~~~~~~~~~~~~~

If you do not want to pass a namespace_prefix for metrics, set this to:

::

  ''

Let us say you want to fetch some metrics from graphite.example.org and say the
metric name spaces are as follows:

::

    stats.etcd-1.cpu.user
    stats.etcd-2.cpu.user

Now let us say you have your own `stats` name space, in the above example
`VISTA_FETCH_METRICS` fetch tuple we have declared the `namespace_prefix` as
`'graphite_example_org'` so Vista would populate the following Graphite name
space on your Graphite:

::

    vista.graphite_example_org.stats.etcd-1.cpu.user
    vista.graphite_example_org.stats.etcd-2.cpu.user

Note the namespace_prefix in the fetch tuple must NOT have a trailing dot, e.g.
::

  'vista.graphite_example_org'

populate_at_resolutions
~~~~~~~~~~~~~~~~~~~~~~~

You may wish to pre-populate your Graphite with historic data for metrics so
that Skyline can immediately begin analysing the metric with historic data and
seasonality patterns.

In the :mod:`settings.VISTA_FETCH_METRICS` tuple the final element in the tuple
is a tuple referred to as `populate_at_resolutions`, in this tuple you can
define resolutions that you want to pre-populate your Graphite with.  If you do
not want to pre-populate Graphite then do not use a tuple simply set this to
`()`.

Let us take a look at how this setting looks and works, from the above example

.. code-block:: python

        # (remote_host, remote_host_type, frequency, remote_target, graphite_target, uri, namespace_prefix, api_key, token, user, password, (populate_at_resolution_1, populate_at_resolution_2, ...)),
        ('https://graphite.example.org', 'graphite', 60, 'stats.web01.cpu.user', 'stats.web01.cpu.user', '/render/?from=-10minutes&format=json&target=', 'vista.graphite_example_org', None, None, None, None, ('90days', '7days', '24hours', '6hours')),

Here the final tuple defining `populate_at_resolutions` is set to
`('90days', '7days', '24hours', '6hours')`

So in this instance when these metrics are first added to Vista, Vista will
submit a request to /flux/populate_metric for the metric to populate the metric
at the resolutions defined in the  `populate_at_resolutions` tuple.

It must be noted that the pre-populating of Prometheus metrics is done using a
resample of the raw data.  In all other instances, Vista uses the Prometheus
`query_range` API method with a `step=60s`, where Prometheus does the resampling.
Unfortunately this API method is limited to 11000 data points, on a period and
data set that has more than 11000 data points the query endpoint should be used
to get raw data - https://github.com/prometheus/prometheus/issues/2253#issuecomment-346288842

Therefore to pre-populate Vista needs to do the resampling using the 1Min mean
of the raw data, which should return what a Prometheus `query_range` would
return with `step=60`, an average per minute, but there could be some slight
differences.

Taking a default Prometheus example where there is only 15 days of data.

.. code-block:: python

  # (remote_host, remote_host_type, frequency, remote_target, graphite_target, uri, namespace_prefix, api_key, token, user, password, (populate_at_resolution_1, populate_at_resolution_2, ...)),
  ('http://prometheus.example.org:9090', 'prometheus', 60, 'node_load1', 'node_load1', 'default', 'vista.prometheus_example_org', None, None, None, None, , ('15d',)),

So in this instance when the Prometheus metrics are first added to Vista,
Vista would submit a request to `/flux/populate_metric` to populate the metric
at 15d using the query query and resampling based on the average
`/api/v1/query?query=<URLENCODED_TARGET>[15d]`

.. warning:: If you are only declaring a single resolution in the
  `populate_at_resolution` tuple, such as `('15d',)` you MUST have a trailing
  comma behind the resolution to define it as a tuple of tuples.  If you fail
  to add the trailing comma, '15d' will be interpreted as:

  1

  5

  d

  Not 15d.

Missing data
------------

Vista makes best effort to retrieve any missing metric data by referring to the
last timestamp for data reported by flux.  Take for example a case where the
Vista service has stopped or there was a network partition between your Skyline
server and the remote source, Vista will attempt to backfill any missing data
points for metrics it fetches, either itself, if the gap is less than
`(frequency + 300)` seconds or with /flux/populate_metric if the gap is longer.
Once again this may result in some small dissimilarities between the data sets,
which is better than air gaps.
