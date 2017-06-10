.. role:: skyblue
.. role:: red

Strictly increasing monotonicity
================================

Skyline specifically handles **positive**, strictly increasing monotonic
timeseries.  These are metrics that are integral (an incrementing count) in
Graphite and converts the values to their derivative products.

In terms of visualisation Graphite has the nonNegativeDerivative function,
which converts an integral or incrementing count to a derivative by
calculating the delta between subsequent datapoints.  The function ignores
datapoints that trend down.  With Skyline's new functionality this is a
requirement for metrics that only increase over time and then at some point
reset to 0 (e.g. interface packets counts, varnish or haproxy backend
connections, etc).

By default, Skyline now identifies and converts monotonically increasing
timeseries to their derivative products through most of the Skyline
analysis apps (Analyzer, Mirage and Ionosphere).

Unless you tell Skyline not to.  You may have some monotonically increasing
metric that you do not want to be forced to convert to its derivative product
just because it is more convenient for Skyline to analyse, be taught and learn
the derivatives.  You may have a metric that does not reset and want analysed
the way it used to be analysed in the old days as you feel it worked, that is OK,
you can, using :mod:`settings.NON_DERIVATIVE_MONOTONIC_METRICS`

However, Skyline will automatically classify metrics into derivative_metrics
(strictly positive monotonic metrics) and non_derivative_metrics.  This process
has been automated so that new metrics of this nature will be identified and
processed via their derivative if a new namespace of this nature should to added
without the requirement for each namespace pattern to be declared in the settings.

Really why is this important?
-----------------------------

If you are still reading...

With the original Analyzer/Mirage, single time resolution and 3-sigma based
algorithms, these types of metrics with strictly increasing monotonicity
that reset always triggered 3-sigma and alerts (false positives).  That is
not to say that they were not effective once the metric had
:mod:`settings.FULL_DURATION` seconds of timeseries data, while it was
sending the incrementing data 3-sigma would work, until it reset :)  So it
worked most of time and falsely some of the time.  Not ideal.

Widening the range of the time resolutions that Skyline operates in with
Mirage and Ionosphere requires that this class of metrics be converted
otherwise they are limited in terms of the the amount of seasonal analysis,
profiling and learning that can be done on this type of timeseries data.

So Skyline now analyses all monotonic metrics via their derivatives as
these work in terms of more historical analysis, features calculations and
comparison, and learning patterns due to the step nature of the data and
the variability of the steps, in terms of profiling features these type of
data are almost like snowflakes.  The derivatives of these strictly
increasing metrics are the kind of patterns that lend themselves to
Skyline's timeseries comparison and analysis methods.

The Skyline nonNegativeDerivative is based on part of the Graphite render
function nonNegativeDerivative at:
https://github.com/graphite-project/graphite-web/blob/1e5cf9f659f5d4cc0fa53127f756a1916e62eb47/webapp/graphite/render/functions.py#L1627
