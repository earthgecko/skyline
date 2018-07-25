.. role:: skyblue
.. role:: red

Luminosity
==========

Requires - Panorama to be enabled and running.  It is enabled and configured
via the ``LUMINOSITY_`` variables in ``settings.py``.

Luminosity takes the time series of an anomalous metric and then cross
correlates the time series against all the other metrics time series to find and
record metrics that are strongly correlated to the anomalous metric, using
Linkedin's Luminol library.

This information can help in root cause analysis and gives the operator a view
of other metrics, perhaps on different servers or in different namespaces that
could be related to anomalous event.

It handles metrics having a time lagged effect on other metrics is handled with
time shifted cross correlations time shifted too.  So that if metric.a did
something 120 seconds ago and metric.b become anomalous 120 seconds later, if
there is correlation between the time shifted time series, these will be found
as well and recorded with time shifted value e.g. -120 and a
shifted_coefficient

Cross correlations are only currently viewable in Ionosphere on training_data
and features profile pages, however you can query MySQL directly to report on
any :mod:`settings.ALERTS` anomalies ad hoc until a full Luminosity webapp page
is created.

These are the correlations from the entire metric population that fall within
:mod:`settings.LUMINOL_CROSS_CORRELATION_THRESHOLD`, they are not necessarily
all contextually related, but the contextually related correlations are listed
for the anomaly.  These are simply the numeric correlations it is currently
up to the operator to review them.  As it is assumed that the operator may know
which metrics are likely to correlate with this anomaly on the specific metric,
until such a time as Skyline can, something on the roadmap.

At the current at this stage of development Luminosity adds lots of noise along
with the signals in the correlations, somewhat similar to the original Analyzer.
This is just the start, to be able to try and make it better and useful, the
data is needed first.

Luminosity as a replacement for Kale Oculus
-------------------------------------------

Luminosity is the beginning of an attempt to implement a pure Python replacement
for the Oculus anomaly correlation component of the Etsy Kale stack.

Oculus was not moved forward with Skyline in this version of Skyline for a
number of reasons, those mainly being Ruby, Java and Elasticsearch.
Luminosity is not about being better than Oculus, Oculus was very good, it is
about adding additional information about anomalies and metrics.

Luminosity uses some of the functionality of Linkedin's luminol Anomaly
Detection and Correlation library to add this near real time correlation back
into Skyline.

Luminosity is different to Oculus in a number of ways, however it does provide
similar information.

Differences between Oculus and Luminosity
-----------------------------------------

Oculus had a very large architectural footprint.
Luminosity runs in parallel to all the other Skyline apps on the same Skyline
commodity machine, with very little overhead.

Oculus used a very novel technique to do similarity search in time series using
a Shape Description Alphabet:
1. Map line segments to tokens based on gradient. a b c d e
2. Index tokens with Elasticsearch.
3. Search for similar subsequences using sloppy phrase queries.
Then search these candidates exhaustively, using Fast Dynamic Time Warping.
Oculus had a searchable frontend UI.

Luminosity uses the Linkedin luminol.correlator to determine the cross
correlation coefficients of each metric pulling the data directly from Redis
only when an anomaly alert is triggered.  The resulting cross correlated metrics
and their results are then recorded in the Skyline MySQL database and related to
the anomaly.  Viewable via the Ionosphere and Luminosity UI pages.

Luminosity adds
---------------

Luminosity has the advantage over Oculus of storing the history of metrics,
anomalies and their correlations.  This gives Skyline a near real time root
cause analysis component and over time enables Skyline to determine and report
the most strongly correlated metrics.

luminol.correlate is based on http://paulbourke.net/miscellaneous/correlate/
and for the purposes of understanding the luminol.correlate, a pure Python
implementation of the Paul Bourke method was implemented and verified with the
results of the luminol.correlate.

Running Luminosity on multiple, distributed Skyline instances
-------------------------------------------------------------

Please see `Running multiple Skyline instances <running-multiple-skylines.html>`__
