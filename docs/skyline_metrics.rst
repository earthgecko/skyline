.. role:: skyblue
.. role:: red

:skyblue:`Sky`:red:`line` metrics
==================================

Like all applications, :skyblue:`Sky`:red:`line` makes its own metrics.
A lot of cloud native things expose a LOT of internal metrics and generally
modern applications tend to make a lot of metrics, verbosely describing the
application.  Although there is nothing wrong with this, it does result in
metric overkill a lot of the time for people running the applications.  Often
there is little documentation on what all the metrics are and more importantly
information on what metrics are important.  We end up with 1000s of metrics
being sent and scraped from applications and most of them we have very little
understanding of.  What are the important metrics of application?  Although
a developer of an application may be interested in a metric like
``application.go_gc_duration_seconds.instance.123%2E232%2E12%2E13:1234.job.application.quantile.0%2E25``
the user of the application probably is not going to be interested in that at
all.

:skyblue:`Sky`:red:`line` has always attempted to not make too many metrics.

Metric definitions
------------------

Here each :skyblue:`Sky`:red:`line` metric is described.  Metrics are only sent
if you run a Skyline app, if you are not running flux do not expect to get flux
metrics.  **This is list not complete it is a work in progress**

- ``skyline.analyzer.$host.exceptions.Boring``: the number of metrics with timeseries
  that have the same value for :mod:`settings.MAX_TOLERABLE_BOREDOM` (default 100)
  in a row.
- ``skyline.analyzer.$host.exceptions.Other``: unknown exceptions
- ``skyline.analyzer.$host.exceptions.TooShort``: the number of metrics that have
  timeseries that are too short to be analysed (:mod:`settings.MIN_TOLERABLE_LENGTH`)
- ``skyline.analyzer.$host.exceptions.EmptyTimeseries``: the number of metrics
  that have empty timeseries.
- ``skyline.analyzer.$host.exceptions.Stale``: **IMPORTANT** the number of
  metrics that have not sent new data in :mod:`settings.STALE_PERIOD`.  If this
  number spikes up it means that there is a problem with the data being fed to
  Skyline for a metric/s.
- ``skyline.analyzer.$host.total_analyzed``: **IMPORTANT** the number of metrics
  analysed.  When this drops there is possibly a problem.  The number will
  increase and decrease when you add and remove machines/metrics.
- ``skyline.analyzer.$host.metrics_sparsity.metrics_fully_populated"
- ``skyline.analyzer.$host.metrics_sparsity.metrics_sparsity_decreasing``
- ``skyline.analyzer.$host.metrics_sparsity.metrics_sparsity_increasing``
- ``skyline.analyzer.$host.metrics_sparsity.avg_sparsity``: **IMPORTANT** this
  is number describes how well populated the metric timeseries population is.
  100 is perfect, going below 93% about on high resolution metrics indicates a
  problem, either the metrics are missing data or some have gone offline.
- ``skyline.horizon.$host.worker.queue_size``: this is average size of the queue
  in the last 10 seconds.
- ``skyline.horizon.$host.worker.queue_size_60s_avg``: **IMPORTANT** this is the
  average size of the queue in the last 60 seconds. If this value gets to high
  you may need to add another Horizon worker.
- ``skyline.horizon.$host.worker.datapoints_sent_to_redis``: **IMPORTANT** the
  number of data points that Horizon is receiving from Graphite and pushing into
  Redis.  If this number drops Skyline is not getting the expected amount of
  data from Graphite.
- ``skyline.horizon.$host.worker.datapoints_sent_to_redis_<worker_id>``: as
  above when Horizon runs multiple workers.
- ``skyline.analyzer.$host.total_anomalies``
- ``skyline.snab.$host.checks.analyzer.testing``
- ``skyline.snab.$host.checks.analyzer.anomalies``
- ``skyline.analyzer.$host.ionosphere_metrics``
- ``skyline.mirage.$host.checks.done``
- ``skyline.snab.$host.checks.analyzer.realtime``
- ``skyline.snab.$host.checks.mirage.testing``
- ``skyline.mirage.$host.checks.pending``: **IMPORTANT** the number of checks
  pending on be be doone by Mirage second stage analysis.  If this is too high
  analysis will not be being completed in a timely, real time manner.  Consider
  increasing :mod:`settings.MIRAGE_PROCESSES`.
- ``skyline.vista.$host.worker.vista.fetcher.metrics.json``
- ``skyline.analyzer.$host.run_time``: : **VERY IMPORTANT** this should be under
  60 seconds.  If it is higher, analyzer is overloaded and not getting through
  the analysis quick enough.  Consider increasing :mod:`settings.ANALYZER_PROCESSES`
- ``skyline.luminosity.$host.avg_runtime``
- ``skyline.analyzer.$host.mirage_metrics``
- ``skyline.luminosity.$host.metrics_checked_for_correlation``
- ``skyline.snab.$host.checks.mirage.realtime``
- ``skyline.luminosity.$host.classify_metrics.classified``
- ``skyline.snab.$host.checks.mirage.anomalies``
- ``skyline.luminosity.$host.correlations``
- ``skyline.mirage.$host.checks.stale_discarded``
- ``skyline.vista.$host.fetcher.time_to_fetch``
- ``skyline.flux.$host.worker.metrics_sent_to_graphite``
- ``skyline.luminosity.$host.classify_metrics.proceessed``
- ``skyline.flux.$host.worker.httpMetricDataQueue.size``
- ``skyline.snab.$host.checks.mirage.falied``
- ``skyline.snab.$host.checks.analyzer.falied``
- ``skyline.snab.$host.checks.processed``
- ``skyline.vista.$host.worker.metrics_sent_to_flux``
- ``skyline.vista.$host.fetcher.metrics_fetched``
- ``skyline.vista.$host.fetcher.metrics_to_fetch``
- ``skyline.vista.$host.fetcher.sent_to_flux``
