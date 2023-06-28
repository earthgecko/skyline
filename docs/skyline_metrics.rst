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

Skyline metrics are sent to Graphite and can be viewed in the Graphite Browser.

Metric definitions
------------------

Here each :skyblue:`Sky`:red:`line` metric is described.  Metrics are only sent
if you run a Skyline app or specific feature, if you are not running flux do not
expect to get flux metrics.  **This is list not complete it is a work in
progress**.  There are some ADVANCED_FEATURE metrics that are not listed or
described here.  If a metric below does not have a description it is an
informational or development metrics and its namespace probably says what it
does.

A note on ``skyline.analyzer.$host.labelled_metrics.`` metrics and
``skyline.analyzer.$host.`` metrics.  Although ``labelled_metrics`` are within
the ``skyline.analyzer.$host.`` namespace, ``skyline.analyzer.$host.`` metrics
and ``skyline.analyzer.$host.labelled_metrics.`` metrics are distinct groups.
``skyline.analyzer.$host.`` metrics being related to the analysis of
traditional Graphite metrics and ``skyline.analyzer.$host.labelled_metrics.``
metrics being related to the analysis of Prometheus/VictoriaMetrics style
labelled/tagged metrics.

- ``skyline.$skyline_app.$host.logged_errors``: **IMPORTANT** every Skyline app
  records the number of errors that have been logged.  Skyline handle error
  logging very specifically because when analysis is being on 1000s and 1000s of
  metrics in real time constantly, any issue can cause a thundering herd of
  errors.  During the normal operations of Skyline some errors are always
  expected.  This is especially true when there are some socket.timeouts while
  sending metrics to Graphite, ingesting metrics from remote clients where the
  client terminates the connection, etc, etc.  Very brief network partitions are
  expected.  There are occasional timeout on the server connecting to the Redis
  socket or local MariaDB, these things happen and get logged, they are not
  necessarily abnormal.  All ``skyline.logged_errors`` namespaces are analysed
  and not skipped via their inclusion in :mod:`settings.DO_NOT_SKIP_LIST`
- ``skyline.analyzer.$host.algorithm_breakdown.$algorithm.timing.{median_time,times_run,total_time}``: these
  are performance metrics for each of Analyzer's algorithms.  These are mostly
  for information and detecting performance changes if any algorithm is modified.
  Mostly for development but are data that can be used in the future to infer
  the overall state of the entire metric population in terms of how much flux is
  being experienced.  By default not analysed and excluded via
  :mod:`settings.SKIP_LIST` as they are feedback metrics.
- ``skyline.analyzer.$host.anomaly_breakdown.$algorithm``: How many times each
  algorithm triggerred.  Same category as the above metrics.
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
  Skyline for a metric/s.  These metrics are excluded from analysis by default
  via :mod:`settings.SKIP_LIST`, because Skyline analyzer/metrics_manager and
  thunder will report on stale metrics.
- ``skyline.analyzer.$host.labelled_metrics.exceptions.$exception``: **IMPORTANT** the
  same as the analyzer exceptions metrics above, but for the analyzer/labelled_metrics
  process.
- ``skyline.analyzer.$host.labelled_metrics.3sigma_analysed``: the number of
  labelled metrics that were analysed with 3sigma on 24 hours of data in a
  skyline/analyzer_labelled_metrics analysis run.
- ``skyline.analyzer.$host.labelled_metrics.3sigma_timings``:
- ``skyline.analyzer.$host.labelled_metrics.analysed``:
- ``skyline.analyzer.$host.labelled_metrics.anomalous``: **IMPORTANT** the
  number of 1st stage analysis anomalies were detected on all the Prometheus,
  influxdb, VictoriaMetrics, skyline/flux based labelled metrics.  Although
  1st stage analysis anomalies are not important, if there is a surge in the
  number of 1st stage anomalies it is indicative of a change in the overall
  flux of a number of metrics in the population.  This is analysed and
  not skipped via inclusion in :mod:`settings.DO_NOT_SKIP_LIST`
- ``skyline.analyzer.$host.labelled_metrics.checked``: **IMPORTANT** the
  number of labelled_metrics checked.  This number is all metrics that were
  assigned to be checked, it includes skipped. It does not include analyzer
  metrics, ``skyline.analyzer.$host.labelled_metrics.*`` are seperate and
  distinct from ``skyline.analyzer.$host.`` metrics. When this drops there is possibly
  an issue with the remote_write (et al) source/s and/or metric ingestion with
  skyline/flux/prometheus/write and/or horizon/prometheus writing the metric
  data. The number will increase and decrease when you add and remove
  machines/metrics.  Analysed and not skipped via inclusion in
  :mod:`settings.DO_NOT_SKIP_LIST`
- ``skyline.analyzer.$host.labelled_metrics.downsample_timings``:
- ``skyline.analyzer.$host.labelled_metrics.downsampled``:
- ``skyline.analyzer.$host.labelled_metrics.mad_analysed``:
- ``skyline.analyzer.$host.labelled_metrics.mad_anomalous``: the number of
  labelled metrics that triggered MAD (Median Absolute Deviation) with 3 hours
  of data and were pushed on to having 24 hours of data analysed with 3sigma in
  a skyline/analyzer_labelled_metrics analysis run.
- ``skyline.analyzer.$host.labelled_metrics.mad_timings``:
- ``skyline.analyzer.$host.labelled_metrics.mem_usage``:
- ``skyline.analyzer.$host.labelled_metrics.monotonicity_changed``:
- ``skyline.analyzer.$host.labelled_metrics.monotonicity_checked``:
- ``skyline.analyzer.$host.labelled_metrics.not_anomalous``:
- ``skyline.analyzer.$host.labelled_metrics.not_stationary``:
- ``skyline.analyzer.$host.labelled_metrics.run_time``: **VERY IMPORTANT** reports
  how many seconds skyline/analyzer_labelled_metrics took to analyse all the
  labelled_metrics. This should be under 60 seconds.  If it is higher,
  skyline/analyzer_labelled_metrics is overloaded and not getting through the
  analysis quick enough.  Consider increasing :mod:`settings.ANALYZER_PROCESSES`
  or using a cluster. Analysed and not skipped via inclusion in
  :mod:`settings.DO_NOT_SKIP_LIST`
- ``skyline.analyzer.$host.labelled_metrics.redis_full_duration_timings``: the
  number of seconds it took to get :mod:`settings.FULL_DURATION` seconds of data
  for MAD triggered metrics from RedisTimeseries for 3sigma analysis in a
  skyline/analyzer_labelled_metrics analysis run.
- ``skyline.analyzer.$host.labelled_metrics.redis_timings``: the number of
  seconds it took to get 3 hours of data for ALL metrics from RedisTimeseries
- ``skyline.analyzer.$host.labelled_metrics.skipped``: the number of metrics
  skipped due to TooShort, Boring, etc in a skyline/analyzer_labelled_metrics
  analysis run.
- ``skyline.analyzer.$host.labelled_metrics.stationary``:
- ``skyline.analyzer.$host.labelled_metrics.stationary_analysed``:
- ``skyline.analyzer.$host.labelled_metrics.stationary_not_expired``:
- ``skyline.analyzer.$host.labelled_metrics.stationary_timing``:
- ``skyline.analyzer.$host.low_priority_metrics.{dynamically_analyzed,total}``: this
  is an ADVANCE FEATURE set of metrics that just keep track of how many low
  priority metrics get analysed and how many there are total.
- ``skyline.analyzer.$host.metrics_sparsity.metrics_fully_populated``: the
  number of metrics in the population that are fully populated.
- ``skyline.analyzer.$host.metrics_sparsity.metrics_sparsity_decreasing``: the
  number of metrics that are moving towards a fully populated state.
- ``skyline.analyzer.$host.metrics_sparsity.metrics_sparsity_increasing``: the
  number of metrics that are moving away from a fully populated state.
- ``skyline.analyzer.$host.metrics_sparsity.avg_sparsity``: **IMPORTANT** this
  is number describes how well populated the metric timeseries population is.
  100 is perfect, going below about 93% on high resolution metrics indicates a
  problem, either the metrics are missing data, some have gone offline or
  network partitions are being encountered.
- ``skyline.analyzer.$host.duration``: The duration (in hours) of the
  :mod:`settings.CANARY_METRIC` timeseries data.
- ``skyline.analyzer.$host.ionosphere_metrics``: the number of metrics in the
  population that have been trained.
- ``skyline.analyzer.$host.metrics_manager_run_time``: **IMPORTANT** the number
  of seconds it takes for analyzer/metrics_manager to run.  analyzer/metrics_manager
  does a **lot** of work creating and managing internal data sets related to the
  metrics, settings, activation and removal of things which can change quite
  dramatically depending on the volumes of metrics.
- ``skyline.analyzer.$host.projected``: This is the projected number of seconds
  that Analyzer should take to run.  It is the projected value of
  ``skyline.analyzer.$host.run_time`` and these 2 metrics should be very similar.
  If the run_time is much greater than than the projected there is something
  amiss.
- ``skyline.analyzer.$host.run_time``: **VERY IMPORTANT** reports how many
  seconds Analyzer took to analyse all the traditional Graphite based metrics.
  This should be under 60 seconds.  If it is higher, analyzer is overloaded and
  not getting through the analysis quick enough.  Consider increasing
  :mod:`settings.ANALYZER_PROCESSES`
- ``skyline.analyzer.$host.sent_to_ionosphere``: a count of how many anomalous
  metrics were sent to Ionosphere by Analyzer during an analysis run.  These
  metrics are metrics that are not a mirage metrics, they do have previous
  training and they were anomalous in the 1st stage analysis and seeing as they
  are not Mirage metrics, it is Analyzer's responsibility to check Ionosphere to
  see if they match any pattern of trained behaviour.
- ``skyline.analyzer.$host.sent_to_mirage``: a count of how many anomalous
  metrics were sent to Mirage by Analyzer analysis run for 2nd stage analysis.
- ``skyline.analyzer.$host.sent_to_panorama``: a count of how many anomalous
  metrics were sent to Panorama by Analyzer during an analysis run.  These
  metrics are metrics that are not a mirage metrics (or they are waterfall
  alerted on), Ionosphere did not match any of the patterns in the trained
  behaviour (if previously trained) and Analyzer instructs Panorama to record an
  anomaly for the metric.  This only applies to metrics that are not mirage
  enabled by having no SECOND_ORDER_RESOLUTION_HOURS declared in an alert tuple
  they match in :mod:`settings.ALERTS`.
- ``skyline.analyzer.$host.total_analyzed``: **IMPORTANT** the number of metrics
  analysed.  When this drops there is possibly a problem.  The number will
  increase and decrease when you add and remove machines/metrics. Analysed
  and not skipped via inclusion in :mod:`settings.DO_NOT_SKIP_LIST`
- ``skyline.analyzer.$host.total_anomalies``:  **IMPORTANT** the number of
  anomalies that were detected on all the traditional Graphite based metrics.
  Analysed and not skipped via inclusion in :mod:`settings.DO_NOT_SKIP_LIST`
- ``skyline.analyzer.$host.total_metrics``: the total number of traditional
  Graphite based metrics.
- ``skyline.boundary.$host.*``: these metrics are very similar to the analyzer
  namespace counterparts described above.
- ``skyline.flux.$host.listen.discarded.invalid_value``: the number of metrics
  flux discarded as they did not have a float value in a 60 second period.
- ``skyline.flux.$host.listen.dropped_non_numeric_metrics``: the number of
  metrics flux discarded as they did not have a float value in a 60 second
  period.
- ``skyline.flux.$host.listen.over_quota_metrics_count``: ADVANCED_FEATURE
  **VERY IMPORTANT**
- ``skyline.flux.$host.listen.discarded.invalid_timestamp``: the number of
  metrics flux discarded in a 60 second period because the timestamp was too
  old (e.g > :mod:`settings.FLUX_MAX_AGE`).
- ``skyline.flux.$host.worker.discarded.already_received``: the number of
  metrics flux discarded in a 60 second period because data for the timestamp
  has already been received, deduplication.
- ``skyline.flux.$host.listen.added_to_queue``: the number of metrics that
  flux/listen add in a 60 second period.
- ``skyline.flux.$host.listen.discarded.invalid_key``: the number of
  metrics flux discarded in a 60 second period because submitted api key was not
  valid.
- ``skyline.flux.$host.worker.metrics_sent_to_graphite``: the number of
  traditional Graphite dotted namespaced metrics flux/worker submitted to in a
  60 second period.
- ``skyline.flux.$host.aggregator.queue.size``:
- ``skyline.flux.$host.listen.discarded.invalid_parameters``: the number of
  metrics flux discarded in a 60 second period because an incorrect or missing
  parameter/s were submitted.
- ``skyline.flux.$host.listen.discarded.metric_name``:
- ``skyline.flux.$host.listen.added_to_aggregation_queue``:
- ``skyline.flux.$host.worker.httpMetricDataQueue.size``:
- ``skyline.horizon.$host.prometheus.flux_received``: **IMPORTANT** this is the
  number of metrics flux/prometheus/write has received in the last 60 seconds.
  Given potential for high cardinality this metric is important to indicate if
  lots of new metrics come onboard.
- ``skyline.horizon.$host.worker.queue_size_60s_avg``: **IMPORTANT** this is the
  average size of the queue in the last 60 seconds for traditional Graphite
  dotted namespaced metric ingestion. If this value gets to high you may need to
  add another Horizon worker.
- ``skyline.horizon.$host.worker.datapoints_sent_to_redis``: **IMPORTANT** the
  number of data points that Horizon is receiving from Graphite and pushing into
  Redis.  If this number drops Skyline is not getting the expected amount of
  data from Graphite.
- ``skyline.horizon.$host.worker.datapoints_sent_to_redis_<id>``: **IMPORTANT** same
  as the above metric when Horizon runs multiple workers.
- ``skyline.horizon.$host.worker.queue_size``: this is average size of the queue
  in the last 10 seconds.
- ``skyline.horizon.$host.worker.metrics_received``: **IMPORTANT** this is the
  number of traditional Graphite dotted namespaced metrics that flux/worker has
  received in the last 60 seconds. Given potential for high cardinality this
  metric is important to indicate if lots of new metrics come onboard.
- ``skyline.ionosphere.$host.features_calculation_time``:
- ``skyline.ionosphere.$host.fps_checked``:
- ``skyline.ionosphere.$host.layers_checked``:
- ``skyline.ionosphere.$host.not_anomalous``: **IMPORTANT** the number of
  potentially anomalous metrics that matched a trained pattern of behaviour as
  not_anomalous in a 60 period.  Important because it is an indicator of how
  effective the training and pattern matching is.
- ``skyline.ionosphere.$host.sent_to_panorama``: **IMPORTANT** the number of
  anomalous metrics that did not match a trained pattern of behaviour in a 60
  second period.
- ``skyline.ionosphere.$host.total_anomalies``: The same as sent_to_panorama
  above.
- ``skyline.ionosphere.$host.training_metrics``:
- ``skyline.luminosity.$host.metrics_checked_for_correlation``:
- ``skyline.luminosity.$host.classify_metrics.classified``:
- ``skyline.luminosity.$host.correlations``:
- ``skyline.luminosity.$host.classify_metrics.proceessed``:
- ``skyline.luminosity.$host.avg_runtime``:
- ``skyline.mirage.$host.checks.done``:
- ``skyline.mirage.$host.checks.stale_discarded``: **IMPORTANT** the number of
  mirage checks that are discarded because they too old.  If this is too high
  2nd stage analysis is not be being completed in a timely, real time manner.
  Consider increasing :mod:`settings.MIRAGE_PROCESSES`.  This is analysed and
  not skipped via inclusion in :mod:`settings.DO_NOT_SKIP_LIST`
- ``skyline.mirage.$host.checks.pending``: **IMPORTANT** the number of checks
  pending to be done by Mirage second stage analysis.  If this is too high
  analysis will not be being completed in a timely, real time manner.  Consider
  increasing :mod:`settings.MIRAGE_PROCESSES`. This is analysed and not skipped
  via inclusion in :mod:`settings.DO_NOT_SKIP_LIST`
- ``skyline.mirage.$host.labelled_metrics.checks.done``:
- ``skyline.mirage.$host.labelled_metrics.checks.pending``:
- ``skyline.mirage.$host.labelled_metrics.checks.stale_discarded``: This is
  analysed and not skipped via inclusion in :mod:`settings.DO_NOT_SKIP_LIST`
- ``skyline.mirage.$host.labelled_metrics.run_time``:
- ``skyline.mirage.$host.labelled_metrics.sent_to_ionosphere``:
- ``skyline.mirage.$host.labelled_metrics.sent_to_panorama``:
- ``skyline.mirage.$host.run_time``:**IMPORTANT** If this is too high analysis
  will not be being completed in a timely, real time manner.  Consider
  increasing :mod:`settings.MIRAGE_PROCESSES`. This is analysed and not skipped
  via inclusion in :mod:`settings.DO_NOT_SKIP_LIST`
- ``skyline.mirage.$host.sent_to_ionosphere``:
- ``skyline.mirage.$host.sent_to_panorama``:
- ``skyline.mirage.$host.vortex.checks.done``:
- ``skyline.mirage.$host.vortex.run_time``:
- ``skyline.snab.$host.checks.analyzer.testing``
- ``skyline.snab.$host.checks.analyzer.anomalies``
- ``skyline.snab.$host.checks.analyzer.realtime``
- ``skyline.snab.$host.checks.mirage.testing``
- ``skyline.snab.$host.checks.mirage.realtime``
- ``skyline.snab.$host.checks.mirage.anomalies``
- ``skyline.snab.$host.checks.mirage.falied``
- ``skyline.snab.$host.checks.analyzer.falied``
- ``skyline.snab.$host.checks.processed``
- ``skyline.vista.$host.fetcher.metrics_fetched``:
- ``skyline.vista.$host.worker.metrics_sent_to_flux``:
- ``skyline.vista.$host.fetcher.time_to_fetch``:
- ``skyline.vista.$host.fetcher.metrics_to_fetch``:
- ``skyline.vista.$host.fetcher.sent_to_flux``:
- ``skyline.vista.$host.worker.vista.fetcher.metrics.json``:
