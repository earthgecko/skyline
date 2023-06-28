.. role:: skyblue
.. role:: red

Ionosphere learning from repetitive patterns
============================================

By default learning from repetitive patterns is not enabled because you should
define the metrics to exclude from this learning **before** enabling it.

Exclude and include
-------------------

You need to decide what metrics you want to exclude from learning from
repetitive patterns.  You do not want Skyline learning from repetitive patterns
on ``bad`` metrics, like metrics that are related to errors, 50x status codes,
access_denied, etc.  These types of metrics can be excluded from the learning by
defining them in the :mod:`settings.IONOSPHERE_REPETITIVE_PATTERNS_EXCLUDE`.

For convenience sake there is a :mod:`settings.IONOSPHERE_REPETITIVE_PATTERNS_INCLUDE`
setting which allows you to define only certain metrics to be learnt from
repetitive patterns. If it is defined it is evaluated before the EXCLUDE to
filter only the metrics that match definitions.  The EXCLUDE (if defined) is
then applied to the INCLUDE metrics list. This setting is to allow for testing
repetitive learning with limited set of metrics before implementing it on the
entire metric population.

For a detailed description of the data structures both of these dictionaries see
the annotated example under :mod:`settings.IONOSPHERE_REPETITIVE_PATTERNS_EXCLUDE`
in settings.py

Methods
-------

Two methods of learning from repetitive patterns can be implemented in
Ionosphere.  Each method has slight differences and running both achieves the
best results.

Often metrics exhibit repetitive patterns where there are large fluctuations
periodically.  A number of these are caused by normal operations of a process,
for example something like ``prometheus_tsdb_compactions_total`` or VictoriaMetrics
``process_io_write_syscalls_total`` are processes that occur periodically and
tend to generate spikes of similar and varying magnitude.  There are many other
types of metrics that fit into this category, things like backups which
generally cause spikes in disk I/O, network bandwidth, etc, and other things
like cron jobs, log rotation, etc, that run on a scheduled basis can result in
these types of patterns in metrics.

The learn_repetitive_patterns processes evaluates current and previous anomalies
on metrics and calculate the features profile for each of these, then
comparisons are made between all the calculated features profiles and if 3 are
found to be similar then these patterns are learnt as being normal behaviour.

The two methods, namely ``learn_repetitive_patterns`` and
``find_repetitive_patterns`` are described here, both are similar but each
searches for a different kind of pattern.

learn_repetitive_patterns
-------------------------

This method learns daily periodic patterns.  In new metrics these patterns can
be found and learnt after 10 days.  Thereafter if the pattern changes the method
can learn the new daily periodic pattern after 7 days.

This method is run periodically against metrics that have **training data**.  It
is used to find metrics that exhibit daily patterns in terms of anomalies which
occur during similar periods, e.g. between 00h00 and 00h15 or 03h15 and 03h30.
Processes such as cron jobs, compaction, log rotation, storage bucket rolling,
etc, all tend to cause this type of behaviour in metrics.  The training data
for each metric is evaluated and any training data that is discovered with
anomalies aligned in similar periods/windows are evaluated.  The evaluation
compares the features profile sums of each training data set and if 3 training
data sets are found to be significantly similar, these are classified as
normal and they are automatically trained on.

find_repetitive_patterns
------------------------

This method can learn patterns in metrics after 7 days.

This method is run periodically against all anomalies.  Unlike
``learn_repetitive_patterns`` this method does not use training data or
period alignment, it uses anomalies from the metric in the last 30 days.  This
results in finding patterns of normal behaviour that are not necessarily
periodically aligned but that can occur sporadically but are normal over a
longer period and happen frequently enough to be considered as normal.

After the normal Ionosphere learn window has passed, the
``find_repetitive_patterns`` process determines all anomalies which have not
been trained on since the last evaluation (the first run considers only the
previous 24 hours).

- The process surfaces every anomaly that occurred on the metric in the past 30
  days.
- The time series data for each of these anomalies is then fetched and the
  features profile sum is calculated for each anomalous time series.
- These feature profile sums are then compared to the features profile sums of
  all the other anomalies, which do not fall in same weekly period as the
  feature profile sum being evaluated.  This means that only anomalies that are
  in different weeks are evaluated against each other and anomalies in the same
  weekly window are not considered.  Ensuring that the patterns found occur
  frequently and are not just a representation of recent behaviour.
- A confusion matrix of similarity is generated and if any 3 or more are found
  to be similar they are classified as normal and trained upon.

An implementation factor to be aware of is that due to the fact that normal
MinMax scaling checks and comparisons that occur in the normal Ionosphere
process based on similar ranges is not implemented here.  MinMax scaling is
implemented via threshold on the average value of the time series defined in
:mod:`settings.IONOSPHERE_REPETITIVE_PATTERNS_MINMAX_AVG_VALUE`.  Due to
the fact that patterns and not absolutes are being looked for here and that
the features profiles being compared are separated by at least 7 days, the
features profile sums should be sufficient to differentiate dissimilar patterns.
There is a very small possibility that the MinMax scaling will result in
false positive matches, however the metric would have to change proportionally
in average, peak and trough values for this to occur which is probably unlikely
and even if it did, unless those changes were **significant** in magnitude, it
is desirable that they do match as similar.

:mod:`settings.IONOSPHERE_REPETITIVE_PATTERNS_MINMAX_AVG_VALUE` can be set
to 0 to disable MinMax scaling comparisons.
