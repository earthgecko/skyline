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
``find_repetitive_patterns`` are described here, both are somewhat similar but
each searches for a different kind of pattern and ``find_repetitive_patterns``
implements an additional method.

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

motif_annihilation
------------------

When ``find_repetitive_patterns`` is enabled it also makes use of an additional
method of learning normal based on the annihilation of commonly occurring
motifs.

The motif_annihilation method is inspired by the concept of data smashing.

While reviewing the initial performance of the original
``find_repetitive_patterns`` method it showed that the performance is not bad
given zero knowledge discovery.  This lead back to October 2021 when reviewing
the now removed timesmash (https://github.com/zeroknowledgediscovery/timesmash
and pypi timesmash-0.2.23) which was published from
http://zed.uchicago.edu/data_smashing.html

.. 
  Universal similarity amongst arbitrary data streams without a priori knowledge, features, or training.
  Can be used to solve time series clustering and classification problems.

In reviewing the idea of data smashing it was clear that perhaps motifs could
be used to do data smashing.

- Take the anomalous_timeseries in its 7 day context.
- Surface -35d of data, the previous 5 weeks (pw5_timeseries) before the anomaly
  e.g. ``from=(metric_timestamp - ((86400 * 7) * 5)), until=(metric_timestamp + 3600)``
- MinMax scale the pw5_timeseries
- Extract the pw4_timeseries from the MinMax scaled pw5_timeseries, e.g.
  ``pw4_timeseries = [item for item in pw5_timeseries if item[0] >= (metric_timestamp - ((86400 * 7) * 5)) and item[0] <= (metric_timestamp - (86400 * 7))]``
- Break up the pw4_timeseries into motifs of each of batch_size 6 and for each
  see how many times each motif occurs in the pw4_timeseries per hour (meaning
  only allow for a motif to be counted if it does not occur within 1h of the
  previous occurrence).
- Record every motif that occurs more than 3 times in the pw4_timeseries, these
  are considered normal behaviour, they occurred at least 3 times so they are
  common patterns.
- Extract the 7d anomaly_timeseries from the MinMax scaled pw5_timeseries, e.g.
  ``anomaly_timeseries = [item for item in minmax_timeseries if item[0] >= (metric_timestamp - (86400 * 7)) and item[0] <= metric_timestamp]``
- Take each recorded motif and if a motif is found in the anomaly_timeseries,
  those data points in the anomalous time series can be removed (data smashed,
  annihilated).
- Record all indices for the periods that a match is found for.
- Create a set of the indices and remove those indices from the
  anomaly_timeseries. What is left?
- In addition to the use of 6 data point motifs, a method of using micro motifs
  has been added that is applied to any remaining data points that are part of
  motif which are < 6 data points in length which have not been annihilated.
  These remaining motifs in the anomaly_timeseries that are < batch_size in size
  are then considered per data point then attempt to annihilate each index based
  on the value occurring >= 4 times in the pw5_timeseries AND the delta of the
  index value from the previous value occurring at least 4 times as well.  This
  delta check ensures that the check is similar to a micro motif, not just value
  but range too.

If the time series can be annihilated by common motifs and micro motifs, the
time series is not anomalous and can be classified as normal and can be learnt.

Novel idea maybe...

Yes it was and it works **extremely** well at unsupervised, autonomous learning
and is highly accurate and reliable.