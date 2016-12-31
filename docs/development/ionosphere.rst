************************
Development - Ionosphere
************************

What Ionosphere is for
======================

I want to monitor metrics on small VPSs that do not do a great deal, meaning
there is no high, rate constant work or 3sigma consistency in the metrics.
There are 7 apache.sending a day, not 7000 a minute.

Or there are peaks of 3 on the metric stats_counts.statsd.bad_lines_seen

.. image:: ../images/ionosphere/21060819.stats_counts.statsd.bad_lines_seen.png

This is not anomalous.

Ionosphere's goal is to allow us to train Skyline on what is not anomalous.

Almost a year in review
=======================

For a large part of 2016 a method of implementing the Ionosphere concept has
been sort after.

Ramblings - the development of Ionosphere
-----------------------------------------

The original idea behind Ionosphere was...

Ionosphere is a user feedback/input to Skyline that allows for the creation of
timeseries specific "algorithms".

Skyline Mirage (and Analyzer) work well on fairly constant rate, high range
metrics, however on low rate, low range metrics like machine stats metrics it
tends to be less effective and these types of timeseries make Mirage chatty
even when set too very high resolution e.g. 730 hours.

Ionosphere enables the user to create custom algorithms for specific timeseries.
While defining all the custom "rules" to match different types of timeseries and
metrics, much of the recent efforts in anomaly detection have been dedicated to
creation of automated, algorithmic and machine learning processes doing
everything with minimal user input, other than some configurations.

However, in truth anomaly detection is not necessarily equal among everything
and the pure computational anomaly detection system is not there yet. There is
still an aspect of art in the concept.

Ionosphere adds a missing piece to the Skyline anomaly detection stack, in our
attempts to computationally handle the anomaly detection process we have removed
or neglected a very important input, allowing the human to fly the craft.

Requirements
~~~~~~~~~~~~

Ionosphere is dependent on the panorama branch (Branch #870: panorama)

20160719 update

We have Panorama now.

Allow us to have an input to allow us to tell Skyline that an alert was not
anomalous after it had been reviewed in context and probably many, many times.
Certain timeseries patterns are normal, but they are not normal in terms of
3sigma and never will be.  However, they are not anomalous in their context.

It would be great if Skyline could learn these.

The initial idea was to attempt to start introducing modern machine learning
models and algorithms into the mix, to essentially learn machine learning, the
models, algorithms and methods.  This it appears is easier said than done, it
appears that machine learning with timeseries is not simple or straight forward.
In fact, it seems in terms of machine learning, well timeseries are the one of
the things that machine learning is not really good at yet.

A seemingly obvious method would be to consider trying to determine similarities
between 2 timeseries, once again easier said than done.

Determining timeseries similarities - 20160719
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Researching computing timeseries similarities in terms of both machine learning
and statistics means, it appears that there is a fair bit of stuff in R that
handles timeseries quite well.

- bsts: Bayesian Structural Time Series - Time series regression using dynamic
  linear models fit using MCMC (Google) -
  https://cran.r-project.org/web/packages/bsts/index.html
- The R package PDC provides complexity-based dissimilarity calculation and
  clustering, and also provides p values for a null hypothesis of identical
  underlying generating permutation distributions. The R package TSclust was
  recently updated and provides (among PDC) a number of approaches to time
  series dissimilarities.
  (https://www.researchgate.net/post/How_can_I_perform_time_series_data_similarity_measures_and_get_a_significance_level_p-value)

And python maybe RMS - Erol Kalkan · United States Geological Survey, "Another
approach to compute the differences between two time series is moving window
root-mean-square. RMS can be run for both series separately. This way, you can
compare the similarities in energy (gain) level of time series. You may vary the
window length for best resolution." (https://www.researchgate.net/post/How_can_I_perform_time_series_data_similarity_measures_and_get_a_significance_level_p-value)
http://stackoverflow.com/questions/5613244/root-mean-square-in-numpy-and-complications-of-matrix-and-arrays-of-numpy#

However finding a good similarity measure between time series is a very
non-trivial task.

http://alexminnaar.com/time-series-classification-and-clustering-with-python.html

Spent tinkered with LBKeogh and sklearn.metrics import classification_report, he
is not wrong. Any which way I think bringing R into the evaluation is going to
be useful long term. rpy2

Other:

- Fast Time Series Evaluation (FTSE) - older algorithm from 2007 but potential -
  http://dbgroup.eecs.umich.edu/files/sigmod07timeseries.pdf
- https://www.semanticscholar.org/paper/Benchmarking-dynamic-time-warping-on-nearest-Tselas-Papapetrou/69683d13b7dfac64bd6d8cd6654b617361574baf

Aggregation. Further complicates... a lot I think.

Clustering not the way?

- Clustering of Time Series Subsequences is Meaningless: Implications for
  Previous and Future Research - http://www.cs.ucr.edu/~eamonn/meaningless.pdf -
  covers K-means and STS
- http://amid.fish/anomaly-detection-with-k-means-clustering
- https://github.com/mrahtz/sanger-machine-learning-workshop

Time clustering algorithm. An idea. We need to give time more dimensionality.
http://www.cs.unm.edu/~mueen/FastestSimilaritySearch.html

Which ends up relating to Feature #1572: htsa - Haversine Time Similarity
Algorithm, an idea to perhaps pursue in the future, non-trivial, but may be able
to add some additional clusterable dimensionalities to timeseries at some point
in the future.  Are timeseries difficult for machine learning to understand as
in simple timeseries, a timestamp is a 1 dimensional things and has no
relationship to anything else.  Whereas time really has a number of additional
aspects to it as well.  In the very least, it has a spatial dimension as well.
Lucky with timeseries we have metric_name, timestamp and value, we could cluster
on grouping of classifiable (classable) parts of namespaces say.  Which would
at least add a certain layer of machine learning, but to what end currently?

It is not easy.

But maybe it can be.

Originally Ionosphere was envisioned as bringing some machine learning to
Skyline and lots of paths and methods, etc have been reviewed and unfortunately
no simple way can be seen of achieving this in any meaningful way in terms of
Skyline and it original purpose, anomaly detecting in machine metrics timeseries.
Although it would be nice to update to the current Skyline stack and pipeline to
use something not from the 1920's, it is alas a fork to far at this point.

And a bit of 1920's with a bit of modern key value and some Python, with a dash
of "lets try CONSENSUS", does not do bad a job.  Considering off the production
line and into the digital pipeline, with a sprinkling of new ideas.

However, not all metrics do well with 3sigma either :)
Machine learning, scikit-learn, tensorflow, NuPIC, TPOT, k-means, et al are not
helping either.

Thresholding, no.  Although thresholding in Boundary is useful for it purpose,
direct thresholding for Ionosphere has been constantly been looked away from as
it is felt that simple thresholding is not useful or helpful in terms of
learning for Skyline and people.  We have done thresholding and we can.

Ionosphere should be about learning and teaching, for want of better words.
Machine learning has training data sets.  So Ionosphere needs training data sets.
So lets give it training data sets.

Updated by Gary Wilson 3 months ago

Small steps
~~~~~~~~~~~

For some reason I think this should be at least started on before feature
extraction.

Add user input

- via Panorama tickbox - not anomalous
- via alert with link to Panorama not anomalous
- Calculate things
  - stdDev
  - slope
  - linear regression
  - variability
  - etc - values for the timeseries
- Store the triggered timeseries for X amount of time to allow the user to process the anomaly and timeseries as it was, real timeseries data that was used and review the metric, if no action is taken on the anomaly, prune older than X.
- Begins to provide training data sets or at least calculated metrics as above about anomalies

Analyzer says this is anomalous, user can evaluate and say:

This is not anomalous at X time range.
This is not anomalous on a Tuesday.

Updated by Gary Wilson about 1 month ago

Simple Ionosphere
~~~~~~~~~~~~~~~~~

Some nice simple ideas, yesterday morning and I think it might be workable.

- Operator flags as not_anomalous (within a 24 hr period)
- Take saved Redis timeseries and:
  - sum
  - determine mean
  - determine median
  - determine min
  - determine max
  - determine 3sigma
  - determine range
  - determine range in the 95% percentile
  - determine count in the 95% percentile
  - determine range in the 5% percentile
  - determine count in the 5% percentile
- Save not_anomalous details to Ionosphere table

The devil may be in the details

- Match entire namespace, wildcarding, may be difficult and expensive, but may
  not if Redis keyed
- Expire training sets and trained results, how long?
- Ionosphere before Mirage? May be mute point as should work in both, each with
  their own metrics as alerts will come from the responsible app and therefore
  they can be tagged independently.

Workflow
~~~~~~~~

This works for Analyzer and Mirage, only Analyzer is described here as it has
the additional task of checking if it is a Mirage metric.

- Analyzer -> detect anomaly
- Analyzer checks mirage.unique_metrics set, `if mirage_metric == True` send to
  Mirage and skip sending to Ionosphere, continue.
- Analyzer sends to Ionosphere if not a Mirage metric
- Analyzer then checks ionosphere.unique_metrics set, to see if it is an
  Ionosphere metric, if True, Analyzer skips alerting and continues. Ionosphere
  will analyze the metric that Analyzer just sent and score it and alert if it
  is anomalous.
- Ionosphere -> calculate a similarity score for the anomalous timeseries based
  on trained values. If less than x similar, alert, else proceed as normal.

Updated by Gary Wilson 18 days ago

Maybe we took a step - FRESH
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

In terms of one aspect of machine learning timeseries, tsfresh

The paper is interesting - https://arxiv.org/pdf/1610.07717v1.pdf

Updated by Gary Wilson 1 day ago

Task #1718: review.tsfresh - still in progress
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

tsfresh possibly does have the potential to fulfill some of the functionality of
Ionosphere as described above.

It is not super fast, quite processor intensive, but... lots of features!!!

.. code-block: bash

  # @added 20161124184300 UTC
  # THIS NEEDS to be update as soon as I got the PoC matched ONCE only... I ran
  # It is SUPER fast on Graphite csv timeseries parsed timeseries and not
  # processor intensive at all.  Storing all those features without violating
  # the First Normal Form, still all in progress.


THE FIRST
=========

\o/ before the end of the year!!!

This was the first production Ionosphere matched tsfresh features profile after
a year in the waiting and making.

A special thanks to @MaxBenChrist, @nils-braun and @jneuff over at
https://github.com/blue-yonder/tsfresh

Well it took a year :)  But... it works :)

* Branch #922: ionosphere - created 2015-12-26 11:21 AM
* Task #1658: Patterning Skyline Ionosphere

ionosphere.log
~~~~~~~~~~~~~~

.. code-block: bash

    2016-12-31 06:43:54 :: 31856 :: starting 1 of 1 spin_process/es
    2016-12-31 06:43:54 :: 18700 :: child_process_pid - 18700
    2016-12-31 06:43:54 :: 18700 :: loading metric variables from import - metric_check_file - /opt/skyline/ionosphere/check/1483166562.stats.skyline1.io.received.txt
    2016-12-31 06:43:54 :: 18700 :: got MySQL engine
    2016-12-31 06:43:54 :: 18700 :: metrics_table OK for stats.skyline1.io.received
    2016-12-31 06:43:54 :: 18700 :: ionosphere_enabled is 1 for metric id 171 - stats.skyline1.io.received
    2016-12-31 06:43:54 :: 18700 :: Ionosphere is enabled on stats.skyline1.io.received
    2016-12-31 06:43:54 :: 18700 :: training data ts json available - /opt/skyline/ionosphere/data/1483166562/stats/skyline1/io/received/stats.skyline1.io.received.json
    2016-12-31 06:43:54 :: 18700 :: getting MySQL engine
    2016-12-31 06:43:54 :: 18700 :: got MySQL engine
    2016-12-31 06:43:54 :: 18700 :: ionosphere_table meta reflected OK
    2016-12-31 06:43:54 :: 18700 :: ionosphere_table OK
    2016-12-31 06:43:54 :: 18700 :: determined 3 fp ids for stats.skyline1.io.received
    2016-12-31 06:43:54 :: 18700 :: ionosphere feature profile creation requested for stats.skyline1.io.received at 1483166562
    2016-12-31 06:43:54 :: 18700 :: No features profile details file exist - /opt/skyline/ionosphere/data/1483166562/stats/skyline1/io/received/1483166562.stats.skyline1.io.received.fp.details.txt
    2016-12-31 06:43:54 :: 18700 :: No features profile created file exist - /opt/skyline/ionosphere/data/1483166562/stats/skyline1/io/received/1483166562.stats.skyline1.io.received.fp.created.txt
    2016-12-31 06:43:54 :: 18700 :: DataFrame created with /opt/skyline/ionosphere/data/1483166562/stats/skyline1/io/received/stats.skyline1.io.received.tsfresh.input.csv
    2016-12-31 06:43:54 :: 18700 :: starting extract_features
    2016-12-31 06:43:55 :: 18700 :: features extracted from /opt/skyline/ionosphere/data/1483166562/stats/skyline1/io/received/stats.skyline1.io.received.tsfresh.input.csv data
    2016-12-31 06:43:55 :: 18700 :: feature extraction took 0.815440 seconds
    2016-12-31 06:43:55 :: 18700 :: features transposed
    2016-12-31 06:43:55 :: 18700 :: features saved to /opt/skyline/ionosphere/data/1483166562/stats/skyline1/io/received/stats.skyline1.io.received.tsfresh.input.csv.features.csv
    2016-12-31 06:43:55 :: 18700 :: transposed features saved to /opt/skyline/ionosphere/data/1483166562/stats/skyline1/io/received/stats.skyline1.io.received.tsfresh.input.csv.features.transposed.csv
    2016-12-31 06:43:55 :: 18700 :: total feature profile completed in 0.908591 seconds
    2016-12-31 06:43:55 :: 18700 :: removed /opt/skyline/ionosphere/data/1483166562/stats/skyline1/io/received/stats.skyline1.io.received.tsfresh.input.csv
    2016-12-31 06:43:55 :: 18700 :: calculated features available - /opt/skyline/ionosphere/data/1483166562/stats/skyline1/io/received/stats.skyline1.io.received.tsfresh.input.csv.features.transposed.csv
    2016-12-31 06:43:55 :: 18700 :: determined 150 features for fp_id 5
    2016-12-31 06:43:55 :: 18700 :: converting tsfresh feature names to Skyline feature ids
    2016-12-31 06:43:55 :: 18700 :: determining common features
    2016-12-31 06:43:55 :: 18700 :: comparing on 150 common features
    2016-12-31 06:43:55 :: 18700 :: sum of the values of the 150 common features in features profile - 1267803329.88
    2016-12-31 06:43:55 :: 18700 :: sum of the values of the 150 common features in the calculated features - 926774397.936
    2016-12-31 06:43:55 :: 18700 :: percent_different between common features sums - -26.8991983147
    2016-12-31 06:43:55 :: 18700 :: updating checked details in db for 5
    2016-12-31 06:43:55 :: 18700 :: updated checked_count for 5
    2016-12-31 06:43:55 :: 18700 :: debug :: 5 is a features profile for stats.skyline1.io.received
    2016-12-31 06:43:55 :: 18700 :: determined 150 features for fp_id 25
    2016-12-31 06:43:55 :: 18700 :: converting tsfresh feature names to Skyline feature ids
    2016-12-31 06:43:55 :: 18700 :: determining common features
    2016-12-31 06:43:55 :: 18700 :: comparing on 150 common features
    2016-12-31 06:43:55 :: 18700 :: sum of the values of the 150 common features in features profile - 460020831.778
    2016-12-31 06:43:55 :: 18700 :: sum of the values of the 150 common features in the calculated features - 926774397.936
    2016-12-31 06:43:55 :: 18700 :: percent_different between common features sums - 101.463571629
    2016-12-31 06:43:55 :: 18700 :: updating checked details in db for 25
    2016-12-31 06:43:55 :: 18700 :: updated checked_count for 25
    2016-12-31 06:43:55 :: 18700 :: debug :: 25 is a features profile for stats.skyline1.io.received
    2016-12-31 06:43:55 :: 18700 :: determined 150 features for fp_id 26
    2016-12-31 06:43:55 :: 18700 :: converting tsfresh feature names to Skyline feature ids
    2016-12-31 06:43:55 :: 18700 :: determining common features
    2016-12-31 06:43:55 :: 18700 :: comparing on 150 common features
    2016-12-31 06:43:55 :: 18700 :: sum of the values of the 150 common features in features profile - 929947185.801
    2016-12-31 06:43:55 :: 18700 :: sum of the values of the 150 common features in the calculated features - 926774397.936
    2016-12-31 06:43:55 :: 18700 :: percent_different between common features sums - -0.341179360791
    2016-12-31 06:43:55 :: 18700 :: updating checked details in db for 26
    2016-12-31 06:43:55 :: 18700 :: updated checked_count for 26
    2016-12-31 06:43:55 :: 18700 :: not anomalous - features profile match - stats.skyline1.io.received
    2016-12-31 06:43:55 :: 18700 :: calculated features sum are within 1 percent of fp_id 26 with 0.341179360791, not anomalous
    2016-12-31 06:43:55 :: 18700 :: updated matched_count for 26
    2016-12-31 06:43:55 :: 18700 :: debug :: 26 is a features profile for stats.skyline1.io.received
    2016-12-31 06:43:55 :: 18700 :: metric_check_file removed - /opt/skyline/ionosphere/check/1483166562.stats.skyline1.io.received.txt
    2016-12-31 06:43:56 :: 31856 :: ionosphere :: 1 spin_process/es completed in 1.35 seconds
    2016-12-31 06:43:56 :: 31856 :: updated Redis key for ionosphere up
    2016-12-31 06:43:56 :: 31856 :: purging any old training data
