.. role:: skyblue
.. role:: red

`matrixprofile`_
================

The Matrix Profile algorithm was pioneered by Professor Eamonn Keogh at the
University of California-Riverside and Professor Abdullah Mueen at the
University of New Mexico in 2015.  When it came onto the scene in 2015 it made
some remarkable claims, unfortunately its initial implementation was in matlab
but the claims made by Eamonn Keogh et al could not be ignored, the algorithm
was set to change the face of time series mining and it has.  Those claims were
not unfounded and thanks to the efforts of the matrixprofile.org team the
`matrixprofile`_ algorithm is now available in Python and if you want a CUDA
enabled version there is STUMPY.

UPDATE - Skyline as of v4.0.0 uses `stumpy`_ as `matrixprofile`_ development was
discontinued.

`matrixprofile`_ uses the `MASS`_ similarity search method to identify motifs (
shapelets, a short subsequence of a time series) that are discords (not similar
or least similar to anything else in the time series) and these are anomalies.

Skyline can use `matrixprofile`_ as additional algorithm in the analysis pipeline.
The use of `matrixprofile`_ massively reduces false positive detections compared to
the basic three-sigma based algorithm methods on time series of 7 days in length.
However as the saying goes, all magic comes with a price.

The price in the case of `matrixprofile`_ is a few false negatives.  Where
three-sigma based algorithms would detect an instance as anomalous and you would
agree it was, rarely, but occasionally `matrixprofile`_ will identify it as not
anomalous.  A comparison of three-sigma vs. `matrixprofile`_ after the review of
859 real production data events are as follows:

+-------------------+------+------+------+------+
|  algorithm_group  |  tP  |  fP  |  tN  |  fN  |
+===================+======+======+======+======+
|  three-sigma      |  69  |  790 |  N/A |  N/A |
+-------------------+------+------+------+------+
|  matrixprofile    |  50  |  93  |  697 |  19  |
+-------------------+------+------+------+------+

tN and fN are not scored for three-sigma as it was the baseline.  Each
three-sigma triggered instance was sent to SNAB and `matrixprofile`_ was run
against the instance results recorded for comparison.

The logical place for `matrixprofile`_ to sit in Skyline is as a custom algorithm
that is run only by Mirage after three-sigma consensus has been achieved.  It is
too computationally and time expensive to run in Analyzer, although it could run
in Analyzer if you only have a few 100 metrics.

Considerations when implementing matrixprofile
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

By default in v3.0.0 matrixprofile is not enabled as an additional algorithm to
run.  This is because it is a user decision as to whether they wish to implement
matrixprofile as an algorithm to be added to the analysis pipeline.  The
addition of matrixprofile has a significant impact on the amount of events that
are recorded and alerted on.  This is due to matrixprofile significantly
decreasing the number of false positive alerts that are sent.  **HOWEVER** due
to this being a fundamental change to Skyline, the current Skyline users (
running v2.0.0) must be aware of what this means in practice.

1. matrixprofile will result in a small number of false negatives (~2.48%)
2. matrixprofile will filter out a very large number of false positives (~88.4%)

These two points must be stressed because for any users that are accustomed to
using Skyline as an information stream of changes in your environments, if you
turn on matrixprofile, you may question if Skyline is running properly with
matrixprofile probably result in filtering out ~88.4% of your events.

Although the paneacea of anomaly detection **alerting** is 0 false positives and
0 false negatives, anomaly detection is not only about alerting, it is about the
bad, the good and the unknown.  This drastic decrease in events also results in
a reduction of correlation data, training data opportunities and an overall
reduced picture events in your timeline.

If you are a current Skyline user and you implement matrixprofile be prepared
for most of the events you have become accustomed too vanishing.  For instance
when you or developers deploy something, those subtle changes that used to
trigger and send an event into slack, probably will not happen any more.  Most,
if not all of the events triggered when changes were made to things or some
action was taken will stop. This results in those feedback reinforcements events
that used to trigger in slack vanishing.  You will be accustomed to making
changes or doing something and often seeing events being triggered in slack,
that reinforcement feedback mostly gets lost with addition of maxtrixprofile as
an algorithm.

For users of Skyline who have possibly learnt a lot about their systems and
specifically the interactions between the components in those systems,
discovering related things that were unknown, discovering and learning what
metrics they had never heard of and found things that they should watch and
understand more.  A lot of our learnings have been as a result of these
events that fired on subtle changes.  So make a careful consideration and
assessment before thinking that anomaly **alerting** panacea is what you
really want.

However that said, if you are already running 1000s of metrics in Skyline and
you have training on metrics, you have probably learnt a lot already, so you
would probably want to enable it.  Just prepare yourself for a big change.

To further inform your considerations, it must be pointed out that although
matrixprofile is not enabled by default, the use of `MASS`_ in the Ionosphere is
enabled by default.  This means that the core functionality of matrixprofile is
implemented in the pattern matching side of things.  If you find Skyline very
useful an informational stream and not simply for alerting becasue of the above
stated reasons, carefully consider whether you want to use matrixprofile in
the analysis workflow as you will lose a lot of visibility you have become
accustom to having.  The consideration here is that if you do not enable
matrixprofile on the algorithm analysis side, Skyline can still provides its get
core similar search functionality on all your trained features profiles.  Maybe
that is the best of both worlds, the decision in yours.

You can consider adding some namespaces to be analyzed by matrixprofile and
assess the results before jumping in with both feet, it really depends if you
are wanting to use Skyline as an alerter only or as an informational stream of
the bad, the good and the unknown, that YOU learn from.

HOWEVER - trigger_history_override
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

To overcome matrixprofile perhaps silencing too much, the ``trigger_history_override``
parameter can be passed on skyline_matrixprofile :mod:`settings.CUSTOM_ALGORITHMS`
a tradeoff value to use here is 6, meaning that if sigma algorithms have triggered
6 times in a row and matrixprofile does not trigger and classifies the datapoint
as NOT anomalous, this result will be overridden and the datapoint will be
recorded as an anomaly (unless there is Ionosphere training the matches
further down the analysis pipeline).

Enabling matrixprofile to run in Mirage
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

See the current ``'skyline_matrixprofile'`` example in
:mod:`settings.CUSTOM_ALGORITHMS` and change the ``'namespaces':`` parameter as
appropriate.

.. _MASS: https://www.cs.unm.edu/~mueen/FastestSimilaritySearch.html
.. _matrixprofile: https://github.com/matrix-profile-foundation/matrixprofile
.. _stumpy: https://github.com/TDAmeritrade/stumpy
