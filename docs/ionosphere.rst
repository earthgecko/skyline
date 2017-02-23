.. role:: skyblue
.. role:: red

Ionosphere
==========

Ionosphere is a story about timeseries.

Overview
--------

- Ionosphere gives the operator an input into Skyline to allow them to train it
  and compliment the existing statistical 3sigma anomaly detection methods.
- Ionosphere then starts to learn, retrospectively.
- Is Ionosphere the panacea for anomaly detection?  No.
- Is Ionosphere immediately useful for web scale anomaly detection?  No.  Unless
  you are already doing web scale anomaly detection with Skyline, then still no.
  However over time yes.  You cannot rush timeseries.
- Is Ionosphere a lot of work to train, yes.

Still interested?
-----------------

.. image:: images/ionosphere/what_if_i_told_you_train.jpeg

.. image:: images/ionosphere/what_if_i_told_you_learn.jpeg

What Ionosphere is for - humble beginnings
------------------------------------------

**YOU** can teach a system.

**YOU** can help a system to LEARN.

You want to monitor metrics on small VPSs that do not do a great deal, meaning
there is no high, rate constant work or 3sigma consistency in the metrics.
There are 0 to 7 apache.sending a day, not 7000 a minute. Or there are some
peaks on the metric stats_counts.statsd.bad_lines_seen

.. image:: images/ionosphere/21060819.stats_counts.statsd.bad_lines_seen.png

This is :red:`NOT ANOMALOUS`

Even in massive, workload intensive, dynamic systems there are always some
metrics that are low rate and relatively low range.  And at 3sigma... that means
noisy.

Ionosphere's goal is to allow us to train Skyline on what is not anomalous and
thereby compliment statistical 3simga means with some new methodologies of
trying to "teach" the system.  Of course teaching statistics algorithms
something new is unfortunately not possible, but luckily teaching the system is
possible.  Giving the system better contextual data to work with is the key
factor.

Things to consider
------------------

Contextual anomalies - Earthquakes and Earth tremors
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

A point anomaly is no more contextually than the timeframe in which it is
considered anomalous in.

The following metaphor can be used to describe this concept.  As this concept is
important to understand in terms of Ionosphere and Mirage.

Let us take Bob as an example, Bob lived in the UK and decided to move to
San Francisco because he landed a good job at a cool San Jose data analytics
upstart.  In the first week he is there, the Bay Area has a few Earth tremors,
to Bob from the UK this is ANOMALOUS!!! Luckily Bob has a friendly co-worker
called Alice and she directs him to http://earthquaketrack.com/us-ca-san-jose/recent
and shows him it is not that anomalous, it is quite normal.  Alice shows Bob to
consider the context of these events in at a fuller duration.

- 5 earthquakes in the past 24 hours
- 5 earthquakes in the past 7 days
- 28 earthquakes in the past 30 days
- 381 earthquakes in the past 365 days

.. seealso:: Bob's Hampshire earthquake data, if there is any...

And the penny drops for Bob that HERE in the Bay Area this is obviously this
quite normal.

Bob then wonders to himself why he did not think about this before leaving
his stable shire in the UK.  He consoles himself by thinking "Well all the VCs
and players are here...  so it obviously cannot be a big single point of
failure.

.. code-block:: python

  bob = 'Skyline'
  alice = 'You'

Skyline does not know all the contexts to the data, you do.  Ionosphere lets
us teach Bob **that is not an earthquake!!!** and enables Bob to look and ask,
"Did Alice say this was not an earthquake, let me look".

"Create" or "Create and LEARN"
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

With Ionosphere, you have the option to allow it to learn these things for
itself, as long as you tell it what it is ALLOWED to learn at the fuller
duration.

So Ionosphere gives you 2 options:

.. figure:: images/ionosphere/create.and.do.not.learn.png

  Only make a features profile based on the ``FULL_DURATION`` data.

.. figure:: images/ionosphere/create.and.learn.png

  This is not an anomaly now or then or in the forseeable future if it
  looks anything like the FULL_DURATION or any of the multiple resoluiton
  Graphite graphs, LEARN it.

This means you do not have to ship that earthquake that happened 17 days ago into
Ionosphere's features profiles and teach it BAD, badly.  You can just tell it
to see the relevant Analyzer ``FULL_DURATION`` or Mirage the
``SECOND_ORDER_RESOLUTION_HOURS`` data as not anomalous and **not** learn at
the fuller duration of the metric's ``learn_full_duration``.

You can teach Ionosphere badly, but to unteach it is just a SQL update.

No UI data update method
^^^^^^^^^^^^^^^^^^^^^^^^

There is no method to modify the DB data via the UI.  If you want to make any
changes, they must be made directly against the DB.  Disabling a features
profile, deleting features profiles, changing any of the metrics values once set
for metrics e.g. ``learn_full_duration``, ``learn_valid_ts_older_than``,
``max_generations`` or ``max_percent_diff_from_origin``

Backup
^^^^^^

- Backup the MySQL DB to another machine or better slave it and backup the slave.
- rsync backup /opt/skyline/ionosphere/features_profiles to another machine,
  frequently (for the time being, until autobuild is available, however
  autobuild will not a able to recreate all the resources, but most).

Ionosphere static demo pages
----------------------------

The demo pages show examples of how Ionosphere presents the operator with the
metric's details for the specific point anomaly and the all the Graphite
metric graphs at multiple resolutions so that the operator can evaluation the
anomaly in the full context of the metric's history.

Graphs demo page - multiple resolution Graphite graphs for context
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The demo page uses an edge case anomaly one which is not easy to decide whether
it is or is not an anomaly.

Would **YOU** consider it anomalous or not?  Would you want an alert?

Note that this is demo page is from an older version of Ionosphere before learn
was introduced.  So it does not give the operator the 2 options stated above:

- Create features profile and DO NOT LEARN
- Create features profile and LEARN

It only gives the one option, however this demo page is about the **multiple
resolution Graphite graphs** giving the operator context, so do not worry about
the buttons, look at the graphs.

See |graphs_demo_page| for a clearer picture (opens in a new browser tab).  Is
it anomalous or not anomalous?

.. |graphs_demo_page| raw:: html

   <a href="_static/ionosphere_demo/training-data-stats.gauges.statsd.timestamp_lag-20170110161506UTC/Skyline.Ionosphere.training-data-stats.gauges.statsd.timestamp_lag-20170110161506UTC.html" target="_blank">Ionosphere static multiple resolution graphs demo page</a>

Features profile demo page with matched graphs
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

This demo page shows an existing features profile with all the graph resources
that the profile was created, see the :skyblue:`Graphite ::` :red:`graphs WHEN created ::` **at 7h, 24h, 7d and 30d**
section in the below demo page.

A series of matched graphs, showing the instances where Ionosphere has
analyzed the Analyzer detected anomalouse timeseries and found it be not
anomalous because the calculated ``features_sum`` of the Analyzer anomalous
timeseries were within 1% difference of the ``features_sum :: 73931.7673978000``
that was calculated for features profile 269 see
:skyblue:`Graphite ::` :red:`graphs MATCHED` section in the below demo page.

See |fp_demo_page| for a clearer picture.

.. |fp_demo_page| raw:: html

   <a href="_static/ionosphere_demo/features-profile.stats.statsd.graphiteStats.calculationtime/Skyline.Ionosphere.features-profile.stats.statsd.graphiteStats.calculationtime.html" target="_blank">Ionosphere static features profile demo page with matched graphs</a>

Features profile search demo page with generational information
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

See |fp_gen_demo_page| for a clearer picture.

.. |fp_gen_demo_page| raw:: html

   <a href="_static/ionosphere_demo/features-profile-search.stats.statsd.graphiteStats.calculationtime/Skyline.Ionosphere.features-profile-search.stats.statsd.graphiteStats.calculationtime.html" target="_blank">Ionosphere static search features profiles demo page with generation info</a>

What Ionosphere is for - learning
---------------------------------

Ionosphere may have had humble beginnings, but adding this seemingly trivial
function was anything but humble, simple or easy.  So to solve the seemingly
simple problem, something completely new had to be pieced together.

Ionosphere "learns" timeseries and makes decisions based on a timeseries
similarities comparison method, based on a method using the tsfresh package.

This "learning" is base upon determining the similarities in timeseries that
could be best described as attempting to determine how similar 2 timeseries are
in terms of the amount of "power/energy", range and "movement" there is within
the timeseries data set.  A fingerprint or signature if you like, but understand
that neither are perfect.  This timeseries similarities comparison method is not
perfect, but it achieves the goal of being useful.  Contextually it will never
be perfect, unless 2 timeseries have identical data and it may be possible that
an identical timeseries reversed may give the same or negative of a features
sum.

**TODO**: test that theory, reversed timeseries the same or the inverse?

Evolutionary learning - generations
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Ionosphere uses an evolutionary learning model that records (and limits) the
generations of trained and learnt features profiles per metric.  Limits can be
set in settings.py and played around with

No machine learning
^^^^^^^^^^^^^^^^^^^

Ionosphere brings **no** machine learning to Skyline per se.  It is merely making
programmatic decisions based on the data it is provided with, things a human
operator tells it are not anomalous.  Ionosphere is an attempt to give Skyline
an Apollo Program refit.  Enabling the pilots to take control, have inputs.

If achieves the sentiments expressed in Brian L. Troutwine @bltroutwine
seminal Belgium 2014 devopsdays presentation -
Automation with Humans in Mind: Making Complex Systems Predictable, Reliable and
Humane - https://legacy.devopsdays.org/events/2014-belgium/proposals/automation-with-humans-in-mind/ -
video - http://www.ustream.tv/recorded/54703629 - then it has achieved a goal.

Ionosphere first and foremost was created to give this dimension of human
piloting where necessary.  Giving Skyline that ability to allow human input in
some form to "teach" Skyline what is not anomalous comes with a number
additional benefits, like giving the Skyline the information needed to learn
how to make decisions based on the input data it is provided within.

The initial goal has been achieved, but it comes at a price.  Everything has a
cost and here the cost is the operator needs to
``train_ionosphere_learn == time_in_seconds  # about 12 seconds``.

Ionosphere can only be activated by the input from a human neocortex to tell it
what is not anomalous.  Some brain CPU cycles, opening emails and clicks,
assess 1 or 2 more clicks.  It is not easy, however that said it is effective at
what it set out to achieve.

It appears that Ionosphere is better at doing what it was intended for than
doing what it was not intended for.  All timeseries not being created equal.

Ionosphere does low range, low rate metrics very well.

Ionosphere does them better than high rate, highly variable metrics, when it saw
first light at least.  This is not to say that it does not do high rate, highly
variable metrics, it just needs a lot more features profiles for the metric
describing what is not anomalous.  Over the fullness of time and data, these
learning efficiency metrics will be available via the database data for analysis.
However that said, Ionosphere aims to be useful.

The tsfresh package and features extraction functions, enabled this ability of
features calculation on a wholesale scale, without having to design lots of
algorithms to calculate the timeseries features for.  The tsfresh package
enabled Ionosphere to happen much FASTER, it calculates all the features that
are required to make this method viable and work.  They said:

> Spend less time on feature engineering

They were not wrong.  Skyline has added a lot of "checks" to ensure consistency
in the tsfresh calculated features so that a features profile is not affected
by any changes that may be implemented in the tsfresh package.  All of this
has been pushed back into tsfresh and may be one of the reasons why the actual
development of Ionosphere took so long, but you cannot rush timeseries.

This overview of Ionosphere could not be complete without a special thanks to
the tsfresh people @MaxBenChrist, @nils-braun and @jneuff who are some of nicest
people in open source, on par with @astanway :)

Thanks to @blue-yonder for supporting the open sourcing of tsfresh.

How Ionosphere works - a simple overview as possible
-----------------------------------------------------

Firstly one needs to understand there is a chicken and egg aspect to Ionosphere.
Ionosphere has a number of roles that are centered on feature extractions,
features calculates and comparisions.

- Ionosphere creates a features data set for each triggered anomaly that is
  alerted on via SMTP, ready for a human to come along in the webapp Ionosphere
  UI and say, "that is not anomalous".
- At this point all the features profile values that was created for the not
  anomalous timeseries are entered into the MySQL database and the metric
  becomes an ionosphere_enabled metric.
- Once a metric is ionosphere_enabled, both Analyzer and Mirage will refer any
  anomalies found in an ionosphere_enabled metric to Ionosphere instead of
  alerting.
- Ionosphere then creates a features profile for the 3sigma anomalous timeseries
  and compares common features sums with previously recorded not anomalous
  features profiles.
- If Ionosphere does not find a match, it tells the originating app (Analyzer or
  Mirage) to send out the anomaly alert.

- If Analyzer or Mirage detect an anomaly
- Ionosphere firstly extracts the features of all SMTP alerted anomalies
- more here

- Skyline Analyzer or Mirage detect an anomalous metric

  - Save training data set and the anomaly details
  - If the metric is not an Ionosphere metric, a smtp anomaly alert is
    triggered and the alert images are saved in the training data dir

- Ionosphere

  - User submits a training data set for a metric as not anomalous
  - Ionosphere calculates the timeseries features with tsfresh
  - Ionosphere creates features profile resources in the Skyline MySQL database
    and saves the training data.

Once a metric has a features profile created it becomes an Ionosphere metric.
Thereafter, when both Analyzer or Mirage detect an anomaly for the metric, they
will just save the training data and anomaly details and defer alerting on the
anomaly to Ionosphere.  If Ionosphere does not match a features profile for the
anomlous metric, it hands the anomaly back to Analyzer or Mirage to alert on as
normal only, with a ``[Skyline alert] - Ionosphere ALERT`` subject.

A metric can have more than one features profile, a lot more.

Ionosphere analysis
-------------------

When a check is submitted for on Ionosphere metric the following analysis takes
place.

- Ionosphere extracts the features from the anomalous timeseries in the related
  training data set.
- Ionosphere then calculates the sum of all features.
- Ionosphere then determines all features profile ids for the metric from the
  database and for each features profile:

  - Generates a list of [feature_name, value] for all features that exist in the
    anomalous timeseries feature profile.  It is important to note that if a
    feature does not exist in anomalous timeseries being analyzed, it will not
    be used for comparison, even if the feature_name and value exist in a
    features profile.
  - Calculates the sum of all matching features in the features profile data.
  - Compare the sum of the features values of the anomalous timeseries to the
    sum of the features values from the features profile data.
  - If the two values are less than :mod:`settings.IONOSPHERE_MAX_DIFFERENCE`

    - Ionosphere will deem the timeseries as not anomalous and remove the
      related training data.

  - If the timeseries is not found to match any features profile, Ionosphere
    will alert as per Analyzer or Mirage would and submit the anomaly details to
    Panorama.

Input
-----

When an anomaly alert is sent out via email, a link to the Ionosphere training
data is included in the alert.  This link opens the Ionosphere UI with the all
training data for the specific anomaly where the user can submit the metric
timeseries as not anomalous and generate have Skyline generate a features
profile with tsfresh.

features profiles
-----------------

When a training data set is submitted as not anomalous for a metric a features
profile is extracted from the timeseries using tsfresh.  This features profile
contains the about values of 216 features (currently as of tsfresh-0.4.0), such
as median, mean, variance, etc, for a full list of known features that are
calculated see :mod:`tsfresh_feature_names.TSFRESH_FEATURES`.

This features profile is then stored in the Skyline MySQL database in the
following manner.  For every metric that has a features profile that is created,
2 MySQL InnoDB tables are created for the metric.

- The features profile details are inserted into the ionosphere table and the
  features profile gets a unique id.
- z_fp_<metric_id> - features profile metric table which contains the features
  profile id, feature name id and the calculated value of the feature.
- z_ts_<metric_id> - the timeseries data for the metric on which a features
  profile was calculated.

These tables are prefixed with ``z_`` so that they are all listed after all core
Skyline database tables.  Once a metric has a z_fp_<metric_id> and a
z_ts_<metric_id>, these tables are updated any future features profiles and
timeseries data.  So there is are 2 tables per metric, not tables per features
profile.

MySQL considerations
--------------------

There could be a lot of tables. **DEFINITELY** implement ``innodb_file_per_table``
in MySQL.

Ionosphere - autobuild features_profiles dir
--------------------------------------------

.. todo:: autobuild - TBD at some point in the future, for now see the Backup
  section above.

The number of features_profiles dirs that Ionosphere learn could spawn and the
amount of data storage that would result is unknown. It is possible the operator
is going to need to prune this data a lot of which will probably never be looked
at. Or a Skyline node is going to fail, not have the features_profiles dirs
backed up and all the data is going to be lost or deleted. So it is possible for
Ionosphere to created all the human interrupted resources for the features
profile back under a best effort methodology. Although the original Redis graph
image would not be available, nor the Graphite graphs in the resolution at which
the features profile was created, however the fp_ts is available so the Redis
plot could be remade and all the Graphite graphs could be made as best effort
with whatever resolution is available for that time period.

This allows the operator to delete/prune feature profile dirs by possibly least
matched by age, etc or all and still be able to surface the available features
profile page data on-demand.

.. todo:: expire features profiles older than?  Ionosphere forget.

See `Development - Ionosphere <development/ionosphere.html>`__
