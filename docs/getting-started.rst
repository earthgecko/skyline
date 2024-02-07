Getting started
===============

.. _Installation: ../latest/installation.html

See the `Installation`_ page **after** reviewing the below.

Managed Service
---------------

You can sign up to Anomify, a cutting edge Skyline service, built and managed by
the team behind Skyline.  Anomify is offering a free
`service for beta users <https://anomify.ai/skyline>`_.  Or you can elect to
clone the Skyline repository from Github and run Skyline on premise or in your
own cloud infrastructure and follow the steps below.

A collection of applications
----------------------------

Skyline is made up of a collection of applications that interact with each other
but all run independently, each fulfilling a specific role in the analysis pipeline.
Lets go through them briefly.  Horizon, Analyzer, Mirage, Panorama, Boundary,
Crucible, Ionosphere, Luminosity, Vista, Flux and the webapp (and snab).  That
is a lot... full featured.  Only the core real times applications will be
introduced here.

- Graphite - although Graphite is not part of Skyline it is Skyline's long term
  time series database.
- VictoriaMetrics - not part of Skyline but can be used as Skyline's long term
  time series database for label based metrics.
- Redis - database for transient real time data and sharing data between Skyline
  applications.
- Horizon - receives metrics from Graphite (and labelled metrics from Flux) and
  feeds them into Redis.  And also prunes old time series data from Redis.
- Flux ingests metrics via HTTP in various formats, including JSON and Prometheus
  protocol buffer format.
- Analyzer - every minute, gets 24 hours of metric data from Redis and analyses
  them all with 9 three-sigma based algorithms and pushes potential anomalies to
  Mirage or Ionosphere.
- Mirage analyses potential anomalies with the same 9 three-sigma based
  algorithms but against a longer period of data than 24 hours, normally 7 days.
  It also analyses the metric with any defined custom algorithms such as
  matrixprofile. If the instance is still considered anomalous, Mirage forwards
  it to Ionosphere (if the metric has been trained on) or alerts on the metric.
- Ionosphere extracts features and potentially anomalous shapelets from the
  data and runs similarity searches across all trained data, if still anomalous,
  it sends an event to Panorama and back to the originating app, e.g. Mirage to
  route the event as appropriate.
- Panorama receives detected anomalies from Analyzer, Mirage, Boundary and
  Ionosphere and records the details in the database.
- Boundary analyses defined metrics against specified thresholds and configured
  dynamic thresholds and alerts if breached.
- Luminosity cross correlates anomalies with the entire or defined metric
  population and can classify anomaly types.
- Panorama records and tracks anomalies in the DB

Realistic expectations
----------------------

Anomaly detection is not easy.  Skyline is not easy to set up, it has a number
of moving parts that need to be orchestrated.  Further to this, for Skyline to
be configured, trained and start learning takes time.  But rest assured once set
up, it just runs and runs and runs, requiring minimal maintenance, if any (apart
from when things need to be upgraded).

Anomaly detection is a journey not an app
-----------------------------------------

Anomaly detection is partly about automated anomaly detection and partly about
knowing your metrics and time series patterns.  Not all time series are created
equally.

It helps to think of anomaly detection as an ongoing journey.  Although ideally
it would be great to be able to computationally detect anomalies with a high
degree of certainty, there is no getting away from the fact that the more
**you** learn, "help" and tune your anomaly detection, the better it will become.

The fact that Skyline **does** computationally detect anomalies with a
high degree of certainty, can be a problem in itself.  **But** it is not
Skyline's fault that:

- a lot of your metrics **are** anomalous
- that feeding all your metrics to Skyline and setting alerting on all your
  metric namespaces is too noisy to be generally considered useful

Enabling metrics incrementally
------------------------------

Skyline was originally pitched to automatically monitor #allthethings, all your
metrics, it can but...

Skyline should have been pitched to monitor your KEY metrics.

To begin with decide what your 100 most important metrics are and **only**
configure :mod:`settings.ALERTS` and :mod:`settings.SMTP_OPTS` (you can use
``no_email`` to not recieve alerts).  If you have Slack, set up a channel and
send these to begin with and get to know what Skyline does with those.  Add
more key metric groups as you get the hang of it.

You cannot rush time series.

Enabling Skyline modules incrementally
--------------------------------------

Skyline's modules do different things differently and understanding the process
and pipeline helps to tune each Skyline module appropriately for your data.

Each analysis based module, Analyzer, Mirage, Boundary, Ionosphere, Luminosity
(and Crucible), have their own specific configurations.  These configurations are
not extremely complex, but they are not obvious or trivial either when you are
starting out.  Bringing Skyline modules online incrementally over time, helps
you to understand the processes and their different configuration settings
easier.  Easier than trying to get the whole stack up and running straight off.

Start with Horizon, Analyzer, Mirage, Ionosphere and Webapp, Luminosity and Panorama
------------------------------------------------------------------------------------

It is advisable to only start the Horizon, Analyzer, Mirage, Ionosphere, Webapp,
Luminosity and Panorama daemons initially and take time to understand what
Skyline is doing.  Take some time to tune Analyzer's :mod:`settings.ALERTS` and
learn the patterns in your IMPORTANT metrics:

- which metrics trigger anomalies?
- when the metrics trigger anomalies?
- why/what known events are triggering anomalies?
- are there seasonality/periodicity in anomalies some metrics?
- what metrics are critical and what metrics are just "normal"/expected noise

Panorama will help you view what things are triggering as anomalous.

Once you have got an idea of what you want to anomaly detect on and more
importantly, on what and when you want to alert, you can start to define the
settings for other Skyline modules such as Mirage, Boundary and Ionosphere and
bring them online too.  However do consider enabling Ionosphere from the outset
as well.

Add Mirage parameters to :mod:`settings.ALERTS`
-----------------------------------------------

Once you have an overview of metrics that have seasonality that are greater
than the :mod:`settings.FULL_DURATION`, you can add their Mirage parameters to
the :mod:`settings.ALERTS` tuples to be analysed by Mirage.  And set
:mod:`ENABLE_MIRAGE` to ``True``.

Add Boundary settings
---------------------

You will know what your **key** metrics are and you can define their acceptable
boundaries and alerting channels in the :mod:`settings.BOUNDARY_METRICS` tuples
and start the Boundary daemon.

Train Ionosphere
----------------

Via the alert emails or in the Skyline Ionosphere UI, train Ionosphere on what
is NOT anomalous.

Play with Vortex
~~~~~~~~~~~~~~~~

After you have over 7 days of data in the system, you can run whatever metrics
you want through all the different algorithms and see their results, just for
fun.

Ignore Crucible
---------------

Still EXPERIMENTAL - for the time being.

You will probably like Vortex more.

By default Crucible is enabled in the ``settings.py`` however, for other Skyline
modules to send Crucible data, Crucible has to be enabled via the appropriate
``settings.py`` variable for each module, Analyzer and Mirage, etc.

Crucible has 2 roles:

1. Store resources (time series json and graph pngs) for triggered anomalies -
   note this can consume a lot of disk space if enabled.
2. Run ad hoc analysis on any time series and create matplotlib plots for the
   run algorithms.

It is not advisable to enable Crucible on any of the other modules unless you
really want to "see" anomalies in great depth.  Crucible allows the user to test
any time series of any metric directly through the webapp UI.
