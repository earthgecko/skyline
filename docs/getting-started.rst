Getting started
===============

At some point hopefully it will be `pip install skyline` but for now see the
`Installation`_ page **after** reviewing the below

.. _Installation: ../html/installation.html

Realistic expectations
----------------------

Anomaly detection is not easy.  Skyline is not easy to set up, it has a number
of moving parts that need to be orchestrated.  Further to this, for Skyline to
be configured, trained and start learning takes a lot of time.

Take time to read through the documentation and review ``settings.py`` at the
same time.  There are lots of settings in ``settings.py`` do not feel
intimidated by this. The default settings should be adequate and reasonable for
starting out with.  The settings that you must change and take note of are all
documented on the `Installation`_ page

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

To begin with decide what your 10 most important Graphite metrics are and
**only** configure :mod:`settings.ALERTS` and :mod:`settings.SMTP_OPTS` on those
to begin with and get to know what Skyline does with those.  Add more key metric
groups as you get the hang of it.

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

Start with Horizon, Analyzer, Webapp, Luminosity and Panorama
-------------------------------------------------------------

It is advisable to only start the Horizon, Analyzer, Luminosity, Webapp and
Panorama daemons initially and take time to understand what Skyline is doing.
Take some time to tune Analyzer's mod:`settings.ALERTS` and learn the patterns
in your IMPORTANT metrics:

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
the :mod:`settings.ALERTS` tuples and start the Mirage daemon.

Add Boundary settings
---------------------

You will know what your **key** metrics are and you can define their acceptable
boundaries and alerting channels in the :mod:`settings.BOUNDARY_METRICS` tuples
and start the Boundary daemon.

Train Ionosphere
----------------

Via the alert emails or in the Skyline Ionosphere UI, train Ionosphere on what
is NOT anomalous.

Ignore Crucible
---------------

Still EXPERIMENTAL - for the time being.

By default Crucible is enabled in the ``settings.py`` however, for other Skyline
modules to send Crucible data, Crucible has to be enabled via the appropriate
``settings.py`` variable for each module, Analyzer and Mirage, etc.

Crucible has 2 roles:

1. Store resources (time series json and graph pngs) for triggered anomalies -
   note this can consume a lot of disk space if enabled.
2. Run ad hoc analysis on any time series and create matplotlib plots for the
   run algorithms.

It is not advisable to enable Crucible on any of the other modules unless you
really want to "see" anomalies in great depth.  Crucible is enabled as there is
a Crucible frontend view on the roadmap that will allow the user to test any
time series of any metric directly through the UI.
