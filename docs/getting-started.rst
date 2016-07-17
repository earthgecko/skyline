Getting started
===============

At some point **soon** hopefully it will be `pip install skyline` but for now
see `Installation`_ **after** reviewing the below

.. _Installation: ../html/installation.html

Not just a program
------------------

Anomaly detection is not easy.

> "It seems difficult"

It helps if you start out with realistic expectations.  Anomaly detection takes
time and effort, it is not simply a case of...

**install = "all singing and dancing anomaly detection system"**

There is no off-the-shelf, panacea, anomaly detection system or framework that
is going to provide you with "WOW" without some effort, learning and tuning
your system to your needs.

It is not simple, it is not perfect.  It is difficult.  Skyline configuration is
probably on par in complexity with other similar "things" such as Riemann or an
Elasticsearch cluster perhaps... probably less complex, but not easy initially
either.

That said, there is probably less of a learning curve and, upfront setup and
configuration and than other machine learning and like projects such as NuPIC,
scikit-learn, theano, tensorflow, keras, etc and all the similar ilk.

Hopefully Skyline will interface with some of the above in the not too distant
future - see `roadmap`_

.. _roadmap: ../html/roadmap.html

Anomaly detection is a journey not an app
-----------------------------------------

Anomaly detection is partly about automated anomaly detection and partly about
knowing your metrics and timeseries patterns.  Not all timeseries are created
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

Enabling Skyline modules incrementally
--------------------------------------

Skyline's modules do different things differently and understanding the process
and pipeline helps to tune each Skyline module appropriately for your data.

Each analysis based module, Analyzer, Mirage, Boundary and Crucible, have their
own specific configurations.  These configurations are not extremely complex, but
they are not obvious or trivial either when you are starting out.  Bringing
Skyline modules online incrementally over time, helps one to understand the
processes and their different configuration settings easier.  Easier than trying
to get the whole stack up and running straight off.

Start with Horizon, Analyzer, Webapp and Panorama
-------------------------------------------------

It is advisable to only start the original Horizon, Analyzer and Webapp daemons
initially and take time to understand what Skyline is doing.  Take some time to
tune Analyzer's `ALERTS` and learn the patterns in your metrics:

- which metrics trigger anomalies?
- when the metrics trigger anomalies?
- why/what known events are triggering anomalies?
- are there seasonalities/periodicity in anomalies some metrics?
- what metrics are critical and what metrics are just "normal"/expected noise

Panorama will help you view what things are triggering as anomalous.

Once you have got an idea of what you want to anomaly detect and more
importantly, on what and when you want to alert, you can start to define the
settings for other Skyline modules such as Mirage and Boundary and bring them
online too.

Add Mirage parameters to the `ALERTS`
-------------------------------------

Once you have an overview of metrics that have seasonalities that are greater
than the `FULL_DURATION`, you can add their Mirage parameters to the `ALERTS`
tuples and start the Mirage daemon.

Add Boundary settings
---------------------

You will know what your **key** metrics are and you can define their acceptable
boundaries and alerting channels in the `BOUNDARY_METRICS` tuples and start the
Boundary daemon.

You might want Crucible, you probably do not
--------------------------------------------

By default Crucible is enabled in the ``settings.py`` however, for other Skyline
modules to send Crucible data, Crucible has to be enabled via the appropriate
``settings.py`` variable for each module.

Crucible has 2 roles:

1. Store resources (timeseries json and graph pngs) for triggered anomalies - note
   this can consume a lot of disk space if enabled.
2. Run ad-hoc analysis on any timeseries and create matplotlib plots for the
   run algorithms.

It is not advisable to enable Crucible on any of the other modules unless you
really want to "see" anomalies in great depth.  Crucible is enabled as there is
a Crucible frontend view on the roadmap that will allow the user to test any
timeseries of any metric directly through the UI.
