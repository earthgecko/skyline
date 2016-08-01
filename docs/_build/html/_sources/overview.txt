.. role:: skyblue
.. role:: red
.. role:: brow

Overview
========

A brief history
---------------

Skyline was originally open sourced by `Etsy`_ as a real-time anomaly detection
system. It was originally built to enable passive monitoring of hundreds of
thousands of metrics, without the need to configure a model/thresholds for each
one, as you might do with Nagios.  It was designed to be used wherever there are
a large quantity of high-resolution timeseries which need constant monitoring.
Once a metric stream was set up (from statsd, graphite or other), additional
metrics are automatically added to Skyline for analysis, anomaly detection,
alerting and briefly publishing in the Webapp frontend.  `github/etsy`_ stopped
actively maintaining Skyline in 2014.

Skyline - as a work in progress
-------------------------------

`Etsy`_ found the "one size fits all approach" to anomaly detection wasn't
actually proving all that useful to them.

There is **some truth** in that in terms of the one size fits all methodology that
Skyline was framed around.  With hundreds of thousands of metrics this does make
Skyline fairly hard to tame, in terms of how useful it is and tuning the noise
is difficult.  Tuning the noise to make it constantly useful and not just noisy,
removes the "without the need to configure a model/thresholds" element somewhat.

So why continue developing Skyline?

The architecture/pipeline works very well at doing what it does.  It is solid and battle tested.
To overcome some of the limitations of Skyline.  This project extends it.

The new look of Skyline apps
----------------------------

* Horizon - feed metrics to Redis via a pickle input
* Analyzer - analyze metrics
* Mirage - analyze specific metrics at a custom time range
* Boundary - analyze specific timeseries for specific conditions
* Crucible - store anomalous timeseries resources and ad-hoc analysis of any
  timeseries
* Panorama - anomalies database and historical views
* Webapp - frontend to view current and histroical anomalies and browse Redis
  with :red:`re`:brow:`brow`

Skyline is still a near real-time anomaly detection system, however it has
various modes of operation that are modular and self contained, so that only the
desired apps need to be enabled.

Skyline can now be feed/query and analyze timeseries on an ad-hoc basis, on the
fly.  This means Skyline can now be used to analyze and process static data too,
it is no longer just a machine/app metric fed system.

A simplified workflow of Skyline
--------------------------------

.. figure:: images/skyline.simplified.workflow.annotated.gif
   :alt: A simplified workflow of Skyline

`Fullsize image <_images/skyline.simplified.workflow.annotated.gif>`_ for a clearer picture.

What's new
----------

See `whats-new <whats-new.html>`__ for a comprehensive overview and description
of the latest version/s of Skyline.

What's old
----------

It must be stated the original core of Skyline has not been altered in any way,
other than some fairly minimal Pythonic performance improvements, a bit of
optimization in terms of the logic used to reach :mod:`settings.CONSENSUS` and a
package restructure.  In terms of the original Skyline Analyzer, it does the
same things just a little differently, hopefully better and a bit more.

There is little point in trying to improve something as simple and elegant in
methodology and design as Skyline, which has worked so outstandingly well to
date.  This is a testament to a number of things, in fact the sum of all it's
parts, `Etsy`_, Abe and co. did a great job in the conceptual design,
methodology and actual implementation of Skyline and they did it with very good
building blocks from the scientific community.

The architecture in a nutshell
------------------------------
Skyline uses to following technologies and libraries at its core:

1. **Python** - the main skyline application language - `Python`_
2. **Redis** - `Redis`_ an in-memory data structure store
3. **numpy** - `NumPy`_ is the fundamental package for scientific computing with Python
4. **scipy** - `SciPy`_ Library - Fundamental library for scientific computing
5. **pandas** - `pandas`_ - Python Data Analysis Library
6. **mysql/mariadb** - a database - `MySQL`_ or `MariaDB`_
7. :red:`re`:brow:`brow` - Skyline uses a modified port of Marian
   Steinbach's excellent `rebrow`_

.. _Etsy: https://www.etsy.com/
.. _github/etsy: https://github.com/etsy/skyline
.. _whats-new: ../html/whats-new.html
.. _Python: https://www.python.org/
.. _Redis: http://Redis.io/
.. _NumPy: http://www.numpy.org/
.. _SciPy: http://scipy.org/
.. _pandas: http://pandas.pydata.org/
.. _MySQL: https://www.mysql.com/
.. _rebrow: https://github.com/marians/rebrow
.. _MariaDB: https://mariadb.org/
