============
Requirements
============

The requirements are:

- First you need Graphite
- Then you need some things feeding Graphite metrics (see statsd, sensu, riemann
  buckyserver, nc)
- Linux (and probably any environment that supports Python virtualenv
  and bash)
- virtualenv
- Python-2.7.14 (running in an isolated vitualenv)
- Redis
- MySQL or mariadb [optional - required for Panorama]
- A Graphite implementation sending data would help :)

``requirements.txt``
####################

The ``requirements.txt`` file lists the required packages and the last
verified as working version with Python-2.7 within a virtaulenv.

Recent changes in the pip environment
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. note:: If you use pip or virtualenv in any other context, please note the
  following and assess if and/or how it pertains to you environment

The new pip, setuptools, distribute and virtualenv methodology that the
pypa community has adopted is in a bit of a state of flux in terms of
the setuptools version and virtualenv implementation of making the
``--distribute`` and ``--setuptools`` legacy parameters has caused pip
8.1.1 and setuptools 20.9.0 and virtualenv 15.0.1 to complain if
distribute is one of the requirements on packages that pip tries to
install. However, it is needed for Mirage if you are trying to
run/upgrade on Python-2.6

virtualenv 15.2.0 is the last version of virtualenv that supports Python 2.6,
virtualenv 16.0.0 requires Python 2.7

.. note:  Everything below this point on the page is for people upgrading.  It
  concerns the implications on/with older versions of things.

pandas
======

Updated: 20161120 - can no longer be supported as of Ionosphere (probably)

The ``PANDAS_VERSION`` variable was added to settings.py to handle
backwards compatability with any instances of Skyline that are run older
versions that perhaps cannot upgrade to a later version due to any
mainstream packaging restrictions. This ``PANDAS_VERSION`` is required
to run the applicable panda functions dependent on the version that is
use.

-  pandas 0.17.0 deprecated the pandas.Series.iget in favour of
   ``.iloc[i]`` or ``.iat[i]``
-  pandas 0.18.0 introduced a change in the Exponentially-weighted
   moving average function used in a number of algorithms

.. code-block:: python

        if PANDAS_VERSION < '0.18.0':
            expAverage = pandas.stats.moments.ewma(series, com=15)
        else:
            expAverage = pandas.Series.ewm(series, ignore_na=False, min_periods=0, adjust=True, com=15).mean()

        if PANDAS_VERSION < '0.17.0':
            return abs(series.iget(-1)) > 3 * stdDev
        else:
            return abs(series.iat[-1]) > 3 * stdDev

Skyline should be able to run on pandas versions 0.12.0 - 0.18.0 (or
later)

Python-2.6 - may work, but not supported
========================================

Skyline can still run on Python-2.6. However deploying Skyline on
Python-2.6 requires jumping through some hoops. This is because of the
dependencies and pip packages moving a lot. At some point some older pip
package is not going to be available any longer and it will no longer be
possible, unless you are packaging the pip packages into your own packages, e.g.
with fpm or such.

Updated: 20161120 - can no longer be supported as of Ionosphere (probably)

RedHat family 6.x
=================

If you are locked into mainstream versions of things and cannot run
Skyline in an isolated virtualenv, but have to use the system Python and
pip. The following pip installs and versions are known to working, with
a caveat on the scipy needs to be installed via yum NOT pip and you need
to do the following:

Updated: 20161120 - this information is now too old to be applicable really.

.. code-block:: bash

    pip install numpy==1.8.0
    yum install scipy
    pip install --force-reinstall --ignore-installed numpy==1.8.0

Known working scipy rpm - scipy-0.7.2-8.el6.x86\_64

pip packages (these are the requirements and their dependencies), it is
possible that some of these pip packages will no longer exist or may not
exist in the future, this is documented here for info ONLY.

::

    argparse (1.2.1)
    backports.ssl-match-hostname (3.4.0.2)
    cycler (0.9.0)
    distribute (0.7.3)
    Flask (0.9)
    hiredis (0.1.1)
    iniparse (0.3.1)
    iotop (0.3.2)
    Jinja2 (2.7.2)
    lockfile (0.9.1)
    MarkupSafe (0.23)
    matplotlib (1.5.0)
    mock (1.0.1)
    msgpack-python (0.4.2)
    nose (0.10.4)
    numpy (1.7.0)
    ordereddict (1.2)
    pandas (0.12.0)
    patsy (0.2.1)
    pip (1.5.4)
    pycurl (7.19.0)
    pygerduty (0.29.1)
    pygpgme (0.1)
    pyparsing (1.5.6)
    python-daemon (1.6)
    python-dateutil (2.3)
    python-simple-hipchat (0.3.3)
    pytz (2014.4)
    redis (2.7.2)
    requests (1.1.0)
    scipy (0.7.2)
    setuptools (11.3.1)
    simplejson (2.0.9)
    six (1.6.1)
    statsmodels (0.5.0)
    tornado (2.2.1)
    unittest2 (0.5.1)
    urlgrabber (3.9.1)
    Werkzeug (0.9.4)
    yum-metadata-parser (1.1.2)
