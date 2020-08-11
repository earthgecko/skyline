#########
Upgrading
#########

.. toctree::

  py2-to-py3.rst
  crucible-to-ionosphere.rst
  etsy-to-ionosphere.rst
  etsy-to-crucible.rst

Backup your Redis!
##################

Do it.

Backup your database!
#####################

Do it.

Upgrading from version to version
#################################

All update procedures are documented per release in the relevant Releases page.
Please ensure you read and carry out as appropriate the steps in each version
update.  Some releases can skip through a few releases to go from v1.1.8 to
v.1.1.10 and some releases cannot, you cannot jump from v1.1.x to v.1.2.2 you
must go from v1.1.x to v1.2.0 and then v.1.2.2

Although it may be painful going through each version upgrade, it informs you
each time what has changed and been added.

In almost all cases one should be able to craft a single SQL from each version
that has one and diff your current ```settings.py``` with the new version
```settings.py``` and with a combination of stopping all Skyline services,
backing up DB, applying single SQL and switching to the new Skyline version code
and your new updated ```settings.py``` should be possible if the user does not
wish to run through the version updates sequentially.  However other than this
description, the steps and what SQL updates and additions and/or need to be
applied is left up to the user to determine.  The user should still read each
release page to ensure they are aware of if any changes are required to any of
the components related to Skyline, e.g. to the Apache/reverse proxy config,
Redis, Graphite, etc. or additions outside of Python and pip.

Upgrading from the Etsy version of Skyline
##########################################

This section covers the steps involved in upgrading an existing Skyline
implementation. For the sake of fullness this describes the changes from
the last
`github/etsy/skyline <https://github.com/etsy/skyline/commit/22ae09da716267a65835472da89ac31cc5cc5192>`__
commit to date.

Do a new install
================

If you are upgrading from an Etsy version consider doing a new install, all you
need is your Redis data (but BACK IT UP FIRST)

However you may have some other monitoring or custom inits, etc set up on your
Skyline then the below is a best effort guide.

Things to note
==============

This new version of Skyline sees a lot of changes, however although
certain things have changed and much has been added, whether these
changes are backwards incompatible in terms of functionality is
debatable. That said, there is a lot of change.  Please review the following
key changes relating to upgrading.

Directory structure changes
~~~~~~~~~~~~~~~~~~~~~~~~~~~

In order to bring Skyline in line with a more standard Python package
structure the directory structure has had to changed to accommodate
sphinx autodoc and the setuptools framework.  This will affect any configuration
management paths for a templated settings.py and/or to path to the Skyline bin
directory, etc.


settings.py
~~~~~~~~~~~

The ``settings.py`` has had a face lift too. This will probably be the
largest initial change that any users upgrading from Skyline < 1.0.0
will find.

The format is much less friendly to the eye as it now has all the
comments in Python docstrings rather than just plain "#"ed lines.
Apologies for this, but for complete autodoc coverage and referencing it
is a necessary change.

Analyzer optimizations
~~~~~~~~~~~~~~~~~~~~~~

There has been some fairly substantial performance improvements that may affect
your Analyzer settings.  The optimizations should benefit any deployment,
however they will benefit smaller Skyline deployments more than very large
Skyline deployments.  Finding the optimum settings for your Analyzer deployment
will require some evaluation of your Analyzer run_time, total_metrics and
:mod:`settings.ANALYZER_PROCESSES`.

See `Analyzer Optimizations <../analyzer-optimizations.html>`__ and regardless of
whether your deployment is small or large the new
:mod:`settings.RUN_OPTIMIZED_WORKFLOW` timeseries analysis will improve
performance and benefit all deployments.

Skyline graphite namespace for metrics
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The original metrics namespace for skyline was ``skyline.analyzer``,
``skyline.horizon``, etc. Skyline now shards metrics by the Skyline
server host name. So before you start the new Skyline apps when
referenced below in the Upgrade steps, ensure you move/copy your whisper
files related to Skyline to their new namespaces, which are
``skyline.<hostname>.analyzer``, ``skyline.<hostname>.horizon``, etc. Or
use Graphite's whisper-fill.py to populate the new metric namespaces
from the old ones.

Logging
~~~~~~~

There have been changes made in the logging methodology, see
`Logging <../logging.html>`__.

New required components
~~~~~~~~~~~~~~~~~~~~~~~

MySQL and memcached.

Clone and have a look
~~~~~~~~~~~~~~~~~~~~~

If you are quite familiar with your Skyline setup it would be useful to
clone the new Skyline and first have a look at the new structure and
assess how this impacts any of your deployment patterns, init,
supervisord, etc and et al.  Diff the new settings.py and your existing
settings.py to see the additions and all the changes you need to make.
