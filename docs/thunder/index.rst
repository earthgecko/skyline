#######
Thunder
#######

.. toctree::

  thunder_analyzer.rst
  thunder_horizon.rst

.. role:: skyblue
.. role:: red

Thunder
=======

What monitors Skyline and how do you monitor Skyline?  What are the key things
that need to be monitored? What are the expected values?  All good questions.

Thunder is a Skyline app that monitors all the Skyline apps and Redis (mariadb,
memcached and Graphite to be added in the near future) and notifies on
significant changes to any of these components.  Thunder also monitors
operational changes in terms of volumes of metrics received, app run times, etc
and also notifies on these aspects.

Although Skyline will monitor all of these things via the normal analysis of
its own metrics, Thunder ensures that the key the operational aspects of all
the parts of the Skyline stack are performing as expected without the user
needing to configure alerts for lots metrics which they do not know.

Thunder can alert via SMTP, Slack and/or PagerDuty and as with all Skyline
things each check can be enabled/disabled and configured in
:mod:`settings.THUNDER_OPTS` and :mod:`settings.THUNDER_CHECKS`, the defaults
should be sufficient to start with. But because every deployed Skyline instance
will have different numbers of metrics being analysed, etc, many Thunder checks
use proportional values to determine if changes are significant, all these
values can be adjusted in :mod:`settings.THUNDER_CHECKS`

Thunder monitors that all enabled apps for DOWN and UP (recovered), each app
creates a Redis key for itself periodically to let all the other apps know that
it is UP.

Thunder monitors various aspects of each app and each of those are covered in
their own pages, but basically Thunder monitors:

- Apps and Redis are UP
- Significant changes in metric volumes
- Significant changes to run times and overruns
