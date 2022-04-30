.. role:: skyblue
.. role:: red

Ionosphere echo
===============

Ionosphere echo is a extension of Ionosphere specifically for Mirage metircs.

The initial implementation of Ionosphere regarding Mirage metrics, only created
features profiles based on the Mirage ``SECOND_ORDER_RESOLUTION_SECONDS`` time
series.  If Ionosphere echo is enabled, Ionosphere now also automatically
creates a features profile for Mirage metrics based on the
:mod:`settings.FULL_DURATION` data.

Testing has shown that Ionosphere echo makes Ionosphere matching between 30% to
50% more effective on Mirage metrics (depending on the metric).  Ionosphere echo
also implements Minmax scaling analysis on the :mod:`settings.FULL_DURATION`
data.

The Ionosphere echo analysis is run between the normal Mirage features profiles
analysis and the layers analysis, so the analysis pipeline with Ionosphere echo
enabled is as follows:

- Ionosphere finding similar matching motifs for existing
  ``SECOND_ORDER_RESOLUTION_SECONDS`` features profiles time series and
  :mod:`settings.FULL_DURATION` features profiles
- Ionosphere comparison to Mirage ``SECOND_ORDER_RESOLUTION_SECONDS`` features
  profiles and minmax scaled features profiles
- Ionosphere echo comparison to Mirage :mod:`settings.FULL_DURATION` features
  profiles
- Ionosphere comparison to Mirage :mod:`settings.FULL_DURATION` minmax scaled
  features profiles
- Ionosphere layers comparisons
