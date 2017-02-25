Ionosphere
==========

Ionosphere is a work in progress.  EXPERIMENTAL

The Ionosphere service is responsible for recording the timeseries for each
anomaly to enable the a human to intervene and help Skyline "learn" things that
are not anomalous.

Ionosphere records the timeseries for each anomaly to disk (preferably SSD) and
stores them for X.  The operator/user/teacher can via the Ionosphere webapp UI
match and review any Skyline alert and flag the timeseries as not anomalous.

See `Development - Ionosphere <development/ionosphere.html>`__
