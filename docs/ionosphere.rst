Ionosphere
==========

Ionosphere is a work in progress.  EXPERIMENTAL

The Ionosphere service is records training data for every anomalous metric that
is alerted on. This training data is available in the Webapp UI and Ionosphere
keeps the training data sets for the amount of time specified by
:mod:`settings.IONOSPHERE_KEEP_TRAINING_TIMESERIES_FOR` to allow a human to
intervene and help Skyline "learn" things that are not anomalous.

This training data consists of:

- The metric check file with all the anomaly details.
- The exact timeseries data which triggered the alert.
- Related timeseries data (e.g. Redis data at
  :mod:`settings.IONOSPHERE_KEEP_TRAINING_TIMESERIES_FOR` if the anomaly was
  triggered by Mirage).
- Related Graphite graph and Redis plot images.

.. note:: Ionosphere only handles metrics from Analyzer and Mirage which have a
  smtp alert defined in :mod:`settings.ALERTS`

How Ionosphere works - simple overview
--------------------------------------

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
Thereafter, when both Analyzer and Mirage detect an anomaly for the metric, they
will just save the training data and anomaly details and defer alerting on the
anomaly to Ionosphere.

A metric can have more than one features profile.

Ionosphere analysis
^^^^^^^^^^^^^^^^^^^

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
^^^^^

When an anomaly alert is sent out via email, a link to the Ionosphere training
data is included in the alert.  This link opens the Ionosphere UI with the all
training data for the specific anomaly where the user can submit the metric
timeseries as not anomalous and generate have Skyline generate a features
profile with tsfresh.

features profiles
^^^^^^^^^^^^^^^^^

When a training data set is submitted as not anomalous for a metric a features
profile is extracted from the timeseries using tsfresh.  This features profile
contains the about values of 215 features (currently as of tsfresh-0.3.0), such
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

These tables are prefixed with z_ so that they are all listed after all core
Skyline database tables.  Once a metric has a z_fp_<metric_id> and a
z_ts_<metric_id>, these tables are updated any future features profiles and
timeseries data.  So there is are 2 tables per metric, not tables per features
profile.

MySQL considerations
^^^^^^^^^^^^^^^^^^^^

There could be a lot of tables.




See `Development - Ionosphere <development/ionosphere.html>`__
