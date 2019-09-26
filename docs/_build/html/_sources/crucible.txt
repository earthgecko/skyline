========
Crucible
========

Crucible is an extension of Skyline based on Abe Stanway's
`Crucible <https://github.com/astanway/crucible>`_ testing suite.  Crucible has
been integrated into Skyline as a module and daemon to allow for the following:

- Allowing for the ad-hoc analysis of a time series and generation of resultant
  resources (e.g. plot images, etc).
- Allowing for the ad-hoc analysis of a time series by a specific or adhoc
  defined algorithm and generation of resultant resources (e.g. plot images,
  etc).
- For storing the individual time series data for triggered anomalies.

Be forewarned that Crucible can generated a substantial amount of data in the
time series json archives, especially if it is enabled in any of the following
contexts:

* :mod:`settings.ANALYZER_CRUCIBLE_ENABLED`
* :mod:`settings.MIRAGE_CRUCIBLE_ENABLED`
* :mod:`settings.BOUNDARY_CRUCIBLE_ENABLED`

It should not be enabled in any of the any settings unless it is for development
or testing purposes.

Usage
=====

Crucible is not meant for generic Analyzer, Mirage or Boundary inputs although
it does works and was patterned using data fed to it from Analyzer.  Crucible is
aimed at having the ability to add miscellaneous time series and algorithms in
an ad-hoc manner.  Enabling the use of data sources other than Graphite and
Redis data for testing or anomaly detecting on and for testing algorithms in an
ad-hoc manner as well.

Crucible is enabled by default in its own settings block in ``settings.py``, but
it disabled default in each of the abovementioned app settings.

Manually creating check files
------------------------------

To analyze metrics or data on an ad-hoc basis, Crucible needs to be initiated
with a check file.  The check file contains variables instructing Crucible what
to check.

The crucible check file has the following lines, each line being a variable:

- metric [required] - the metric name
- value [required] - the anomalous value or 0
- from_timestamp [required] - the from unix timestamp for Graphite, can be 0 if
  the graphite_metric and graphite_override_uri_parameters are passed with the
  timestamp
- metric_timestamp [required] - the from unix timestamp for Graphite, can be 0
  if the graphite_metric and graphite_override_uri_parameters are passed with
  the timestamp
- algorithms [optional] - what algorithms to run, if not passed defaults to
  ['all'] which runs all Skyline algorithms define in :mod:`settings.ALGORITHMS`
  and the :py:func:`skyline.crucible.crucible_algorithms.detect_drop_off_cliff <crucible.crucible_algorithms.detect_drop_off_cliff>`
  algorithm.
- anomaly_dir [required] - the directory in which to find the data and save
  outputted resources.  It will be created if it does not exist.
- graphite_metric [optional] - True or False
- run_crucible_tests [optional] - True or False
- added_by [optional] - string
- graphite_override_uri_parameters [optional] - True or False, default is False
  if set to True graphite_metric must be set to True too.

If the graphite_override_uri_parameters variable is set in the check file the
graphite_metric variable must be set to True for it be take effect.  If the
graphite_override_uri_parameters is set Crucible will ignore the from_timestamp
and until_timestamp and use the passed graphite_override_uri_parameters URI
parameters.  graphite_override_uri_parameters can be set to surface any Graphite
metric with any Graphite function/s applied.  An example of a
graphite_override_uri_parameters would be:

```"from=00%3A00_20190527&until=23%3A59_20190612&target=movingMedian(nonNegativeDerivative(stats.machine-1.vda.readTime)%2C24)"```

The full graphite URL and render URI parameter must not be declared as Crucible
will construct the full URL using normal settings.py GRAPHITE variables.

Here are some examples of check files:

.. code-block:: python

    metric = "stats.machine-1.vda.readTime"
    value = "123.24"
    from_timestamp = "1558911600"
    metric_timestamp = "1560380399"
    algorithms =  ["histogram_bins", "first_hour_average", "stddev_from_average", "grubbs", "ks_test", "mean_subtraction_cumulation", "median_absolute_deviation", "stddev_from_moving_average", "least_squares"]
    anomaly_dir = "/opt/skyline/mirage/data/stats.machine-1.vda.readTime"
    graphite_metric = True
    run_crucible_tests = True
    added_by = "earthgecko"
    graphite_override_uri_parameters = "from=00%3A00_20190527&until=23%3A59_20190612&target=movingMedian(nonNegativeDerivative(stats.zpf-watcher-prod-1-30g-doa2.vda.readTime)%2C24)"



Why use `.txt` check files
--------------------------

The rationale behind offloading checks.

A number of the Skyline daemons create txt check and metadata files, and json
time series files.

For example Analyzer creates txt check files for Mirage, Crucible and Panorama.
These txt files are created in logical directory structures that mirror
Graphite's whisper storage directories for stats, etc.  Often other time series
data sets that are not Graphite, machine or app related metrics, are also
structured in a directory or tree structure and follow similar naming
convention, which allows for this tree and txt files design to work with a large
number of other time series data sets too.

Although there is an argument that checks and their related metadata could also
be queued through Redis or another queue application, using the local filesystem
is the simplest method to pass data between the Skyline modules, without
introducing additional dependencies.

While ensuring that Redis is being queried as minimally as required to do
analysis.  The shuffling of data and querying of "queues" is offloaded to the
filesystem.  Resulting in each module being somewhat autonomous in terms of
managing its own work, decoupled from the other modules.

Today's filesystems are more than capable of handling this load.  The use of txt
files also provides an event history, which transient Redis data does not.

Importantly in terms of Crucible ad-hoc testing, txt and json time series files
provide a simple and standardised method to push ad-hoc checks into Crucible.

What **Crucible** does
======================

Crucible has 3 roles:

1. Store resources (timeseries json and graph pngs) for triggered anomalies.
2. Run ad-hoc analysis on any timeseries and create matplotlib plots for the
   run algorithms.
3. To update the Panorama database (tbd for future Panorama branch)

Crucible can be used to analyse any triggered anomaly on an ad-hoc basis.  The
timeseries is stored in gzipped json for triggered anomalies so that
retrospective full analysis can be carried out on a snapshot of the timeseries
as it was when the trigger/s fired without the timeseries being changed by
aggregation and retention operations.

Crucible can create a large amount of data files and require significant disk
space.
