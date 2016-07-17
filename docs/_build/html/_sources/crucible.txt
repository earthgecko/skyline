========
Crucible
========

Crucible is an extension of Skyline based on Abe Stanway's
`Crucible <https://github.com/astanway/crucible>`_ testing suite.  Crucible has
been integrated into Skyline as a module and daemon to allow for the following:

- Allowing for the ad-hoc analysis of a timeseries and generation of resultant
  resources (e.g. plot images, etc).
- Allowing for the ad-hoc analysis of a timeseries by a specific or adhoc
  defined algorithma nd generation of resultant resources (e.g. plot images,
  etc).
- For storing the individual timeseries data for triggered anomalies.

Be forewarned that Crucible can generated a substantial amount of data in the
timeseries json archives, especially if it is enabled in any of the following
contexts:

* :mod:`settings.ANALYZER_CRUCIBLE_ENABLED`
* :mod:`settings.MIRAGE_CRUCIBLE_ENABLED`
* :mod:`settings.BOUNDARY_CRUCIBLE_ENABLED`

Usage
=====

Crucible is not necessarily meant for generic Analyzer, Mirage or Boundary
inputs although it does works and was patterned using data fed to it from
Analyzer.  Crucible is more aimed and having the ability to add miscellaneous
timeseries and algorithms in an ad-hoc manner.  Enabling the use of data
sources other than Graphite and Redis data and testing alogrithms in an ad-hoc
manner too.

Crucible is enabled by default in its own settings block in ``settings.py``, but
it disabled default in each of the apps own settings.

Why use `.txt` check files
--------------------------

The rationale behind offloading checks.

A number of the Skyline daemons create txt check and metadata files, and json
timeseries files.

For example Analyzer creates txt check files for Mirage, Crucible and Panorama.
These txt files are created in logical directory structures that mirror
Graphite's whisper storage directories for stats, etc.  Often other timeseries
data sets that are not Graphite, machine or app related metrics, are also
structured in a directory or tree structure and follow similar naming
convention, which allows for this tree and txt files design to work with a large
number of other timeseries data sets too.

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

Importantly in terms of Crucible ad-hoc testing, txt and json timeseries files
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
