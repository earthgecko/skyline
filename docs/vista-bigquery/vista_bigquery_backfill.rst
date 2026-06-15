=======================================
Vista - BigQuery - Testing and Backfill
=======================================

This section describes the steps required to test and/or backfill Skyline with
data from BigQuery.

After deploying a skyline-bq virtualenv and installing the isolated BigQuery
related packages and preparing the table/query and access credentials for
BigQuery you need to carry out the following steps to test and/or backfill data
from BigQuery.

Even if you do not wish to backfill Skyline with data from BigQuery you can
still use the backfill functionality to testing the data fetching and submission
process **before** enabling a fecth process.

Please have the Skyline webapp UI Utilites BigQuery backfill tab open in your
browser to refer to while working through this page.

It is recommended that you change the following variables in settings.py to
values similiar to the following but which are relevant to your data:

.. code-block:: bash

    BATCH_PROCESSING = True

    BATCH_PROCESSING_STALE_PERIOD = <HOW_OLD_THE_DATA_MUST_BE_TO_BE_DEEMED_TO_OLD_TO_ANALYSE>

    BATCH_PROCESSING_DEBUG = True

    BATCH_PROCESSING_NAMESPACES = ['customer.account_activity']

    ROOMBA_DO_NOT_PROCESS_BATCH_METRICS = True

    ROOMBA_BATCH_METRICS_CUSTOM_DURATIONS = [['customer.account_activity', 31536000]]

    BATCH_METRICS_CUSTOM_FULL_DURATIONS = {'customer.account_activity': 31536000}

    VISTA_ENABLED = True

    VISTA_BQ_ACCOUNTS = {}  # see

Unfortunately you will have to have fair idea of what the metrics you are
ingesting are like, in terms of monotonicity.

You will also require a number of terminal windows open so you can tail -f the
following logs during the process:
- /var/logs/skyline/vista.log
- /var/logs/skyline/flux.log
- /var/logs/skyline/analyzer.log
- /var/logs/skyline/analyzer_batch.log

Testing
~~~~~~~

Because this is a complex process with complicated settings variables you will
want to test.  The webapp UI Utilities page Backfill BigQuery function (or 
direct via the /bq_backfill Admin endpoint) allows you to specify a job and run
it as dry_run to test with.

When a job is run as dry_run and flux_test the following processes occur:
- The job is added to the Vista work queue (Redis hash vista.bq_backfill.work)
- Vista then spawns a bq_backfill process
- The bq_backfill process determines the intervals which need to be fetched and
  iterates through each one and does the following
  - Fetches the data from BigQuery for the interval (saves to archive csv if enabled)
  - Converts the data to metrics
  - POSTs the metrics to flux
  - flux will accept the data and report what was accepted but will not process it

Backfilling is handled via the webapp UI under Utilities tab (or directly via
/bq-backfill with a JSON payload of the variables required).  It is important to
understand that the backfilling variables that are required are tied to the
:mod:`settings.VISTA_BQ_ACCOUNTS` configuration that you will define for the
ongoing data retrieval and analysis.  To ensure that the configuration used for
a backfilling task is the same as the configuration that is to be used for
ongoing retrieval, a backfill job will only be run if a matching key is found in
the :mod:`settings.VISTA_BQ_ACCOUNTS` dict in settings.py

So first you need to add a dict for the job to :mod:`settings.VISTA_BQ_ACCOUNTS`
and **most importantly** set the schedule elements all to 0, which disables the
job but allows it to be declared and present. This is equally applicable to
external_settings should you happen to use those.

In terms of Graphite you need to ensure that the value you pass for
``max_creates_per_minute`` for the job aligns with the ``MAX_CREATES_PER_MINUTE``
that is defined in carbon.conf.  If these are not algined carbon will drop
metrics.  During the job run keep an eye on the Graphite's own ``creates`` and
``droppedCreates`` metrics. 

It is recommended that you run the backfill in 2 stages:

- First stage run a main backfill job from the start of the date up until 3
  intervals **BEFORE** the current data.  Allow Analyzer to process the metrics
  and assign them as batch metrics and record the last timestamps for the
  metrics.  Here you are going to watch the tail -f on the logs to ensure that
  the metrics are being processed by analyzer and analyzer_batch.  Review the logs.
- Wait for at least 10 minutes for the analyzer/metrics_manager process to
  update all the internal data sets, etc  and review the analyzer.log grepping
  for ``"metrics_manager\|error :: "``
- Second stage run - this time set the replace_redis_timeseries to false, set
  the from_timestamp using the until_timestamp from the first stage backfill job
  e.g.to (until_timestamp + interval) and set the until_timestamp for this
  backfill job to (current_interval_timestamp - interval) the
  current_interval_timestamp being the most recent timestamp in BigQuery. Run
  the job and watch the logs.

At this point Skyline will have added the metrics internally and you are ready
to enable the schedule in your :mod:`settings.VISTA_BQ_ACCOUNTS` (or
external_settings) and restart Vista.

The backfill job will be run using the variables defined in the settings (or
external_settings), with the exception of the date conditional being replaced
with the valid DATE_FORMAT at each interval.  Once the backfill job is complete,
you can then set the required values for the schedule in
:mod:`settings.VISTA_BQ_ACCOUNTS` (or external_settings) and restart Vista.

