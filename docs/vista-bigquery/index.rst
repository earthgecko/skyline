.. role:: skyblue
.. role:: red

Vista - BigQuery
================

Vista can be used to fetch data from BigQuery and submit it to Skyline as
metrics for analysis and monitoring.

This functionality is limited to adding metrics to Graphite as normal Graphite
dotted namespace metrics.  It is currently not possible to add metrics from
BigQuery data to VictoriaMetrics as labelled key/value formatted (Prometheus
style) metrics.

It is probable that data from BigQuery will not be real time data with a high
frequency and the data will probably be more suited to much longer term storage
and require batch processing.  Therefore using Graphite for the metrics is
preferable to enable long retention and to facilitate batch processing.  Batch
processing is not currently available for labelled metrics.

The keys aspects to this Vista functionality are:

- The BigQuery table being queried should be fit for purpose.
- Skyline requires an additional isolated Python virtualenv for Google packages.
- Testing and backfilling data before fetching current data is possible [optional].
- Google authentication resources are required.
- The required configuration to enable Vista to fetch and process BigQuery data
  needs to be implemented.

Step 1: prepare the BigQuery side of things and create a service account or oauth credentials which has access to the data see 
Step 2: create a skyline-bq virtualenv
Step 3: define the fetch and analysis parameters in :mod:`settings.VISTA_BQ_ACCOUNTS` and other Vista and BigQuery related settings and restart the services
Step 4: run a test and/or backfill with a Utilities /bq_backfill job [optional]
Step 5: enable the schedule for the ongoing analysis in :mod:`settings.VISTA_BQ_ACCOUNTS` and restart vista
Step 6: define any alerts you want by adding relevant tuples to :mod:`settings.ALERTS`
Step 7: define any boundary metrics or custom_algorithms you a want run on any of the BigQuery namespace metrics [optional]

There are a number of considerations and requirements for BigQuery and each is
covered in order and detail in the page links below.


.. toctree::

  vista_bigquery_data_structure.rst
  vista_bigquery_virtualenv.rst
  vista_bigquery_backfill.rst
  vista_bigquery_configuration.rst
