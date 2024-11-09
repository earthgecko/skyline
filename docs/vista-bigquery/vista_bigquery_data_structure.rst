=================================
Vista - BigQuery - Data Structure
=================================

This page describes the data structure that Skyline requires for BigQuery data.

It is recommended that you create a specific "view" or table of only the data
you want to fetch on BigQuery.  This means that ideally you should have a table
or query on BigQuery that only holds or returns the data which you want Skyline
to ingest.  The goal here is to keep the query and interpolation of the data as
simple as possible.  This also means that the Google credentials you create for
Skyline to use to query can be limited to this table only.

The preferred authentication method is to use a service account that only has
access to the specific table.  However it is possible to have a user added by
email address and and then for that user to create their own oauth credentials
but that is **very** complicated and requires provisioning your own oauth app to
generate the credentials (not for the faint-hearted).  Whether it is a service
account or a user email using oauth access both these types of access require
the ``roles/bigquery.dataViewer`` permission on the table to access the data.
How to add these resources are documented by Google please refer to Google
documentation for this.

It is up to you to prepare the required data/table on BigQuery that meets the
following requirements.

- The table must have a DATE or TIMESTAMP column.
- The table must have a primary_dimension column (which metrics will be keyed on).
- The table must have at least one or more additional columns of type INTEGER or
  FLOAT (a column for each dimension that you want a metric for).

Although it is possible to have a query that just selects all columns from an
exisitng table for a time period and the metric_dimensions on which metrics are
created in passed in a list in the configuration.  But to reduce the access and
data surfaces a table specific for the purpose is recommended.

Here is an **example** of an ideal table/dataframe structure and data:

::

    +---+------------+--------+----------+-----------+---------+
    |   |   date     | acc_id | requests | clickthru | signups |
    +===+============+========+==========+===========+=========+
    | 0 | 2024-05-09 | 345    | 16       | 1         | 0       |
    +---+------------+--------+----------+-----------+---------+
    | 1 | 2024-05-09 | 14     | 1408     | 89        | 3       |
    +---+------------+--------+----------+-----------+---------+
    | 2 | 2024-05-09 | 167    | 48       | 2         | 0       |
    +---+------------+--------+----------+-----------+---------+
    | 3 | 2024-05-09 | 489    | 98       | 5         | 1       |
    +---+------------+--------+----------+-----------+---------+
    | 4 | 2024-05-09 | 31     | 309      | 3         | 0       |
    +---+------------+--------+----------+-----------+---------+
    | 5 | 2024-05-09 | 211    | 484      | 17        | 2       |
    +---+------------+--------+----------+-----------+---------+
    | 6 | 2024-05-09 | 180    | 843      | 91        | 7       |
    +---+------------+--------+----------+-----------+---------+
    | 7 | 2024-05-09 | 59     | 0        | 0         | 0       |
    +---+------------+--------+----------+-----------+---------+
    | 8 | 2024-05-09 | 226    | 692      | 19        | 12      |
    +---+------------+--------+----------+-----------+---------+


The following settings would be added to fetch the previous day's data once a
day at 01:00:00 UTC (there is a detailed description of the
:mod:`settings.VISTA_BQ_ACCOUNTS` settings on the configuration page).

::python

    VISTA_BQ_ACCOUNTS = {
        'my_bq_data': {
            'flux_api_token': 'xxxxx',
            'oauth_credentials': {},
            'oauth_credentials_file': '/path/to/file/<credentials_filename>.json',
            'service_credentials': {},
            'service_credentials_file': None,
            'project_id': 'account-data-prod',
            'query': 'SELECT * FROM `account-data-prod.skyline.account_activity` WHERE date = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)',
            'min_expected_results': 900,
            'metric_format': 'graphite',
            'metric_prefix': 'customer.account_activity',
            'primary_dimension': 'acc_id',
            'metric_dimensions': ['requests', 'clickthru', 'signups'],
            'labelled_metrics_key_by': 'primary_dimension',
            'schedule': {'minute': 0, 'hour': 1, 'day_of_month': '*', 'month': '*', 'day_of_week': '*'},
            'schedule_interval': 86400,
            'full_duration': 31536000,
            'stale_period': 604800,
            'resolution': 86400,
            'batch_processing_namespace': 'customer.account_activity',
            'non_derivative_namespaces': ['customer.account_activity'],
            'zero_fill_analysis': True,
        }
    }

This would result in the query being run at 01:00 UTC daily against BigQuery to
fetch the previous day's values for the acc_ids.  This data is then parsed
and submitted to Skyline via flux as the following metrics which would be
written to Redis and Graphite:

::

    customer.account_activity.345.requests 1715209200 16
    customer.account_activity.345.clickthru 1715209200 1
    customer.account_activity.345.signup 1715209200 0
    customer.account_activity.14.requests 1715209200 1408
    customer.account_activity.14.clickthru 1715209200 89
    customer.account_activity.14.signup 1715209200 3
    ...

