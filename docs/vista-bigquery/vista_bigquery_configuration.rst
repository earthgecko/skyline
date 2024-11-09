================================
Vista - BigQuery - Configuration
================================

This section describes the configuration required for enabling fetching data
from BigQuery.

After deploying a skyline-bq virtualenv and installing the isolated BigQuery
related packages you need to carry out the following steps.

There are two aspects to enabling BigQuery data:

- Define settings for a periodic BigQuery data retrieval with the schedule disabled
- Backfill Skyline with BigQuery data [optional]
- Test BigQuery data fetching with a test backfill job [optional]
- Enable the schedule for the periodic BigQuery data retrieval

You do not have to backfill BigQuery data to Skyline, you can just define a job
and schedule and start with current data, which will become more populated as
time goes by.

However even if you do not run a backfill job do run at least a test backfill
job to ensure your settings are valid and data can be fecthed and ingested via
flux.  The test will also allow you to validate the metrics names are as desired
and the data is as expected.

Updating previous data
----------------------

There are data that may not be updated in real time and results in changes in
values to for previous exisitng timestamps.  This type of situation can be a
result of data being submitted by 3rd parties and either providing new data for
some previous timestamps that did not have data or providing new/changes to
values for previous timestamps.  In terms of anomaly detection this is quite
problematic.



Recommendations
---------------


::python

    VISTA_BQ_VIRTUALENV = '/opt/python_virtualenv/projects/skyline-bq/lib/python3.10/site-packages'
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
            'date_field': 'dt',
            'date_field_format': '%Y-%m-%d',
            'primary_dimension': 'acc_id',
            'metric_dimensions': ['requests', 'clickthru', 'signups'],
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

Credentials
-----------

Credentials can either be passed to Skyline in a dict format directly declared
in the settings or via a json file that the Skyline system user has read
permissions on.

oauth_credentials
~~~~~~~~~~~~~~~~~

If you are using oauth credentials to authenticate with BigQuery the following
example dict/json shows what is required in the dict or json file.

::json

    {
        "token": "xxxx",
        "refresh_token": "xxxx",
        "token_uri": "https://oauth2.googleapis.com/token",
        "client_id": "xxxx.apps.googleusercontent.com",
        "client_secret": "xxxx"
    }


service_credentials
~~~~~~~~~~~~~~~~~~~

If you are using service account credentials to authenticate with BigQuery the
following example dict/json shows what is required in the dict or json file.

::json

    {
        "type": "service_account",
        "project_id": "xxxx",
        "private_key_id": "xxxx",
        "private_key": "-----BEGIN PRIVATE KEY-----\nxxxx\nxxxx\nxxxx\nxxxx\nxxxx\nxxxx\n...\n-----END PRIVATE KEY-----\n",
        "client_email": "xxxx@xxxx.iam.gserviceaccount.com",
        "client_id": "xxxx",
        "auth_uri": "https://accounts.google.com/o/oauth2/auth",
        "token_uri": "https://oauth2.googleapis.com/token",
        "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
        "client_x509_cert_url": "https://www.googleapis.com/robot/v1/metadata/x509/xxxx%40xxxx.iam.gserviceaccount.com",
        "universe_domain": "googleapis.com"
    }
