==================================
upload_data to Flux - EXPERIMENTAL
==================================

**POST variables may change as this is a experimental feature**

**SIMPLE** data files can be uploaded via the /upload_data HTTP/S endpoint for
Flux to process and fed to Graphite.  A number of things need to be enabled and
running to allow for processing data file uploads, which are not enabled by
default.

For information regarding configuring Skyline to allow Flux to process uploaded
data files see the `Process uploaded data <flux.html#process-uploaded-data>`__
section on the Flux page.

Skyline currently allows for the uploading of the following format data files:

- csv (tested)
- xlsx (tested)
- xls (not tested)

Seeing as data files can be large, the following archive formats are accepted:

- gz (tested)
- zip (partly tested, multiple data files per archive have not been thoroughly tested)

A single file or archive can be uploaded or many data files can be uploaded in
a single archive.  A `info.csv` must also be included in the archive, more
on that below.  **HOWEVER** if you wish to determine the status of the upload
programmatically you will want to upload one data file per upload, otherwise
making sense of the upload_status will be very difficult.

So you could upload `data.csv`, `data.csv.gz` or `data.zip` with the `data.csv`
file inside the zip archive.

Any files in an archive that are not of an allowed format are not extracted or
they are deleted.  Try and submit one data file per upload as ordering has not
been tested.

Multiple data files should be uploaded and processed sequentially.  At the
moment upload the oldest data file first and then after validating the status of
the upload (the /api?upload_status link is return on the Flux_frontend page or
in the json response), continue with the next upload, etc.

Skyline automates the conversion of SIMPLE columnar data into time series data,
however seeing as not all data is defined or created equally, some information
about the data needs to be passed with the data file/s to inform Skyline about
the metric structure.  This makes setting up a upload process more work, but if
you are uploading frequently then it is one off work.

To ensure that naive data in the datetime column  can be handled, meaning not
timezone aware date times e.g. `16-05-2020 11:00` you always need to pass a
valid pytz timezone as listed in `Timezones list for pytz version <development/pytz.html#timezones-list-for-pytz-version>`__.
A timezone aware timestamp either specifies the UTC offset or the timezone
itself.

- 2020-05-16 14:00:00 -04:00
- 2020-05-16 07:46:25 BST 2020

However the timezone itself may not be a valid pytz timezone, so a valid pytz
timezone must always be passed.

In many instances it is possible that the column names and format in a data file
will not describe the measurements in terms of metrics name or be in a suitable
metric name format.  Take a csv example of a very normal type of data structure,
lets call it 2020-05-16-11.device_id.012383.csv

::

    Device ID,012383
    Serial Number,1234579853
    Location,Warehouse 2
    From,16/05/2020
    To,16/05/2020
    Date,Roof Temperature,Floor Temperature
    16-05-2020 11:00,45.72,22.78
    16-05-2020 11:15,45.94,22.92
    16-05-2020 11:30,46.13,22.98
    16-05-2020 11:45,46.34,23.06

This data informs us of the times and values and things, but it does not tell us
what metrics they should represent.  Skyline also needs to be informed about the
header row and rows to ignore.  A info file is used to inform Skyline how to
read and metric the data, take 2020-05-16-11.device_id.012383.info.json
Note, in processing rows are 0-indexed

info.json

::

    {
      "parent_metric_namespace": "temp_monitoring.warehouse.2.012383",
      "timezone": "GMT",
      "skip_rows": 5,
      "header_row": 0,
      "date_orientation": "rows",
      "columns_to_metrics": "date,roof,floor"
    }

Your date time column MUST be named date in the columns_to_metrics mapping.

For convenience sake you can also add additional elements to the info.json:

- `"debug": "true"` which outputs additional information regarding the imported
  dataframe in the flux.log to aid with debugging.
- `"dryrun": "true"` which runs through the processing but does not submit data
  to Graphite.
- `"ignore_submitted_timestamps": "true"`, a check is normally done of the last
  timestamp submitted to flux for the metric to ensure that data is not
  submitted multiple times. If you wish to override this check to resubmit data,
  update or override already submitted data pass this in the info.json. However
  do **note**,  if you wish to resubmit data that is **NOT IN THE** latest
  Graphite retention (old data) this will not have the desired affect as
  Graphite down sampling (aggregation) will already have occurred.

This tells Skyline what the parent metric namespace should be which would
result in metrics:

- temp_monitoring.warehouse.2.012383.roof
- temp_monitoring.warehouse.2.012383.floor

xlsx files are 0-indexed, csv files are not 0-indexed.

It tells Skyline to ignore rows 1, 2, 3, 4, 5 (but if it were 0-indexed skip_row
would be set to 4).
It tells Skyline to use row 0 as the header row, e.g. column names. Note that
if you skip_rows your header row must be 0.
It tells Skyline how to map the column names to metric names.  A one to one
mapping it required for every column.  Once again, your date time column MUST be
named date in the columns_to_metrics mapping.

Only alphanumeric chars and '.', '_', '-' are allowed in the metric name, e.g.
the parent_metric_namespace and columns_to_metrics that you pass.

Requirements of the data file.  The data file must have a header row.

The required information elements are in the POST variables are:

- key (str)
- parent_metric_namespace (str)
- archive (str) - gz, zip or tar_gz
- format (str)
- skip_rows ('none' or int)
- header_row (int)
- date_orientation (str) - currently only 'rows' is supported
- data_file (required in the post variables)
- columns_to_metrics (str) - comma separated list of names (no spaces)
- data_file (file)
- info_file (file)
- json_response

An example of how to POST the above csv and info.csv with curl be would be as
follows.  Note that in this instance you would need a your
:mod:`settings.FLUX_UPLOADS_KEYS` to be set with:

.. code-block:: python

    FLUX_UPLOADS_KEYS = {
        'temp_monitoring.warehouse.2.012383': '484166bf-df66-4f7d-ad4a-9336da9ef620',
    }

curl request.

.. code-block:: bash

    curl \
         -F "key=484166bf-df66-4f7d-ad4a-9336da9ef620" \
         -F "timezone=GMT" \
         -F "parent_metric_namespace=temp_monitoring.warehouse.2.012383" \
         -F "archive=none" \
         -F "format=csv" \
         -F "skip_rows=5" \
         -F "header_row=0" \
         -F "date_orientation=rows" \
         -F "columns_to_metrics=date,roof,floor" \
         -F "data_file=@<FULL_PATH_TO_FILE>/2020-05-16-11.device_id.012383.csv" \
         -F "info_file=@<FULL_PATH_TO_FILE>/info.json" \
         -F "json_response=true" \
         https://$SKYLINE_HOST/upload_data
