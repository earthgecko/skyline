.. role:: skyblue
.. role:: red

Flux
====

Flux enables Skyline to receive metrics via HTTP GET and POST requests and
submits them to Graphite, so they can be pickled to Skyline for analysis in near
real time via the normal Skyline pipeline.

Flux uses falcon the bare-metal web API framework for Python to serve the API
via gunicorn.  The normal nginx reverse proxy Skyline vhost is used to serve the
/flux endpoint and proxy requests to flux.

It is preferable to use the POST Flux endpoint to submit metrics so that the
Skyline flux API key can be encrypted via SSL in the POST data.

Flux is **not** an aggregator
-----------------------------

One data point per metric per Graphite retention time period.

Flux is **not** an aggregator.  A metric sent to flux should only have a single
data point submitted per time period.  Flux should not be used like statsd,
meaning that if a metric retention in Graphite 1 data point per 60 seconds then
only 1 data point should be submitted.  If multiple data points are submitted
for a period for the same metric, flux will submit them all **but** Graphite
will only record the last submitted value as the value for the period.  This is
just how Graphite works.

For example if you sent the following 3 data points to flux for a metric:

.. code-block:: json

  {
  	"metric": "test.multiple_timestamps_per_period.3",
  	"timestamp": 1478021701,
  	"value": 3.0,
  	"key": "YOURown32charSkylineAPIkeySecret"
  }

  {
  	"metric": "test.multiple_timestamps_per_period.3",
  	"timestamp": 1478021715,
  	"value": 6.0,
  	"key": "YOURown32charSkylineAPIkeySecret"
  }

  {
  	"metric": "test.multiple_timestamps_per_period.3",
  	"timestamp": 1478021745,
  	"value": 1.0,
  	"key": "YOURown32charSkylineAPIkeySecret"
  }

Graphite would store the last submitted value for the period, e.g:

.. code-block:: json

  [
    {
      "target": "test.multiple_timestamps_per_period.3",
      "tags": {"name": "test.multiple_timestamps_per_period.3"},
      "datapoints": [[1.0, 147802170]]
    }
  ]


De-duplication
--------------

Flux can de-duplicates metric data that it receives by maintaining a Redis key
for each metric that is submitted.  When the flux worker successfully submits a
data point and timestamp for a metric to Graphite, flux updates the
`flux.last.<metric>` Redis key with the data point timestamp.  If a data point
is submitted to flux with a timestamp <= to the timestamp value in the metric
Redis key, flux discards the data.  However this comes with a computational cost
of Redis keys having to be maintained for each metric, if you have lots of
metrics coming into flux, it is better to ensure that your application/s are
submitting to flux correctly and set :mod:`settings.FLUX_CHECK_LAST_TIMESTAMP`
to ``False``.  Considering that if your application does resubmit data, as long
as the data is the same data as previously sent, it will be sent to Graphite and
Graphite will just update the value to the same value that was already stored
(as described above).

Vista and :mod:`settings.FLUX_CHECK_LAST_TIMESTAMP`
---------------------------------------------------

Do note that `flux.last.<metric>` Redis keys are still used in Vista even if
:mod:`settings.FLUX_CHECK_LAST_TIMESTAMP` is set to ``False``. Vista uses the
same key namespace to handle data fetched from remote hosts even if it is
disabled and Flux will still use the keys for all vista.metrics even if it is
disabled.

Allowed characters in metric names
----------------------------------

As below.

.. code-block:: python

  ALLOWED_CHARS = ['+', '-', '%', '.', '_', '/', '=', ':']
  for char in string.ascii_lowercase:
      ALLOWED_CHARS.append(char)
  for char in string.ascii_uppercase:
      ALLOWED_CHARS.append(char)
  for char in string.digits:
      ALLOWED_CHARS.append(char)

Webserver configuration to limit content size on flux POST requests
-------------------------------------------------------------------

Because flux uses falcon, you should consider limiting the content length in
your webserver configuration, because falcon (being a bare-metal web API
framework) does not check content length.  See a basic nginx example using
``client_max_body_size`` in the location /flux/ block in
https://github.com/earthgecko/skyline/blob/master/etc/skyline.nginx.conf.d.example

POST request
------------

The POST endpoint is `/flux/metric_data_post` and this accepts JSON data.  The
json can have data for a single metric or for multiple metrics in a single POST.
POST requests which submit multiple metrics **should be limited** to a maximum
of **450 metrics per request**.

A successful POST will respond with no content and a 204 HTTP response code.
Unsuccessful requests will respond with a 4xx code and a json body with
additional information about why the request failed.

Here is an example of the data a single metric POST requires and an example POST
request.

.. code-block:: json

  {
  	"key": "YOURown32charSkylineAPIkeySecret",
  	"metric": "vista.nodes.skyline-1.cpu.user",
  	"timestamp": 1478021700,
  	"value": 1.0
  }

.. code-block:: bash

  curl -vvv -u username:password -d '{"key":"YOURown32charSkylineAPIkeySecret","metric":"vista.nodes.skyline-1.cpu.user","timestamp":1478021700,"value":1.0}' -H "Content-Type: application/json" -X POST https://skyline.example.org/flux/metric_data_post

Here is an example of the data a multiple metrics POST requires and an example
POST request for multiple metrics:

.. warning:: When submitting multiple metrics in a POST, if **any** one element
  of any metric is not valid the entire POST will be rejected.

.. code-block:: json

  {
  	"key": "YOURown32charSkylineAPIkeySecret",
    "metrics": [
      {
      	"metric": "vista.nodes.skyline-1.cpu.user",
      	"timestamp": 1478021700,
      	"value": 1.0
      },
      {
      	"metric": "vista.nodes.skyline-1.cpu.system",
      	"timestamp": 1478021700,
      	"value": 0.2
      }
    ]
  }

.. code-block:: bash

  curl -v -u username:password -d '{"key":"YOURown32charSkylineAPIkeySecret","metrics":[{"metric":"vista.nodes.skyline-1.cpu.user","timestamp":1478021700,"value":1.0},{"metric":"vista.nodes.skyline-1.cpu.system","timestamp":1478021700,"value":0.2}]}' -H "Content-Type: application/json" -X POST https://skyline.example.org/flux/metric_data_post

It is possible to backfill old data via flux, using the ``"fill": "true"``
element in the json.  By default flux checks timestamps to see if they are
sensible and valid, however if ``"fill": "true"`` is present in the metric
json, these checks will be skipped and the data will be submitted to Graphite
as is.  For example:

.. code-block:: json

  {
  	"key": "YOURown32charSkylineAPIkeySecret",
    "metrics": [
      {
      	"metric": "vista.nodes.skyline-1.cpu.user",
      	"timestamp": 1374341700,
      	"value": 1.5,
        "fill": "true"
      },
      {
      	"metric": "vista.nodes.skyline-1.cpu.system",
      	"timestamp": 1374341700,
      	"value": 0.5,
        "fill": "true"
      }
    ]
  }


**HOWEVER** be aware that if you are back filling old data, ensure that Graphite
does not have multiple retentions for the namespace and that the single
retention is sufficient to hold all the data at the resolution it is being
submitted at.  Once the metrics have been back filled you can use the Graphite
whisper-resize.py to resize the metric whispers files to the desired retentions
and update the Graphite storage-schema.conf to reflect the desired retentions.

Also to note is that when back filling via flux, although Graphite will pickle
the data to Horizon, Horizon will not submit data older though
:mod:`settings.FULL_DURATION` to Redis, because that is how Horizon is supposed
to work.  When the back fill timestamps are within the :mod:`settings.FULL_DURATION`
period, Horizon will then add the last days worth of data to Redis as normal and
the metrics will begin to be analysed as soon as real time data for the metrics
comes in.

GET request
-----------

However if the flux instance in question is only receiving metrics on a local
network or protected network and you do not mind sending the API key in
plaintext, the GET method can be used.

A successful GET request will respond with no content and a 204 HTTP response code.

The `/flux/metric_data` endpoint is called via a GET request with the URI
parameters as defined below:

.. code-block:: bash

  # /flux/metric_data?metric=<metric|str>&timestamp=<timestamp|int>&value=<value|float>&key=<key|str>
  # For example:
  curl -v -u username:password "https://skyline.example.org/flux/metric_data?metric=vista.nodes.skyline-1.cpu.user&timestamp=1478021700&value=1.0&key=YOURown32charSkylineAPIkeySecret"

telegraf
--------

Flux can accept metrics from directly from telegraf in the telegraf json format.
To configure telegraf to send metrics to Flux you can enable the `outputs.http`
plugin in telegraf as shown below.

Firstly a few things must be pointed out:

- Flux applies the same logic that is used by the telegraf Graphite template
  pattern e.g. `template = "host.tags.measurement.field"` - https://github.com/influxdata/telegraf/tree/master/plugins/serializers/graphite
- Flux replaces  `.` for `_` and `/` for `-` in any tags, measurement or fields
  in which these characters are found.
- The additional `outputs.http.headers` **must** be specified.
- `content_encoding` must be set to gzip
- There are a number of status codes that telegraf should not resubmit data on
  these are to ensure telegraf does not attempt to send the same bad data or
  too much data over and over again.
- If there is a long network partition between telegraf agents and Flux,
  sometimes some data may be dropped, but this can often be preferable to the
  thundering herd swamping I/O

To use the telegraf Graphite template pattern the following options in the
telegraf `[agent]` configuration section are required for telegraf to add the
host to the tags:

.. code-block::

  ## Maximum number of unwritten metrics per output.
  # Ideally change this from 10000 to 1000 to minimise thundering herd issues
  # metric_buffer_limit = 10000
  metric_buffer_limit = 1000

  ## Override default hostname, if empty use os.Hostname()
  hostname = ""
  ## If set to true, do no set the "host" tag in the telegraf agent.
  omit_hostname = false


Also add the `[[outputs.http]]` to the telegraf config as below replacing
`<YOUR_SKYLINE_HOST>` and `<settings.FLUX_SELF_API_KEY>`:

.. code-block::

  [[outputs.http]]
    ## URL is the Skyline flux address to send metrics to
    url = "https://<YOUR_SKYLINE_HOST>/flux/metric_data_post"
    ## Set a long timeout because if a network partition occurs between telegraf
    ## and the flux, telegraf will keep and batch the metrics to send through AND
    ## 10s of 1000s of metrics can then be sent when the network partition is
    ## resolved, these can take a while to process as often many other telegraf
    ## instances may have been partitioned at the same time, so a thundering herd
    ## is sent to flux.
    timeout = "120s"
    method = "POST"
    data_format = "json"
    use_batch_format = true
    json_timestamp_units = "1s"
    content_encoding = "gzip"
    ## A list of statuscodes (<200 or >300) upon which requests should not be retried
    non_retryable_statuscodes = [400, 403, 409, 413, 499, 500, 502, 503]
    [outputs.http.headers]
      Content-Type = "application/json"
      key = "<settings.FLUX_SELF_API_KEY>"
      telegraf = "true"
      ## Optionally you can pass a prefix e.g.
      # prefix = "telegraf"


populate_metric endpoint
------------------------

Skyline Vista is tightly integrated with Flux. Vista uses flux to submit metric
data to Graphite for the metrics that Vista fetches.  Vista does not connect
to flux via the reverse proxy, it connects directly to flux and uses the
:mod:`settings.FLUX_SELF_API_KEY` to authenticate itself.  Flux has a specific
`/flux/populate_metric` endpoint and worker so that Vista can submit historical
metric data to in order to pre-populate Graphite with data when new
metrics are added to Vista at multiple resolutions.  However, this endpoint is
also used by Vista to catchup/backfill metrics if for any reason data has not
been retrieved for a metric for in `(frequency + 300)` seconds and has fallen
behind.

The populate_metric worker uses the `last_flux_timestamp` from the
`flux.last.<metric>` Redis keys to ensure that only missing data is retrieved
and submitted to Graphite.  If no `flux.last.<metric>` Redis key exists, the
worker checks Graphite to see if Graphite has any data for the metric and if so,
flux uses the last data point timestamp from Graphite as the
`last_flux_timestamp` and retrieves data > `last_flux_timestamp`.

The flux populate_metric_worker submits pickled data to Graphite via the Carbon
PICKLE_RECEIVER_PORT, therefore ensure that there is a firewall rule allowing
the Skyline node to connect to the Graphite node on this port.

The populate_metric_worker applies resampling at 1Min, but see Vista
populate_at_resolutions for more detailed information.

Process uploaded data
---------------------

Skyline Flux can be enabled to process data uploaded via the webapp and submit
data to Graphite.  This allows for the automated uploading and processing of
batched measurements data and reports to time series data which is analysed in
the normal Skyline workflow.  An example use case would be if you had an hourly
report of wind related metrics that had a reading every 5 minutes for an hour
period, for x number of stations.  As long as the data is in uploaded in an
acceptable format, it can be preprocessed by flux and submitted to Graphite.
The metric namespace/s need be declared as batch processing metrics in
:mod:`settings.BATCH_PROCESSING_NAMESPACES` and :mod:`settings.BATCH_PROCESSING`
has to be enabled.

By default flux is not enabled to process uploaded data and the webapp is not
configured to accept uploaded data.

To enable Flux to process uploaded data the following settings need to be set
and services running:

- analyzer_batch needs to be enabled and running, see `Analyzer - analyzer_batch <analyzer.html#analyzer_batch>`__.
- :mod:`settings.BATCH_PROCESSING` need to be set to `True`
- The `parent_metric_namespace` or all the metric namespace in question relating
  to the specific data being uploaded need to be declared in
  :mod:`settings.BATCH_PROCESSING_NAMESPACES`
- :mod:`settings.DATA_UPLOADS_PATH` is required
- :mod:`settings.WEBAPP_ACCEPT_DATA_UPLOADS` must be enabled
- :mod:`settings.FLUX_PROCESS_UPLOADS` must be enabled
- If the data is being uploaded ia an automated process, curl, etc the
  `parent_metric_namespace` needs a key set in the
  :mod:`settings.FLUX_UPLOADS_KEYS` dictionary e.g.

.. code-block:: python

    FLUX_UPLOADS_KEYS = {
        'temp_monitoring.warehouse.2.012383': '484166bf-df66-4f7d-ad4a-9336da9ef620',
    }


- Optionally :mod:`settings.FLUX_SAVE_UPLOADS` and
  :mod:`settings.FLUX_SAVE_UPLOADS_PATH` can be used if you wish to save the
  uploaded data.

For specific details about the data formats and methods for uploading and
processing data files see the `upload_data to Flux <upload-data-to-flux.html>`__
page.
