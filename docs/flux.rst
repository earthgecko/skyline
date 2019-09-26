.. role:: skyblue
.. role:: red

Flux
====

Flux enables Skyline to receive metrics via HTTP GET and POST requests and
submits them to Graphite, so they can be pickled to Skyline for analysis in near
real time via the normal Skyline pipeline.

Flux uses falcon the bare-metal web API framework for Python to serve the API
via gunicorn.  The normal Apache reverse proxy Skyline vhost is used to serve
the /flux endpoint and proxy requests to flux.

It is preferable to use the POST Flux endpoint to submit metrics so that the
Skyline flux API key can be encrypted via SSL in the POST data.

De-duplication
--------------

Flux de-duplicates metric data that it receives by maintaining a Redis key for
each metric that is submitted.  When the flux worker successfully submits a data
point and timestamp for a metric to Graphite, flux updates the
`flux.last.<metric>` Redis key with the data point timestamp.  If a data point
is submitted to flux with a timestamp <= to the timestamp value in the metric
Redis key, flux discards the data.

POST request
------------

The POST endpoint is `/flux/metric_data_post` and this accepts JSON data.
Here is an example of the data it requires and an example POST request.

.. code-block:: json

  {
  	"metric": "vista.nodes.skyline-1.cpu.user",
  	"timestamp": "1478021700",
  	"value": "1.0",
  	"key": "YOURown32charSkylineAPIkeySecret"
  }

.. code-block:: bash

  curl -vvv -u username:password -d '{"metric":"vista.nodes.skyline-1.cpu.user","timestamp":"1478021700","value":"1.0","key":"YOURown32charSkylineAPIkeySecret"}' -H "Content-Type: application/json" -X POST https://skyline.example.org/flux/metric_data_post

GET request
-----------

However if the flux instance in question is only receiving metrics on a local
network or protected network and you do not mind sending the API key in
plaintext, the GET method can be used.

The `/flux/metric_data` endpoint is called via a GET request with the URI
parameters as defined below:

.. code-block:: bash

  # /flux/metric_data?metric=<metric|str>&timestamp=<timestamp|int>&value=<value|float>&key=<key|str>
  # For example:
  curl -vvv -u username:password "https://skyline.example.org/flux/metric_data?metric=vista.nodes.skyline-1.cpu.user&timestamp=1478021700&value=1.0&key=YOURown32charSkylineAPIkeySecret"

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
