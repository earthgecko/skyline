.. role:: skyblue
.. role:: red

Prometheus
==========

Skyline can ingest Prometheus metrics.  This requires Prometheus to be
configured to send data to Skyline using the Prometheus `remote_write`
configuration option or using a Prometheus `remote_storage_adapter`.

Skyline does not use Prometheus as a data source.  Skyline uses RedisTimeSeries
for storing :mod:`settings.FULL_DURATION` data and VictoriaMetrics as it's long
term backend storage and for queries longer than :mod:`settings.FULL_DURATION`.
There are a number of reasons for this.

- Skyline can receive data from multiple Prometheus servers and using a
  VictoriaMetrics server removes any reliance on Skyline having to have access
  to the Prometheus source server/s.
- Each Prometheus server has it's metrics labelled with a ``x_tenant_id`` and
  ``_server`` in both RedisTimeSeries and VictoriaMetrics data so that each
  Prometheus server's metrics can be uniquely identified and isolated, ensuring
  no there are no metric name conflicts or duplications.
- It isolates Skyline in terms of data from the Prometheus server/s so it will
  not overload the source Prometheus server/s with lots of long queries.

The architecture decision to use VictoriaMetrics is based on performance and
totally isolating Skyline in terms of load on your Prometheus monitoring.
Given that enabling ``remote_write`` on Prometheus generally results in ~25%
increased memory usage, adding additional load with lots of long duration
queries is probably undesirable.  Even if you are only using one Prometheus
server to send Skyline metrics, it is recommended that you still use
VictoriaMetrics as the Skyline backend store to allow for greater data retention
than the default Prometheus 14 days, because Skyline uses longer duration data
for learning.

Ingestion
~~~~~~~~~

Skyline Flux accepts metrics from Prometheus and appends them to a Redis set.
Every 60 seconds Skyline Horizon processes the Redis set and does a number of
things depending on the Skyline set up.  The values of the headers section of
the remote_write add the following labels to incoming metrics:

- x-tenant-id
- x-server-id
- x-server-url

The x-tenant-id header value identifies the entity, an organisation, a customer,
a site, etc.  Each x-tenant-id can have multiple x-server-ids and x-server-urls,
one for each sending servers.  Each Prometheus or VictoriaMetrics server that
sends data will have a unique ID and an associated URL.

Note that Redis time series does not accept commas in label values and therefore
on ingestion of any metrics with label values that include commas, Skyline replaces
commas with underscores, e.g. ``'tags': 'netgo,builtinassets,stringlabels'`` will
become ``'tags': 'netgo_builtinassets_stringlabels'``.

If a target is sending any noisy metrics that you don't want sent, you can
either specify that Prometheus should drop that data in the remote_write config
using the write_relabel_configs and drop options

.. code-block:: yaml

  write_relabel_configs:
    - source_labels: ['__name__', 'instance']
      regex: '(node_memory_active_bytes|localhost:9100)'
      action: 'drop'

Or you can add these as a comma separated list of patterns (including regex
patterns) to the remote_write dropMetrics header.

In order to remove specific labels or attributes from data points if a target is
sending labels you are not interested in receiving, you can remove these from the
metrics.  Often targets will send labels you are not interested in, like high
cardinality attributes such as container ids or other unique identifiers. To
achieve this in Prometheus you need to change both the remote_write and the
scrape_configs sections.  However changing the scrape_configs on your side will
remove those labels from your Prometheus as well.  This can be troublesome as
you also need to define the labels to keep in the scrape_configs too.  Skyline
therefore allows you to pass the dropLabels header with a comma separated
list of labels (also with a value pattern) to drop in the remote_write
dropLabels header which means that these labels will still be recorded on your
Prometheus but Skyline will remove the labels without any change to your
existing scrape_configs.  You can also pass a dropMetrics in the headers to tell
Skyine to drop metrics that match a pattern in the dropMetrics list.  Obviously
you can also apply normal Prometheus remote_write configuration blocks to the
remote_write seciton such as ``write_relabel_configs`` as well.

.. code-block:: yaml

  remote_write:
  - url: https://<YOUR_SKYLINE_FQDN>/flux/prometheus/write
    queue_config:
      max_samples_per_send: 1000
      batch_send_deadline: 30s
      min_backoff: 10s
      max_backoff: 30s
    write_relabel_configs:
    - source_labels: [alias]
      regex: "(Loki|Prometheus|Grafana|Telegraf)"
      action: drop
    - source_labels: [job]
      regex: "(loki_exporter)"
      action: drop
    - source_labels: [__name__]
      regex: "(code:prometheus_http_requests_total:sum)"
      action: drop
    headers:
      key: <FLUX_SELF_API_KEY or a FLUX_API_KEYS>
      x-tenant-id: 1
      x-server-id: 1
      x-server-url: https://<YOUR_PROMETHEUS_FQDN>
      dropLabels: "[['monitor','master'],['another_label_to_drop','with_value_of_*']]"
      dropMetrics: "['^flag.*','^go_.*','^info_.*']"
    metadata_config:
      send: true
      send_interval: 1m
      max_samples_per_send: 1000

Prometheus retry
~~~~~~~~~~~~~~~~

By default the Prometheus remote_write retry related settings result in
Prometheus trying to process the retry queue VERY fast.  The defaults are:

.. code-block:: yaml

  [ max_samples_per_send: <int> | default = 500]
  # Maximum time a sample will wait in buffer.
  [ batch_send_deadline: <duration> | default = 5s ]
  # Initial retry delay. Gets doubled for every retry.
  [ min_backoff: <duration> | default = 30ms ]
  # Maximum retry delay.
  [ max_backoff: <duration> | default = 5s ]

If there is an issue with flux ingestion or on the server itself or a
long network partition, any delay or failure will result in Prometheus
retrying everything, very fast.  Due to the fact that the operator
may not have access to Prometheus to stop remote_write or even define
the Prometheus remote_write min_backoff or max_backoff or any access
to configure Prometheus, Skyline must be opinionated in this matter to
prevent lots of thundering retry requests.  The Prometheus defaults make
retry go FAST, 50 retries of all queued batches over 5s.  This results
in flux being swamped with retry connections and many batches will
timeout.  Prometheus will just then try again and again and there is
no way to move forward as it insistently wants to retry everything,
every time.  The end result is that flux can never catch up because it is being
sent 10s of 1000s of samples every retry, which most fail entering
into a retry death spiral.
There is no backfill with Prometheus, thereafter Skyline is ONLY
expecting real time data.  If data is not current return a 400 and
Prometheus will not retry.
As of Prometheus v2.50.0 you will be able to set the sample_age_limit which will
be allowed in the remote_write config to allow for disabling retries on samples
older than sample_age_limit, by default is disabled.

Therefore Skyline itself by default is set to return 400 for any submissions
that are older than :mod:`settings.FLUX_PROMETHEUS_MAX_AGE` (300 seconds by
default).  Prometheus receiving a 400 constitutes a non-recoverable error which
is not retried and Prometheus moves on to the next batch.  Although this may
result in some gaps in the Skyline :mod:`settings.FULL_DURATION` Redis data,
it means that any lengthy issue or network partition will not result in a retry
spiral of death.

The example settings on this page show more reasonable values for Prometheus
remote_write queue settings which at least allow flux to process all the retries
if an issue does develop and are much less likely to result in a retry spiral of
death.  When loaded and pushed with a lot of data, flux can be expected to take
> 5s to process a request so batch_send_deadline is set to 30s, a more
reasonable value and the request will 499 if flux is running behind nginx just
before 30s and Prometheus will receive that response and not retry.  After an
issue, flux will rarely respond in < 5s when thundered with retries. min_backoff
and max_backoff are also set to more realistic values in terms of flux.

.. code-block:: yaml

  remote_write:
  - url: https://<YOUR_SKYLINE_FQDN>/flux/prometheus/write
    queue_config:
      max_samples_per_send: 1000
      batch_send_deadline: 30s
      min_backoff: 10s
      max_backoff: 30s


Format
~~~~~~

Skyline supports the standard Prometheus protobuf data ingestion and influxdb
format therefore you can send Prometheus data to Skyline in two ways:

- Directly from Prometheus using remote_write to Skyline/flux
- Using Prometheus remote_write sending to a Prometheus remote_storage_adapter
  and configuring the remote_storage_adapter to forward data in influxdb format
  to Skyline/flux.

However do note that the preferred and more featureful method is to use
remote_write directly.  The remote_storage_adapter method is very limited in
terms of information that can be passed with the data.  It is not possible to
use any of the headers functionality (dropMetrics, dropLabels, etc) with the
remote_storage_adapter, it's functionality is very basic.

Given the large number of metrics that can be generated by Prometheus exporters
users are encouraged to assess all the metrics that Prometheus will send to
Skyline and implement appropriate :mod:`settings.SKIP_LIST` and
:mod:`settings.DO_NOT_SKIP_LIST` rules to ensure that you are only storing and
analysing the key metrics and Skyline/horizon is dropping metrics that are of no
interest.  Although flux ingests the Prometheus metrics they are processed and
submitted to Redis via the normal horizon route, therefore the normal horizon
SKIP_LIST and DO_NOT_SKIP_LIST methods are still applicable.

In order to manage Prometheus metrics all Prometheus metrics must be prefixed
with a namespace that can be used to manage the metrics.  The namespace prefixing
is done using the :mod:`settings.FLUX_API_KEYS` mapping.  In the case of using
the remote_storage_adapter method the ``--influxdb.database=<PREFIX>`` is also
used, more on this in the remote_storage_adapter section.

It is recommended that you add the Prometheus namespace globally to the SKIP_LIST
and then add metrics that you want to analyse to the DO_NOT_SKIP_LIST.

Both ways work almost the same with the exception of a prefix must be appended to

Considerations
~~~~~~~~~~~~~~

There is an issue in Prometheus metric types in so far as developers will not always
adhere to metric typing rules and in some exporter you will find COUNTER and GAUGE
metrics in the same metric namespace.  To overcome this analyzer periodically checks
the metrics and identifies the skyline_metric_type, because the Prometheus metadata
cannot be trusted.

Skyline is not storing the data for longer than :mod:`settings.FULL_DURATION`
and it is storing preprocessed data.  Consider Skyline analysis as a broad
overview of your Prometheus metrics, it is not about fine granular data it is a
set of tradeoffs.  It gives you real time monitoring of your metrics in broad
strokes, but it has cardinality and resolution limitations.

Skyline requires the following additional information per request to be passed
along with the metric data.

- source: the Prometheus URL at which the metrics being submitted are available at.
- key: the FLUX_API_KEY key
- prefix: a string to prefix the metrics with internally in Skyline

Although the Prometheus metric name may be something along the lines of:
``prometheus_http_requests_total{alias="Prometheus",code="200",handler="/graph",instance="localhost:9090",job="prometheus"}``
Skyline's internal representation of that would be prefixed the x-tenant-id and x-server-id labels:
``prometheus_http_requests_total{_tenant_id="<x-tenant-id>",_server_id="<x-tenant-id>",alias="Prometheus",code="200",handler="/graph",instance="localhost:9090",job="prometheus"}``

The reason these labels are required is because Skyline may be receiving metrics
from multiple Prometheus instances and whereas each metric and labels may
be unique on one Prometheus server the same metric and labels may exist on
another Prometheus server, so Skyline adds labels to every Prometheus metric.

Pass this additional information using the remote_write and JSON methods is easy
as it just just passed as headers (as shown in the remote_write example above),
passing it using the remote_storage_adapter is a bit more obfuscate.


Ingesting JSON
~~~~~~~~~~~~~~

It is possible to post key value style metrics via JSON as well.  This allows
for custom key/value metrics that perhaps are not collected in Prometheus or
VictoriaMetrics to be sent directly to Skyline as "prometheus metrics" with the
metric|str, timestamp|int and value|float

.. code-block:: bash

  SAMPLE_TIMESTAMP=$(date +%s)
  curl -v -d '{
    "key":"<FLUX_SELF_API_KEY or a FLUX_API_KEYS>",
    "metrics":[
      {"metric":"cpu_user{instance=\"server1\"}","timestamp":'$SAMPLE_TIMESTAMP',"value":1.0},
      {"metric":"cpu_system{instance=\"server1\"}","timestamp":'$SAMPLE_TIMESTAMP',"value":2.2}]}' \
      -H "Content-Type: application/json" \
      -H "x-test-only: true" \
      -H "x-tenant-id: org_1" \
      -H "x-server-id: 1" \
      -H "x-server-url: http://localhost:9090" \
      -X POST https://skyline.example.org/flux/prometheus/write

The dropLabels and dropMetrics headers referenced above in the remote_write
config section can also be used here.  Limit payloads to 1024K and if you have
a large number of metrics being submitted consider using a gzip encoded request.

Note for the JSON format requests if any one metric or part of the metric data
is invalid it will be rejected.  A response code of 207 is returned for any
requests that POST a mixture of valid data and invalid data.  Along with the 207
response is a JSON response reporting the invalid metric data and the valid
metric data along with the response for each item, either a 200 for valid data
and a 400 for invalid data.  If all the data in the submission is valid a
response of 204 is used and if no data is valid a response of 400 is returned.

If you have thunder enabled then thunder will send out an invalid_metrics alert
every hour if invalid metrics are submitted and a 207 response is returned with
a link to the metric data which was invalid which is available for 2 hours.  Do
note that only one thunder alert will be sent for a namespace per hour no matter
how many requests are submitted with invalid data.


remote_storage_adapter
~~~~~~~~~~~~~~~~~~~~~~

Due to the limited number of config options that the remote_storage_adapter
can be run with some config options are used for different purposes than they
are intended for.  This is because there is no ability to add headers to the
remote_storage_adapter requests, therefore we have to use what options are
available to relay the required parameters to Skyline.

``--influxdb.database`` - in terms of Skyline the database option is used to
prefix the metric with this string.
Skyline uses the
``--influxdb.username`` - dict with all the required headers/labels?


.. code-block:: yaml

  remote_write:
    - url: "https://skyline.example.org/flux/prometheus/write"
      queue_config:
        max_samples_per_send: 1000
        batch_send_deadline: 30s
        min_backoff: 10s
        max_backoff: 30s
      headers:
        key: 123456abcdefghijklmnopqrstuvwxyz
        x-tenant-id: 123
        x-server-id: 2
        x-server-url: http://minikube.mc12
        dropMetrics:
        dropLabels:
      write_relabel_configs:
        - target_label: "x_org_id"
          replacement: 123
        - target_label: "x_server_id"
          replacement: 2
      metadata_config:
        send: true
        send_interval: 1m
        max_samples_per_send: 1000
