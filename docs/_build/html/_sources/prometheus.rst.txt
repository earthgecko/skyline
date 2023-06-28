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
things depending on the Skyline set up.

- x-tenant-id
- x-server-id
- x-server-url

Each x-tenant-id can have multiple x-server-ids and x-server-urls, one for
each sending server.

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
      max_samples_per_send: 8000
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
set of tradeoffs.  It gives you real time monitoring your metrics in broad
strokes, but it has cardinality and resolution limitations.

Skyline requires the following additional information per request to be passed
along with the metric data.

- source: the Prometheus URL at which the metrics being submitted are available at.
- key: the FLUX_API_KEYS key
- prefix: a string to prefix the metrics with internally in Skyline

Although the Prometheus metric name may be something along the lines of:
``prometheus_http_requests_total{alias="Prometheus",code="200",handler="/graph",instance="localhost:9090",job="prometheus"}``
Skyline's internal representation of that would be prefixed the x-tenant-id and x-server-id labels:
``prometheus_http_requests_total{_tenant_id="<x-tenant-id>",_server_id="<x-tenant-id>",alias="Prometheus",code="200",handler="/graph",instance="localhost:9090",job="prometheus"}``

The reason these labels are required is because Skyline may be receiving metrics
from multiple Prometheus instances and whereas each metric and labels may
be unique on one Prometheus server the same metric and labels may exist on
another Prometheus server, so Skyline adds labels to every Prometheus metric.

Pass this additional information using the remote_write method is easy, passing
it using the remote_storage_adapter is a bit more obfuscate.

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
      headers:
        key: 1234hbfq89iUGGDn9qiUHuads7we1234
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
