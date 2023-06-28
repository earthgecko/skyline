opentelemetry
=============

Skyline webapp can be configured to send opentelemetry traces to Jaeger or the
opentelemetry collector (otelcol).

Currently only the webapp is instrumented with traces for a number of reasons.

- It is experimenting with opentelemetry.
- Given the nature of Skyline itself, it is assumed that metrics will already be
  being collected for all the Skyline components such as Redis, MariaDB and
  memcached via other instrumentation such as Telegraf and being fed to Skyline
  already.  Therefore instrumenting traces from the other Skyline apps could be
  seen as duplication because there will already be monitoring of metrics on
  things like Redis cmd timings, etc.

Monitoring observability data
-----------------------------

Skyline is not only experimenting with the addition of traces but also the
ingestion of opentelemetry OTLP trace data via Flux and converting service/method
timings and trace counts into aggregated metrics.  With a view to handle OTLP
metrics and logging when they became stable.

This experiment is being done to determine whether it is useful to monitor and
anomaly detect on the observability data itself.

Proceed with caution
--------------------

Should you wish to experiment with Flux trace to metric conversions in your own
apllication/s first assess the total number of Service and Operation counts in
Jaegar.  And be very aware that instrumentation telemetry has the ability change
to **high cardinality** if something in your application design changes.

Skyline opentelemetry experimental architecture
-----------------------------------------------

Although Skyline webapp can send traces directly to Jaeger, the experimental
set up is using the opentelemetry collector as a router to send trace data to
both Jaeger and Flux.

.. code-block::

  webapp -> opentelemetry JaegerExporter -> otelcol |-> Jaeger
                                                    |-> Flux

On receiving trace data Flux will automatically aggregate traces and send the
trace timings and trace counts per method as metrics to Graphite.  In terms of
monitoring these metrics we want to monitor them as last known value metrics.
These are similar to statsd gauge type metrics, which means that in the
analysis stages, a metric will maintain its last value until a new one is
received.  This results in monitoring the behaviour of the timings and trace
counts, rather than the frequency.  This method allows for the monitoring of the
behaviour of the methods rather than their frequency.

This allows for the identification of significant changes in either
the time taken for a method/s to complete and the number of traces in a method,
e.g. the number of methods called by a method.

For example let us say the skyline.webapp/rebrow/keys method makes on average 8
GET Redis method calls and we introduce a change to determine the size of each
key returned and this results in the method making 116 GET Redis method calls,
Skyline will trigger an anomaly and alert on that change.  The same is true for
method timings.

The objective here being to monitor the behaviour of the methods in the traces
rather than the frequency of the methods.

It must be stated that this suited to situations where traces fairly frequently,
at least a few times a day.  If trace data is sent less than once per day this
analysis method will not detect changes those methods as there is insufficient
data.

Configuration
-------------

To achieve the following you need Skyline/flux, Jaeger and otelcol all running
on the same server.  Although if you have your own otelcol or Jaeger set up you
can configure this as it suits your set up.

opentelemetry trace from webapp can be enabled via settings.py under the
opentelemetry settings section:

- :mod:`settings.OTEL_ENABLED` to enable traces from the webapp
- :mod:`settings.OTEL_JAEGEREXPORTER_AGENT_HOST_NAME`
- :mod:`settings.OTEL_JAEGEREXPORTER_AGENT_PORT`
- :mod:`settings.WEBAPP_SERVE_JAEGER`
- :mod:`settings.FLUX_OTEL_ENABLED` to enable Flux to accept OTLP traces from
  otelcol and convert them into metrics which will be namespaced as
  ``otel.traces.skyline.webapp.*``
- :mod:`settings.LAST_KNOWN_VALUE_NAMESPACES` if you wish to monitor otel trace
  ensure that the ``otel.traces`` namespace is declared in this setting.

Further to this the ``OTEL_EXPORTER_JAEGER_AGENT_SPLIT_OVERSIZED_BATCHES`` ENV
variable must be set to prevent the opentelemetry.exporter.jaeger.thrift
JaegerExporter from warning that Data exceeds the max UDP packet size and losing
data on large traces.

Therefore ensure that your systemd service file has the following declared:

.. code-block:: bash

  Environment=OTEL_EXPORTER_JAEGER_AGENT_SPLIT_OVERSIZED_BATCHES=True

If you start the web service differently ensure that the ENV variable is set:

.. code-block:: bash

    export OTEL_EXPORTER_JAEGER_AGENT_SPLIT_OVERSIZED_BATCHES="True"


The general installation of otelcol and Jaeger is not covered here see the
respective documentation for each:
https://www.jaegertracing.io/docs
https://opentelemetry.io/docs/collector/

Skyline specific configurations for otelcol and Jaeger are as follows.

For an example config.yaml for otelcol to achieve the described set up see:
https://github.com/earthgecko/skyline/blob/master/etc/otel.config.yaml.example

It is possible to serve Jaeger authenticated via the Skyline nginx config as a
Skyline endpoint if you do not want to expose Jaeger or set up an another vhost
to handle Jaeger individually.  See:
https://github.com/earthgecko/skyline/blob/master/etc/skyline.nginx.conf.d.example

This assumes that there is a local instance of Jaeger running with mostly default
Jaeger configuration.  However do note if you want to serve Jaeger via the
Skyline UI ``--query.base-path=/jaeger`` must be specified, e.g. an all-in-one
instance with memory-mode:

.. code-block:: bash

  /opt/jaeger/jaeger-1.32.0-linux-amd64/jaeger-all-in-one --memory.max-traces 10000 --query.base-path=/jaeger >> /var/log/jaeger.log 2>&1 &



Future plans
------------

Tracing will be added to the other Skyline apps as appropriate.  One possible
use case is instrumenting SQL traces because the granular timings on SQL queries
is not something that the current MariaDB metrics cover in a manner which can be
easily monitored.

Currently the Redis metric instrumentation is sufficient to monitor the timings
of all Redis methods in use and therefore to reduce bloat, tracing
instrumentation will probably not be implemented in the Redis methods context in
any other apps apart from the webapp itself.
