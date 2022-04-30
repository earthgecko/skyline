import sys
import os.path
import logging
# import multiprocessing
# import traceback
from logging.handlers import TimedRotatingFileHandler, MemoryHandler

sys.path.append(os.path.join(os.path.dirname(os.path.realpath(__file__)), os.pardir))
sys.path.insert(0, os.path.dirname(__file__))

if True:
    import settings
    # @added 20220328 - Feature #4018: thunder - skyline.errors
    from functions.redis.RedisErrorLogHandler import RedisErrorLogHandler

    # @added 20220405 - Task #4514: Integrate opentelemetry
    #                   Feature #4516: flux - opentelemetry traces
    OTEL_ENABLED = False
    OTEL_JAEGEREXPORTER_AGENT_HOST_NAME = '127.0.0.1'
    OTEL_JAEGEREXPORTER_AGENT_PORT = 26831
    try:
        OTEL_ENABLED = settings.OTEL_ENABLED
        OTEL_JAEGEREXPORTER_AGENT_HOST_NAME = settings.OTEL_JAEGEREXPORTER_AGENT_HOST_NAME
        OTEL_JAEGEREXPORTER_AGENT_PORT = settings.OTEL_JAEGEREXPORTER_AGENT_PORT
    except AttributeError:
        OTEL_ENABLED = False
    except Exception as err:
        OTEL_ENABLED = False
    if OTEL_ENABLED:
        from opentelemetry import trace
        # from opentelemetry.exporter import jaeger
        from opentelemetry.exporter.jaeger.thrift import JaegerExporter
        from opentelemetry.sdk.resources import SERVICE_NAME, Resource
        from opentelemetry.sdk.trace import TracerProvider
        # from opentelemetry.sdk.trace.export import BatchExportSpanProcessor
        from opentelemetry.sdk.trace.export import BatchSpanProcessor
    #    from typing import Iterable
    #    from opentelemetry.exporter.otlp.metrics_exporter import OTLPMetricsExporter
    #    from opentelemetry.sdk.metrics import MeterProvider
    #    from opentelemetry.sdk.metrics.export.controller import PushController
    # @added 20220405
    # Not stable or ready for prime time and only ConsoleMetricExporter is available
        # from opentelemetry._metrics import get_meter_provider, set_meter_provider
        # from opentelemetry.exporter.otlp.metrics_exporter import OTLPMetricsExporter
        # from opentelemetry._metrics.measurement import Measurement
        # from opentelemetry.sdk._metrics import MeterProvider
        # from opentelemetry.sdk._metrics.export import (
        #     ConsoleMetricExporter,
        #     PeriodicExportingMetricReader,
        # )

bind = '%s:%s' % (settings.WEBAPP_IP, str(settings.WEBAPP_PORT))
# @modified 20201011 - Reduce the number of workers on machine with lots of
#                      CPUs
# workers = multiprocessing.cpu_count() * 2 + 1
# workers = 2
# backlog = 10
try:
    workers = settings.WEBAPP_GUNICORN_WORKERS
except:
    workers = 4
try:
    backlog = settings.WEBAPP_GUNICORN_BACKLOG
except:
    backlog = 254

skyline_app = 'webapp'
skyline_app_logger = '%sLog' % skyline_app
logfile = '%s/%s.log' % (settings.LOG_PATH, skyline_app)
logger = logging.getLogger(skyline_app_logger)

pidfile = '%s/%s.pid' % (settings.PID_PATH, skyline_app)

accesslog = '%s/webapp.access.log' % (settings.LOG_PATH)
access_log_format = '%(h)s %(l)s %(u)s %(t)s "%(r)s" %(s)s %(b)s "%(f)s" "%(a)s"'

errorlog = '%s/webapp.log' % (settings.LOG_PATH)

logger.setLevel(logging.DEBUG)

formatter = logging.Formatter("%(asctime)s :: %(process)s :: %(message)s", datefmt="%Y-%m-%d %H:%M:%S")
handler = logging.handlers.TimedRotatingFileHandler(
    logfile,
    when="midnight",
    interval=1,
    backupCount=5)

memory_handler = logging.handlers.MemoryHandler(100,
                                                flushLevel=logging.DEBUG,
                                                target=handler)
handler.setFormatter(formatter)
logger.addHandler(memory_handler)

# @added 20220328 - Feature #4018: thunder - skyline.errors
# For every error logged set a count in the app Redis key which is consumed
# by thunder and creates the sskyline.<hostname>.<skyline_app>.logged_errors
# metric
redis_error_log_handler = RedisErrorLogHandler(skyline_app)
redis_error_log_handler.setLevel(logging.ERROR)
redis_error_log_handler.setFormatter(formatter)
logger.addHandler(redis_error_log_handler)


# @added 20220405 - Task #4514: Integrate opentelemetry
#                   Feature #4516: flux - opentelemetry traces
# As per https://opentelemetry-python.readthedocs.io/en/latest/examples/fork-process-model/README.html#gunicorn-post-fork-hook
# required for opentelemetry to work with gunicorn
def post_fork(server, worker):

    if not OTEL_ENABLED:
        pass
    else:
        service_name = 'skyline.%s.webapp' % settings.SERVER_METRICS_NAME
        trace.set_tracer_provider(
            TracerProvider(
                resource=Resource.create({SERVICE_NAME: service_name})
            )
        )

        # SpanExporter receives the spans and send them to the target location.
        # exporter = jaeger.JaegerSpanExporter(
        jaeger_exporter = JaegerExporter(
            # service_name='webapp',
            # agent_host_name='127.0.0.1',
            agent_host_name=OTEL_JAEGEREXPORTER_AGENT_HOST_NAME,
            # Direct to jaeger
            # agent_port=6831,
            # Send to otelcol
            # agent_port=26831,
            agent_port=OTEL_JAEGEREXPORTER_AGENT_PORT,
        )
        span_processor = BatchSpanProcessor(jaeger_exporter)
        trace.get_tracer_provider().add_span_processor(span_processor)

        # Meter is responsible for creating and recording metrics
    #    metrics.set_meter_provider(MeterProvider())
        # opentelemetry.metrics.get_meter(instrumenting_module_name, instrumenting_library_version='', meter_provider=None)
    #    meter = metrics.get_meter(__name__)
        # controller collects metrics created from meter and exports it via the
        # exporter every interval
    #    controller = PushController(meter, metric_exporter, 60)
