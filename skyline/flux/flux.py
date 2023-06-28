"""
flux.py
"""
import sys
import os
from multiprocessing import Queue

# @added 20201018 - Feature #3798: FLUX_PERSIST_QUEUE
from ast import literal_eval
import traceback

import falcon

sys.path.append(os.path.join(os.path.dirname(os.path.realpath(__file__)), os.pardir))
sys.path.insert(0, os.path.dirname(__file__))

# @modified 20191115 - Branch #3262: py3
# This prevents flake8 E402 - module level import not at top of file
if True:
    import settings
    # @added 20201018 - Feature #3798: FLUX_PERSIST_QUEUE
    from skyline_functions import get_redis_conn_decoded, mkdir_p
    from logger import set_up_logging
    # @added 20221118 - Feature #4732: flux vortex
    #                   Feature #4734: mirage_vortex
    # Added VortexDataPost and VortexResults
    from listen import MetricData, MetricDataPost, VortexDataPost, VortexResults
    # from listen_post import MetricDataPost
    from worker import Worker
    from populate_metric import PopulateMetric
    from populate_metric_worker import PopulateMetricWorker
    # @added 20210406 - Feature #4004: flux - aggregator.py and FLUX_AGGREGATE_NAMESPACES
    from aggregator import Aggregator

    # @added 20200517 - Feature #3550: flux.uploaded_data_worker
    try:
        flux_process_uploads = settings.FLUX_PROCESS_UPLOADS
    except:
        flux_process_uploads = False
    if flux_process_uploads:
        from uploaded_data_worker import UploadedDataWorker

    # @added 20220622 - Task #2732: Prometheus to Skyline
    #                   Branch #4300: prometheus
    try:
        PROMETHEUS_INGESTION = settings.PROMETHEUS_INGESTION
    except:
        PROMETHEUS_INGESTION = False

    # @added 20211026 - Branch #4300: prometheus
    prometheus_settings = {}
    try:
        prometheus_settings = settings.PROMETHEUS_SETTINGS
    except AttributeError:
        prometheus_settings = {}
    except Exception as err:
        prometheus_settings = {}
    # @modified 20220622 - Task #2732: Prometheus to Skyline
    #                      Branch #4300: prometheus
    # if prometheus_settings:
    if prometheus_settings or PROMETHEUS_INGESTION:
        try:
            from prometheus import PrometheusMetricDataPost
        except Exception as err:
            print(traceback.format_exc())
            print('flux :: error from prometheus import PrometheusMetricDataPost - %s' % err)

    # @added 20220405 - Feature #4516: flux - opentelemetry traces
    FLUX_OTEL_ENABLED = False
    try:
        FLUX_OTEL_ENABLED = settings.FLUX_OTEL_ENABLED
    except AttributeError:
        FLUX_OTEL_ENABLED = False
    except Exception as err:
        FLUX_OTEL_ENABLED = False
    if FLUX_OTEL_ENABLED:
        from listen import OTLPTracePost

    # @added 20220530 - Feature #4596: flux - prometheus_alerts
    # @modified 20230110 - Task #4778: v4.0.0 - update dependencies
    # Commented out PrometheusAlertPost as WIP not complete
    # from prometheus_alerts import PrometheusAlertPost

# @added 20201018 - Feature #3798: FLUX_PERSIST_QUEUE
try:
    FLUX_PERSIST_QUEUE = settings.FLUX_PERSIST_QUEUE
except:
    FLUX_PERSIST_QUEUE = False

# @added 20221126 - Feature #4732: flux vortex
#                   Feature #4734: mirage_vortex
try:
    VORTEX_ENABLED = settings.VORTEX_ENABLED
except:
    VORTEX_ENABLED = True

logger = set_up_logging(None)
pid = os.getpid()
logger.info('flux :: starting flux listening on %s with pid %s' % (str(settings.FLUX_IP), str(pid)))

logger.info('flux :: creating queue httpMetricDataQueue')
# @modified 20191010 - Bug #3254: flux.populateMetricQueue Full
# httpMetricDataQueue = Queue(maxsize=3000)
# @modified 20191129 - Bug #3254: flux.populateMetricQueue Full
# Set to infinite
# httpMetricDataQueue = Queue(maxsize=30000)
httpMetricDataQueue = Queue(maxsize=0)

# @added 20201018 - Feature #3798: FLUX_PERSIST_QUEUE
redis_conn_decoded = get_redis_conn_decoded('flux')
logger.info('flux :: checked flux.queue Redis set to add any persisted items to the queue')
saved_queue_data = []
saved_queue_data_raw = None
try:
    saved_queue_data_raw = redis_conn_decoded.smembers('flux.queue')
except:
    saved_queue_data_raw = None
if not FLUX_PERSIST_QUEUE and saved_queue_data_raw:
    logger.info('flux :: flux.queue Redis set found but FLUX_PERSIST_QUEUE is set to %s' % str(FLUX_PERSIST_QUEUE))
    logger.info('flux :: deleting flux.queue Redis set')
    try:
        redis_conn_decoded.delete('flux.queue')
    except:
        saved_queue_data_raw = None
    saved_queue_data_raw = None
if saved_queue_data_raw:
    logger.info('flux :: %s raw items found in the flux.queue Redis set' % str(len(saved_queue_data_raw)))
    for item in saved_queue_data_raw:
        queue_item = literal_eval(item)
        saved_queue_data.append(queue_item)
saved_queue_data_put_error_logged = False
saved_items_added_to_queue = 0
if saved_queue_data:
    logger.info('flux :: %s items in the flux.queue Redis set to add to queue' % str(len(saved_queue_data)))
    for metric_data in saved_queue_data:
        try:
            httpMetricDataQueue.put(metric_data, block=False)
            saved_items_added_to_queue += 1
        except:
            if not saved_queue_data_put_error_logged:
                logger.error(traceback.format_exc())
                logger.error('error :: failed to add saved_queue_data item to flux.httpMetricDataQueue - %s' % str(metric_data))
                saved_queue_data_put_error_logged = True
    logger.info('flux :: %s items from the flux.queue Redis set added to queue' % str(saved_items_added_to_queue))
    try:
        metric_data_queue_size = httpMetricDataQueue.qsize()
        logger.info('flux :: httpMetricDataQueue queue size is now - %s' % str(metric_data_queue_size))
    except:
        logger.error(traceback.format_exc())
        logger.error('error :: failed to determine size of queue httpMetricDataQueue')
else:
    if FLUX_PERSIST_QUEUE:
        logger.info('flux :: there were no items in the flux.queue Redis set to add to the queue')
    else:
        logger.info('flux :: FLUX_PERSIST_QUEUE is set to %s, not persisting' % str(FLUX_PERSIST_QUEUE))

logger.info('flux :: starting %s aggregator processes' % str(settings.FLUX_WORKERS))
Aggregator(pid).start()

# @modified 20220428 - Feature #4536: Handle Redis failure
# logger.info('flux :: starting %s gunicorn worker/s' % str(settings.FLUX_WORKERS))
# Worker(httpMetricDataQueue, pid).start()
logger.info('flux :: starting %s gunicorn worker/s, each with 2 threads' % str(settings.FLUX_WORKERS))
Worker(httpMetricDataQueue, pid).start()
Worker(httpMetricDataQueue, pid).start()
if settings.FLUX_WORKERS >= 2:
    Worker(httpMetricDataQueue, pid).start()
    Worker(httpMetricDataQueue, pid).start()
if settings.FLUX_WORKERS >= 3:
    Worker(httpMetricDataQueue, pid).start()
    Worker(httpMetricDataQueue, pid).start()
if settings.FLUX_WORKERS > 3:
    Worker(httpMetricDataQueue, pid).start()
    Worker(httpMetricDataQueue, pid).start()

# @modified 20191010 - Bug #3254: flux.populateMetricQueue Full
# populateMetricQueue = Queue(maxsize=3000)
# @modified 20191116 - Bug #3254: flux.populateMetricQueue Full
# @modified 20191129 - Bug #3254: flux.populateMetricQueue Full
# Set to infinite
# populateMetricQueue = Queue(maxsize=300000)
populateMetricQueue = Queue(maxsize=0)

logger.info('flux :: starting populate_metric_worker')
PopulateMetricWorker(populateMetricQueue, pid).start()

# @added 20200517 - Feature #3550: flux.uploaded_data_worker
if flux_process_uploads:
    logger.info('flux :: starting uploaded_data_worker')
    UploadedDataWorker(pid).start()

# api = application = falcon.API()
api = application = falcon.App()
# api.req_options.auto_parse_form_urlencoded=True

httpMetricData = MetricData()
populateMetric = PopulateMetric()
httpMetricDataPost = MetricDataPost()

# @added 20211026 - Branch #4300: prometheus
prometheusMetricDataPost = None
# @modified 20220622 - Task #2732: Prometheus to Skyline
#                      Branch #4300: prometheus
# if prometheus_settings:
if prometheus_settings or PROMETHEUS_INGESTION:
    try:
        logger.info('flux :: starting prometheusMetricDataPost')
        try:
            from prometheus import PrometheusMetricDataPost
            logger.info('flux :: imported prometheusMetricDataPost')
        except Exception as err:
            print(traceback.format_exc())
            print('flux :: error from prometheus import PrometheusMetricDataPost - %s' % err)

        prometheusMetricDataPost = PrometheusMetricDataPost()
    except Exception as err:
        logger.debug(traceback.format_exc())
        logger.debug('flux :: error starting prometheusMetricDataPost - %s' % err)


# @added 20220408 - Feature #4516: flux - opentelemetry traces
if FLUX_OTEL_ENABLED:
    otelTracePost = OTLPTracePost()

# @added 20220530 - Feature #4596: flux - prometheus_alerts
# @modified 20230110 - Task #4778: v4.0.0 - update dependencies
# Commented out PrometheusAlertPost as WIP not complete
# prometheusAlertPost = PrometheusAlertPost()

api.add_route('/metric_data', httpMetricData)
api.add_route('/populate_metric', populateMetric)
api.add_route('/metric_data_post', httpMetricDataPost)
# @added 20211026 - Branch #4300: prometheus
# @modified 20220622 - Task #2732: Prometheus to Skyline
#                      Branch #4300: prometheus
# if prometheus_settings:
if prometheus_settings or PROMETHEUS_INGESTION:
    if prometheusMetricDataPost:
        api.add_route('/prometheus/write', prometheusMetricDataPost)

# @added 20220408 - Feature #4516: flux - opentelemetry traces
if FLUX_OTEL_ENABLED:
    api.add_route('/otlp/v1/trace', otelTracePost)

# @added 20220530 - Feature #4596: flux - prometheus_alerts
# @modified 20230110 - Task #4778: v4.0.0 - update dependencies
# Commented out PrometheusAlertPost as WIP not complete
# api.add_route('/prometheus_alerts', prometheusAlertPost)

# @added 20221118 - Feature #4732: flux vortex
#                   Feature #4734: mirage_vortex
if VORTEX_ENABLED:
    vortex_path = '%s/flux/vortex/data' % settings.SKYLINE_DIR
    if not os.path.exists(vortex_path):
        try:
            mkdir_p(vortex_path)
        except Exception as err:
            logger.error('error :: flux :: failed to create dir - %s - %s' % (
                vortex_path, err))
    vortexResults = VortexResults()
    api.add_route('/vortex_results', vortexResults)
    vortexPost = VortexDataPost()
    api.add_route('/vortex', vortexPost)
