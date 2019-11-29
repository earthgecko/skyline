import sys
import os
from multiprocessing import Queue

import falcon

sys.path.append(os.path.join(os.path.dirname(os.path.realpath(__file__)), os.pardir))
sys.path.insert(0, os.path.dirname(__file__))

# @modified 20191115 - Branch #3262: py3
# This prevents flake8 E402 - module level import not at top of file
if True:
    import settings
    from logger import set_up_logging
    from listen import MetricData, MetricDataPost
    # from listen_post import MetricDataPost
    from worker import Worker
    from populate_metric import PopulateMetric
    from populate_metric_worker import PopulateMetricWorker

logger = set_up_logging(None)
pid = os.getpid()
logger.info('flux :: starting flux listening on %s with pid %s' % (str(settings.FLUX_IP), str(pid)))

logger.info('flux :: starting worker')
# @modified 20191010 - Bug #3254: flux.populateMetricQueue Full
# httpMetricDataQueue = Queue(maxsize=3000)
# @modified 20191129 - Bug #3254: flux.populateMetricQueue Full
# Set to infinite
#httpMetricDataQueue = Queue(maxsize=30000)
httpMetricDataQueue = Queue(maxsize=0)
Worker(httpMetricDataQueue, pid).start()

# @modified 20191010 - Bug #3254: flux.populateMetricQueue Full
# populateMetricQueue = Queue(maxsize=3000)
# @modified 20191116 - Bug #3254: flux.populateMetricQueue Full
# @modified 20191129 - Bug #3254: flux.populateMetricQueue Full
# Set to infinite
#populateMetricQueue = Queue(maxsize=300000)
populateMetricQueue = Queue(maxsize=0)

logger.info('flux :: starting populate_metric_worker')
PopulateMetricWorker(populateMetricQueue, pid).start()

api = application = falcon.API()
# api.req_options.auto_parse_form_urlencoded=True

httpMetricData = MetricData()
populateMetric = PopulateMetric()
httpMetricDataPost = MetricDataPost()

api.add_route('/metric_data', httpMetricData)
api.add_route('/populate_metric', populateMetric)
api.add_route('/metric_data_post', httpMetricDataPost)
