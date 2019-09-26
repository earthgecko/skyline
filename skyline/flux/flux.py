import sys
import os
from multiprocessing import Queue

import falcon

sys.path.append(os.path.join(os.path.dirname(os.path.realpath(__file__)), os.pardir))
sys.path.insert(0, os.path.dirname(__file__))
import settings

from logger import set_up_logging
from listen import MetricData, MetricDataPost
# from listen_post import MetricDataPost
from worker import Worker
from populate_metric import PopulateMetric
from populate_metric_worker import PopulateMetricWorker

logger = set_up_logging(None)
pid = os.getpid()
logger.info('flux :: starting flux with pid %s' % str(pid))

logger.info('flux :: starting worker')
httpMetricDataQueue = Queue(maxsize=3000)
Worker(httpMetricDataQueue, pid).start()

populateMetricQueue = Queue(maxsize=3000)
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
