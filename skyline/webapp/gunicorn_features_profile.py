"""
gunicorn_features_profile.py
"""
import sys
import os.path
import logging
from logging.handlers import MemoryHandler

sys.path.append(os.path.join(os.path.dirname(os.path.realpath(__file__)), os.pardir))
sys.path.insert(0, os.path.dirname(__file__))

if True:
    import settings

# @added 20221208 - Feature #4756: Use gevent gunicorn worker_class
#                   Feature #4732: flux vortex
# A workaround process to handle tsfresh multiprocessing as it is not
# possible with the current webapp layout and gunicorn running a gevent
# worker_class.  This app uses the sync worker_class and allows for the
# normal calculate_features_profile function to be run.
# worker_class = 'sync'
worker_class = 'gthread'

bind = '127.0.0.1:%s' % str((settings.WEBAPP_PORT + 1))
# These setting allows for the concurrent creation of 4 features profiles at
# once, which should be sufficient in normal circumstances.  Bearing in mind
# that this applies only to requests by a user via the webapp to create
# features profiles, this does not apply to Ionosphere, which has no such limits
workers = 2
threads = 2
# This timeout is set high for instances where the server is heavily loaded and
# requests are made to create features profiles on high resolution data e.g.
# > 10000 data points.  In these circumstances features profile generation can
# take a while.
timeout = 120
backlog = 10

skyline_app = 'webapp'
skyline_app_logger = '%sLog' % skyline_app
logfile = '%s/%s.log' % (settings.LOG_PATH, skyline_app)
logger = logging.getLogger(skyline_app_logger)

pidfile = '%s/%s_features_profile.pid' % (settings.PID_PATH, skyline_app)

accesslog = '%s/webapp.access.log' % (settings.LOG_PATH)
access_log_format = '%(h)s %(l)s %(u)s %(t)s "%(r)s" %(s)s %(b)s "%(f)s" "%(a)s"'

errorlog = '%s/webapp.log' % (settings.LOG_PATH)

logger.setLevel(logging.DEBUG)

formatter = logging.Formatter("%(asctime)s :: %(process)s :: %(message)s", datefmt="%Y-%m-%d %H:%M:%S")
handler = logging.FileHandler(logfile, mode='a')
memory_handler = logging.handlers.MemoryHandler(100,
                                                flushLevel=logging.DEBUG,
                                                target=handler)
handler.setFormatter(formatter)
logger.addHandler(memory_handler)
