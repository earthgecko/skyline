import sys
import os.path
import logging
# import multiprocessing
# import traceback
from logging.handlers import TimedRotatingFileHandler, MemoryHandler

sys.path.append(os.path.join(os.path.dirname(os.path.realpath(__file__)), os.pardir))
sys.path.insert(0, os.path.dirname(__file__))
import settings

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
