import sys
import os.path
# import logging
# import multiprocessing
# import traceback
# from logging.handlers import TimedRotatingFileHandler, MemoryHandler

from logger import set_up_logging

sys.path.append(os.path.join(os.path.dirname(os.path.realpath(__file__)), os.pardir))
sys.path.insert(0, os.path.dirname(__file__))

# @modified 20191115 - Branch #3262: py3
# This prevents flake8 E402 - module level import not at top of file
if True:
    import settings

bind = '%s:%s' % (settings.FLUX_IP, str(settings.FLUX_PORT))
# workers = multiprocessing.cpu_count() * 2 + 1
workers = settings.FLUX_WORKERS
backlog = settings.FLUX_BACKLOG


skyline_app = 'flux'
# skyline_app_logger = '%sLog' % skyline_app
# logfile = '%s/%s.log' % (settings.LOG_PATH, skyline_app)
# logger = logging.getLogger(skyline_app_logger)
logger = set_up_logging(None)

pidfile = '%s/%s.pid' % (settings.PID_PATH, skyline_app)

# access_log_format = '%(h)s %(l)s %(u)s %(t)s "%(r)s" %(s)s %(b)s "%(f)s" "%(a)s"'

# errorlog = '%s/flux.log' % (settings.LOG_PATH)

# logger.setLevel(logging.DEBUG)

# formatter = logging.Formatter("%(asctime)s :: %(process)s :: %(message)s", datefmt="%Y-%m-%d %H:%M:%S")
# handler = logging.handlers.TimedRotatingFileHandler(
#    logfile,
#    when="midnight",
#    interval=1,
#    backupCount=5)

# memory_handler = logging.handlers.MemoryHandler(100,
#                                                flushLevel=logging.DEBUG,
#                                                target=handler)
# handler.setFormatter(formatter)
# logger.addHandler(memory_handler)
