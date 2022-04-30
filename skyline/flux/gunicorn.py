import sys
import os.path
from os import getpid

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
    from skyline_functions import get_redis_conn

# @added 20220427 - Feature #4536: Handle Redis failure
if settings.MEMCACHE_ENABLED:
    from functions.memcache.set_memcache_key import set_memcache_key
    from functions.memcache.delete_memcache_key import delete_memcache_key
else:
    set_memcache_key = None
    delete_memcache_key = None

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

pid = getpid()
redis_conn = get_redis_conn(skyline_app)
try:
    redis_conn.set('flux.main_process_pid', pid)
    logger.info('flux :: gunicorn.py :: set the Redis key flux.main_process_pid to %s' % (
        str(pid)))
    del redis_conn
except Exception as err:
    logger.error('error :: flux :: gunicorn.py :: failed to set the Redis key flux.main_process_pid to %s, exit - %s' % (
        str(pid), str(err)))
# @added 20220428 - Feature #4536: Handle Redis failure
# Add flux required data to memcache as well
success = False
if settings.MEMCACHE_ENABLED:
    try:
        success = set_memcache_key(skyline_app, 'flux.main_process_pid', pid)
        if success:
            logger.info('flux :: gunicorn.py :: set memcache flux.main_process_pid')
    except Exception as err:
        logger.error('error :: metrics_manager :: set_memcache_key failed to set flux.main_process_pid - %s' % (
            err))
    success = False
    try:
        success = delete_memcache_key(skyline_app, 'flux.worker.primary_worker_key')
    except Exception as err:
        logger.error('error :: worker :: failed to delete memcache flux.worker.primary_worker_key - %s' % (str(err)))
    if success:
        logger.info('worker :: deleted flux.worker.primary_worker_key memcache key')


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
