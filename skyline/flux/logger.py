import logging
import logging.handlers
from logging.handlers import TimedRotatingFileHandler, MemoryHandler
import os
from datetime import datetime
import sys

import os.path
sys.path.append(os.path.join(os.path.dirname(os.path.realpath(__file__)), os.pardir))
sys.path.insert(0, os.path.dirname(__file__))

# @modified 20191115 - Branch #3262: py3
# This prevents flake8 E402 - module level import not at top of file
if True:
    import settings

skyline_app = 'flux'
skyline_app_logger = '%sLog' % skyline_app
logger = logging.getLogger(skyline_app_logger)
logfile = '%s/%s.log' % (settings.LOG_PATH, skyline_app)


def set_up_logging(app):
    if not os.path.exists(settings.LOG_PATH):
        os.makedirs(settings.LOG_PATH)

    # current_time = datetime.now()
    # current_date = current_time.strftime("%Y-%m-%d")
    # file_name = current_date + '.log'
    # file_location = log_location + file_name
    # with open(logfile, 'a+'):
    if app:
        use_logfile = '%s/%s.%s.log' % (settings.LOG_PATH, skyline_app, app)
    else:
        use_logfile = logfile
    with open(use_logfile, 'a+'):
        pass

    # @modified 20200519 - Bug #3548: flux log rotation wiping rotated log
    # Wrap this in len(logger.handlers) as a fix for duplicate log lines
    if not len(logger.handlers):
        logger.setLevel(logging.DEBUG)
        formatter = logging.Formatter("%(asctime)s :: %(process)s :: %(message)s", datefmt="%Y-%m-%d %H:%M:%S")
        handler = logging.handlers.TimedRotatingFileHandler(
            use_logfile,
            when="midnight",
            interval=1,
            backupCount=5)

        memory_handler = logging.handlers.MemoryHandler(256,
                                                        flushLevel=logging.DEBUG,
                                                        target=handler)
        handler.setFormatter(formatter)
        logger.addHandler(memory_handler)

        # logger = logging.getLogger(skyline_app)
        # format = '[%(asctime)s] [%(levelname)s] [%(message)s] [--> %(pathname)s [%(process)d]:]'
        # format = '%(asctime)s [%(levelname)s] %(process)d: %(message)s'
        # To store in file
        # logging.basicConfig(format=format, filemode='a+', filename=file_location, level=logging.DEBUG)
        # logging.basicConfig(format=format, filemode='a', filename=file_location)
        # logging.basicConfig(filename='app.log', filemode='w', format='%(name)s - %(levelname)s - %(message)s')
        # To print only
        # logging.basicConfig(format=format, level=logging.DEBUG)

    return logger
