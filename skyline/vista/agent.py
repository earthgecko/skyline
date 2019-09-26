import logging
import sys
import traceback
from os import getpid
from os.path import isdir
from daemon import runner
from time import sleep, time

from logging.handlers import TimedRotatingFileHandler, MemoryHandler

import os.path
sys.path.append(os.path.join(os.path.dirname(os.path.realpath(__file__)), os.pardir))
sys.path.insert(0, os.path.dirname(__file__))

if True:
    import settings
    from validate_settings import validate_settings_variables

from fetcher import Fetcher
from worker import Worker

skyline_app = 'vista'
skyline_app_logger = '%sLog' % skyline_app
logger = logging.getLogger(skyline_app_logger)
logfile = '%s/%s.log' % (settings.LOG_PATH, skyline_app)


class Vista():
    """
    Initializes Vista
    """

    def __init__(self):
        self.stdin_path = '/dev/null'
        self.stdout_path = '%s/%s.log' % (settings.LOG_PATH, skyline_app)
        self.stderr_path = '%s/%s.log' % (settings.LOG_PATH, skyline_app)
        self.pidfile_path = '%s/%s.pid' % (settings.PID_PATH, skyline_app)
        self.pidfile_timeout = 5

    def run(self):
        logger.info('%s :: agent starting' % skyline_app)
        pid = getpid()

        # Start the workers
        for i in range(settings.VISTA_WORKER_PROCESSES):
            if i == 0:
                logger.info('%s :: agent :: starting Worker 1' % skyline_app)
                Worker(pid).start()
            else:
                logger.info('%s :: agent :: starting Worker %s' % (skyline_app, str(i)))
                Worker(pid).start()

        # Start the fetcher
        logger.info('%s :: agent :: starting Fetcher' % skyline_app)
        Fetcher(pid).start()

        while 1:
            sleep(100)

def run():
    """
    Start the Vista.

    Start the logger.
    """
    if not isdir(settings.PID_PATH):
        print('pid directory does not exist at %s' % settings.PID_PATH)
        sys.exit(1)

    if not isdir(settings.LOG_PATH):
        print('log directory does not exist at %s' % settings.LOG_PATH)
        sys.exit(1)

    logger.setLevel(logging.DEBUG)
    formatter = logging.Formatter("%(asctime)s :: %(process)s :: %(message)s", datefmt="%Y-%m-%d %H:%M:%S")
    handler = logging.handlers.TimedRotatingFileHandler(
        logfile,
        when="midnight",
        interval=1,
        backupCount=5)

    memory_handler = logging.handlers.MemoryHandler(256,
                                                    flushLevel=logging.DEBUG,
                                                    target=handler)
    handler.setFormatter(formatter)
    logger.addHandler(memory_handler)

    # Validate settings variables
    valid_settings = validate_settings_variables(skyline_app)

    if not valid_settings:
        print ('error :: agent :: invalid variables in settings.py - cannot start')
        sys.exit(1)

    vista = Vista()

    logger.info('agent :: starting vista')

    memory_handler.flush

    if len(sys.argv) > 1 and sys.argv[1] == 'run':
        vista.run()
    else:
        daemon_runner = runner.DaemonRunner(vista)
        daemon_runner.daemon_context.files_preserve = [handler.stream]
        daemon_runner.do_action()

    logger.info('stopping vista')
    memory_handler.flush


if __name__ == '__main__':
    run()
