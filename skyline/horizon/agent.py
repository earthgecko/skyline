import logging
import time
import sys
from os import getpid
from os.path import dirname, abspath, isdir, join
from multiprocessing import Queue
from daemon import runner
from logging.handlers import TimedRotatingFileHandler, MemoryHandler

import os.path
sys.path.append(os.path.join(os.path.dirname(os.path.realpath(__file__)), os.pardir))
sys.path.insert(0, os.path.dirname(__file__))
import settings

from listen import Listen
from roomba import Roomba
from worker import Worker

skyline_app = 'horizon'
skyline_app_logger = '%sLog' % skyline_app
# logger = logging.getLogger("HorizonLog")
# TODO: http://stackoverflow.com/questions/6728236/exception-thrown-in-multiprocessing-pool-not-detected
logger = logging.getLogger(skyline_app_logger)
skyline_app_logfile = '%s/%s.log' % (settings.LOG_PATH, skyline_app)
skyline_app_loglock = '%s.lock' % skyline_app_logfile
skyline_app_logwait = '%s.wait' % skyline_app_logfile
logfile = '%s/%s.log' % (settings.LOG_PATH, skyline_app)


class Horizon():
    """
    Initializes Horizon
    """

    def __init__(self):
        self.stdin_path = '/dev/null'
        self.stdout_path = '%s/%s.log' % (settings.LOG_PATH, skyline_app)
        self.stderr_path = '%s/%s.log' % (settings.LOG_PATH, skyline_app)
        self.pidfile_path = '%s/%s.pid' % (settings.PID_PATH, skyline_app)
        self.pidfile_timeout = 5

    def run(self):
        """
        Determine the `MAX_QUEUE_SIZE` for the listen process.

        Determine if horizon should populate the mini redis store for Oculus.

        Starts the defined number of `WORKER_PROCESSES`, with the first worker
        populating the canary metric.

        Start the pickle (and UDP) listen processes.

        Start roomba.
        """
        logger.info('agent starting skyline %s' % skyline_app)
        listen_queue = Queue(maxsize=settings.MAX_QUEUE_SIZE)
        pid = getpid()

        # If we're not using oculus, don't bother writing to mini
        try:
            skip_mini = True if settings.OCULUS_HOST == '' else False
        except Exception:
            skip_mini = True

        # Start the workers
        for i in range(settings.WORKER_PROCESSES):
            if i == 0:
                logger.info('%s :: starting Worker - canary' % skyline_app)
                Worker(listen_queue, pid, skip_mini, canary=True).start()
            else:
                logger.info('%s :: starting Worker' % skyline_app)
                Worker(listen_queue, pid, skip_mini).start()

        # Start the listeners
        logger.info('%s :: starting Listen - pickle' % skyline_app)
        Listen(settings.PICKLE_PORT, listen_queue, pid, type="pickle").start()
        logger.info('%s :: starting Listen - udp' % skyline_app)
        Listen(settings.UDP_PORT, listen_queue, pid, type="udp").start()

        # Start the roomba
        logger.info('%s :: starting Roomba' % skyline_app)
        Roomba(pid, skip_mini).start()

        # Warn the Mac users
        try:
            listen_queue.qsize()
        except NotImplementedError:
            logger.info('WARNING: Queue().qsize() not implemented on Unix platforms like Mac OS X. Queue size logging will be unavailable.')

        # Keep yourself occupied, sucka
        while 1:
            time.sleep(100)


def run():
    """
    Start the Horizon agent and logger.
    """
    if not isdir(settings.PID_PATH):
        print ('pid directory does not exist at %s' % settings.PID_PATH)
        sys.exit(1)

    if not isdir(settings.LOG_PATH):
        print ('log directory does not exist at %s' % settings.LOG_PATH)
        sys.exit(1)

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

    horizon = Horizon()

    if len(sys.argv) > 1 and sys.argv[1] == 'run':
        horizon.run()
    else:
        daemon_runner = runner.DaemonRunner(horizon)
        daemon_runner.daemon_context.files_preserve = [handler.stream]
        daemon_runner.do_action()

if __name__ == "__main__":
    run()
