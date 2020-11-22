import logging
import time
import sys
from os import getpid
# @modified 20191115 - Branch #3262: py3
# from os.path import dirname, abspath, isdir, join
from os.path import isdir

from multiprocessing import Queue
from daemon import runner
from logging.handlers import TimedRotatingFileHandler, MemoryHandler

import os.path
sys.path.append(os.path.join(os.path.dirname(os.path.realpath(__file__)), os.pardir))
sys.path.insert(0, os.path.dirname(__file__))

# @modified 20191115 - Branch #3262: py3
# This prevents flake8 E402 - module level import not at top of file
if True:
    import settings
    from validate_settings import validate_settings_variables
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

# @added 20201122 - Feature #3820: HORIZON_SHARDS
# Add an additional listen process on a different port for the shard
try:
    HORIZON_SHARDS = settings.HORIZON_SHARDS.copy()
except:
    HORIZON_SHARDS = {}
try:
    HORIZON_SHARD_PICKLE_PORT = settings.HORIZON_SHARD_PICKLE_PORT
except:
    HORIZON_SHARD_PICKLE_PORT = None


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
                # @modified 20201017 - Feature #3788: snab_flux_load_test
                #                      Feature #3680: horizon.worker.datapoints_sent_to_redis
                # Added worker_number
                logger.info('%s :: starting Worker - canary, worker number %s' % (skyline_app, str(i)))
                Worker(listen_queue, pid, skip_mini, worker_number=i, canary=True).start()
            else:
                logger.info('%s :: starting Worker, worker number %s' % (skyline_app, str(i)))
                Worker(listen_queue, pid, skip_mini, worker_number=i, canary=False).start()

        # Start the listeners
        logger.info('%s :: starting Listen - pickle' % skyline_app)
        Listen(settings.PICKLE_PORT, listen_queue, pid, type="pickle").start()
        logger.info('%s :: starting Listen - udp' % skyline_app)
        Listen(settings.UDP_PORT, listen_queue, pid, type="udp").start()

        # @added 20201122 - Feature #3820: HORIZON_SHARDS
        # Add an additional listen process on a different port for the shard
        if HORIZON_SHARDS:
            if HORIZON_SHARD_PICKLE_PORT:
                logger.info('%s :: starting Listen - pickle for horizon shard on port %s' % (
                    skyline_app, str(settings.HORIZON_SHARD_PICKLE_PORT)))
                try:
                    Listen(settings.HORIZON_SHARD_PICKLE_PORT, listen_queue, pid, type="pickle").start()
                except Exception as e:
                    logger.error('error :: agent.py falied to start Listen for horizon shard - %s' % str(e))
            else:
                logger.error('error :: agent.py could not start the horizon shard Listen process as no port')

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
        print('pid directory does not exist at %s' % settings.PID_PATH)
        sys.exit(1)

    if not isdir(settings.LOG_PATH):
        print('log directory does not exist at %s' % settings.LOG_PATH)
        sys.exit(1)

    # @added 20201103 - Feature #3820: HORIZON_SHARDS
    try:
        HORIZON_SHARDS = settings.HORIZON_SHARDS.copy()
    except:
        HORIZON_SHARDS = {}
    horizon_shards_validated = True
    if HORIZON_SHARDS:
        for shard_host in HORIZON_SHARDS:
            try:
                int(HORIZON_SHARDS[shard_host])
            except:
                horizon_shards_validated = False
        if not horizon_shards_validated:
            print('the HORIZON_SHARDS dict in settings.py does not have valid shard numbers' % str(settings.HORIZON_SHARDS))
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

    # Validate settings variables
    valid_settings = validate_settings_variables(skyline_app)

    if not valid_settings:
        print('error :: invalid variables in settings.py - cannot start')
        sys.exit(1)

    horizon = Horizon()

    if len(sys.argv) > 1 and sys.argv[1] == 'run':
        horizon.run()
    else:
        daemon_runner = runner.DaemonRunner(horizon)
        daemon_runner.daemon_context.files_preserve = [handler.stream]
        daemon_runner.do_action()


if __name__ == "__main__":
    run()
