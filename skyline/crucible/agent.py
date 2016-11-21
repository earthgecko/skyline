import logging
import sys
import os
from os import getpid
from os.path import isdir
from daemon import runner
from time import sleep
from logging.handlers import TimedRotatingFileHandler, MemoryHandler

import os.path
sys.path.append(os.path.join(os.path.dirname(os.path.realpath(__file__)), os.pardir))
sys.path.insert(0, os.path.dirname(__file__))
import settings
from validate_settings import validate_settings_variables

from crucible import Crucible
from crucible_algorithms import run_algorithms

python_version = int(sys.version_info[0])

skyline_app = 'crucible'
skyline_app_logger = '%sLog' % skyline_app
logger = logging.getLogger(skyline_app_logger)
logfile = '%s/%s.log' % (settings.LOG_PATH, skyline_app)


class CrucibleAgent():
    """
    Initialize CrucibleAgent
    """
    def __init__(self):
        self.stdin_path = '/dev/null'
        self.stdout_path = '%s/%s.log' % (settings.LOG_PATH, skyline_app)
        self.stderr_path = '%s/%s.log' % (settings.LOG_PATH, skyline_app)
        self.pidfile_path = '%s/%s.pid' % (settings.PID_PATH, skyline_app)
        self.pidfile_timeout = 5

    def run(self):
        logger.info('agent starting skyline %s' % skyline_app)
        Crucible(getpid()).start()

        while 1:
            if settings.ENABLE_CRUCIBLE_DEBUG:
                logger.info('CrucibleAgent alive')
            sleep(100)


def run():
    """
    Start the Crucible agent and ensure all the required directories exist,
    creating the crucible directories if they do not exist
    """
    if not isdir(settings.PID_PATH):
        print ('pid directory does not exist at %s' % settings.PID_PATH)
        sys.exit(1)

    if not isdir(settings.LOG_PATH):
        print ('log directory does not exist at %s' % settings.LOG_PATH)
        sys.exit(1)

    # Make sure the required directories exists
    if not os.path.exists(settings.CRUCIBLE_CHECK_PATH):
        try:
            os.makedirs(settings.CRUCIBLE_CHECK_PATH, mode=0o755)
        except:
            print ('failed to create directory - %s' % settings.CRUCIBLE_CHECK_PATH)
            sys.exit(1)

    if not os.path.exists(settings.CRUCIBLE_DATA_FOLDER):
        try:
            os.makedirs(settings.CRUCIBLE_DATA_FOLDER, mode=0o755)
        except:
            print ('failed to create directory - %s' % settings.CRUCIBLE_DATA_FOLDER)
            sys.exit(1)

    failed_checks_dir = settings.CRUCIBLE_DATA_FOLDER + '/failed_checks'
    if not os.path.exists(failed_checks_dir):
        try:
            os.makedirs(failed_checks_dir, mode=0o755)
        except:
            print ('failed to create directory - %s' % failed_checks_dir)
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
        print ('error :: invalid variables in settings.py - cannot start')
        sys.exit(1)

    crucible = CrucibleAgent()

    if len(sys.argv) > 1 and sys.argv[1] == 'run':
        logger.info('starting skyline crucible via run')
        crucible.run()
    else:
        logger.info('starting skyline crucible via daemon')
        daemon_runner = runner.DaemonRunner(crucible)
        daemon_runner.daemon_context.files_preserve = [handler.stream]
        daemon_runner.do_action()

if __name__ == '__main__':
    run()
