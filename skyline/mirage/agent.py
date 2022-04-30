import logging
import sys
import traceback
from os import getpid
from os.path import isdir
from daemon import runner
from time import sleep, time
from logging.handlers import TimedRotatingFileHandler, MemoryHandler
from sys import version_info

import os.path
sys.path.append(os.path.join(os.path.dirname(os.path.realpath(__file__)), os.pardir))
sys.path.insert(0, os.path.dirname(__file__))

# @modified 20191115 - Branch #3262: py3
# This prevents flake8 E402 - module level import not at top of file
if True:
    import settings
    from mirage import Mirage
    from mirage_algorithms import *
    from validate_settings import validate_settings_variables
    # @added 20220328 - Feature #4018: thunder - skyline.errors
    from functions.redis.RedisErrorLogHandler import RedisErrorLogHandler

skyline_app = 'mirage'
skyline_app_logger = '%sLog' % skyline_app
logger = logging.getLogger(skyline_app_logger)
logfile = '%s/%s.log' % (settings.LOG_PATH, skyline_app)
# @added 20191024 - Branch #3262: py3
python_version = int(version_info[0])


class MirageAgent():
    def __init__(self):
        self.stdin_path = '/dev/null'
        self.stdout_path = '%s/%s.log' % (settings.LOG_PATH, skyline_app)
        self.stderr_path = '%s/%s.log' % (settings.LOG_PATH, skyline_app)
        self.pidfile_path = '%s/%s.pid' % (settings.PID_PATH, skyline_app)
        self.pidfile_timeout = 5

    def run(self):
        logger.info('agent starting skyline %s' % skyline_app)
        Mirage(getpid()).start()

        while 1:
            sleep(100)


def run():
    """
    Start the Mirage agent.
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

    handler.setFormatter(formatter)
    logger.addHandler(handler)

    # @added 20220328 - Feature #4018: thunder - skyline.errors
    # For every error logged set a count in the app Redis key which is consumed
    # by thunder and creates the sskyline.<hostname>.<skyline_app>.logged_errors
    # metric
    redis_error_log_handler = RedisErrorLogHandler(skyline_app)
    redis_error_log_handler.setLevel(logging.ERROR)
    redis_error_log_handler.setFormatter(formatter)
    logger.addHandler(redis_error_log_handler)

    # Validate settings variables
    valid_settings = validate_settings_variables(skyline_app)

    if not valid_settings:
        print('error :: invalid variables in settings.py - cannot start')
        sys.exit(1)

    # Make sure we can run all the algorithms
    try:
        timeseries = map(list, zip(map(float, range(int(time()) - 86400, int(time()) + 1)), [1] * 86401))

        # @added 20191024 - Branch #3262: py3
        # Convert map to list
        if python_version == 3:
            if isinstance(timeseries, map):
                timeseries = list(timeseries)

        second_order_resolution_seconds = 86400
        ensemble = [globals()[algorithm](timeseries, second_order_resolution_seconds) for algorithm in settings.MIRAGE_ALGORITHMS]
    except KeyError as e:
        print('Algorithm %s deprecated or not defined; check settings.MIRAGE_ALGORITHMS' % e)
        sys.exit(1)
    except Exception as e:
        print('Algorithm test run failed - %s' % e)
        traceback.print_exc()
        sys.exit(1)

    logger.info('Tested algorithms')
    del timeseries
    del ensemble

    mirage = MirageAgent()

    if len(sys.argv) > 1 and sys.argv[1] == 'run':
        mirage.run()
    else:
        daemon_runner = runner.DaemonRunner(mirage)
        daemon_runner.daemon_context.files_preserve = [handler.stream]
        daemon_runner.do_action()


if __name__ == '__main__':
    run()
