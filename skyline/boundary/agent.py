import logging
import sys
import traceback
from os import getpid
from os.path import dirname, abspath, isdir
from daemon import runner
from time import sleep, time
from logging.handlers import TimedRotatingFileHandler, MemoryHandler

import os.path
sys.path.append(os.path.join(os.path.dirname(os.path.realpath(__file__)), os.pardir))
sys.path.insert(0, os.path.dirname(__file__))
import settings

from boundary import Boundary
from boundary_algorithms import *

skyline_app = 'boundary'
skyline_app_logger = '%sLog' % skyline_app
logger = logging.getLogger(skyline_app_logger)
logfile = '%s/%s.log' % (settings.LOG_PATH, skyline_app)


class BoundaryAgent():
    def __init__(self):
        self.stdin_path = '/dev/null'
        self.stdout_path = '%s/%s.log' % (settings.LOG_PATH, skyline_app)
        self.stderr_path = '%s/%s.log' % (settings.LOG_PATH, skyline_app)
        self.pidfile_path = '%s/%s.pid' % (settings.PID_PATH, skyline_app)
        self.pidfile_timeout = 5

    def run(self):
        logger.info('agent starting skyline %s' % skyline_app)
        Boundary(getpid()).start()

        while 1:
            sleep(100)


def run():
    """
    Start the Boundary agent.
    """
    if not isdir(settings.PID_PATH):
        print ('pid directory does not exist at %s' % settings.PID_PATH)
        sys.exit(1)

    if not isdir(settings.LOG_PATH):
        print ('log directory does not exist at %s' % settings.LOG_PATH)
        sys.exit(1)

    # Make sure all the BOUNDARY_ALGORITHMS are valid
    try:
        if settings.BOUNDARY_ALGORITHMS:
            configuration_error = False
            for algorithm in settings.BOUNDARY_ALGORITHMS:
                valid = True
                if not isinstance(algorithm, str):
                    valid = False

                if not valid:
                    configuration_error = True
                    print ('configuration error in tuple, expected: str')
                    print('configuration error in BOUNDARY_ALGORITHMS tuple: %s' % str(algorithm))
    except:
        try:
            if configuration_error:
                print ('There are configuration issues in BOUNDARY_ALGORITHMS in settings.py')
        except:
            print ('There are no BOUNDARY_ALGORITHMS in settings.py. try adding some, nothing to do')
        sys.exit(1)

    # Make sure we can run all the algorithms
    try:
        timeseries = map(list, zip(map(float, range(int(time()) - 86400, int(time()) + 1)), [1] * 86401))
        ensemble = [globals()[algorithm](
            timeseries, 'test', 3600, 100, 300, 1
        ) for algorithm in settings.BOUNDARY_ALGORITHMS]
    except KeyError as e:
        print ('Algorithm %s deprecated or not defined; check settings.BOUNDARY_ALGORITHMS' % e)
        sys.exit(1)
    except Exception as e:
        print ('Algorithm test run failed.')
        traceback.print_exc()
        sys.exit(1)

    # Make sure all the BOUNDARY_METRICS are valid
    try:
        if settings.BOUNDARY_METRICS:
            configuration_error = False
            for metric in settings.BOUNDARY_METRICS:
                valid = True
                strings = []
                strings.append(metric[0])
                strings.append(metric[1])
                strings.append(metric[7])
                for string in strings:
                    if not isinstance(string, str):
                        valid = False

                values = []
                values.append(metric[2])
                values.append(metric[3])
                values.append(metric[4])
                values.append(metric[5])
                values.append(metric[6])
                for value in values:
                    if not isinstance(value, int):
                        valid = False

                alert_via = metric[7]
                for alerter in alert_via.split("|"):
                    if not isinstance(alerter, str):
                        valid = False

                if not valid:
                    configuration_error = True
                    print ('configuration error in tuple, expected: str, str, int, int, int, int, str')
                    print('configuration error in BOUNDARY_METRICS tuple: %s' % str(metric))
    except:
        if configuration_error:
            print ('There are configuration issues in BOUNDARY_METRICS in settings.py')
        else:
            print ('There are no BOUNDARY_METRICS in settings.py. try adding some, nothing to do')

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

    boundary = BoundaryAgent()

    if len(sys.argv) > 1 and sys.argv[1] == 'run':
        boundary.run()
    else:
        daemon_runner = runner.DaemonRunner(boundary)
        daemon_runner.daemon_context.files_preserve = [handler.stream]
        daemon_runner.do_action()

if __name__ == "__main__":
    run()
