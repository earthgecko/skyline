import logging
import sys
import traceback
from os import getpid
from os.path import dirname, abspath, isdir
from daemon import runner
from time import sleep, time

# @added 20150914 - added log rotation
from logging.handlers import TimedRotatingFileHandler

# add the shared settings file to namespace
sys.path.insert(0, dirname(dirname(abspath(__file__))))
import settings

from mirage import Mirage


class MirageAgent():
    def __init__(self):
        self.stdin_path = '/dev/null'
        self.stdout_path = settings.LOG_PATH + '/mirage.log'
        self.stderr_path = settings.LOG_PATH + '/mirage.log'
        self.pidfile_path = settings.PID_PATH + '/mirage.pid'
        self.pidfile_timeout = 5

    def run(self):
        logger.info('starting skyline mirage')
        Mirage(getpid()).start()

        while 1:
            sleep(100)

if __name__ == "__main__":
    """
    Start the Mirage agent.
    """
    if not isdir(settings.PID_PATH):
        print 'pid directory does not exist at %s' % settings.PID_PATH
        sys.exit(1)

    if not isdir(settings.LOG_PATH):
        print 'log directory does not exist at %s' % settings.LOG_PATH
        sys.exit(1)

    # Make sure we can run all the algorithms
    try:
        from algorithms import *
        timeseries = map(list, zip(map(float, range(int(time()) - 86400, int(time()) + 1)), [1] * 86401))
        second_order_resolution_seconds = 86400
        ensemble = [globals()[algorithm](timeseries, second_order_resolution_seconds) for algorithm in settings.MIRAGE_ALGORITHMS]
    except KeyError as e:
        print "Algorithm %s deprecated or not defined; check settings.MIRAGE_ALGORITHMS" % e
        sys.exit(1)
    except Exception as e:
        print "Algorithm test run failed."
        traceback.print_exc()
        sys.exit(1)

    mirage = MirageAgent()

    logger = logging.getLogger("MirageLog")
    logger.setLevel(logging.DEBUG)
    formatter = logging.Formatter("%(asctime)s :: %(process)s :: %(message)s", datefmt="%Y-%m-%d %H:%M:%S")
# @modified 20150914 - added log rotation
#    handler = logging.FileHandler(settings.LOG_PATH + '/mirage.log')
    handler = logging.handlers.TimedRotatingFileHandler(settings.LOG_PATH + '/mirage.log',
                                       when="midnight",
                                       interval=1,
                                       backupCount=5)

    handler.setFormatter(formatter)
    logger.addHandler(handler)

    if len(sys.argv) > 1 and sys.argv[1] == 'run':
        mirage.run()
    else:
        daemon_runner = runner.DaemonRunner(mirage)
        daemon_runner.daemon_context.files_preserve = [handler.stream]
        daemon_runner.do_action()
