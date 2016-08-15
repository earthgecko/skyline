import logging
import sys
import traceback
from os import getpid
# from os.path import dirname, abspath, isdir
from os.path import isdir
from daemon import runner
from time import sleep, time

from logging.handlers import TimedRotatingFileHandler, MemoryHandler

import warnings
warnings.filterwarnings('error', 'a', RuntimeWarning, 'pandas', 0)

import os.path
if 'memory_profiler.py' in __file__:
    sys.path.insert(0, '/opt/skyline/github/skyline/skyline')
    sys.path.insert(0, '/opt/skyline/github/skyline/skyline/analyzer_debug')
    print(sys.path)
else:
    sys.path.append(os.path.join(os.path.dirname(os.path.realpath(__file__)), os.pardir))
    sys.path.insert(0, os.path.dirname(__file__))

import settings
from validate_settings import validate_settings_variables

from analyzer_debug import Analyzer

skyline_app = 'analyzer_debug'
skyline_app_logger = '%sLog' % skyline_app
logger = logging.getLogger(skyline_app_logger)
logfile = '%s/%s.log' % (settings.LOG_PATH, skyline_app)

"""
ENABLE_MEMORY_PROFILING - DEVELOPMENT ONLY

# @added 20160806 - Bug #1558: Memory leak in Analyzer
# Added all the memory profiling blocks - mem_top, pympler, objgraph, gc
Garbage collection et al, should not be run in anything but development model,
therefore these variables are hard coded and not accessible via settings.py,
if you are in here reading this then knock yourself out.  gc and dump_garbage
can be useful for getting an idea about what all the objects in play are, but
garbage collection will just take longer and longer to run.

"""
import resource
pytracker_enabled = False
heapy_enabled = False

# @added 20160808 - Bug #1558: Memory leak in Analyzer
if pytracker_enabled:
    from pytracker import Tracker, Trackable, set_global_tracker
    pytrk = Tracker()
    set_global_tracker(pytrk)

if heapy_enabled:
    from guppy import hpy


class AnalyzerAgent():
    """
    The AnalyzerAgent class does the follow:

    ensures that the required OS resources as defined by the various settings
    are available for the app.
    """

    def __init__(self):
        self.stdin_path = '/dev/null'
        self.stdout_path = '%s/%s.log' % (settings.LOG_PATH, skyline_app)
        self.stderr_path = '%s/%s.log' % (settings.LOG_PATH, skyline_app)
        self.pidfile_path = '%s/%s.pid' % (settings.PID_PATH, skyline_app)
        self.pidfile_timeout = 10

    def run(self):

        if pytracker_enabled:
            logger.info('pytracker dump in agent pre start')
            logger.info(pytrk.dump())

        if heapy_enabled:
            logger.info('debug :: Memory usage in agent before start: %s (kb)' % resource.getrusage(resource.RUSAGE_SELF).ru_maxrss)
            hp = hpy()
            before = hp.heap()
            logger.info('heapy dump in agent pre start')
            logger.info(before)

        logger.info('agent starting skyline %s' % skyline_app)
        Analyzer(getpid()).start()

        while 1:
            if pytracker_enabled:
                logger.info('pytracker dump in agent')
                logger.info(pytrk.dump())

            if heapy_enabled:
                after = hp.heap()
                logger.info('heapy in agent after start')
                logger.info(after)
                logger.info('heapy in agent lefover after start')
                leftover = after - before
                logger.info(leftover)
                logger.info('debug :: Memory usage in agent after start: %s (kb)' % resource.getrusage(resource.RUSAGE_SELF).ru_maxrss)

            sleep(100)


# @profile
def run():
    """
    Check that all the `ALGORITHMS` can be run.

    Start the AnalyzerAgent.

    Start the logger.
    """
    if len(sys.argv) > 1 and sys.argv[1] != 'stop':
        if not isdir(settings.PID_PATH):
            print ('pid directory does not exist at %s' % settings.PID_PATH)
            sys.exit(1)

        if not isdir(settings.LOG_PATH):
            print ('log directory does not exist at %s' % settings.LOG_PATH)
            sys.exit(1)

    logger.setLevel(logging.DEBUG)
    formatter = logging.Formatter("%(asctime)s :: %(process)s :: %(message)s", datefmt="%Y-%m-%d %H:%M:%S")
    if len(sys.argv) > 1 and sys.argv[1] == 'stop':
        handler = logging.FileHandler(
            settings.LOG_PATH + '/' + skyline_app + '.stop.log',
            mode='a', delay=False)
    else:
        handler = logging.handlers.TimedRotatingFileHandler(
            logfile,
            when="midnight",
            interval=1,
            backupCount=5)

        memory_handler = logging.handlers.MemoryHandler(3,
                                                        flushLevel=logging.DEBUG,
                                                        target=handler)

    handler.setFormatter(formatter)
    if len(sys.argv) > 1 and sys.argv[1] != 'stop':
        logger.addHandler(memory_handler)
    else:
        logger.addHandler(handler)

    if len(sys.argv) > 1 and sys.argv[1] != 'stop':
        # Validate settings variables
        valid_settings = validate_settings_variables(skyline_app)

        if not valid_settings:
            print ('error :: invalid variables in settings.py - cannot start')
            sys.exit(1)

    if len(sys.argv) > 1 and sys.argv[1] == 'stop':
        do_not_overwrite_log = True
    else:
        if heapy_enabled:
            logger.info('debug :: Memory usage in agent before testing algorithms: %s (kb)' % resource.getrusage(resource.RUSAGE_SELF).ru_maxrss)
            hp = hpy()
            before = hp.heap()
            logger.info('heapy dump in agent before testing algorithms')
            logger.info(before)
        # Make sure we can run all the algorithms
        try:
            # from analyzer import algorithms
            import algorithms
            logger.info('Testing algorithms')
            timeseries = map(list, zip(map(float, range(int(time()) - 86400, int(time()) + 1)), [1] * 86401))
            # ensemble = [globals()[algorithm](timeseries) for algorithm in settings.ALGORITHMS]
            ensemble = [getattr(algorithms, algorithm)(timeseries) for algorithm in settings.ALGORITHMS]
            if heapy_enabled:
                logger.info('debug :: Memory usage in agent after testing algorithms: %s (kb)' % resource.getrusage(resource.RUSAGE_SELF).ru_maxrss)
                after = hp.heap()
                logger.info('heapy dump in agent after testing algorithms')
                logger.info(after)
                logger.info('heapy dump in agent leftover after testing algorithms')
                leftover = after - before
                logger.info(leftover)
        except KeyError as e:
            print ('Algorithm %s deprecated or not defined; check settings.ALGORITHMS' % e)
            sys.exit(1)
        except Exception as e:
            print ('Algorithm test run failed.')
            traceback.print_exc()
            sys.exit(1)

        sleep(3)
        logger.info('Tested algorithms')
        if heapy_enabled:
            logger.info('debug :: Memory usage in agent before del timeseries and ensemble: %s (kb)' % resource.getrusage(resource.RUSAGE_SELF).ru_maxrss)
            hp = hpy()
            before = hp.heap()
            logger.info('heapy dump in agent before del timeseries and ensemble')
            logger.info(before)
        del timeseries[:]
        del ensemble[:]
        if heapy_enabled:
            after = hp.heap()
            logger.info('heapy dump in agent after del timeseries and ensemble')
            logger.info(after)
            logger.info('heapy dump in agent leftover after del timeseries and ensemble')
            leftover = after - before
            logger.info(leftover)
            logger.info('debug :: Memory usage in agent after del timeseries and ensemble: %s (kb)' % resource.getrusage(resource.RUSAGE_SELF).ru_maxrss)

    analyzer = AnalyzerAgent()

    if len(sys.argv) > 1 and sys.argv[1] == 'stop':
        do_not_overwrite_log = True
    else:
        logger.info('starting analyzer_debug run')
        # memory_handler.flush

    if len(sys.argv) > 1 and sys.argv[1] == 'run':
        analyzer.run()
    else:
        daemon_runner = runner.DaemonRunner(analyzer)
        daemon_runner.daemon_context.files_preserve = [handler.stream]
        daemon_runner.do_action()

    if len(sys.argv) > 1 and sys.argv[1] == 'stop':
        do_not_overwrite_log = True
    else:
        logger.info('stopping analyzer_debug')
        memory_handler.flush

if __name__ == '__main__':
    run()
