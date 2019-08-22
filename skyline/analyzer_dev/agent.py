import logging
import sys
import traceback
from os import getpid, kill
import signal
from os.path import dirname, abspath, isdir
from daemon import runner
from time import sleep, time

from logging.handlers import TimedRotatingFileHandler, MemoryHandler

import warnings
warnings.filterwarnings('error', 'a', RuntimeWarning, 'pandas', 0)

import os.path
sys.path.append(os.path.join(os.path.dirname(os.path.realpath(__file__)), os.pardir))
sys.path.insert(0, os.path.dirname(__file__))

import settings
from validate_settings import validate_settings_variables

from analyzer_dev import Analyzer

skyline_app = 'analyzer_dev'
skyline_app_logger = '%sLog' % skyline_app
logger = logging.getLogger(skyline_app_logger)
logfile = '%s/%s.log' % (settings.LOG_PATH, skyline_app)


class AnalyzerDevAgent():
    def __init__(self):
        self.stdin_path = '/dev/null'
        self.stdout_path = '%s/%s.log' % (settings.LOG_PATH, skyline_app)
        self.stderr_path = '%s/%s.log' % (settings.LOG_PATH, skyline_app)
        self.pidfile_path = '%s/%s.pid' % (settings.PID_PATH, skyline_app)
        self.pidfile_timeout = 5

    def run(self):
        if len(sys.argv) > 1 and sys.argv[1] == 'stop':
            do_not_overwrite_log = True
            # This should hopefully take care of a TODO from the bin files,
            # TODO: write a real kill script
            # This is basically from the python-daemon function:
            # def _terminate_daemon_process from:
# https://github.com/elephantum/python-daemon/blob/a38aefd37d319586a9e7ab034435928b1c243e49/daemon/runner.py#L133
            # logging with multiprocessing and log rotation is difficult.  I am
            # certain that many a people have been in a helpless and hopeless
            # state when trying to debug Skyline, those python truncating log
            # handlers, it is not easy.  Many, many combinations of things have
            # been attempted in this area to attempt to be able to have the
            # agents just append the log.  The TimedRotatingFileHandler does not
            # help matters either as it has no mode='a'.  It could be handled by
            # normal log rotation but multiprocessing does not make that easy
            # either.  It is a difficult problem and adding a multiprocessing
            # log Queue with the agent listening and writing has been consider
            # too.  It may work, but adds a lot more complexity, for me anyway.
            # The ideal is to have to agent.py creating/appending to log
            # and not overwriting the damn thing and TimedRotatingFileHandler,
            # everything is possible :) And the new bin bash files do a pretty
            # good job anyway and have for 2 years now, maybe havign lost 1 or 2
            # in the 2 years that they have been managing the logs :)
            # @earthgecko 20160520
            pid = int(open(pidfile_path).read())
            try:
                kill(pid, signal.SIGTERM)
                print '%s pid %s stopped' % (skyline_app, str(pid))
                sys.exit(0)
            except OSError, exc:
                print 'Failed to kill pid %s' % str(pid)
                sys.exit(1)
        else:
            logger.info('starting skyline ' + skyline_app)
            Analyzer(getpid()).start()

            while 1:
                sleep(10)


def run():
    """
    Check that all the `ALGORITHMS` can be run.

    Start the AnalyzerAgent.

    Start the logger.
    """
    if not isdir(settings.PID_PATH):
        print('pid directory does not exist at %s' % settings.PID_PATH)
        sys.exit(1)

    if not isdir(settings.LOG_PATH):
        print('log directory does not exist at %s' % settings.LOG_PATH)
        sys.exit(1)

    if len(sys.argv) > 1 and sys.argv[1] == 'stop':
        do_not_overwrite_log = True
        # This should hopefully take care of a TODO from the bin files,
        # TODO: write a real kill script
        # as above @earthgecko 20160520
        pidfile_path = settings.PID_PATH + '/' + skyline_app + '.pid'
        pid = int(open(pidfile_path).read())
        try:
            kill(pid, signal.SIGTERM)
            print '%s pid %s stopped' % (skyline_app, str(pid))
            sys.exit(0)
        except OSError, exc:
            print 'Failed to kill pid %s' % str(pid)
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

    if len(sys.argv) > 1 and sys.argv[1] == 'stop':
        do_not_overwrite_log = True
    else:
        # Make sure we can run all the algorithms
        try:
            # from analyzer import algorithms
            import algorithms_dev
            logger.info('Testing algorithms')
            timeseries = map(list, zip(map(float, range(int(time()) - 86400, int(time()) + 1)), [1] * 86401))
            # ensemble = [globals()[algorithm](timeseries) for algorithm in settings.ALGORITHMS]
            ensemble = [getattr(algorithms_dev, algorithm)(timeseries) for algorithm in settings.ALGORITHMS]
            logger.info('Tested algorithms OK')
            logger.info('ensemble: %s' % str(ensemble))
        except KeyError as e:
            print('Algorithm %s deprecated or not defined; check settings.ALGORITHMS' % e)
            sys.exit(1)
        except Exception as e:
            print('Algorithm test run failed.')
            traceback.print_exc()
            sys.exit(1)

        logger.info('Tested algorithms')
        del timeseries
        del ensemble

    analyzer = AnalyzerDevAgent()

    if len(sys.argv) > 1 and sys.argv[1] == 'stop':
        do_not_overwrite_log = True
    else:
        logger.info('starting analyzer_dev.run')
        memory_handler.flush

    if len(sys.argv) > 1 and sys.argv[1] == 'run':
        analyzer.run()
    else:
        daemon_runner = runner.DaemonRunner(analyzer)
        daemon_runner.daemon_context.files_preserve = [handler.stream]
        daemon_runner.do_action()

    if len(sys.argv) > 1 and sys.argv[1] == 'stop':
        do_not_overwrite_log = True
    else:
        logger.info('stopped analyzer_dev')

if __name__ == '__main__':
    run()
