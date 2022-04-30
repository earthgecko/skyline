import logging
import sys
import traceback
from os import getpid
from os.path import isdir
from daemon import runner
from time import sleep
from sys import version_info
import os

import mysql.connector
from mysql.connector import errorcode

from logging.handlers import TimedRotatingFileHandler, MemoryHandler

import os.path
sys.path.append(os.path.join(os.path.dirname(os.path.realpath(__file__)), os.pardir))
sys.path.insert(0, os.path.dirname(__file__))

# @modified 20191115 - Branch #3262: py3
# This prevents flake8 E402 - module level import not at top of file
if True:
    import settings
    from luminosity import Luminosity
    from validate_settings import validate_settings_variables
    # @added 20210730 - Feature #4164: luminosity - cloudbursts
    if settings.LUMINOSITY_CLOUDBURST_ENABLED:
        from cloudburst import Cloudburst
        # @added 20210806 - Feature #4164: luminosity - cloudbursts
        from cloudbursts import Cloudbursts
    # @added 20210929 - Feature #4264: luminosity - cross_correlation_relationships
    if settings.LUMINOSITY_RELATED_METRICS:
        from related_metrics import RelatedMetrics
    # @added 20220328 - Feature #4018: thunder - skyline.errors
    from functions.redis.RedisErrorLogHandler import RedisErrorLogHandler

skyline_app = 'luminosity'
skyline_app_logger = skyline_app + 'Log'
logger = logging.getLogger(skyline_app_logger)
logfile = '%s/%s.log' % (settings.LOG_PATH, skyline_app)

python_version = int(version_info[0])

# Database configuration - Luminosity uses the same database settings as Panorama
try:
    config = {'user': settings.PANORAMA_DBUSER,
              'password': settings.PANORAMA_DBUSERPASS,
              'host': settings.PANORAMA_DBHOST,
              'port': settings.PANORAMA_DBPORT,
              'database': settings.PANORAMA_DATABASE,
              'raise_on_warnings': True}
except:
    print('error: failed to determine database settings from settings.py')
    sys.exit(1)


class LuminosityAgent():
    def __init__(self):
        self.stdin_path = '/dev/null'
        self.stdout_path = '%s/%s.log' % (settings.LOG_PATH, skyline_app)
        self.stderr_path = '%s/%s.log' % (settings.LOG_PATH, skyline_app)
        # http://www.gavinj.net/2012/06/building-python-daemon-process.html
        # gavinj.net does
        # self.stdout_path = '/dev/tty'
        # self.stderr_path = '/dev/tty'
        # And this agent pattern is pretty much right from gavinj.net
        # For years the overwriting of logs was a bug bear which was eventually
        # handled through a combination of bash and a bit of Python waiting,
        # and the bin files were used to manage the log so that any Python
        # through some
        self.pidfile_path = '%s/%s.pid' % (settings.PID_PATH, skyline_app)
        self.pidfile_timeout = 5

    def run(self):
        logger.info('agent :: starting Skyline Luminosity')
        Luminosity(getpid()).start()

        # @added 20210730 - Feature #4164: luminosity - cloudbursts
        if settings.LUMINOSITY_CLOUDBURST_ENABLED:
            logger.info('agent starting Cloudburst')
            try:
                Cloudburst(getpid()).start()
            except Exception as e:
                logger.info(traceback.format_exc())
                logger.error('error :: agent failed to start Cloudburst - %s' % e)

            # @added 20210806 - Feature #4164: luminosity - cloudbursts
            logger.info('agent starting Cloudbursts')
            try:
                Cloudbursts(getpid()).start()
            except Exception as e:
                logger.info(traceback.format_exc())
                logger.error('error :: agent failed to start Cloudbursts - %s' % e)
        else:
            logger.info('agent not starting Cloudburst because LUMINOSITY_CLOUDBURST_ENABLED is %s' % str(settings.LUMINOSITY_CLOUDBURST_ENABLED))

        if settings.LUMINOSITY_RELATED_METRICS:
            logger.info('agent starting RelatedMetrics')
            try:
                RelatedMetrics(getpid()).start()
            except Exception as e:
                logger.info(traceback.format_exc())
                logger.error('error :: agent failed to start RelatedMetrics - %s' % e)
        else:
            logger.info('agent not starting RelatedMetrics because LUMINOSITY_RELATED_METRICS is %s' % str(settings.LUMINOSITY_RELATED_METRICS))

        while 1:
            sleep(100)


if __name__ == "__main__":
    """
    Start the Luminosity agent.
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

    # @added 20191031 - Feature #3310: gracefully handle db failure
    #                   Branch 3262: py3
    try:
        start_if_no_db = settings.START_IF_NO_DB
    except:
        start_if_no_db = False

    # Make sure mysql is available
    mysql_up = False
    try:
        configuration_error = True
        # Try connect to mysql
        try:
            cnx = mysql.connector.connect(**config)
            configuration_error = False
            mysql_up = True
            cnx.close()
            logger.info('database user name or password - connected OK')
        except mysql.connector.Error as err:
            logger.error(traceback.format_exc())
            if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
                logger.error('error :: something is wrong with your database user name or password')
            elif err.errno == errorcode.ER_BAD_DB_ERROR:
                logger.error('error :: the %s database does not exist' % settings.PANORAMA_DATABASE)
            else:
                logger.error('error :: mysql error - %s' % str(err))
    except:
        try:
            if configuration_error:
                logger.error(traceback.format_exc())
                logger.error('error :: The database is not available')
                print('The database is not available')
        except:
            logger.error(traceback.format_exc())
            logger.error('error :: The database is not available')
            print('The database is not available')
        # @modified 20191031 - Feature #3310: gracefully handle db failure
        #                      Branch 3262: py3
        # sys.exit(1)
        if start_if_no_db:
            logger.warning('warning :: mysql_up is %s but START_IF_NO_DB is %s, so starting' % (
                str(mysql_up), str(start_if_no_db)))
            mysql_up = True
        else:
            sys.exit(1)

    # @added 20191031 - Feature #3310: gracefully handle db failure
    #                   Branch 3262: py3
    if start_if_no_db:
        if not mysql_up:
            logger.warning('warning :: mysql_up is %s but START_IF_NO_DB is %s, so starting' % (
                str(mysql_up), str(start_if_no_db)))
            mysql_up = True

    if not mysql_up:
        sys.exit(1)

    luminosity = LuminosityAgent()

    if len(sys.argv) > 1 and sys.argv[1] == 'run':
        luminosity.run()
    else:
        daemon_runner = runner.DaemonRunner(luminosity)
        daemon_runner.daemon_context.files_preserve = [handler.stream]
        daemon_runner.do_action()
