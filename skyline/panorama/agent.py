import logging
import sys
import traceback
from os import getpid
# from os.path import dirname, abspath, isdir
from os.path import isdir
from daemon import runner
from time import sleep
from sys import version_info

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
    from validate_settings import validate_settings_variables
    from panorama import Panorama

skyline_app = 'panorama'
skyline_app_logger = skyline_app + 'Log'
logger = logging.getLogger(skyline_app_logger)
logfile = '%s/%s.log' % (settings.LOG_PATH, skyline_app)

python_version = int(version_info[0])

# Database configuration
try:
    config = {'user': settings.PANORAMA_DBUSER,
              'password': settings.PANORAMA_DBUSERPASS,
              'host': settings.PANORAMA_DBHOST,
              'port': settings.PANORAMA_DBPORT,
              'database': settings.PANORAMA_DATABASE,
              'raise_on_warnings': True}
except:
    print ('error: failed to determine database settings from settings.py')
    sys.exit(1)


class PanoramaAgent():
    def __init__(self):
        self.stdin_path = '/dev/null'
        self.stdout_path = '%s/%s.log' % (settings.LOG_PATH, skyline_app)
        self.stderr_path = '%s/%s.log' % (settings.LOG_PATH, skyline_app)
        self.pidfile_path = '%s/%s.pid' % (settings.PID_PATH, skyline_app)
        self.pidfile_timeout = 5

    def run(self):
        logger.info('starting skyline panorama')
        Panorama(getpid()).start()

        while 1:
            sleep(100)


if __name__ == "__main__":
    """
    Start the Panorama agent.
    """
    if not isdir(settings.PID_PATH):
        print ('pid directory does not exist at %s' % settings.PID_PATH)
        sys.exit(1)

    if not isdir(settings.LOG_PATH):
        print ('log directory does not exist at %s' % settings.LOG_PATH)
        sys.exit(1)

    if not isdir(settings.PANORAMA_CHECK_PATH):
        print ('Panorama check directory does not exist at %s' % settings.PANORAMA_CHECK_PATH)
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
                print ('The database is not available')
        except:
            logger.error(traceback.format_exc())
            logger.error('error :: The database is not available')
            print ('The database is not available')
        # @modified 20191031 - Feature #3310: gracefully handle db failure
        #                      Branch 3262: py3
        # sys.exit(1)
        if start_if_no_db:
            logger.warn('warning :: mysql_up is %s but START_IF_NO_DB is %s, so starting' % (
                str(mysql_up), str(start_if_no_db)))
            mysql_up = True
        else:
            sys.exit(1)

    # @added 20191031 - Feature #3310: gracefully handle db failure
    #                   Branch 3262: py3
    if start_if_no_db:
        if not mysql_up:
            logger.warn('warning :: mysql_up is %s but START_IF_NO_DB is %s, so starting' % (
                str(mysql_up), str(start_if_no_db)))
            mysql_up = True

    if not mysql_up:
        sys.exit(1)

    panorama = PanoramaAgent()

    if len(sys.argv) > 1 and sys.argv[1] == 'run':
        panorama.run()
    else:
        daemon_runner = runner.DaemonRunner(panorama)
        daemon_runner.daemon_context.files_preserve = [handler.stream]
        daemon_runner.do_action()
