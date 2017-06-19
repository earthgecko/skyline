import logging
import sys
# import traceback
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
        except mysql.connector.Error as err:
            if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
                logger.error('error :: something is wrong with your user name or password')
            elif err.errno == errorcode.ER_BAD_DB_ERROR:
                logger.error('error :: the %s database does not exist' % settings.PANORAMA_DATABASE)
            else:
                logger.error('error :: mysql error - %s' % str(err))
    except:
        try:
            if configuration_error:
                print ('The database is not available')
        except:
            print ('The database is not available')
        sys.exit(1)

    if not mysql_up:
        sys.exit(1)

    panorama = PanoramaAgent()

    if len(sys.argv) > 1 and sys.argv[1] == 'run':
        panorama.run()
    else:
        daemon_runner = runner.DaemonRunner(panorama)
        daemon_runner.daemon_context.files_preserve = [handler.stream]
        daemon_runner.do_action()
