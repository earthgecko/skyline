import logging
import sys
import traceback
from os import getpid
from os.path import dirname, abspath, isdir
from daemon import runner
from time import sleep, time
from sys import version_info
import os

import mysql.connector
from mysql.connector import errorcode

from logging.handlers import TimedRotatingFileHandler, MemoryHandler

import os.path
sys.path.append(os.path.join(os.path.dirname(os.path.realpath(__file__)), os.pardir))
sys.path.insert(0, os.path.dirname(__file__))

import settings
from validate_settings import validate_settings_variables

from ionosphere import Ionosphere

skyline_app = 'ionosphere'
skyline_app_logger = skyline_app + 'Log'
logger = logging.getLogger(skyline_app_logger)
logfile = '%s/%s.log' % (settings.LOG_PATH, skyline_app)

python_version = int(version_info[0])

# Database configuration - Ionosphere uses the same database settings as Panorama
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


class IonosphereAgent():
    def __init__(self):
        self.stdin_path = '/dev/null'
        self.stdout_path = '%s/%s.log' % (settings.LOG_PATH, skyline_app)
        self.stderr_path = '%s/%s.log' % (settings.LOG_PATH, skyline_app)
        self.pidfile_path = '%s/%s.pid' % (settings.PID_PATH, skyline_app)
        self.pidfile_timeout = 5

    def purge_old_data_dirs(self, dir_path, older_than):
        time_now = time()
        logger.info('agent :: checking for old data to remove')
        for path, folders, files in os.walk(dir_path):
            for folder in folders[:]:
                folder_path = os.path.join(path, folder)
                # if (time_now - os.path.getmtime(folder_path)) > older_than:
                #     yield folder_path
                for i_file in files:
                    current_file = '%s/%s' % (str(folder_path), str(i_file))
                    if (time_now - os.path.getmtime(current_file)) > older_than:
                        yield current_file
                        os.remove(current_file)
                        logger.info('agent :: removing old data - %s' % str(current_file))
                        if len(os.listdir(folder_path)) == 0:
                            logger.info('agent :: removing old data dir - %s' % str(folder_path))
                            os.rmdir(folder_path)

    def run(self):
        logger.info('agent :: starting Skyline Ionosphere')
        Ionosphere(getpid()).start()

        while 1:
            sleep(100)

            # Clean up old timeseries data files and folders
            self.purge_old_data_dirs(settings.IONOSPHERE_DATA_FOLDER, settings.IONOSPHERE_KEEP_TRAINING_TIMESERIES_FOR)


if __name__ == "__main__":
    """
    Start the Ionosphere agent.
    """
    if not isdir(settings.PID_PATH):
        print ('pid directory does not exist at %s' % settings.PID_PATH)
        sys.exit(1)

    if not isdir(settings.LOG_PATH):
        print ('log directory does not exist at %s' % settings.LOG_PATH)
        sys.exit(1)

    if not isdir(settings.IONOSPHERE_CHECK_PATH):
        print ('Panorama check directory does not exist at %s' % settings.IONOSPHERE_CHECK_PATH)
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

    ionosphere = IonosphereAgent()

    if len(sys.argv) > 1 and sys.argv[1] == 'run':
        ionosphere.run()
    else:
        daemon_runner = runner.DaemonRunner(ionosphere)
        daemon_runner.daemon_context.files_preserve = [handler.stream]
        daemon_runner.do_action()
