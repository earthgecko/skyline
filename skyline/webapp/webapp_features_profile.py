"""
webapp_features_profile.py
"""
import sys
import os
from time import time, sleep
import traceback
import logging

from flask import Flask, request, jsonify
from daemon import runner

if True:
    sys.path.append(os.path.join(os.path.dirname(os.path.realpath(__file__)), os.pardir))
    sys.path.insert(0, os.path.dirname(__file__))
    import settings
    from features_profile import calculate_features_profile

skyline_app = 'webapp'
skyline_app_logger = '%sLog' % skyline_app
logger = logging.getLogger(skyline_app_logger)
skyline_app_logfile = '%s/%s.log' % (settings.LOG_PATH, skyline_app)
skyline_app_loglock = '%s.lock' % skyline_app_logfile
skyline_app_logwait = '%s.wait' % skyline_app_logfile
logfile = '%s/%s.log' % (settings.LOG_PATH, skyline_app)

# werkzeug access log for Python errors
access_logger = logging.getLogger('werkzeug')

app = Flask(__name__)


# @added 20221208 - Feature #4756: Use gevent gunicorn worker_class
#                   Feature #4732: flux vortex
# A workaround process to handle tsfresh multiprocessing as it is not
# possible with the current webapp layout and gunicorn running a gevent
# worker_class.  This app uses the sync worker_class and allows for the
# normal calculate_features_profile function to be run.
@app.route('/ionosphere_features_profile', methods=['POST'])
def ionosphere_features_profile():
    """
    Calculate features profile and return the details in a json response.
    """
    start = time()

    logger.info('webapp_features_profile :: request.url: %s, request.form: %s' % (
        str(request.url), str(request.form)))
    # logger.debug('request.args: %s' % str(request.args))

    data_dict = {
        "status": {"created": False, "request_time": None},
        "data": {
            "fp_csv": None,
            "successful": False,
            "fp_exists": None,
            "fp_id": None,
            "fail_msg": None,
            "traceback_format_exc": None,
            "f_calc": None,
        }
    }

    # @added 20230622 - Feature #4958: webapp_features_profile - status
    status_check = False
    try:    
        status_check = request.form['status']
    except KeyError:
        pass
    except Exception as err:
        logger.error('error :: webapp_features_profile :: error checking status parameter - %s' % (
            err))
    if status_check:
        data_dict = {
            "status": "OK"
        }
        logger.info('webapp_features_profile :: status OK')
        return jsonify(data_dict), 200

    try:
        requested_timestamp = int(request.form['requested_timestamp'])
        base_name = request.form['base_name']
        context = request.form['context']
    except Exception as err:
        logger.error('error :: webapp_features_profile :: bad parameters passed - %s' % (
            err))
        data_dict['error'] = 'bad parameters'
        data_dict['data']['fail_msg'] = 'bad parameters'
        return jsonify(data_dict), 400

    logger.info('webapp_features_profile :: calculate_features_profile for %s, requested_timestamp: %s, context: %s' % (
        str(base_name), str(requested_timestamp), str(context)))
    try:
        fp_csv, successful, fp_exists, fp_id, fail_msg, traceback_format_exc, f_calc = calculate_features_profile(skyline_app, requested_timestamp, base_name, context)
    except Exception as err:
        trace = traceback.format_exc()
        logger.error(trace)
        logger.error('error :: webapp_features_profile :: calculate_features_profile failed - %s' % (
            err))
        data_dict['error'] = 'calculate_features_profile failed'
        data_dict['data']['fail_msg'] = 'calculate_features_profile failed'
        data_dict['data']['traceback_format_exc'] = trace
        return jsonify(data_dict), 500

    took = (time() - start)
    return_code = 200
    if not successful:
        return_code = 500

    # @added 20230626
    if fail_msg:
        if 'insufficient data to create profile' in fail_msg:
            return_code = 200
            data_dict['data']['fail_msg'] = fail_msg

    if successful:
        logger.info('webapp_features_profile :: features extracted OK')
        data_dict = {
            "status": {"created": successful, "request_time": took},
            "data": {
                "fp_csv": fp_csv,
                "successful": successful,
                "fp_exists": fp_exists,
                "fp_id": fp_id,
                "fail_msg": fail_msg,
                "traceback_format_exc": traceback_format_exc,
                "f_calc": f_calc,
            }
        }
    logger.info('webapp_features_profile :: completed - took: %s seconds' % (str(took)))
    return jsonify(data_dict), return_code


class App():
    def __init__(self):
        self.stdin_path = '/dev/null'
        self.stdout_path = '%s/%s.log' % (settings.LOG_PATH, skyline_app)
        self.stderr_path = '%s/%s.log' % (settings.LOG_PATH, skyline_app)
        self.pidfile_path = '%s/%s.pid' % (settings.PID_PATH, skyline_app)
        self.pidfile_timeout = 5

    def run(self):

        # Log management to prevent overwriting is done in webapp
        # Allow the bin/<skyline_app>.d to manage the log

        now = time.time()
#        log_wait_for = now + 5
        log_wait_for = now + 1
        while now < log_wait_for:
            if os.path.isfile(skyline_app_loglock):
                sleep(.1)
                now = time.time()
            else:
                now = log_wait_for + 1

        logger.info('webapp_features_profile :: starting')

        logger.info('webapp_features_profile :: hosted at 127.0.0.1:%d' % (settings.WEBAPP_PORT + 1))

        app.run('127.0.0.1', (settings.WEBAPP_PORT + 1))


def run():
    """
    Start the webapp_features_profile server
    """
    if not os.path.isdir(settings.PID_PATH):
        print('webapp_features_profile :: pid directory does not exist at %s' % settings.PID_PATH)
        sys.exit(1)

    if not os.path.isdir(settings.LOG_PATH):
        print('webapp_features_profile :: log directory does not exist at %s' % settings.LOG_PATH)
        sys.exit(1)

    logger.setLevel(logging.DEBUG)

    formatter = logging.Formatter("%(asctime)s :: %(process)s :: %(message)s", datefmt="%Y-%m-%d %H:%M:%S")
    handler = logging.FileHandler(logfile, mode='a')
    memory_handler = logging.handlers.MemoryHandler(100,
                                                    flushLevel=logging.DEBUG,
                                                    target=handler)
    handler.setFormatter(formatter)
    logger.addHandler(memory_handler)

    try:
        settings.WEBAPP_PORT
    except:
        logger.error('error :: webapp_features_profile :: failed to determine %s from settings.py' % str('WEBAPP_PORT'))
        print('webapp_features_profile :: Failed to determine %s from settings.py' % str('WEBAPP_PORT'))
        sys.exit(1)

    webapp_features_profile = App()

    daemon_runner = runner.DaemonRunner(webapp_features_profile)
    daemon_runner.daemon_context.files_preserve = [handler.stream]
    daemon_runner.do_action()


if __name__ == "__main__":
    run()
