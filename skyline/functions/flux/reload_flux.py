"""
reload_flux.py
"""
import logging
import os
import subprocess
from time import sleep


# @added 20220117 - Feature #4324: flux - reload external_settings
#                   Feature #4376: webapp - update_external_settings
def reload_flux(current_skyline_app, flux_pid_file):
    """
    Reload the flux gunicorn processes to refresh the valid_keys list
    """

    function_str = 'functions.flux.reload_flux'
    current_skyline_app_logger = current_skyline_app + 'Log'
    current_logger = logging.getLogger(current_skyline_app_logger)

    current_pids = []

    current_logger.info('%s :: initiating flux reload' % function_str)
    flux_pid = 0
    if os.path.isfile(flux_pid_file):
        try:
            with open(flux_pid_file, 'r') as f:
                flux_pid_str = f.read()
            flux_pid = int(flux_pid_str)
        except Exception as err:
            current_logger.info('error :: %s :: failed to flux PID from %s - %s' % (
                function_str, flux_pid_file, err))
            flux_pid = 0
    current_logger.info('%s :: flux_pid: %s' % (function_str, str(flux_pid)))
    worker_processes = subprocess.getoutput('/usr/bin/pgrep -P %s' % str(flux_pid)).split('\n')
    current_logger.info('%s :: worker_processes: %s' % (function_str, str(worker_processes)))
    child_pids = []
    for child_pid in worker_processes:
        p_child_pids = subprocess.getoutput('/usr/bin/pgrep -P %s' % str(child_pid)).split('\n')
        for i in p_child_pids:
            child_pids.append(i)
    current_logger.info('%s :: child_pids: %s' % (function_str, str(child_pids)))
    original_pids = subprocess.getoutput("ps aux | grep flux | grep gunicorn | tr -s ' ' ',' | cut -d',' -f2").split('\n')
    current_logger.info('%s :: original_pids: %s' % (function_str, str(original_pids)))
    kill_usr2_main_pid = subprocess.getstatusoutput('/usr/bin/kill -s USR2 %s' % str(flux_pid))
    current_logger.info('%s :: pid: %s,  kill_usr2_main_pid: %s' % (function_str, str(flux_pid), str(kill_usr2_main_pid)))
    new_pids = subprocess.getoutput("ps aux | grep flux | grep gunicorn | tr -s ' ' ',' | cut -d',' -f2").split('\n')
    current_logger.info('%s :: new_pids: %s' % (function_str, str(new_pids)))
    new_flux_pid = []
    for new_pid in new_pids:
        if new_pid in original_pids:
            continue
        new_flux_pid.append(new_pid)
    current_logger.info('%s :: new_flux_pid: %s' % (function_str, str(new_flux_pid)))
    with open(flux_pid_file, 'w') as fh:
        fh.write(new_flux_pid[0])
    for child_pid in worker_processes:
        kill_usr2_child_pid = subprocess.getstatusoutput('/usr/bin/kill -s USR2 %s' % str(child_pid))
        current_logger.info('%s :: child_pid: %s, kill_usr2_child_pid: %s' % (function_str, str(child_pid), str(kill_usr2_child_pid)))
    kill_winch_main_pid = subprocess.getstatusoutput('/usr/bin/kill -s WINCH %s' % str(flux_pid))
    current_logger.info('%s :: flux_pid: %s, kill_winch_main_pid: %s' % (function_str, str(flux_pid), str(kill_winch_main_pid)))
    kill_quit_main_pid = subprocess.getstatusoutput('/usr/bin/kill -s QUIT %s' % str(flux_pid))
    current_logger.info('%s :: flux_pid: %s, kill_quit_main_pid: %s' % (function_str, str(flux_pid), str(kill_quit_main_pid)))
    sleep(3)
    current_pids = subprocess.getoutput("ps aux | grep flux | grep gunicorn | tr -s ' ' ',' | cut -d',' -f2").split('\n')
    for pid in original_pids:
        if pid in current_pids:
            try:
                kill_quit_child_pid = subprocess.getstatusoutput('/usr/bin/kill -s QUIT %s' % str(pid))
                current_logger.info('%s :: pid: %s, kill_quit_child_pid: %s' % (function_str, str(pid), str(kill_quit_child_pid)))
            except Exception as err:
                current_logger.error('error :: %s :: pid: %s, err: %s' % (function_str, str(pid), err))
    sleep(1)
    current_pids = subprocess.getoutput("ps aux | grep flux | grep gunicorn | tr -s ' ' ',' | cut -d',' -f2").split('\n')
    for pid in original_pids:
        if pid in current_pids:
            try:
                kill_kill_child_pid = subprocess.getstatusoutput('/usr/bin/kill -s KILL %s' % str(pid))
                current_logger.info('%s :: pid: %s, kill_kill_child_pid: %s' % (function_str, str(pid), str(kill_kill_child_pid)))
            except Exception as err:
                current_logger.error('error :: %s :: pid: %s, err: %s' % (function_str, str(pid), err))

    current_pids = subprocess.getoutput("ps aux | grep flux | grep gunicorn | tr -s ' ' ',' | cut -d',' -f2").split('\n')

    return current_pids
