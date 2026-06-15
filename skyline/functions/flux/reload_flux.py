"""
reload_flux.py
"""
import logging
import os
# @modified 20230106 - Task #4022: Move mysql_select calls to SQLAlchemy
#                      Task #4778: v4.0.0 - update dependencies
# Added nosec for bandit B404:blacklist
# Consider possible security implications associated with the subprocess module (CWE-78)
# The subprocess is not using any parameter passed by an external user.  It is
# simply being passed the flux pid file which is determined from settings.PID_PATH
# in metrics_manager.py which is the only thing calling reload_flux
import subprocess  # nosec B404
from time import sleep, ctime
# @added 20230929 - Feature #5090: flux.d reload
#                   Feature #4324: flux - reload external_settings
import sys
try:
    from settings import PID_PATH
except:
    PID_PATH = '/var/run/skyline'

# @added 20220117 - Feature #4324: flux - reload external_settings
#                   Feature #4376: webapp - update_external_settings
def reload_flux(current_skyline_app, flux_pid_file):
    """
    Reload the flux gunicorn processes to refresh the valid_keys list
    """

    function_str = 'functions.flux.reload_flux'

    # @added 20230929 - Feature #5090: flux.d reload
    #                   Feature #4324: flux - reload external_settings
    # Allow for bin/flux.d to call reload_flux and if so, do not log but print
    use_log = True
    use_print = False
    FLUX_PID_FILE = '%s/flux.pid' % PID_PATH
    try:
        if sys.argv[1] == 'flux.d':
            if sys.argv[2] == FLUX_PID_FILE:
                use_log = False
                use_print = True
    except:
        use_log = True
        use_print = False
    if current_skyline_app == 'flux.d':
        if flux_pid_file == FLUX_PID_FILE:
            use_log = False
            use_print = True

    if use_log:
        current_skyline_app_logger = current_skyline_app + 'Log'
        current_logger = logging.getLogger(current_skyline_app_logger)

    current_pids = []

    if use_log:
        current_logger.info('%s :: initiating flux reload' % function_str)
    if print:
        print('%s :: %s :: initiating flux reload' % (function_str, ctime()))

    flux_pid = 0
    if os.path.isfile(flux_pid_file):
        try:
            with open(flux_pid_file, 'r') as f:
                flux_pid_str = f.read()
            flux_pid = int(flux_pid_str)
        except Exception as err:
            if use_log:
                current_logger.info('error :: %s :: failed to flux PID from %s - %s' % (
                    function_str, flux_pid_file, err))
            if print:
                print('error :: %s :: failed to flux PID from %s - %s' % (
                    function_str, flux_pid_file, err))
            flux_pid = 0
    if use_log:
        current_logger.info('%s :: flux_pid: %s' % (function_str, str(flux_pid)))
    if print:
        print('%s :: %s :: flux_pid: %s' % (function_str, ctime(), str(flux_pid)))
    # @modified 20241106 - Task #5526: Build v5.0.0 and upgrade deps
    # bandit - B605:start_process_with_a_shell
    # Starting a process with a shell, possible injection detected, security issue
    worker_processes = subprocess.getoutput('/usr/bin/pgrep -P %s' % str(flux_pid)).split('\n')  # nosec B605
    if use_log:
        current_logger.info('%s :: worker_processes: %s' % (function_str, str(worker_processes)))
    if print:
        print('%s :: %s :: worker_processes: %s' % (function_str, ctime(), str(worker_processes)))
    child_pids = []
    for child_pid in worker_processes:
        # @modified 20241106 - Task #5526: Build v5.0.0 and upgrade deps
        # bandit - B605:start_process_with_a_shell
        p_child_pids = subprocess.getoutput('/usr/bin/pgrep -P %s' % str(child_pid)).split('\n')  # nosec B605
        for i in p_child_pids:
            child_pids.append(i)
    if use_log:
        current_logger.info('%s :: child_pids: %s' % (function_str, str(child_pids)))
    if print:
        print('%s :: %s :: child_pids: %s' % (function_str, ctime(), str(child_pids)))
    # @modified 20241106 - Task #5526: Build v5.0.0 and upgrade deps
    # bandit - B605:start_process_with_a_shell, B607:start_process_with_partial_path
    original_pids = subprocess.getoutput("ps aux | grep flux | grep gunicorn | tr -s ' ' ',' | cut -d',' -f2").split('\n')  # nosec
    if use_log:
        current_logger.info('%s :: original_pids: %s' % (function_str, str(original_pids)))
    if print:
        print('%s :: %s :: original_pids: %s' % (function_str, ctime(), str(original_pids)))
    kill_usr2_main_pid = subprocess.getstatusoutput('/usr/bin/kill -s USR2 %s' % str(flux_pid))  # nosec B605

    # @added 20220512 - Feature #4376: webapp - update_external_settings
    # Write the pid of the current process to the flux.pid file to prevent
    # service controls such as monit not finding the pid file during reload.
    # This gets replaced by the new main process as soon as it is started.
    try:
        with open(flux_pid_file, 'w') as f:
            f.write(str(os.getpid()))
    except Exception as err:
        if use_log:
            current_logger.info('error :: %s :: failed to write the current process pid to %s - %s' % (
                function_str, flux_pid_file, err))
        if print:
            print('error :: %s :: failed to write the current process pid to %s - %s' % (
                function_str, flux_pid_file, err))

    if use_log:
        current_logger.info('%s :: pid: %s,  kill_usr2_main_pid: %s' % (function_str, str(flux_pid), str(kill_usr2_main_pid)))
    if print:
        print('%s :: %s :: pid: %s,  kill_usr2_main_pid: %s' % (function_str, ctime(), str(flux_pid), str(kill_usr2_main_pid)))
    # @modified 20241106 - Task #5526: Build v5.0.0 and upgrade deps
    # bandit - B605:start_process_with_a_shell, B607:start_process_with_partial_path
    new_pids = subprocess.getoutput("ps aux | grep flux | grep gunicorn | tr -s ' ' ',' | cut -d',' -f2").split('\n')  # nosec
    if use_log:
        current_logger.info('%s :: new_pids: %s' % (function_str, str(new_pids)))
    if print:
        print('%s :: %s :: new_pids: %s' % (function_str, ctime(), str(new_pids)))
    new_flux_pids = []
    for new_pid in new_pids:
        if new_pid in original_pids:
            continue
        new_flux_pids.append(new_pid)
    new_flux_pids.sort()
    new_flux_pid = new_flux_pids[0]
    if use_log:
        current_logger.info('%s :: new_flux_pids: %s' % (function_str, str(new_flux_pids)))
        current_logger.info('%s :: new_flux_pid: %s' % (function_str, str(new_flux_pid)))
    if print:
        print('%s :: %s :: new_flux_pids: %s' % (function_str, ctime(), str(new_flux_pids)))
        print('%s :: %s :: new_flux_pid: %s' % (function_str, ctime(), str(new_flux_pid)))
    try:
        with open(flux_pid_file, 'w') as fh:
            fh.write(str(new_flux_pid))
        if use_log:
            current_logger.info('%s :: wrote new pid %s to %s' % (
                function_str, str(new_flux_pid), flux_pid_file))
        if print:
            print('%s :: %s :: wrote new pid %s to %s' % (
                function_str, ctime(), str(new_flux_pid), flux_pid_file))        
    except Exception as err:
        if use_log:
            current_logger.error('error :: %s :: faile to write pid %s to %s, err: %s' % (
                function_str, str(new_flux_pid), flux_pid_file, err))
        if print:
            print('error :: %s :: faile to write pid %s to %s, err: %s' % (
                function_str, str(new_flux_pid), flux_pid_file, err))

    for child_pid in worker_processes:
        try:
            # @modified 20241106 - Task #5526: Build v5.0.0 and upgrade deps
            # bandit - B605:start_process_with_a_shell
            kill_usr2_child_pid = subprocess.getstatusoutput('/usr/bin/kill -s USR2 %s' % str(child_pid))  # nosec B605
            if use_log:
                current_logger.info('%s :: child_pid: %s, kill_usr2_child_pid: %s' % (function_str, str(child_pid), str(kill_usr2_child_pid)))
            if print:
                print('%s :: %s :: child_pid: %s, kill_usr2_child_pid: %s' % (function_str, ctime(), str(child_pid), str(kill_usr2_child_pid)))
        except Exception as err:
            if use_log:
                current_logger.error('error :: %s :: child_pid: %s, kill_usr2_child_pid: %s kill USR2 failed, err: %s' % (
                    function_str, str(child_pid), str(kill_usr2_child_pid), err))
            if print:
                print('error :: %s :: child_pid: %s, kill_usr2_child_pid: %s kill USR2 failed, err: %s' % (
                    function_str, str(child_pid), str(kill_usr2_child_pid), err))

    # @modified 20241106 - Task #5526: Build v5.0.0 and upgrade deps
    # bandit - B605:start_process_with_a_shell
    kill_winch_main_pid = subprocess.getstatusoutput('/usr/bin/kill -s WINCH %s' % str(flux_pid))  # nosec B605
    if use_log:
        current_logger.info('%s :: flux_pid: %s, kill_winch_main_pid: %s' % (function_str, str(flux_pid), str(kill_winch_main_pid)))
    if print:
        print('%s :: %s :: flux_pid: %s, kill_winch_main_pid: %s' % (function_str, ctime(), str(flux_pid), str(kill_winch_main_pid)))
    # @modified 20241106 - Task #5526: Build v5.0.0 and upgrade deps
    # bandit - B605:start_process_with_a_shell
    kill_quit_main_pid = subprocess.getstatusoutput('/usr/bin/kill -s QUIT %s' % str(flux_pid))  # nosec B605
    if use_log:
        current_logger.info('%s :: flux_pid: %s, kill_quit_main_pid: %s' % (function_str, str(flux_pid), str(kill_quit_main_pid)))
    if print:
        print('%s :: %s :: flux_pid: %s, kill_quit_main_pid: %s' % (function_str, ctime(), str(flux_pid), str(kill_quit_main_pid)))
    sleep(10)
    # @modified 20241106 - Task #5526: Build v5.0.0 and upgrade deps
    # bandit - B605:start_process_with_a_shell, B607:start_process_with_partial_path
    current_pids = subprocess.getoutput("ps aux | grep flux | grep gunicorn | tr -s ' ' ',' | cut -d',' -f2").split('\n')  # nosec
    for pid in original_pids:
        if pid in current_pids and pid not in new_flux_pids:
            try:
                # @modified 20241106 - Task #5526: Build v5.0.0 and upgrade deps
                # bandit - B605:start_process_with_a_shell
                kill_quit_child_pid = subprocess.getstatusoutput('/usr/bin/kill -s QUIT %s' % str(pid))  # nosec B605
                if use_log:
                    current_logger.info('%s :: pid: %s, kill_quit_child_pid: %s' % (function_str, str(pid), str(kill_quit_child_pid)))
                if print:
                    print('%s :: %s :: pid: %s, kill_quit_child_pid: %s' % (function_str, ctime(), str(pid), str(kill_quit_child_pid)))
            except Exception as err:
                if use_log:
                    current_logger.error('error :: %s :: pid: %s, err: %s' % (function_str, str(pid), err))
                if print:
                    print('error :: %s :: pid: %s, err: %s' % (function_str, str(pid), err))
    sleep(1)
    # @modified 20241106 - Task #5526: Build v5.0.0 and upgrade deps
    # bandit - B605:start_process_with_a_shell, B607:start_process_with_partial_path
    current_pids = subprocess.getoutput("ps aux | grep flux | grep gunicorn | tr -s ' ' ',' | cut -d',' -f2").split('\n')  # nosec
    for pid in original_pids:
        if pid in current_pids and pid not in new_flux_pids:
            try:
                # @modified 20241106 - Task #5526: Build v5.0.0 and upgrade deps
                # bandit - B605:start_process_with_a_shell
                kill_kill_child_pid = subprocess.getstatusoutput('/usr/bin/kill -s KILL %s' % str(pid))  # nosec B605
                if use_log:
                    current_logger.info('%s :: pid: %s, kill_kill_child_pid: %s' % (function_str, str(pid), str(kill_kill_child_pid)))
                if print:
                    print('%s :: %s :: pid: %s, kill_kill_child_pid: %s' % (function_str, ctime(), str(pid), str(kill_kill_child_pid)))
            except Exception as err:
                if use_log:
                    current_logger.error('error :: %s :: pid: %s, err: %s' % (function_str, str(pid), err))
                if print:
                    print('error :: %s :: pid: %s, err: %s' % (function_str, str(pid), err))

    # @added 20220512 - Feature #4376: webapp - update_external_settings
    # Added kill -9
    sleep(1)
    # @modified 20241106 - Task #5526: Build v5.0.0 and upgrade deps
    # bandit - B605:start_process_with_a_shell, B607:start_process_with_partial_path
    current_pids = subprocess.getoutput("ps aux | grep flux | grep gunicorn | tr -s ' ' ',' | cut -d',' -f2").split('\n')  # nosec
    kill_minus_9_pids_str = ''
    for pid in original_pids:
        if pid in current_pids and not pid == new_flux_pid:
            kill_minus_9_pids_str = '%s %s' % (kill_minus_9_pids_str, str(pid))
    if kill_minus_9_pids_str != '':
        try:
            # @modified 20241106 - Task #5526: Build v5.0.0 and upgrade deps
            # bandit - B605:start_process_with_a_shell
            kill_minus_9_pids = subprocess.getstatusoutput('/usr/bin/kill -9 %s' % str(kill_minus_9_pids_str))  # nosec B605
            if use_log:
                current_logger.info('%s :: kill_minus_9_pids %s: %s' % (function_str, kill_minus_9_pids_str, str(kill_minus_9_pids)))
            if print:
                print('%s :: %s :: kill_minus_9_pids %s: %s' % (function_str, ctime(), kill_minus_9_pids_str, str(kill_minus_9_pids)))
        except Exception as err:
            if use_log:
                current_logger.error('error :: %s :: kill_minus_9_pids - %s, err: %s' % (function_str, kill_minus_9_pids_str, err))
            if print:
                print('error :: %s :: kill_minus_9_pids - %s, err: %s' % (function_str, kill_minus_9_pids_str, err))

    # @modified 20241106 - Task #5526: Build v5.0.0 and upgrade deps
    # bandit - B605:start_process_with_a_shell, B607:start_process_with_partial_path
    current_pids = subprocess.getoutput("ps aux | grep flux | grep gunicorn | tr -s ' ' ',' | cut -d',' -f2").split('\n')  # nosec
    if use_log:
        current_logger.info('%s :: reloaded, current_pids: %s' % (function_str, str(current_pids)))
    if print:
        print('%s :: %s :: reloaded, current_pids: %s' % (function_str, ctime(), str(current_pids)))
        return

    return current_pids
