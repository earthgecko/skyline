#!/bin/bash

# This is used to start the python daemon_runner and issue kill to the parent
# process.  Originally a stop call was made to the python daemon_runner which
# initiated a new instance of the agent and the agent's main module, this often
# resulted in the daemon_runner overwritting to the Skyline app log file due to
# complexities in the python logging context relating to log handlers and the
# use of multiprocessing.  These stop calls are no longer made directly to the
# agent and they are made directly to the parent pid.  In terms of the
# python-daemon this is exactly what initiating a new agent does, the
# python-daemon simply issues a os.kill pid, however initiating a new instance
# of the application results in a new daemon_runner that cannot preserve the
# log file object of the running daemon_runner in terms of:
# daemon_context.files_preserve = [handler.stream]
# which results in the log file being overwritten.

CURRENT_DIR=$(dirname "${BASH_SOURCE[0]}")
#BASEDIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )/.."
BASEDIR=$(dirname "$CURRENT_DIR")

RETVAL=0

SERVICE_NAME=$(basename "$0" | cut -d'.' -f1)

PID=$$

# Python virtualenv support
# A skyline.conf can be used for passing any additional variables to this script
# This was specifically added to allow for the operator to run in a virtualenv
# environment with whichever version of python they choose to run.  Some simple
# sanity checks are made if a virtualenv is used
if [ -f /etc/skyline/skyline.conf ]; then
  # Test the config file has sensible variables
  bash -n /etc/skyline/skyline.conf
  if [ $? -eq 0 ]; then
    . /etc/skyline/skyline.conf
  else
    echo "error: There is a syntax error in /etc/skyline/skyline.conf, try bash -n /etc/skyline/skyline.conf"
    exit 1
  fi
fi
USE_VIRTUALENV=0
if [ "$PYTHON_VIRTUALENV" == "true" ]; then
  if [ ! -f "$USE_PYTHON" ]; then
    echo "error: The python binary specified does not exists as specified as USE_PYTHON in /etc/skyline/skyline.conf"
    exit 1
  fi
# Test the python binary
  $USE_PYTHON --version > /dev/null 2>&1
  if [ $? -eq 0 ]; then
    USE_VIRTUALENV=1
  else
    echo "error: The python binary specified does not execute as expected as specified as USE_PYTHON in /etc/skyline/skyline.conf"
    exit 1
  fi
fi

# Determine LOG and PID PATHs from the settings.py
if [ ! -f "$BASEDIR/skyline/settings.py" ]; then
  echo "error: The Skyline settings.py was not found at $BASEDIR/skyline/settings.py"
  exit 1
fi
LOG_PATH=$(cat "$BASEDIR/skyline/settings.py" | grep -v "^#" | grep "^LOG_PATH = " | sed -e "s/.*= //;s/'//g" | sed -e 's/"//g')
if [ ! -d "$LOG_PATH" ]; then
  echo "error: The LOG_PATH directory in $BASEDIR/skyline/settings.py does not exist"
  exit 1
fi
PID_PATH=$(cat "$BASEDIR/skyline/settings.py" | grep -v "^#" | grep "^PID_PATH = " | sed -e "s/.*= //;s/'//g" | sed -e 's/"//g')
if [ ! -d "$PID_PATH" ]; then
  echo "error: The PID_PATH directory in $BASEDIR/skyline/settings.py does not exist"
  exit 1
fi
SKYLINE_TMP_DIR=$(cat "$BASEDIR/skyline/settings.py" | grep -v "^#" | grep "^SKYLINE_TMP_DIR = " | sed -e "s/.*= //;s/'//g" | sed -e 's/"//g')
if [ ! -d "$SKYLINE_TMP_DIR" ]; then
  echo "notice: The SKYLINE_TMP_DIR directory in $BASEDIR/skyline/settings.py does not exist, creating"
  mkdir -p "$SKYLINE_TMP_DIR"
fi

# @added 20200415 - Branch #3262: py3
# Handle using a skyline user that does not have sudo access
CURRENT_USER=$(whoami)
USE_SUDO="sudo"
if [ "$CURRENT_USER" == "skyline" ]; then
  USE_SUDO=""
fi

FLUX_SERVER="gunicorn"

# Check if it is running and if so its state
RESTART=0
RUNNING=0
VALID_PIDFILE=0
VALID_PID=0
PROCESS_STALLED=0
if [ -f "$PID_PATH/${SERVICE_NAME}.pid" ]; then
  RUNNING=1
  RUNNING_PID=$(cat "$PID_PATH/${SERVICE_NAME}.pid" | head -n 1)
  # Is the RUNNING_PID a valid number?
  # shellcheck disable=SC2065
  test "$RUNNING_PID" -gt 1 > /dev/null 2>&1
  if [ $? -eq 0 ]; then
    VALID_PIDFILE=1
  fi
  if [ $VALID_PIDFILE -eq 1 ]; then
    if [ -f "/proc/$RUNNING_PID/status" ]; then
      RUNNING=1
      VALID_PID=1
    else
      PROCESS_STALLED=1
    fi
  fi
fi

status () {

# As per http://refspecs.linuxbase.org/LSB_3.1.0/LSB-Core-generic/LSB-Core-generic/iniscrptact.html
# 0	program is running or service is OK
# 1	program is dead and /var/run pid file exists
# 2	program is dead and /var/lock lock file exists
# 3	program is not running
# 4	program or service status is unknown

    if [[ $RUNNING -eq 1 && $VALID_PIDFILE -eq 1 ]]; then
      echo "${SERVICE_NAME} is running with pid $RUNNING_PID"
      return 0
    fi

    if [ $PROCESS_STALLED -eq 1 ]; then
      echo "${SERVICE_NAME} is dead and pid file exists - $PID_PATH/${SERVICE_NAME}.pid"
      return 1
    fi

    if [ $RUNNING -eq 0 ]; then
      echo "${SERVICE_NAME} is not running"
      return 3
    fi

}

start () {

    if [ $RESTART -eq 1 ]; then
      # These a reset for the restart context
      RUNNING=0
      VALID_PIDFILE=0
      VALID_PID=0
      PROCESS_STALLED=0
    fi

    if [ $PROCESS_STALLED -eq 1 ]; then
      echo "${SERVICE_NAME} is dead but pid file exists - $PID_PATH/${SERVICE_NAME}.pid"
      return 1
    fi

    if [[ $RUNNING -eq 1 && $VALID_PIDFILE -eq 0 ]]; then
      echo "error: A pid file exists for ${SERVICE_NAME} with no valid pid $PID_PATH/${SERVICE_NAME}.pid"
      return 1
    fi

    if [[ $RUNNING -eq 1 && $VALID_PID -eq 1 ]]; then
      echo "${SERVICE_NAME} is already running with pid $RUNNING_PID"
      return 0
    fi

    rm -f "$BASEDIR/skyline/${SERVICE_NAME}/*.pyc"
    if [ -f "$LOG_PATH/${SERVICE_NAME}.log" ]; then
      cat "$LOG_PATH/${SERVICE_NAME}.log" > "$LOG_PATH/${SERVICE_NAME}.log.last"
    fi

    touch "$LOG_PATH/${SERVICE_NAME}.log.lock"
    touch "$LOG_PATH/${SERVICE_NAME}.log.wait"

    if [ -f "$BASEDIR/skyline/settings.pyc" ]; then
      rm -f "$BASEDIR/skyline/settings.pyc"
    fi

    if [ $USE_VIRTUALENV -eq 0 ]; then
      cd "$BASEDIR/skyline/${SERVICE_NAME}"
      gunicorn --config "$BASEDIR/skyline/${SERVICE_NAME}/gunicorn.py" flux &
    else
      cd "$BASEDIR/skyline/${SERVICE_NAME}"
      PYTHON_VIRTUALENV_BIN_DIR=$(dirname $USE_PYTHON)
      source "$PYTHON_VIRTUALENV_BIN_DIR/activate"
      gunicorn --config "$BASEDIR/skyline/${SERVICE_NAME}/gunicorn.py" flux &
    fi
    RETVAL=$?

    if [ $RETVAL -ne 0 ]; then
      echo "error - failed to start ${SERVICE_NAME}"
      return $RETVAL
    fi

    sleep .5
    if [ -f "$PID_PATH/${SERVICE_NAME}.pid" ]; then
      RUNNING_PID=$(cat "$PID_PATH/${SERVICE_NAME}.pid" | head -n 1)
    else
      RUNNING_PID="unknown"
    fi

    echo "${SERVICE_NAME} started with pid $RUNNING_PID"

    return $RETVAL
}

stop () {

    if [ $PROCESS_STALLED -eq 1 ]; then
      echo "${SERVICE_NAME} is dead but pid file exists - $PID_PATH/${SERVICE_NAME}.pid"
      return 1
    fi

    if [[ $RUNNING -eq 1 && $VALID_PIDFILE -eq 0 ]]; then
      echo "error: A pid file exists for ${SERVICE_NAME} with no valid pid $PID_PATH/${SERVICE_NAME}.pid"
      return 1
    fi

    if [ $RUNNING -eq 0 ]; then
      echo "${SERVICE_NAME} is not running"
      return 0
    fi

    SERVICE_PID=$RUNNING_PID
    SERVICE_RELATED_PID=$(ps aux | grep "gunicorn.py flux" | grep "$SERVICE_NAME" | grep -v grep | awk '{print $2 }' | grep -c "$SERVICE_PID")
    if [ $SERVICE_RELATED_PID -eq 1 ]; then
      echo "$(date +"%Y-%m-%d %H:%M:%S") :: $PID :: ${SERVICE_NAME}.d :: stopping process $SERVICE_PID" >> "$LOG_PATH/${SERVICE_NAME}.log"
# @added 20200415 - Branch #3262: py3
# Handle using a skyline user that does not have sudo access
      $USE_SUDO kill $SERVICE_PID
    fi

    PROCESS_COUNT=$(ps aux | grep "gunicorn.py flux" | grep "$SERVICE_NAME" | grep -v grep | awk '{print $2 }' | wc -l)
    if [ $PROCESS_COUNT -gt 0 ]; then
      sleep 1
    fi

  # TODO: write a real kill script
    PROCESS_COUNT=$(ps aux | grep "gunicorn.py flux" | grep "$SERVICE_NAME" | grep -v grep | awk '{print $2 }' | wc -l)
    if [ $PROCESS_COUNT -gt 0 ]; then
      # kill -15
      ps aux | grep "gunicorn flux" | grep "$SERVICE_NAME" | grep -v grep | awk '{print $2 }' | while read i_pid
      do
        SERVICE_RELATED_PID=$(ps aux | grep "gunicorn.py flux" | grep "$SERVICE_NAME" | grep -v grep | awk '{print $2 }' | grep -c "$i_pid")
        if [ $SERVICE_RELATED_PID -eq 1 ]; then
          echo "$(date +"%Y-%m-%d %H:%M:%S") :: $PID :: ${SERVICE_NAME}.d :: cleaning up process $i_pid" >> "$LOG_PATH/${SERVICE_NAME}.log"
# @added 20200415 - Branch #3262: py3
# Handle using a skyline user that does not have sudo access
          $USE_SUDO kill $i_pid
        fi
      done

      PROCESS_COUNT=$(ps aux | grep "gunicorn.py flux" | grep "$SERVICE_NAME" | grep -v grep | awk '{print $2 }' | wc -l)
      if [ $PROCESS_COUNT -gt 0 ]; then
        # kill -9
        ps aux | grep "gunicorn.py flux" | grep "$SERVICE_NAME" | grep -v grep | awk '{print $2 }' | while read i_pid
        do
          SERVICE_RELATED_PID=$(ps aux | grep "gunicorn.py flux" | grep "$SERVICE_NAME" | grep -v grep | awk '{print $2 }' | grep -c "$i_pid")
          if [ $SERVICE_RELATED_PID -eq 1 ]; then
            echo "$(date +"%Y-%m-%d %H:%M:%S") :: $PID :: ${SERVICE_NAME}.d :: kill -9 process $i_pid" >> "$LOG_PATH/${SERVICE_NAME}.log"
# @added 20200415 - Branch #3262: py3
# Handle using a skyline user that does not have sudo access
            $USE_SUDO kill -9 $i_pid
          fi
        done
        PROCESS_COUNT=$(ps aux | grep "gunicorn.py flux" | grep "$SERVICE_NAME" | grep -v grep | awk '{print $2 }' | wc -l)
        if [ $PROCESS_COUNT -eq 0 ]; then
          rm -f "$PID_PATH/${SERVICE_NAME}.pid"
        fi
      fi
    fi

    PROCESS_COUNT=$(ps aux | grep "gunicorn.py flux" | grep "$SERVICE_NAME" | grep -v grep | awk '{print $2 }' | wc -l)
    if [[ ! -f "$PID_PATH/${SERVICE_NAME}.pid" && $PROCESS_COUNT -eq 0 ]]; then
      echo "$(date +"%Y-%m-%d %H:%M:%S") :: $PID :: ${SERVICE_NAME}.d :: all ${SERVICE_NAME} processes have been stopped - OK" >> "$LOG_PATH/${SERVICE_NAME}.log"
      echo "$SERVICE_NAME has been stopped"
      RETVAL=0
      return $RETVAL
    fi
    if [[ -f "$PID_PATH/${SERVICE_NAME}.pid" && $PROCESS_COUNT -eq 0 ]]; then
      echo "$(date +"%Y-%m-%d %H:%M:%S") :: $PID :: ${SERVICE_NAME}.d :: error - stopped all ${SERVICE_NAME} processes, but a pid file remains" >> "$LOG_PATH/${SERVICE_NAME}.log"
      rm -f "$PID_PATH/${SERVICE_NAME}.pid"
      echo "$(date +"%Y-%m-%d %H:%M:%S") :: $PID :: ${SERVICE_NAME}.d :: pid file removed" >> "$LOG_PATH/${SERVICE_NAME}.log"
      echo "$SERVICE_NAME has been stopped"
      RETVAL=1
    fi
    if [[ -f "$PID_PATH/${SERVICE_NAME}.pid" && $PROCESS_COUNT -gt 0 ]]; then
      echo "$(date +"%Y-%m-%d %H:%M:%S") :: $PID :: ${SERVICE_NAME}.d :: error - failed to stop all ${SERVICE_NAME} processes and pid file remains" >> "$LOG_PATH/${SERVICE_NAME}.log"
      echo "$(date +"%Y-%m-%d %H:%M:%S") :: $PID :: ${SERVICE_NAME}.d :: error - there maybe zombies or multiple instances running" >> "$LOG_PATH/${SERVICE_NAME}.log"
      echo "$SERVICE_NAME.d falied to stop all $SERVICE_NAME processes, there maybe zombies or multiple instances running"
      RETVAL=1
    fi

    # These are reset for the restart context
    RUNNING=0
    VALID_PIDFILE=0
    VALID_PID=0
    PROCESS_STALLED=0

    return $RETVAL
}

# See how we were called.
case "$1" in
  start)
    start
        ;;
  stop)
    stop
        ;;
  restart)
    RESTART=1
    stop
    start
       ;;
  status)
    status
        ;;
  *)
        echo $"Usage: $0 {start|stop|status}"
        exit 2
        ;;
esac
