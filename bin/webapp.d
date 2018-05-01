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

WEBAPP_SERVER=$(cat "$BASEDIR/skyline/settings.py" | grep -v "^#" | grep "^WEBAPP_SERVER = '" | sed -e "s/.*= //;s/'//g" | sed -e 's/"//g')
if [ "$WEBAPP_SERVER" == "" ]; then
  echo "notice: The WEBAPP_SERVER could not be determined from $BASEDIR/skyline/settings.py, please specify gunicorn or flask"
  exit 1
fi
if [ "$WEBAPP_SERVER" == "gunicorn" ]; then
  WEBAPP_SERVICE_STRING="gunicorn.py webapp"
fi
if [ "$WEBAPP_SERVER" == "flask" ]; then
  WEBAPP_SERVICE_STRING="webapp.py start"
fi

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
      if [ "$WEBAPP_SERVER" == "flask" ]; then
        /usr/bin/env python "$BASEDIR/skyline/${SERVICE_NAME}/webapp.py" start
      fi
      if [ "$WEBAPP_SERVER" == "gunicorn" ]; then
        cd "$BASEDIR/skyline/${SERVICE_NAME}"
        gunicorn --config "$BASEDIR/skyline/${SERVICE_NAME}/gunicorn.py" webapp:app &
      fi
    else
      if [ "$WEBAPP_SERVER" == "flask" ]; then
        $USE_PYTHON "$BASEDIR/skyline/${SERVICE_NAME}/webapp.py" start
      fi
      if [ "$WEBAPP_SERVER" == "gunicorn" ]; then
        cd "$BASEDIR/skyline/${SERVICE_NAME}"
        PYTHON_VIRTUALENV_BIN_DIR=$(dirname $USE_PYTHON)
        source "$PYTHON_VIRTUALENV_BIN_DIR/activate"
        gunicorn --config "$BASEDIR/skyline/${SERVICE_NAME}/gunicorn.py" webapp:app &
      fi
    fi
    RETVAL=$?

    if [ $RETVAL -ne 0 ]; then
      if [ "$WEBAPP_SERVER" == "flask" ]; then
        if [ -f "$LOG_PATH/${SERVICE_NAME}.log.last" ]; then
          cat "$LOG_PATH/${SERVICE_NAME}.log.last" "$LOG_PATH/${SERVICE_NAME}.log" > "$LOG_PATH/${SERVICE_NAME}.log.new"
          cat "$LOG_PATH/${SERVICE_NAME}.log.new" > "$LOG_PATH/${SERVICE_NAME}.log"
          rm -f "$LOG_PATH/${SERVICE_NAME}.log.last"
          rm -f "$LOG_PATH/${SERVICE_NAME}.log.new"
        fi
        if [ -f "$LOG_PATH/${SERVICE_NAME}.log.wait" ]; then
          rm -f "$LOG_PATH/${SERVICE_NAME}.log.wait"
        fi
        if [ -f "$LOG_PATH/${SERVICE_NAME}.log.lock" ]; then
          rm -f "$LOG_PATH/${SERVICE_NAME}.log.lock"
        fi
      fi
      echo "error - failed to start ${SERVICE_NAME}"
      return $RETVAL
    fi

    if [ "$WEBAPP_SERVER" == "flask" ]; then
      PROCESS_WAITING=0
      NOW=$(date +%s)
      WAIT_FOR=$((NOW+5))
      while [ $NOW -lt $WAIT_FOR ];
      do
        if [ ! -f "$LOG_PATH/${SERVICE_NAME}.log.wait" ]; then
          NOW=$((WAIT_FOR+1))
          PROCESS_WAITING=1
          echo "$(date +"%Y-%m-%d %H:%M:%S") :: $PID :: ${SERVICE_NAME}.d :: process removed log.wait file, starting log management" >> "$LOG_PATH/${SERVICE_NAME}.log"
        else
          sleep .2
          NOW=$(date +%s)
        fi
      done
    fi

    sleep .5
    if [ -f "$PID_PATH/${SERVICE_NAME}.pid" ]; then
      RUNNING_PID=$(cat "$PID_PATH/${SERVICE_NAME}.pid" | head -n 1)
    else
      RUNNING_PID="unknown"
    fi

    if [ "$WEBAPP_SERVER" == "flask" ]; then
      if [ $PROCESS_WAITING -eq 0 ]; then
        if [ -f "$LOG_PATH/${SERVICE_NAME}.log.wait" ]; then
          rm -f "$LOG_PATH/${SERVICE_NAME}.log.wait"
        fi
        if [ -f "$LOG_PATH/${SERVICE_NAME}.log.lock" ]; then
          rm -f "$LOG_PATH/${SERVICE_NAME}.log.lock"
        fi
        echo "$(date +"%Y-%m-%d %H:%M:%S") :: $PID :: ${SERVICE_NAME}.d :: error - log management failed" >> "$LOG_PATH/${SERVICE_NAME}.log"
        echo "${SERVICE_NAME} started with pid $RUNNING_PID, but log management failed"
      fi

      if [ $PROCESS_WAITING -eq 1 ]; then
        if [ -f "$LOG_PATH/${SERVICE_NAME}.log.last" ]; then
          cat "$LOG_PATH/${SERVICE_NAME}.log.last" "$LOG_PATH/${SERVICE_NAME}.log" > "$LOG_PATH/${SERVICE_NAME}.log.new"
          cat "$LOG_PATH/${SERVICE_NAME}.log.new" > "$LOG_PATH/${SERVICE_NAME}.log"
          rm -f "$LOG_PATH/${SERVICE_NAME}.log.last"
          rm -f "$LOG_PATH/${SERVICE_NAME}.log.new"
        fi
        echo "$(date +"%Y-%m-%d %H:%M:%S") :: $PID :: ${SERVICE_NAME}.d :: log management done" >> "$LOG_PATH/${SERVICE_NAME}.log"
        if [ -f "$LOG_PATH/${SERVICE_NAME}.log.lock" ]; then
          rm -f "$LOG_PATH/${SERVICE_NAME}.log.lock"
        fi
      fi
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

# Originally a stop call was made to the python daemon_runner which
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
#    if [ $USE_VIRTUALENV -eq 0 ]; then
#      /usr/bin/env python "$BASEDIR/skyline/${SERVICE_NAME}/webapp.py" stop
#    else
#      $USE_PYTHON "$BASEDIR/skyline/${SERVICE_NAME}/webapp.py" stop
#    fi
#    RETVAL=$?
#    if [[ $RETVAL -eq 0 ]]; then
#        echo "$(date +"%Y-%m-%d %H:%M:%S") :: $PID :: ${SERVICE_NAME}.d :: stopped ${SERVICE_NAME}-agent"
#    else
#        echo "$(date +"%Y-%m-%d %H:%M:%S") :: $PID :: ${SERVICE_NAME}.d :: error - failed to stop ${SERVICE_NAME}-agent"
#    fi

    SERVICE_PID=$RUNNING_PID
    SERVICE_RELATED_PID=$(ps aux | grep "$WEBAPP_SERVICE_STRING" | grep "$SERVICE_NAME" | grep -v grep | awk '{print $2 }' | grep -c "$SERVICE_PID")
    if [ $SERVICE_RELATED_PID -eq 1 ]; then
      echo "$(date +"%Y-%m-%d %H:%M:%S") :: $PID :: ${SERVICE_NAME}.d :: stopping process $SERVICE_PID" >> "$LOG_PATH/${SERVICE_NAME}.log"
      sudo kill $SERVICE_PID
    fi

    PROCESS_COUNT=$(ps aux | grep "$WEBAPP_SERVICE_STRING" | grep "$SERVICE_NAME" | grep -v grep | awk '{print $2 }' | wc -l)
    if [ $PROCESS_COUNT -gt 0 ]; then
      sleep 1
    fi

  # TODO: write a real kill script
    PROCESS_COUNT=$(ps aux | grep "$WEBAPP_SERVICE_STRING" | grep "$SERVICE_NAME" | grep -v grep | awk '{print $2 }' | wc -l)
    if [ $PROCESS_COUNT -gt 0 ]; then
      # kill -15
      ps aux | grep "webapp.py start" | grep "$SERVICE_NAME" | grep -v grep | awk '{print $2 }' | while read i_pid
      do
        SERVICE_RELATED_PID=$(ps aux | grep "$WEBAPP_SERVICE_STRING" | grep "$SERVICE_NAME" | grep -v grep | awk '{print $2 }' | grep -c "$i_pid")
        if [ $SERVICE_RELATED_PID -eq 1 ]; then
          echo "$(date +"%Y-%m-%d %H:%M:%S") :: $PID :: ${SERVICE_NAME}.d :: cleaning up process $i_pid" >> "$LOG_PATH/${SERVICE_NAME}.log"
          sudo kill $i_pid
        fi
      done

      PROCESS_COUNT=$(ps aux | grep "$WEBAPP_SERVICE_STRING" | grep "$SERVICE_NAME" | grep -v grep | awk '{print $2 }' | wc -l)
      if [ $PROCESS_COUNT -gt 0 ]; then
        # kill -9
        ps aux | grep "$WEBAPP_SERVICE_STRING" | grep "$SERVICE_NAME" | grep -v grep | awk '{print $2 }' | while read i_pid
        do
          SERVICE_RELATED_PID=$(ps aux | grep "$WEBAPP_SERVICE_STRING" | grep "$SERVICE_NAME" | grep -v grep | awk '{print $2 }' | grep -c "$i_pid")
          if [ $SERVICE_RELATED_PID -eq 1 ]; then
            echo "$(date +"%Y-%m-%d %H:%M:%S") :: $PID :: ${SERVICE_NAME}.d :: kill -9 process $i_pid" >> "$LOG_PATH/${SERVICE_NAME}.log"
            sudo kill -9 $i_pid
          fi
        done
      fi
    fi

    PROCESS_COUNT=$(ps aux | grep "$WEBAPP_SERVICE_STRING" | grep "$SERVICE_NAME" | grep -v grep | awk '{print $2 }' | wc -l)
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

run () {
    echo "running ${SERVICE_NAME}"
    if [ $USE_VIRTUALENV -eq 0 ]; then
      /usr/bin/env python "$BASEDIR/skyline/${SERVICE_NAME}/webapp.py" run
    else
      $USE_PYTHON "$BASEDIR/skyline/${SERVICE_NAME}/webapp.py" run
    fi
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
  run)
    run
      ;;
  status)
    status
        ;;
  *)
        echo $"Usage: $0 {start|stop|run|status}"
        exit 2
        ;;
esac
