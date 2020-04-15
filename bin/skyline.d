#!/bin/bash

# This is used to stop and start any running skyline apps

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

restart () {

  FAILURES=0
  STOP_APPS=""
  STOPPED_APPS=""
  find "$CURRENT_DIR/" -type f | grep -v "$SERVICE_NAME\.d" | grep "bin/*.*\.d$" > /tmp/skyline.d.apps.tmp
  # Validate file has contents as a while loop will never complete on a blank
  # input file
  HAS_APPS=$(cat /tmp/skyline.d.apps.tmp | wc -l)
  if [ $HAS_APPS -gt 0 ]; then
    while read binfile
    do
        CHECK_APP=$(basename "$binfile" | cut -d'.' -f1)
        $binfile status
        if [ $? -eq 0 ]; then
          _STOP_APPS="${STOP_APPS}${CHECK_APP} "
          STOP_APPS="$_STOP_APPS"
        fi
    done < /tmp/skyline.d.apps.tmp

    for i_app in $STOP_APPS
    do
      CHECK_APP="$i_app"
      if [ -f "/etc/init.d/$CHECK_APP" ]; then
        /etc/init.d/$CHECK_APP stop
      else
        $CURRENT_DIR/$CHECK_APP.d stop
      fi
      STOP_EXITCODE=$?
      if [ $STOP_EXITCODE -eq 0 ]; then
        _STOPPED_APPS="${STOPPED_APPS}${CHECK_APP} "
        STOPPED_APPS="$_STOPPED_APPS"
      else
        FAILURES=$((FAILURES+1))
      fi
    done
  fi

  if [ $FAILURES -eq 0 ]; then
    echo "skyline apps stopped, removing .pyc files"
    # Remove .pyc files
    find "${BASEDIR}/skyline" -type f -name "*.pyc" | while read pyc_file
    do
      if [ -f "$pyc_file" ]; then
        rm -f "$pyc_file"
      fi
    done
  else
    echo "failures were encountered, not removing .pyc files"
  fi

  for i_app in $STOPPED_APPS
  do
    CHECK_APP="$i_app"
    binfile=$(cat /tmp/skyline.d.apps.tmp | grep "${i_app}.d")
    if [ -f "/etc/init.d/$CHECK_APP" ]; then
      /etc/init.d/$CHECK_APP start
    else
      $binfile start
    fi
    START_EXITCODE=$?
    if [ $START_EXITCODE -ne 0 ]; then
      FAILURES=$((FAILURES+1))
    fi
  done

  if [ $FAILURES -eq 0 ]; then
    echo "skyline apps stopped and restarted"
    RETVAL=0
  else
    echo "skyline apps stopped and started with failures"
    RETVAL=1
  fi
  # cleanup
  if [ -f /tmp/skyline.d.apps.tmp ]; then
    rm -f /tmp/skyline.d.apps.tmp
  fi
  return $RETVAL

}

status () {

# As per http://refspecs.linuxbase.org/LSB_3.1.0/LSB-Core-generic/LSB-Core-generic/iniscrptact.html
# 0	program is running or service is OK
# 1	program is dead and /var/run pid file exists
# 2	program is dead and /var/lock lock file exists
# 3	program is not running
# 4	program or service status is unknown

  find "$CURRENT_DIR/" -type f | grep -v "$SERVICE_NAME\.d" | grep "bin/*.*\.d$" > /tmp/skyline.d.apps.tmp
  # Validate file has contents as a while loop will never complete on a blank
  # input file
  HAS_APPS=$(cat /tmp/skyline.d.apps.tmp | wc -l)
  if [ $HAS_APPS -gt 0 ]; then
    while read binfile
    do
      CHECK_APP=$(basename "$binfile" | cut -d'.' -f1)
      $binfile status
    done < /tmp/skyline.d.apps.tmp
  fi
  # cleanup
  if [ -f /tmp/skyline.d.apps.tmp ]; then
    rm -f /tmp/skyline.d.apps.tmp
  fi
  return $RETVAL

}

stop () {

  FAILURES=0
  STOPPED_APPS=""
  find "$CURRENT_DIR/" -type f | grep -v "$SERVICE_NAME\.d" | grep "bin/*.*\.d$" > /tmp/skyline.d.apps.tmp
  # Validate file has contents as a while loop will never complete on a blank
  # input file
  HAS_APPS=$(cat /tmp/skyline.d.apps.tmp | wc -l)
  if [ $HAS_APPS -gt 0 ]; then
    while read binfile
    do
        CHECK_APP=$(basename "$binfile" | cut -d'.' -f1)
        $binfile status
        if [ $? -eq 0 ]; then
          if [ -f "/etc/init.d/$CHECK_APP" ]; then
            /etc/init.d/$CHECK_APP stop
          else
            $binfile stop
          fi
          STOP_EXITCODE=$?
          if [ $STOP_EXITCODE -eq 0 ]; then
            _STOPPED_APPS="${STOPPED_APPS}${CHECK_APP} "
            STOPPED_APPS="$_STOPPED_APPS"
          else
            FAILURES=$((FAILURES+1))
          fi
        fi
    done < /tmp/skyline.d.apps.tmp
  fi

  if [ $FAILURES -eq 0 ]; then
    echo "skyline apps stopped, removing .pyc files"
    # Remove .pyc files
    find "${BASEDIR}/skyline" -type f -name "*.pyc" | while read pyc_file
    do
      if [ -f "$pyc_file" ]; then
        rm -f "$pyc_file"
      fi
    done
  else
    echo "failures were encountered, not removing .pyc files"
  fi

  if [ $FAILURES -eq 0 ]; then
    echo "skyline apps stopped"
    RETVAL=0
  else
    echo "skyline apps stopped with failures"
    RETVAL=1
  fi
  # cleanup
  if [ -f /tmp/skyline.d.apps.tmp ]; then
    rm -f /tmp/skyline.d.apps.tmp
  fi
  return $RETVAL

}

# See how we were called.
case "$1" in
  stop)
    stop
        ;;
  restart)
    restart
       ;;
  status)
    status
        ;;
  *)
        echo $"Usage: $0 {restart|stop|status}"
        exit 2
        ;;
esac
