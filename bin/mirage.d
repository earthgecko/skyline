#!/bin/bash

# This is used to start/stop the service

BASEDIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )/.."
RETVAL=0

SERVICE_NAME=$(basename "$0" | cut -d'.' -f1)

start () {
    rm -f "$BASEDIR/src/${SERVICE_NAME}/*.pyc"
    if [ -f "/var/log/skyline/${SERVICE_NAME}.log" ]; then
      mv "/var/log/skyline/${SERVICE_NAME}.log" "/var/log/skyline/${SERVICE_NAME}.log.last"
    fi
    /usr/bin/env python "$BASEDIR/src/${SERVICE_NAME}/${SERVICE_NAME}-agent.py" start
        RETVAL=$?
        if [[ $RETVAL -eq 0 ]]; then
            echo "started ${SERVICE_NAME}-agent"
            cat "/var/log/skyline/${SERVICE_NAME}.log.last" "/var/log/skyline/${SERVICE_NAME}.log" > "/var/log/skyline/${SERVICE_NAME}.log.new"
            cat "/var/log/skyline/${SERVICE_NAME}.log.new" > "/var/log/skyline/${SERVICE_NAME}.log"
            rm -f "/var/log/skyline/${SERVICE_NAME}.log.last"
            rm -f "/var/log/skyline/${SERVICE_NAME}.log.new"
        else
            echo "failed to start ${SERVICE_NAME}-agent"
            cat "/var/log/skyline/${SERVICE_NAME}.log.last" "/var/log/skyline/${SERVICE_NAME}.log" > "/var/log/skyline/${SERVICE_NAME}.log.new"
            cat "/var/log/skyline/${SERVICE_NAME}.log.new" > "/var/log/skyline/${SERVICE_NAME}.log"
            rm -f "/var/log/skyline/${SERVICE_NAME}.log.last"
            rm -f "/var/log/skyline/${SERVICE_NAME}.log.new"
        fi
        return $RETVAL
}

stop () {
    # TODO: write a real kill script
    ps aux | grep "${SERVICE_NAME}-agent.py start" | grep -v grep | awk '{print $2 }' | xargs sudo kill -9
    if [ -f "/var/log/skyline/${SERVICE_NAME}.log" ]; then
      cat "/var/log/skyline/${SERVICE_NAME}.log" > "/var/log/skyline/${SERVICE_NAME}.log.last"
    fi
    /usr/bin/env python "$BASEDIR/src/${SERVICE_NAME}/${SERVICE_NAME}-agent.py" stop
        RETVAL=$?
        if [[ $RETVAL -eq 0 ]]; then
            echo "stopped ${SERVICE_NAME}-agent"
        else
            echo "failed to stop ${SERVICE_NAME}-agent"
        fi
        if [ -f "/var/log/skyline/${SERVICE_NAME}.log.last" ]; then
          cat "/var/log/skyline/${SERVICE_NAME}.log.last" > "/var/log/skyline/${SERVICE_NAME}.log"
          LOG_DATE_STRING=$(date "+%Y-%m-%d %H:%M:%S")
          echo "$LOG_DATE_STRING :: $$ :: ${SERVICE_NAME} stopped" >> "/var/log/skyline/${SERVICE_NAME}.log"
          rm -f "/var/log/skyline/${SERVICE_NAME}.log.last"
        fi
        return $RETVAL
}

run () {
    echo "running ${SERVICE_NAME}"
    /usr/bin/env python "$BASEDIR/src/${SERVICE_NAME}/${SERVICE_NAME}-agent.py" run
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
    stop
    start
       ;;
  run)
    run
        ;;

  *)
        echo $"Usage: $0 {start|stop|run}"
        exit 2
        ;;
esac
