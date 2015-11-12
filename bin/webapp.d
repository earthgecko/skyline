#!/bin/bash
#
# This is used to start/stop webapp

BASEDIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )/.."
RETVAL=0

SERVICE_NAME=$(basename "$0" | cut -d'.' -f1)

start () {
    rm -f "$BASEDIR/src/${SERVICE_NAME}/*.pyc"
    if [ -f "/var/log/skyline/${SERVICE_NAME}.log" ]; then
      mv "/var/log/skyline/${SERVICE_NAME}.log" "/var/log/skyline/${SERVICE_NAME}.log.last"
    fi
    /usr/bin/env python "$BASEDIR/src/${SERVICE_NAME}/${SERVICE_NAME}.py" start
        RETVAL=$?
        if [[ $RETVAL -eq 0 ]]; then
          echo "started webapp"
            cat "/var/log/skyline/${SERVICE_NAME}.log.last" "/var/log/skyline/${SERVICE_NAME}.log" > "/var/log/skyline/${SERVICE_NAME}.log.new"
            cat "/var/log/skyline/${SERVICE_NAME}.log.new" > "/var/log/skyline/${SERVICE_NAME}.log"
            rm -f "/var/log/skyline/${SERVICE_NAME}.log.last"
            rm -f "/var/log/skyline/${SERVICE_NAME}.log.new"
        else
          echo "failed to start webapp"
            cat "/var/log/skyline/${SERVICE_NAME}.log.last" "/var/log/skyline/${SERVICE_NAME}.log" > "/var/log/skyline/${SERVICE_NAME}.log.new"
            cat "/var/log/skyline/${SERVICE_NAME}.log.new" > "/var/log/skyline/${SERVICE_NAME}.log"
            rm -f "/var/log/skyline/${SERVICE_NAME}.log.last"
            rm -f "/var/log/skyline/${SERVICE_NAME}.log.new"
        fi
        return $RETVAL
}

stop () {
    if [ -f "/var/log/skyline/${SERVICE_NAME}.log" ]; then
# @modified 20151014 - Feature #20451: skyline mirage
# Preserve logs - the mv operation does not change the file handle
#      mv "/var/log/skyline/${SERVICE_NAME}.log" "/var/log/skyline/${SERVICE_NAME}.log.last"
      cat "/var/log/skyline/${SERVICE_NAME}.log" > "/var/log/skyline/${SERVICE_NAME}.log.last"
    fi
    /usr/bin/env python "$BASEDIR/src/${SERVICE_NAME}/${SERVICE_NAME}.py stop"
        RETVAL=$?
        if [[ $RETVAL -eq 0 ]]; then
            echo "stopped webapp"
        else
            echo "failed to stop webapp"
        fi
        if [ -f "/var/log/skyline/${SERVICE_NAME}.log.last" ]; then
          cat "/var/log/skyline/${SERVICE_NAME}.log.last" > "/var/log/skyline/${SERVICE_NAME}.log"
          LOG_DATE_STRING=$(date "+%Y-%m-%d %H:%M:%S")
          echo "$LOG_DATE_STRING :: $$ :: ${SERVICE_NAME} stopped" >> "/var/log/skyline/${SERVICE_NAME}.log"
          rm -f "/var/log/skyline/${SERVICE_NAME}.log.last"
        fi
        return $RETVAL
}

restart () {
    rm -f $BASEDIR/src/webapp/*.pyc
    if [ -f "/var/log/skyline/${SERVICE_NAME}.log" ]; then
#      mv "/var/log/skyline/${SERVICE_NAME}.log" "/var/log/skyline/${SERVICE_NAME}.log.last"
      cat "/var/log/skyline/${SERVICE_NAME}.log" > "/var/log/skyline/${SERVICE_NAME}.log.last"
    fi
    /usr/bin/env python "$BASEDIR/src/${SERVICE_NAME}/${SERVICE_NAME}.py" restart
        RETVAL=$?
        if [[ $RETVAL -eq 0 ]]; then
            echo "restarted webapp"
        else
            echo "failed to restart webapp"
        fi
        cat "/var/log/skyline/${SERVICE_NAME}.log.last" "/var/log/skyline/${SERVICE_NAME}.log" > "/var/log/skyline/${SERVICE_NAME}.log.new"
        cat "/var/log/skyline/${SERVICE_NAME}.log.new" > "/var/log/skyline/${SERVICE_NAME}.log"
        rm -f "/var/log/skyline/${SERVICE_NAME}.log.last"
        rm -f "/var/log/skyline/${SERVICE_NAME}.log.new"
        return $RETVAL
}

run () {
    echo "running webapp"
    /usr/bin/env python $BASEDIR/src/webapp/webapp.py run
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
    restart
        ;;
  run)
    run
        ;;

  *)
        echo $"Usage: $0 {start|stop|restart|run}"
        exit 2
        ;;
esac
