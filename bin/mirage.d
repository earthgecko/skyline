#!/bin/bash

# This is used to start/stop the mirage

BASEDIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )/.."
RETVAL=0

start () {
    rm -f $BASEDIR/src/mirage/*.pyc
    /usr/bin/env python $BASEDIR/src/mirage/mirage-agent.py start
        RETVAL=$?
        if [[ $RETVAL -eq 0 ]]; then
            echo "started mirage-agent"
        else
            echo "failed to start mirage-agent"
        fi
        return $RETVAL
}

stop () {
    # TODO: write a real kill script
    ps aux | grep 'mirage-agent.py start' | grep -v grep | awk '{print $2 }' | xargs sudo kill -9
    /usr/bin/env python $BASEDIR/src/mirage/mirage-agent.py stop
        RETVAL=$?
        if [[ $RETVAL -eq 0 ]]; then
            echo "stopped mirage-agent"
        else
            echo "failed to stop mirage-agent"
        fi
        return $RETVAL
}

run () {
    echo "running mirage"
    /usr/bin/env python $BASEDIR/src/mirage/mirage-agent.py run
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
