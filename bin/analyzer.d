#!/bin/bash
#
# This is used to start/stop the analyzer

BASEDIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )/.."
RETVAL=0

# Determine the script to run depending on whether this script is in the source
# tree or installed
SCRIPT="${BASEDIR}/bin/analyzer-agent"
if [ -e "${BASEDIR}/setup.py" ]
then
    SCRIPT="${BASEDIR}/src/skyline/analyzer/agent.py"
fi

start () {
    rm -f $BASEDIR/src/analyzer/*.pyc
    /usr/bin/env python $SCRIPT start
        RETVAL=$?
        if [[ $RETVAL -eq 0 ]]; then
            echo "started analyzer-agent"
        else
            echo "failed to start analyzer-agent"
        fi
        return $RETVAL
}

stop () {
    # TODO: write a real kill script
    ps aux | grep 'analyzer-agent.py start' | grep -v grep | awk '{print $2 }' | xargs sudo kill -9
    /usr/bin/env python $SCRIPT stop
        RETVAL=$?
        if [[ $RETVAL -eq 0 ]]; then
            echo "stopped analyzer-agent"
        else
            echo "failed to stop analyzer-agent"
        fi
        return $RETVAL
}

run () {
    echo "running analyzer"
    /usr/bin/env python $SCRIPT run
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
