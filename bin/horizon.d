#!/bin/bash
#
# This is used to start/stop horizon

BASEDIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )/.."
RETVAL=0

# Determine the script to run depending on whether this script is in the source
# tree or installed
SCRIPT="${BASEDIR}/bin/horizon-agent"
if [ -e "${BASEDIR}/setup.py" ]
then
    SCRIPT="${BASEDIR}/src/skyline/horizon/agent.py"
fi

start () {
    rm $BASEDIR/src/horizon/*.pyc
    /usr/bin/env python $SCRIPT start
        RETVAL=$?
        if [[ $RETVAL -eq 0 ]]; then
            echo "started horizon-agent"
        else
            echo "failed to start horizon-agent"
        fi
        return $RETVAL
}

stop () {
    # TODO: write a real kill script
    ps aux | grep 'horizon-agent.py start' | grep -v grep | awk '{print $2 }' | xargs sudo kill -9
    /usr/bin/env python $SCRIPT stop
        RETVAL=$?
        if [[ $RETVAL -eq 0 ]]; then
            echo "stopped horizon-agent"
        else
            echo "failed to stop horizon-agent"
        fi
        return $RETVAL
}

run () {
    echo "running horizon"
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
  run)
    run
        ;;
  restart)
    stop
    start
       ;;
  *)
        echo $"Usage: $0 {start|stop|run}"
        exit 2
        ;;
esac
